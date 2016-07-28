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
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BBSeaBookingEntry : CommonFeatures
    {

        #region "Private Variables"
        private long _PkValueMain;
        private long _PkValueTrans;
        cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();
        public string strRet;
        #endregion
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
        int ComGrp;
        public int CommodityGroup
        {
            get { return ComGrp; }
            set { ComGrp = value; }
        }

        #endregion

        #region "Fetch Sea Existing Booking Entry Details"
        public Int16 fetchBkgStatus(long PKVal)
        {
            try
            {
                string strSQL = "";
                string Status = "";
                WorkFlow objwf = new WorkFlow();
                strSQL = "select booking_trn_sea_pk from booking_trn where booking_mst_fk='" + PKVal + "'";
                Status = objwf.ExecuteScaler(strSQL);
                if (!string.IsNullOrEmpty(Status))
                {
                    strSQL = "select booking_trn_sea_frt_pk from booking_trn_sea_frt_dtls where booking_trn_sea_fk='" + Status + "'";
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchBEntry(DataSet dsBEntry, long lngSBEPk, Int16 EBkg = 0, Int16 Bsts = 0)
        {

            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtTrans = new DataTable();
                DataTable dtFreight = new DataTable();
                Int16 intIsLcl = default(Int16);
                dtMain = (DataTable)FetchSBEntryHeader(dtMain, lngSBEPk);
                intIsLcl = 1;
                dtTrans = (DataTable)FetchSBEntryTrans(dtTrans, lngSBEPk, intIsLcl);
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    dtFreight = (DataTable)FetchSBEntryFreight(dtFreight, lngSBEPk, intIsLcl, EBkg);
                }
                else
                {
                    dtFreight = (DataTable)FetchSBEntryFreight(dtFreight, lngSBEPk, intIsLcl);
                }
                if (dtFreight.Rows.Count == 0)
                {
                    dtFreight = (DataTable)FetchSBEntryFreightNEW(dtFreight, lngSBEPk);
                }
                dsBEntry.Tables.Add(dtTrans);
                dsBEntry.Tables.Add(dtFreight);
                if (dsBEntry.Tables.Count > 1)
                {
                    if (dsBEntry.Tables[1].Rows.Count > 0)
                    {
                        if (intIsLcl == 0)
                        {
                            DataRelation rel = new DataRelation("rl_TRAN_FREIGHT", new DataColumn[] {
                                dsBEntry.Tables[0].Columns["REFNO"],
                                dsBEntry.Tables[0].Columns["TYPE"]
                            }, new DataColumn[] {
                                dsBEntry.Tables[1].Columns["REFNO"],
                                dsBEntry.Tables[1].Columns["TYPE"]
                            });
                            dsBEntry.Relations.Clear();
                            dsBEntry.Relations.Add(rel);

                        }
                        else
                        {
                            DataRelation rel = new DataRelation("rl_TRAN_FREIGHT", new DataColumn[] {
                                dsBEntry.Tables[0].Columns["REFNO"],
                                dsBEntry.Tables[0].Columns["BASIS"],
                                dsBEntry.Tables[0].Columns["COMMODITYPK"]
                            }, new DataColumn[] {
                                dsBEntry.Tables[1].Columns["REFNO"],
                                dsBEntry.Tables[1].Columns["BASIS"],
                                ((dsBEntry.Tables[1].Columns["COMMODITYFK"] == null) ? ((dsBEntry.Tables[1].Columns["COMMODITY_MST_PK"] == null) ? dsBEntry.Tables[1].Columns["COMMODITYPK"] : dsBEntry.Tables[1].Columns["COMMODITY_MST_PK"]) : dsBEntry.Tables[1].Columns["COMMODITYFK"])
                            });
                            dsBEntry.Relations.Clear();
                            dsBEntry.Relations.Add(rel);
                        }
                    }
                }

                DataSet ds = new DataSet();
                ds.Tables.Add(dtMain);
                return ds;
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

        public object FetchSBOFreight(DataTable dtMain, long lngSBEPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();

            strBuilder.Append(" SELECT ");
            strBuilder.Append(" BTSOC.FREIGHT_ELEMENT_MST_FK, ");
            strBuilder.Append(" BTSOC.CURRENCY_MST_FK, ");
            strBuilder.Append(" BTSOC.AMOUNT, ");
            strBuilder.Append(" BTSOC.FREIGHT_TYPE ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" booking_mst_tbl BST, ");
            strBuilder.Append(" BOOKING_TRN_SEA_OTH_CHRG BTSOC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" BTSOC.booking_mst_fk = BST.booking_mst_pk ");
            strBuilder.Append(" AND BTSOC.booking_mst_fk= " + lngSBEPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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
        public object FetchTtlSBOFreight(DataTable dtMain, long lngSBEPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strBuilder.Append(" SELECT ");
            strBuilder.Append(" BTSOC.FREIGHT_ELEMENT_MST_FK, ");
            strBuilder.Append(" BTSOC.CURRENCY_MST_FK, ");
            strBuilder.Append(" (BTSOC.AMOUNT * GET_EX_RATE(BTSOC.CURRENCY_MST_FK, " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ", SYSDATE)) AMOUNT, ");
            strBuilder.Append(" BTSOC.FREIGHT_TYPE ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" booking_mst_tbl BST, ");
            strBuilder.Append(" BOOKING_TRN_SEA_OTH_CHRG BTSOC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" BTSOC.booking_mst_fk = BST.booking_mst_pk ");
            strBuilder.Append(" AND BTSOC.booking_mst_fk= " + lngSBEPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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


        public Int32 FetchBkgPk(Int32 jobpk)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();

            strSql = "select sea.booking_mst_fk from JOB_CARD_TRN sea where sea.JOB_CARD_TRN_PK=" + jobpk;
            try
            {
                return Convert.ToInt32(objwf.ExecuteScaler(strSql));
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
        public Int32 fetchBKPk(string RefNo)
        {
            string strSql = "";
            string Ref = null;
            WorkFlow objwf = new WorkFlow();

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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                    strSqlCDimension = " select c1.commodity_name,(select bb.pack_count  from booking_mst_tbl bb where bb.booking_mst_pk=" + SeaBkgPK + ") pack_count, " + " (select pack.pack_type_desc from PACK_TYPE_MST_TBL pack where pack.pack_type_mst_pk in (select bb.pack_typ_mst_fk from booking_mst_tbl bb where bb.booking_mst_pk=" + SeaBkgPK + ")) pack_type_desc " + " from commodity_mst_tbl c1 where c1.commodity_mst_pk in ( " + " select bb.commodity_mst_fk from booking_trn bb where bb.booking_mst_fk=" + SeaBkgPK + ") ";

                }
                else if (status == 2 | status == 3)
                {
                    strSqlCDimension = "select distinct(commodity_name) ," +  "BST.pack_count," +  "PTMT.PACK_TYPE_DESC " +  "from commodity_mst_tbl CST," +  "JOB_CARD_TRN JCSE, " +  "job_trn_sea_exp_cont JTSE,booking_mst_tbl BST, " +  "PACK_TYPE_MST_TBL PTMT" +  "where JTSE.commodity_mst_fk=CST.commodity_mst_pk" +  "and PTMT.PACK_TYPE_MST_PK=BST.PACK_TYP_MST_FK " +  "and JTSE.JOB_CARD_TRN_FK=JCSE.JOB_CARD_TRN_PK  " +  "and BST.booking_mst_pk=JCSE.booking_mst_fk" +  "and booking_mst_pk= " + SeaBkgPK;
                }
                return objwf.GetDataSet(strSqlCDimension);
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

        public object FetchSBEntryHeader(DataTable dtMain, long lngSBEPK)
        {
            System.Text.StringBuilder strSqlHeader = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();

            strSqlHeader.Append(" SELECT " );
            strSqlHeader.Append(" BST.booking_mst_pk, " );
            strSqlHeader.Append(" BST.BOOKING_REF_NO, " );
            strSqlHeader.Append(" BST.BOOKING_DATE, " );
            strSqlHeader.Append(" BST.CUST_CUSTOMER_MST_FK, " );

            strSqlHeader.Append(" (case when CMTCUST.CUSTOMER_ID is null then " );
            strSqlHeader.Append(" ( SELECT TT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TT WHERE TT.CUSTOMER_MST_PK=BST.CUST_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   ELSE CMTCUST.CUSTOMER_ID END ) CUSTOMERID," );

            strSqlHeader.Append(" (CASE WHEN CMTCUST.CUSTOMER_NAME IS NULL THEN " );
            strSqlHeader.Append(" ( SELECT TT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TT where TT.CUSTOMER_MST_PK=BST.CUST_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   ELSE CMTCUST.CUSTOMER_NAME END ) CUSTOMER_NAME," );

            strSqlHeader.Append(" BST.CONS_CUSTOMER_MST_FK, " );

            strSqlHeader.Append(" (CASE WHEN CMTCONS.CUSTOMER_ID IS NULL THEN " );
            strSqlHeader.Append(" ( SELECT TT.CUSTOMER_ID FROM TEMP_CUSTOMER_TBL TT where TT.CUSTOMER_MST_PK=BST.CONS_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   ELSE CMTCONS.CUSTOMER_ID END ) CONSIGNEEID," );

            strSqlHeader.Append(" (CASE WHEN CMTCONS.CUSTOMER_NAME IS NULL THEN " );
            strSqlHeader.Append(" ( SELECT TT.CUSTOMER_NAME FROM TEMP_CUSTOMER_TBL TT where TT.CUSTOMER_MST_PK=BST.CONS_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   ELSE CMTCONS.CUSTOMER_NAME END ) CONSIGNEE_NAME," );

            strSqlHeader.Append(" BST.PORT_MST_POL_FK, " );
            strSqlHeader.Append(" PL.PORT_ID AS POL, " );
            strSqlHeader.Append(" PL.PORT_NAME AS POLNAME, " );
            strSqlHeader.Append(" BST.PORT_MST_POD_FK, " );
            strSqlHeader.Append(" PD.PORT_ID AS POD, " );
            strSqlHeader.Append(" PD.PORT_NAME AS PODNAME, " );
            strSqlHeader.Append(" BST.COMMODITY_GROUP_FK, " );
            strSqlHeader.Append(" BST.OPERATOR_MST_FK, " );
            strSqlHeader.Append(" OMT.OPERATOR_ID, " );
            strSqlHeader.Append(" BST.SHIPMENT_DATE, " );
            strSqlHeader.Append(" BST.CARGO_TYPE, " );
            strSqlHeader.Append(" BST.CARGO_MOVE_FK, " );
            strSqlHeader.Append(" CMMT.CARGO_MOVE_CODE, " );
            strSqlHeader.Append(" BST.PYMT_TYPE, " );
            strSqlHeader.Append(" BST.COL_PLACE_MST_FK, " );
            strSqlHeader.Append(" PMTPC.PLACE_CODE AS CPLACE, " );
            strSqlHeader.Append(" BST.COL_ADDRESS, " );
            strSqlHeader.Append(" BST.DEL_PLACE_MST_FK, " );
            strSqlHeader.Append(" PMTPD.PLACE_CODE AS DPLACE, " );
            strSqlHeader.Append(" BST.DEL_ADDRESS, " );
            strSqlHeader.Append(" BST.CB_AGENT_MST_FK, " );
            strSqlHeader.Append(" AMTCB.AGENT_ID AS CBAGENT, " );
            strSqlHeader.Append(" AMTCB.AGENT_NAME AS CBAGENT_NAME, " );
            strSqlHeader.Append(" BST.CL_AGENT_MST_FK, " );
            strSqlHeader.Append(" AMTCL.AGENT_ID AS CLAGENT, " );
            strSqlHeader.Append(" AMTCL.AGENT_NAME AS CLAGENT_NAME, " );
            strSqlHeader.Append(" BST.PACK_TYP_MST_FK, " );
            strSqlHeader.Append(" PTMT.PACK_TYPE_ID, " );
            strSqlHeader.Append(" BST.PACK_COUNT, " );
            strSqlHeader.Append(" BST.GROSS_WEIGHT, " );
            strSqlHeader.Append(" BST.NET_WEIGHT, " );
            strSqlHeader.Append(" BST.CHARGEABLE_WEIGHT, " );
            strSqlHeader.Append(" BST.VOLUME_IN_CBM, " );
            strSqlHeader.Append(" BST.LINE_BKG_NO, " );
            strSqlHeader.Append(" VVTMST.VESSEL_NAME, " );
            strSqlHeader.Append(" VVTMST.VESSEL_ID, " );
            strSqlHeader.Append(" VVTTRN.VOYAGE, " );
            strSqlHeader.Append(" VVTTRN.POL_ETD ETD_DATE, " );
            strSqlHeader.Append(" VVTTRN.POD_ETA ETA_DATE, " );
            strSqlHeader.Append(" VVTTRN.POL_CUT_OFF_DATE CUT_OFF_DATE, " );
            strSqlHeader.Append(" BST.STATUS, " );
            strSqlHeader.Append(" DPAMT.AGENT_ID AS DPAGENT, " );
            strSqlHeader.Append(" DPAMT.AGENT_NAME AS DPAGENTNAME, " );
            strSqlHeader.Append(" BST.DP_AGENT_MST_FK, " );
            strSqlHeader.Append(" BST.VERSION_NO, " );
            strSqlHeader.Append(" BST.SHIPPING_TERMS_MST_FK, " );
            strSqlHeader.Append(" BST.CUSTOMER_REF_NO, " );
            strSqlHeader.Append(" BST.VESSEL_VOYAGE_FK, " );
            strSqlHeader.Append(" BST.CREDIT_LIMIT, " );
            strSqlHeader.Append(" BST.CREDIT_DAYS, " );
            strSqlHeader.Append(" CURR.CURRENCY_MST_PK BASE_CURENCY_FK," );
            strSqlHeader.Append(" CURR.CURRENCY_ID, " );
            strSqlHeader.Append(" BST.FROM_FLAG, " );
            strSqlHeader.Append(" BST.POO_FK, " );
            strSqlHeader.Append(" POO.PLACE_CODE POO_ID, " );
            strSqlHeader.Append(" POO.PLACE_NAME POO_NAME, " );
            strSqlHeader.Append(" BST.PFD_FK, " );
            strSqlHeader.Append(" PFD.PLACE_CODE PFD_ID, " );
            strSqlHeader.Append(" PFD.PLACE_NAME PFD_NAME, " );
            strSqlHeader.Append(" BST.HANDLING_INFO, " );
            strSqlHeader.Append(" BST.REMARKS,  " );
            strSqlHeader.Append(" BST.REMARKS_NEW,  " );
            strSqlHeader.Append(" BST.LINE_BKG_DT,  " );
            strSqlHeader.Append(" BST.PO_NUMBER,  " );
            strSqlHeader.Append(" BST.PO_DATE,  " );
            strSqlHeader.Append(" BST.NOMINATION_REF_NO,  " );
            strSqlHeader.Append(" BST.SALES_CALL_FK  " );

            strSqlHeader.Append(" FROM " );
            strSqlHeader.Append(" booking_mst_tbl BST, " );
            strSqlHeader.Append(" CUSTOMER_MST_TBL CMTCUST, " );
            strSqlHeader.Append(" CUSTOMER_MST_TBL CMTCONS, " );
            strSqlHeader.Append(" PORT_MST_TBL PL, " );
            strSqlHeader.Append(" PORT_MST_TBL PD, " );
            strSqlHeader.Append(" OPERATOR_MST_TBL OMT, " );
            strSqlHeader.Append(" CARGO_MOVE_MST_TBL CMMT, " );
            strSqlHeader.Append(" PLACE_MST_TBL PMTPC, " );
            strSqlHeader.Append(" PLACE_MST_TBL PMTPD, " );
            strSqlHeader.Append(" PLACE_MST_TBL POO, " );
            strSqlHeader.Append(" PLACE_MST_TBL PFD, " );
            strSqlHeader.Append(" AGENT_MST_TBL AMTCB, " );
            strSqlHeader.Append(" AGENT_MST_TBL AMTCL, " );
            strSqlHeader.Append(" PACK_TYPE_MST_TBL PTMT, " );
            strSqlHeader.Append(" VESSEL_VOYAGE_TBL VVTMST, " );
            strSqlHeader.Append(" AGENT_MST_TBL DPAMT, " );
            strSqlHeader.Append(" VESSEL_VOYAGE_TRN VVTTRN, " );
            strSqlHeader.Append(" CURRENCY_TYPE_MST_TBL CURR " );

            strSqlHeader.Append(" WHERE(1 = 1) " );
            strSqlHeader.Append(" AND BST.CUST_CUSTOMER_MST_FK=CMTCUST.CUSTOMER_MST_PK (+)" );
            strSqlHeader.Append(" AND BST.CONS_CUSTOMER_MST_FK=CMTCONS.CUSTOMER_MST_PK(+) " );
            strSqlHeader.Append(" AND BST.PORT_MST_POL_FK=PL.PORT_MST_PK (+)" );
            strSqlHeader.Append(" AND BST.PORT_MST_POD_FK=PD.PORT_MST_PK (+)" );
            strSqlHeader.Append(" AND BST.OPERATOR_MST_FK=OMT.OPERATOR_MST_PK (+)" );
            strSqlHeader.Append(" AND BST.CARGO_MOVE_FK=CMMT.CARGO_MOVE_PK(+) " );
            strSqlHeader.Append(" AND BST.COL_PLACE_MST_FK=PMTPC.PLACE_PK(+) " );
            strSqlHeader.Append(" AND BST.DEL_PLACE_MST_FK=PMTPD.PLACE_PK(+) " );
            strSqlHeader.Append(" AND BST.POO_FK=POO.PLACE_PK(+) " );
            strSqlHeader.Append(" AND BST.PFD_FK=PFD.PLACE_PK(+) " );
            strSqlHeader.Append(" AND BST.CB_AGENT_MST_FK=AMTCB.AGENT_MST_PK(+) " );
            strSqlHeader.Append(" AND BST.CL_AGENT_MST_FK=AMTCL.AGENT_MST_PK(+) " );
            strSqlHeader.Append(" AND BST.PACK_TYP_MST_FK=PTMT.PACK_TYPE_MST_PK(+) " );
            strSqlHeader.Append(" AND BST.VESSEL_VOYAGE_FK=VVTTRN.VOYAGE_TRN_PK(+) " );
            strSqlHeader.Append(" AND VVTTRN.VESSEL_VOYAGE_TBL_FK=VVTMST.VESSEL_VOYAGE_TBL_PK(+) " );
            strSqlHeader.Append(" AND BST.DP_AGENT_MST_FK=DPAMT.AGENT_MST_PK(+) " );
            strSqlHeader.Append(" AND CURR.CURRENCY_MST_PK(+) = BST.BASE_CURRENCY_FK " );
            strSqlHeader.Append(" AND BST.booking_mst_pk= " + lngSBEPK);

            try
            {
                dtMain = objwf.GetDataTable(strSqlHeader.ToString());
                return dtMain;
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

        public object FetchSBEntryTrans(DataTable dtTrans, long lngSBEPK, Int16 intIsLcl)
        {
            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT NULL AS TRNTYPEPK,");
            sb.Append("       BTSFL.TRANS_REFERED_FROM AS TRNTYPESTATUS,");
            sb.Append("       DECODE(BTSFL.TRANS_REFERED_FROM,");
            sb.Append("              1, 'Quote',  2, 'Sp Rate', 3, 'Cust Cont',");
            sb.Append("              4, 'SL Tariff',  5, 'SRR', 6, 'Gen Tariff', 7, 'Manual') AS CONTRACTTYPE,");
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

            sb.Append("       ROUND(FETCH_TOTALBKGFRT(BTSFL.BOOKING_TRN_SEA_PK, 2),2) AS BKGRATE,");
            sb.Append("       NULL AS NET,");

            sb.Append("       ROUND(FETCH_TOTALBKGFRT(BTSFL.BOOKING_TRN_SEA_PK, 1),2) AS TOTALRATE,");
            sb.Append("       '1' AS SEL,");
            sb.Append("       ROWNUM AS CONTAINERPK,");
            sb.Append("       BTSFL.COMMODITY_MST_FK AS COMMODITYPK,");
            sb.Append("       BST.OPERATOR_MST_FK AS OPERATORPK,");
            sb.Append("       BTSFL.BOOKING_TRN_SEA_PK AS TRANSACTIONPK,");
            sb.Append("       UOM.DIMENTION_UNIT_MST_PK AS BASISPK");
            sb.Append("  FROM booking_mst_tbl         BST,");
            sb.Append("       booking_trn BTSFL,");
            sb.Append("       DIMENTION_UNIT_MST_TBL  UOM,");
            sb.Append("       COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("       COMMODITY_MST_TBL       CMT,");
            sb.Append("       OPERATOR_MST_TBL        OMT,");
            sb.Append("       PACK_TYPE_MST_TBL       PCK");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND BTSFL.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK(+)");
            sb.Append("   AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
            sb.Append("   AND BTSFL.booking_mst_fk = BST.booking_mst_pk");
            sb.Append("   AND BST.OPERATOR_MST_FK = OMT.OPERATOR_MST_PK(+)");
            sb.Append("   AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("   AND PCK.PACK_TYPE_MST_PK(+) = BTSFL.PACK_TYPE_FK ");
            sb.Append(" AND BST.booking_mst_pk= " + lngSBEPK);
            try
            {
                dtTrans = objwf.GetDataTable(sb.ToString());
                return dtTrans;
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

        public object FetchSBEntryFreight(DataTable dtFreight, long lngSBEPK, Int16 intIsLcl, Int16 Ebkg = 0)
        {

            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT BTSFD.BOOKING_TRN_SEA_FRT_PK AS TRNTYPEFK,");
            sb.Append("       BTSFL.TRANS_REF_NO AS REFNO,");
            sb.Append("       UOM.DIMENTION_ID AS BASIS,");
            sb.Append("       (CMT.COMMODITY_NAME|| '$$;' || FEMT.CREDIT) AS COMMODITY,");

            sb.Append("       BST.PORT_MST_POL_FK AS PORT_MST_PK,");
            sb.Append("       PL.PORT_ID AS POL,");

            sb.Append("       BST.PORT_MST_POD_FK AS PORT_MST_PK1,");
            sb.Append("       PD.PORT_ID AS POD,");
            sb.Append("       BTSFD.FREIGHT_ELEMENT_MST_FK,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_ID,");
            sb.Append("       DECODE(FEMT.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
            sb.Append("       DECODE(BTSFD.CHECK_FOR_ALL_IN_RT, 1, 'TRUE', 'FALSE') AS SEL,");
            sb.Append("       BTSFD.CURRENCY_MST_FK CURRENCY_MST_PK,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       ABS(CASE WHEN BTSFD.SURCHARGE  IS NOT NULL THEN  TO_NUMBER(SUBSTR(BTSFD.SURCHARGE, 0, (INSTR(BTSFD.SURCHARGE, '%', 1, 1) - 1))) ELSE MIN_RATE END) SURCHARGE_VALUE,");
            //'surcharge
            sb.Append("       MIN_RATE AS MIN_RATE,");
            sb.Append("       BTSFD.EXCHANGE_RATE RATE,");
            sb.Append("       BTSFD.TARIFF_RATE AS BKGRATE,");
            sb.Append("       UOM.DIMENTION_UNIT_MST_PK AS BASISPK,");
            sb.Append("       DECODE(BTSFD.PYMT_TYPE, 1, 'PrePaid', 'Collect') AS PYMT_TYPE,");
            sb.Append("       CMT.COMMODITY_MST_PK,BTSFD.BOOKING_TRN_SEA_FRT_PK FreightPK, BTSFD.EXCHANGE_RATE EXCHANGERATE, '' SURCHARGE_TYPE,FEMT.Credit");
            sb.Append("  FROM booking_mst_tbl          BST,");
            sb.Append("       booking_trn  BTSFL,");
            sb.Append("       BOOKING_TRN_SEA_FRT_DTLS BTSFD,");
            sb.Append("       CURRENCY_TYPE_MST_TBL    CTMT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL  FEMT,");
            sb.Append("       COMMODITY_MST_TBL        CMT,");
            sb.Append("       PORT_MST_TBL             PL,");
            sb.Append("       PORT_MST_TBL             PD,");
            sb.Append("       DIMENTION_UNIT_MST_TBL   UOM");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND BTSFL.BOOKING_TRN_SEA_PK = BTSFD.BOOKING_TRN_SEA_FK");
            sb.Append("   AND BTSFD.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
            sb.Append("   AND BTSFD.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK(+)");
            sb.Append("   AND BTSFL.booking_mst_fk = BST.booking_mst_pk");
            sb.Append("   AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
            sb.Append("   AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                if (Ebkg == 1)
                {
                    sb.Append(" AND BTSFD.CHECK_FOR_ALL_IN_RT=1");
                }
            }
            sb.Append(" AND BST.booking_mst_pk=" + lngSBEPK );
            sb.Append(" ORDER BY PREFERENCE ASC");
            try
            {
                dtFreight = objwf.GetDataTable(sb.ToString());
                return dtFreight;
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
        public object FetchSBEntryFreightNEW(DataTable dtFreight, long lngSBEPK, Int16 Ebkg = 0)
        {

            WorkFlow objwf = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT NULL AS TRNTYPEFK,");
            sb.Append("       BTSFL.TRANS_REF_NO AS REFNO,");
            sb.Append("       UOM.DIMENTION_ID AS BASIS,");
            sb.Append("       (CMT.COMMODITY_NAME || '$$;' || FEMT.CREDIT) AS COMMODITY,");
            sb.Append("       BST.PORT_MST_POL_FK AS PORT_MST_PK,");
            sb.Append("       PL.PORT_ID AS POL,");
            sb.Append("       BST.PORT_MST_POD_FK AS PORT_MST_PK1,");
            sb.Append("       PD.PORT_ID AS POD,");
            sb.Append("       FEMT.FREIGHT_ELEMENT_MST_PK AS FREIGHT_ELEMENT_MST_FK,");
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
            sb.Append("       'FALSE' AS SEL,");
            sb.Append("       CTMT.CURRENCY_MST_PK,");
            sb.Append("       CTMT.CURRENCY_ID,");
            sb.Append("       NULL AS SURCHARGE_VALUE,");
            sb.Append("       NULL AS MIN_RATE,");
            sb.Append("       1 AS RATE,");
            sb.Append("       NULL AS BKGRATE,");
            sb.Append("       UOM.DIMENTION_UNIT_MST_PK AS BASISPK,");
            sb.Append("       'PrePaid' AS PYMT_TYPE,");
            sb.Append("       CMT.COMMODITY_MST_PK AS COMMODITYPK,");
            sb.Append("       NULL AS FREIGHTPK,");
            sb.Append("       NULL AS EXCHANGERATE,");
            sb.Append("       '' SURCHARGE_TYPE,");
            sb.Append("       FEMT.CREDIT");
            sb.Append("  FROM booking_mst_tbl         BST,");
            sb.Append("       booking_trn BTSFL,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CTMT,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL FEMT,");
            sb.Append("       COMMODITY_MST_TBL       CMT,");
            sb.Append("       PORT_MST_TBL            PL,");
            sb.Append("       PORT_MST_TBL            PD,");
            sb.Append("       DIMENTION_UNIT_MST_TBL  UOM");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND CTMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
            sb.Append("   AND BTSFL.booking_mst_fk = BST.booking_mst_pk");
            sb.Append("   AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
            sb.Append("   AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
            sb.Append("   AND BST.booking_mst_pk = " + lngSBEPK);
            sb.Append("   AND NVL(FEMT.CHARGE_TYPE, 0) <> 3");
            sb.Append("   AND FEMT.BUSINESS_TYPE = 2");
            sb.Append("   AND FEMT.ACTIVE_FLAG=1 ");
            sb.Append(" ORDER BY PREFERENCE ASC");
            try
            {
                dtFreight = objwf.GetDataTable(sb.ToString());
                return dtFreight;
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
        public object FetchCDimension(DataTable dtMain, long lngSBEPK)
        {
            System.Text.StringBuilder strSqlCDimension = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strSqlCDimension.Append(" SELECT " );
            strSqlCDimension.Append(" ROWNUM AS SNO, " );
            strSqlCDimension.Append(" BACC.CARGO_NOP AS NOP, " );
            strSqlCDimension.Append(" BACC.CARGO_LENGTH AS LENGTH, " );
            strSqlCDimension.Append(" BACC.CARGO_WIDTH AS WIDTH, " );
            strSqlCDimension.Append(" BACC.CARGO_HEIGHT AS HEIGHT, " );
            strSqlCDimension.Append(" BACC.CARGO_CUBE AS CUBE, " );
            strSqlCDimension.Append(" BACC.CARGO_VOLUME_WT AS VOLWEIGHT, " );
            strSqlCDimension.Append(" BACC.CARGO_ACTUAL_WT AS ACTWEIGHT, " );
            strSqlCDimension.Append(" BACC.CARGO_DENSITY AS DENSITY, " );
            strSqlCDimension.Append(" BACC.BOOKING_CARGO_CALC_PK AS PK, " );
            strSqlCDimension.Append(" BACC.booking_mst_fk " );
            strSqlCDimension.Append(" FROM " );
            strSqlCDimension.Append(" BOOKING_CARGO_CALC BACC " );
            strSqlCDimension.Append(" WHERE " );
            strSqlCDimension.Append(" BACC.booking_mst_fk= " + lngSBEPK);
            try
            {
                dtMain = objwf.GetDataTable(strSqlCDimension.ToString());
                return dtMain;
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

        #endregion

        #region "Fetch Temp Data"

        public void UpdateTempCus(int CustomerPK)
        {
            WorkFlow objWF = new WorkFlow();
            bool Result = false;
            try
            {
                Result = objWF.ExecuteCommands("update temp_customer_tbl set transaction_type=2 where customer_mst_pk=' " + CustomerPK + "'");
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
                sqlStr = "select booking_air_pk from booking_mst_tbl where booking_ref_no='" + BookingId + "'";
                BookingPk = Convert.ToInt32(objWF.ExecuteScaler(sqlStr));
                return BookingPk;
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
                sqlStr = "select booking_mst_pk from booking_mst_tbl where booking_ref_no='" + BookingId + "'";
                BookingPk = Convert.ToInt32(objWF.ExecuteScaler(sqlStr));
                return BookingPk;
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                sqlStr = "select IS_EBOOKING from booking_mst_tbl where booking_mst_pk='" + BookingPK + "'";
                IS_EBOOKING = Convert.ToInt16(objWF.ExecuteScaler(sqlStr));
                return IS_EBOOKING;
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

        public string FetchEBKRef(Int16 BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGRef = null;
            try
            {
                sqlStr = "select BOOKING_REF_NO from booking_mst_tbl where booking_mst_pk='" + BookingPK + "' and BOOKING_REF_NO like 'BKG%'";
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                sqlStr = "select BOOKING_DATE from booking_mst_tbl where BOOKING_REF_NO='" + BOOKING_REF_NO + "' ";
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                sqlStr = "select BOOKING_REF_NO from booking_mst_tbl where booking_mst_pk='" + BookingPK + "' ";
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Fetch Sea Quotation Entry Details"

        public void FetchQEntry(DataSet dsQEntry, long lngSQEPk, string strQuotationPOLPK, string strQuotationPODPK)
        {
            try
            {
                DataTable dtMain = new DataTable();
                FetchSQEntryHeader(dtMain, lngSQEPk, strQuotationPOLPK, strQuotationPODPK);

                dsQEntry.Tables.Add(dtMain);
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

        public void FetchSQEntryHeader(DataTable dtMain, long lngSQEPK, string strQuotationPOLPK, string strQuotationPODPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strBuilder.Append("SELECT DISTINCT ");
            strBuilder.Append(" (CASE WHEN QST.CUST_TYPE=1 THEN ( ");
            strBuilder.Append("       select tmp.customer_mst_pk from temp_customer_tbl tmp where tmp.customer_mst_pk=QST.CUSTOMER_MST_FK ");
            strBuilder.Append(" ) ");
            strBuilder.Append(" ELSE QST.CUSTOMER_MST_FK END) AS CUSTOMER_MST_FK,");

            strBuilder.Append(" (CASE WHEN QST.CUST_TYPE=1 THEN (");
            strBuilder.Append("       select tmp.Customer_Name from temp_customer_tbl tmp where tmp.customer_mst_pk=QST.CUSTOMER_MST_FK");
            strBuilder.Append(" ) ");
            strBuilder.Append(" ELSE CMT.Customer_Name END) AS CUSTOMER_ID, ");

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
            strBuilder.Append(" QTSFL.COMMODITY_GROUP_FK, ");
            strBuilder.Append(" QST.COL_PLACE_MST_FK, ");
            strBuilder.Append(" CPMT.PLACE_CODE CPLACECODE, ");
            strBuilder.Append(" (CASE WHEN QST.COL_ADDRESS IS NULL THEN ");
            strBuilder.Append(" (CASE WHEN QST.CUSTOMER_MST_FK IS NOT NULL THEN ");
            strBuilder.Append(" (SELECT CTRN.COL_ADDRESS FROM CUSTOMER_MST_TBL CTRN WHERE ");
            strBuilder.Append(" CTRN.CUSTOMER_MST_PK = (SELECT DISTINCT QHDR.CUSTOMER_MST_FK FROM ");
            strBuilder.Append(" QUOTATION_SEA_TBL QHDR, QUOTATION_TRN_SEA_FCL_LCL QTRN WHERE ");
            strBuilder.Append(" QTRN.QUOTATION_SEA_FK = QHDR.QUOTATION_SEA_PK ");
            strBuilder.Append(" AND QHDR.CUST_TYPE= 0 ");
            strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK= " + lngSQEPK);
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
            strBuilder.Append(" QUOTATION_SEA_TBL QHDR, QUOTATION_TRN_SEA_FCL_LCL QTRN WHERE ");
            strBuilder.Append(" QTRN.QUOTATION_SEA_FK = QHDR.QUOTATION_SEA_PK ");
            strBuilder.Append(" AND QHDR.CUST_TYPE= 0 ");
            strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK= " + lngSQEPK);
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
            strBuilder.Append(" QST.CARGO_TYPE, ");
            strBuilder.Append(" TO_CHAR(QST.EXPECTED_SHIPMENT_DT,'" + dateFormat + "') AS EXP_SHIPMENT_DATE, ");
            strBuilder.Append(" QTSFL.EXPECTED_VOLUME AS VOLUME, QTSFL.EXPECTED_WEIGHT AS WEIGHT, ");
            strBuilder.Append(" QST.shipping_terms_mst_pk, ");
            strBuilder.Append(" NULL LINE_BKG_DT ");
            strBuilder.Append(" FROM QUOTATION_SEA_TBL QST, ");
            strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL QTSFL, ");
            strBuilder.Append(" CUSTOMER_MST_TBL CMT, ");
            strBuilder.Append(" PLACE_MST_TBL CPMT, ");
            strBuilder.Append(" PLACE_MST_TBL DPMT, ");
            strBuilder.Append(" AGENT_MST_TBL CBAMT, ");
            strBuilder.Append(" PORT_MST_TBL POL, ");
            strBuilder.Append(" PORT_MST_TBL POD, ");
            strBuilder.Append(" AGENT_MST_TBL DPAMT, ");
            strBuilder.Append(" CUSTOMER_CATEGORY_MST_TBL CCMT ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" QST.QUOTATION_SEA_PK=QTSFL.QUOTATION_SEA_FK ");
            strBuilder.Append(" AND QST.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+) ");
            strBuilder.Append(" AND CMT.DP_AGENT_MST_FK=DPAMT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND QST.COL_PLACE_MST_FK=CPMT.PLACE_PK(+) ");
            strBuilder.Append(" AND QST.DEL_PLACE_MST_FK=DPMT.PLACE_PK(+) ");
            strBuilder.Append(" AND QST.AGENT_MST_FK=CBAMT.AGENT_MST_PK(+) ");
            strBuilder.Append(" AND QST.CUSTOMER_CATEGORY_MST_FK=CCMT.CUSTOMER_CATEGORY_MST_PK(+) ");
            strBuilder.Append(" AND QTSFL.PORT_MST_POL_FK=POL.PORT_MST_PK(+) ");
            strBuilder.Append(" AND QTSFL.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ");
            strBuilder.Append(" AND QST.QUOTATION_SEA_PK= " + lngSQEPK);
            strBuilder.Append(" AND QTSFL.PORT_MST_POL_FK= " + strQuotationPOLPK);
            strBuilder.Append(" AND QTSFL.PORT_MST_POD_FK= " + strQuotationPODPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
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
            strBuilder.Append(" QUOTATION_SEA_TBL QHDR, ");
            strBuilder.Append(" QUOTATION_TRN_SEA_OTH_CHRG QTSOC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append("  QTSOC.QUOTATION_SEA_FK=QHDR.QUOTATION_SEA_PK ");
            strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK= " + lngSQEPK);

            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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

        public ArrayList FetchQuotationCDimension(int QtnDtlPK, long SlNr)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();
            ArrayList arrCal = new ArrayList();
            DataSet CargoCalDS = null;

            sb.Append("SELECT DISTINCT ROWNUM SNO,");
            sb.Append("                Q.CARGO_LENGTH AS LENGTH,");
            sb.Append("                Q.CARGO_WIDTH AS WIDTH,");
            sb.Append("                Q.CARGO_HEIGHT AS HEIGHT,");
            sb.Append("                Q.CARGO_DIVISION_FACT AS WTUNIT,");
            sb.Append("                Q.CARGO_NOP AS NOP,");
            sb.Append("                Q.CARGO_ACTUAL_WT AS UOM,");
            sb.Append("                0 LTON,");
            sb.Append("                Q.CARGO_CUBE AS CUBE,");
            sb.Append("                0 CBFT,");
            sb.Append("                '" + SlNr + "' QUOTREF,");
            sb.Append("                0 CARGO_CALL_PK,");
            sb.Append("                Q.CARGO_MEASUREMENT MEASUREMENT,");
            sb.Append("                '' REFERENCE_FK,");
            sb.Append("                '' DELFLAG");
            sb.Append("  FROM QUOTATION_SEA_CARGO_CALC  Q,");
            sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QD,");
            sb.Append("       QUOTATION_SEA_TBL         QM");
            sb.Append(" WHERE QD.QUOTE_TRN_SEA_PK = Q.QUOTE_TRN_SEA_FK");
            sb.Append("   AND QD.QUOTATION_SEA_FK = QM.QUOTATION_SEA_PK");
            sb.Append(" AND Q.QUOTE_TRN_SEA_FK= " + QtnDtlPK);
            try
            {
                CargoCalDS = objwf.GetDataSet(sb.ToString());
                arrCal.Add(CargoCalDS);
                return arrCal;
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
        #endregion

        #region "Fetch Sea Spot Rate Entry Details"

        public void FetchSpotRateEntry(DataSet dsSpotRateEntry, long lngSpotRatePk, string strPOLPK, string strPODPK)
        {
            try
            {
                DataTable dtMain = new DataTable();
                FetchSpotRateEntryHeader(dtMain, lngSpotRatePk);

                dsSpotRateEntry.Tables.Add(dtMain);
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

        public void FetchSpotRateEntryHeader(DataTable dtMain, long lngSpotRateEntryPK)
        {
            System.Text.StringBuilder strBuilderSpotRate = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
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
            try
            {
                dtMain = objwf.GetDataTable(strBuilderSpotRate.ToString());
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

        public object FetchSpotRateCDimension(DataTable dtMain, long lngSpotRateEntryPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
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
            strBuilder.Append(" NULL AS FK ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" RFQ_SPOT_SEA_CARGO_CALC RSACC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" RSACC.RFQ_SPOT_SEA_FK= " + lngSpotRateEntryPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
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

        #endregion

        #region "Fetch Grid Values Manual"
        public void CheckQus(string QnsNo)
        {
            WorkFlow objWF = new WorkFlow();
            bool Result = false;
            try
            {
                Result = objWF.ExecuteCommands("update quotation_air_tbl set status=2 where quotation_ref_no='" + QnsNo + "'");
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
                    strSql = "select booking_mst_fk from booking_trn where booking_mst_fk=" + BkgPk;
                }

                if (Biz == 1)
                {
                    strSql = "select BOOKING_AIR_FK from booking_trn_air where BOOKING_AIR_FK=" + BkgPk;
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataSet FetchGridManualValues(DataSet dsGrid, Int16 intIsFcl = 0, string strPOL = "", string strPOD = "", string strContainer = "", Int32 BaseCurrency = 0, bool Fetch = false, string strCommodity = "")
        {
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtChild = new DataTable();
                dtMain = FetchHeaderManual(intIsFcl, strContainer, Fetch, strCommodity);
                dtChild = FetchFreightManual(intIsFcl, strPOL, strPOD, strContainer, BaseCurrency, Fetch, strCommodity);
                int RowCnt = 0;
                double BOF_AMT = 0;
                BOF_AMT = 0;
                for (RowCnt = 0; RowCnt <= dtMain.Rows.Count - 1; RowCnt++)
                {
                    if (dtChild.Rows[RowCnt]["FREIGHT_ELEMENT_ID"] == "BOF")
                    {
                        if (!string.IsNullOrEmpty(dtChild.Rows[RowCnt]["MIN_RATE"].ToString()))
                        {
                            BOF_AMT = Convert.ToDouble(dtChild.Rows[RowCnt]["MIN_RATE"]);
                        }
                        break; // TODO: might not be correct. Was : Exit For
                    }
                    else
                    {
                        BOF_AMT = 1;
                    }
                }
                for (RowCnt = 0; RowCnt <= dtMain.Rows.Count - 1; RowCnt++)
                {
                    if (dtChild.Rows[RowCnt]["FREIGHT_ELEMENT_ID"] != "BOF")
                    {
                        if (dtChild.Rows[RowCnt]["CHARGE_BASIS"] == "%")
                        {
                            if (!string.IsNullOrEmpty(dtChild.Rows[RowCnt]["MIN_RATE"].ToString()))
                            {
                                dtChild.Rows[RowCnt]["BKGRATE"] = (BOF_AMT * Convert.ToDouble(dtMain.Rows[RowCnt]["MIN_RATE"])) / 100;
                            }
                        }
                    }
                }
                //End
                dsGrid.Tables.Add(dtMain);
                dsGrid.Tables.Add(dtChild);

                DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] {
                    dsGrid.Tables[0].Columns["BASISPK"],
                    dsGrid.Tables[0].Columns["COMMODITYPK"]
                }, new DataColumn[] {
                    dsGrid.Tables[1].Columns["BASISPK"],
                    dsGrid.Tables[1].Columns["COMMODITYPK"]
                });
                dsGrid.Relations.Clear();
                dsGrid.Relations.Add(rel);
                return dsGrid;
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
        private DataTable FetchHeaderManual(Int16 intIsFcl = 0, string strContainer = "", bool Fetch = false, string strCommodity = "")
        {
            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                int intFetch = 2;
                if (Fetch)
                    intFetch = 1;
                if (intIsFcl != 2)
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEPK," );
                    strQuery.Append("       '7' AS TRNTYPESTATUS," );
                    strQuery.Append("       'Manual' AS CONTRACTTYPE," );
                    strQuery.Append("       NULL AS REFNO," );
                    strQuery.Append("       NULL AS OPERATOR," );
                    strQuery.Append("       OCTMT.CONTAINER_TYPE_MST_ID AS TYPE," );
                    strQuery.Append("       '' AS BOXES," );
                    strQuery.Append("       NULL AS COMMODITY," );
                    strQuery.Append("       NULL AS RATE," );
                    strQuery.Append("       '' AS BKGRATE," );
                    strQuery.Append("       '' AS NET," );
                    strQuery.Append("       '' AS TOTALRATE," );
                    strQuery.Append("       '0' AS SEL," );
                    strQuery.Append("       OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK," );
                    strQuery.Append("       NULL AS COMMODITYPK," );
                    strQuery.Append("       NULL AS OPERATORPK," );
                    strQuery.Append("       '' AS TRANSACTIONPK," );
                    strQuery.Append("       NULL AS BASISPK" );
                    strQuery.Append("  FROM CONTAINER_TYPE_MST_TBL OCTMT" );
                    strQuery.Append(" WHERE (1 = " + intFetch + ")" );
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN" );
                        strQuery.Append("       (" + strContainer + ")" );
                    }
                    strQuery.Append(" AND OCTMT.ACTIVE_FLAG = 1 " );
                    strQuery.Append(" GROUP BY OCTMT.CONTAINER_TYPE_MST_ID, OCTMT.CONTAINER_TYPE_MST_PK" );
                }
                else
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEPK," );
                    strQuery.Append("       '7' AS TRNTYPESTATUS," );
                    strQuery.Append("       'Manual' AS CONTRACTTYPE," );
                    strQuery.Append("       NULL AS REFNO," );
                    strQuery.Append("       NULL AS OPERATOR,CMT.COMMODITY_NAME COMMODITY," );
                    strQuery.Append("   0 PACK_PK,'' PACK_TYPE,'' AS BASIS," );
                    strQuery.Append("       '' AS QTY,'0.000' CARGO_WT,'0.000' CARGO_VOL,'' CARGO_CALC," );
                    strQuery.Append("       NULL AS RATE," );
                    strQuery.Append("       '' AS BKGRATE," );
                    strQuery.Append("       '' AS NET," );
                    strQuery.Append("       '' AS TOTALRATE," );
                    strQuery.Append("       '0' AS SEL," );
                    strQuery.Append("       ROWNUM  CONTAINERPK," );
                    strQuery.Append("       CMT.COMMODITY_MST_PK AS COMMODITYPK," );
                    strQuery.Append("       NULL AS OPERATORPK," );
                    strQuery.Append("       '' AS TRANSACTIONPK," );
                    strQuery.Append("       NULL AS BASISPK" );
                    strQuery.Append("  FROM COMMODITY_MST_TBL CMT" );
                    strQuery.Append(" WHERE (1 = 1)" );
                    if (strCommodity.Length > 0)
                    {
                        strQuery.Append("   AND CMT.COMMODITY_MST_PK IN  (" + strCommodity + ")" );
                    }

                    strQuery.Append(" GROUP BY CMT.COMMODITY_NAME,ROWNUM,CMT.COMMODITY_MST_PK" );
                }

                return (new WorkFlow()).GetDataTable(strQuery.ToString());
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
        private DataTable FetchFreightManual(Int16 intIsFcl = 0, string strPOL = "", string strPOD = "", string strContainer = "", Int32 BaseCurrency = 0, bool Fetch = false, string strCommodity = "")
        {
            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                int intFetch = 2;
                if (Fetch)
                    intFetch = 1;
                if (intIsFcl != 2)
                {
                    strQuery.Append("SELECT  NULL AS TRNTYPEFK," );
                    strQuery.Append("                NULL AS REFNO," );
                    strQuery.Append("                NULL AS TYPE," );
                    strQuery.Append("                NULL AS COMMODITY," );
                    strQuery.Append("                OPL.PORT_MST_PK," );
                    strQuery.Append("                OPL.PORT_ID AS POL," );
                    strQuery.Append("                OPD.PORT_MST_PK," );
                    strQuery.Append("                OPD.PORT_ID AS POD," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_ID," );
                    strQuery.Append("                DECODE(0, 1, 'true', 'false') SEL," );
                    strQuery.Append("                OCUMT.CURRENCY_MST_PK," );
                    strQuery.Append("                OCUMT.CURRENCY_ID," );
                    strQuery.Append("                '' AS RATE," );
                    strQuery.Append("                '' AS BKGRATE," );
                    strQuery.Append("                '' AS BASISPK," );
                    strQuery.Append("                '1' AS PYMT_TYPE," );
                    strQuery.Append("                CONTAINER_TYPE_MST_PK AS CONTAINERPK" );
                    strQuery.Append("  FROM CONTAINER_TYPE_MST_TBL  OCTMT," );
                    strQuery.Append("       PORT_MST_TBL            OPL," );
                    strQuery.Append("       PORT_MST_TBL            OPD," );
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   OCUMT" );
                    strQuery.Append(" WHERE (1 = " + intFetch + ")" );
                    strQuery.Append("   AND OPL.PORT_MST_PK = " + strPOL );
                    strQuery.Append("   AND OPD.PORT_MST_PK = " + strPOD );
                    strQuery.Append("   AND OCUMT.CURRENCY_MST_PK = " + BaseCurrency );
                    strQuery.Append("   AND OFEMT.BUSINESS_TYPE =2" );
                    strQuery.Append("   AND OFEMT.ACTIVE_FLAG = 1" );
                    strQuery.Append("   AND OCTMT.ACTIVE_FLAG = 1" );
                    //OFEMT.CHARGE_BASIS
                    strQuery.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3" );
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN" );
                        strQuery.Append("       (" + strContainer + ")" );
                        //ACTIVE_FLAG
                    }
                    strQuery.Append("   ORDER BY OFEMT.PREFERENCE ASC" );

                }
                else
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEFK," );
                    strQuery.Append("                NULL AS REFNO," );
                    strQuery.Append("                OFEMT.Credit AS BASIS," );
                    strQuery.Append("                CMT.COMMODITY_NAME AS COMMODITY," );
                    strQuery.Append("                OPL.PORT_MST_PK," );
                    strQuery.Append("                OPL.PORT_ID AS POL," );
                    strQuery.Append("                OPD.PORT_MST_PK," );
                    strQuery.Append("                OPD.PORT_ID AS POD," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_ID," );
                    strQuery.Append("                DECODE(OFEMT.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS," );
                    strQuery.Append("                DECODE(0, 1, 'true', 'false') SEL," );
                    strQuery.Append("                OCUMT.CURRENCY_MST_PK," );
                    strQuery.Append("                OCUMT.CURRENCY_ID," );
                    strQuery.Append("                NULL SURCHARGE_VALUE," );

                    strQuery.Append("                null AS MIN_RATE," );
                    strQuery.Append("                1 AS RATE," );
                    strQuery.Append("                null AS BKGRATE," );
                    strQuery.Append("                NULL AS BASISPK," );
                    strQuery.Append("                '1' AS PYMT_TYPE," );
                    strQuery.Append("                CMT.COMMODITY_MST_PK COMMODITYPK,null FreightPK,null EXCHANGERATE, " );
                    strQuery.Append("                '' SURCHARGE_TYPE,OFEMT.Credit" );
                    strQuery.Append("" );
                    strQuery.Append("  FROM PORT_MST_TBL            OPL," );
                    strQuery.Append("       PORT_MST_TBL            OPD," );
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   OCUMT," );
                    strQuery.Append("       COMMODITY_MST_TBL CMT" );
                    strQuery.Append(" WHERE (1 = 1)" );
                    strQuery.Append("   AND OPL.PORT_MST_PK = " + strPOL );
                    strQuery.Append("   AND OPD.PORT_MST_PK = " + strPOD );
                    strQuery.Append("   AND OCUMT.CURRENCY_MST_PK = " + BaseCurrency );
                    strQuery.Append("   AND OFEMT.BUSINESS_TYPE IN (2, 3)" );
                    strQuery.Append("   AND OFEMT.ACTIVE_FLAG = 1" );
                    strQuery.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3 " );
                    if (strCommodity.Length > 0)
                    {
                        strQuery.Append("   AND CMT.COMMODITY_MST_PK IN (" + strCommodity + ")" );
                    }
                    strQuery.Append("   ORDER BY OFEMT.PREFERENCE ASC" );
                }
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
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
                sb.Append(" 'FALSE' AS CHECK_FOR_ALL_IN_RT, ");
                sb.Append(" '' as CURRENCY_MST_FK,");
                sb.Append(" '' as CURRENCY_ID,");
                sb.Append(" NULL AS MIN_RATE, ");
                sb.Append(" NULL AS RATE, ");
                sb.Append(" NULL AS BKGRATE, ");
                sb.Append(" NULL AS BASIS, ");
                sb.Append(" 'PrePaid' AS PYMT_TYPE, ");
                sb.Append(" '' as BOOKING_TRN_SEA_FRT_PK ");
                sb.Append(" FROM");
                sb.Append(" booking_mst_tbl BST, ");
                sb.Append(" booking_trn BTSFL, ");
                sb.Append(" CURRENCY_TYPE_MST_TBL CTMT, ");
                sb.Append(" FREIGHT_ELEMENT_MST_TBL FEMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" PORT_MST_TBL PL, ");
                sb.Append(" PORT_MST_TBL PD, ");
                sb.Append(" DIMENTION_UNIT_MST_TBL UOM");
                sb.Append(" WHERE(1 = 1)");
                sb.Append(" AND BTSFL.booking_mst_fk=BST.booking_mst_pk");
                sb.Append(" AND BTSFL.booking_mst_fk=BST.booking_mst_pk ");
                sb.Append(" AND BTSFL.BASIS=UOM.DIMENTION_UNIT_MST_PK(+) ");
                sb.Append(" AND BST.PORT_MST_POL_FK=PL.PORT_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POD_FK=PD.PORT_MST_PK (+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BST.booking_mst_pk= " + lngSBEPK);
                sb.Append(" and ctmt.currency_mst_pk = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                sb.Append(" and femt.business_type = 2");
                sb.Append(" AND nvl(FEMT.CHARGE_TYPE,0) <> 3");
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
                sb.Append(" 'FALSE' AS CHECK_FOR_ALL_IN_RT, ");
                sb.Append(" '' as CURRENCY_MST_FK, ");
                sb.Append(" '' as CURRENCY_ID, ");
                sb.Append(" NULL AS RATE, ");
                sb.Append(" NULL AS BKGRATE, ");
                sb.Append(" NULL AS BASIS, ");
                sb.Append(" 'PrePaid' AS PYMT_TYPE, ");
                sb.Append(" '' as BOOKING_TRN_SEA_FRT_PK ");
                sb.Append(" FROM ");
                sb.Append(" booking_mst_tbl BST, ");
                sb.Append(" booking_trn BTSFL, ");
                sb.Append(" CURRENCY_TYPE_MST_TBL CTMT, ");
                sb.Append(" FREIGHT_ELEMENT_MST_TBL FEMT, ");
                sb.Append(" CONTAINER_TYPE_MST_TBL CTMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" PORT_MST_TBL PL, ");
                sb.Append(" PORT_MST_TBL PD ");
                sb.Append(" WHERE(1 = 1) ");
                sb.Append(" AND BTSFL.booking_mst_fk=BST.booking_mst_pk ");
                sb.Append(" AND BTSFL.CONTAINER_TYPE_MST_FK=CTMT.CONTAINER_TYPE_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POL_FK=PL.PORT_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POD_FK=PD.PORT_MST_PK (+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BST.booking_mst_pk= " + lngSBEPK);
                sb.Append(" and ctmt.currency_mst_pk = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                sb.Append(" and femt.business_type = 2");
                sb.Append(" AND nvl(FEMT.CHARGE_TYPE,0) <> 3");
            }
            try
            {
                dtFreight = objwf.GetDataTable(sb.ToString());
                return dtFreight;
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
        #endregion

        #region "Fetch Grid Values"
        public DataSet FetchGridValues(DataSet dsGrid, Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, short intSRateStatus = 0, short intCContractStatus = 0, short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, string strPOL = "",
        string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", string strContainer = "", Int32 intSpotRatePk = 0, string CustContRefNr = "")
        {
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtChild = new DataTable();
                if (!string.IsNullOrEmpty(CustContRefNr) & CustContRefNr != "0")
                {
                    intCContractStatus = 1;
                }
                if (intQuotationPK == 0 & intSpotRatePk == 0)
                {
                    dtMain = FetchHeader(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, intSRateStatus, intCContractStatus,
                    intOTariffStatus, intGTariffStatus, intSRRContractStatus,0 , CustContRefNr);
                    dtChild = FetchFreight(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, intSRateStatus, intCContractStatus,
                    intOTariffStatus, intGTariffStatus, intSRRContractStatus,0 , CustContRefNr);
                }
                else if (intSpotRatePk == 0)
                {
                    dtMain = FetchHeader(intQuotationPK, intIsFcl, 0, strPOL, strPOD, 0, "", strContainer, 0, 0, 0, 0, 0);
                    dtChild = FetchFreight(intQuotationPK, intIsFcl, 0, strPOL, strPOD, 0, "", strContainer, 0, 0, 0, 0, 0);
                }
                else
                {
                    dtMain = FetchHeader(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, 0, 0,0, 0, 0, intSpotRatePk);
                    dtChild = FetchFreight(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, 0, 0,0, 0, 0, intSpotRatePk);
                }
                dsGrid.Tables.Add(dtMain);
                dsGrid.Tables.Add(dtChild);
                if (dsGrid.Tables.Count > 1)
                {
                    if (dsGrid.Tables[1].Rows.Count > 0)
                    {
                        if (intIsFcl == 2)
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
                                    dsGrid.Tables[0].Columns["COMMODITYPK"]
                                }, new DataColumn[] {
                                    dsGrid.Tables[1].Columns["REFNO"],
                                    dsGrid.Tables[1].Columns["COMMODITY_MST_PK"]
                                });
                                dsGrid.Relations.Clear();
                                dsGrid.Relations.Add(rel);
                            }

                        }
                    }
                }
                return dsGrid;
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
        #endregion

        #region "Fetch Header"

        public DataTable FetchHeader(Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, string strPOL = "", string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", string strContainer = "", short intSRateStatus = 0, short intCContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, Int32 intSpotRatePK = 0, string CustContRefNr = "")
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
                    strCustomer = " AND (SRRSRST.CUSTOMER_MST_FK= " + intCustomerPK + " OR SRRSRST.CUSTOMER_MST_FK IS NULL)";
                }
                if (!(intQuotationPK == 0))
                {
                    strSql = Convert.ToString(funQuotationHeader(strPOL, strPOD, intQuotationPK, intIsFcl));
                }
                else
                {
                    //
                    if (!(intSpotRatePK == 0))
                    {
                        strSql = funSpotRateHeader(arrCCondition, strCustomer, Convert.ToString(intCommodityPK), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 1).ToString();
                    }
                    if (intSRateStatus == 1 & intSpotRatePK == 0)
                    {
                        strSql = funSpotRateHeader(arrCCondition, strCustomer, Convert.ToString(intCommodityPK), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 2).ToString();
                    }
                    if (intCContractStatus == 1)
                    {
                        strSql = funCustContHeader(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl, Convert.ToString(getDefault(CustContRefNr, ""))).ToString();
                    }
                    if (intSRRContractStatus == 1)
                    {
                        strSql = funSRRHeader(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl).ToString();
                    }
                    if (intOTariffStatus == 1)
                    {
                        strSql = funSLTariffHeader(arrCCondition, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl).ToString();
                    }
                    if (intGTariffStatus == 1)
                    {
                        strSql = funGTariffHeader(arrCCondition, Convert.ToString(intCommodityPK), strPOL, strPOD, strSDate, intIsFcl).ToString();
                    }
                }
                return (new WorkFlow()).GetDataTable(strSql);
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
        public object funQuotationHeader(string strPOL, string strPOD, Int32 intQuotationPK, Int16 intIsFcl)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT QHDR.QUOTATION_SEA_PK AS TRNTYPEPK,");
                sb.Append("       '1' AS TRNTYPESTATUS,");
                sb.Append("       'Quote' AS CONTRACTTYPE,");
                sb.Append("       QHDR.QUOTATION_REF_NO AS REFNO,");
                sb.Append("       QOMT.OPERATOR_ID AS OPERATOR,");
                sb.Append("       QCMT.COMMODITY_NAME AS COMMODITY,");
                sb.Append("       PK.PACK_TYPE_MST_PK PACK_PK,");
                sb.Append("       PK.PACK_TYPE_ID PACK_TYPE,");
                sb.Append("       DM.DIMENTION_ID BASIS,");
                sb.Append("       QTRN.EXPECTED_BOXES AS QTY,");
                sb.Append("       QTRN.EXPECTED_WEIGHT CARGO_WT,");
                sb.Append("       QTRN.EXPECTED_VOLUME CARGO_VOL,");
                sb.Append("       '' CARGO_CALC,");
                sb.Append("       NULL AS RATE,");
                sb.Append("  ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2) AS BKGRATE,");
                sb.Append("       '' AS NET,");
                sb.Append("        CASE");
                sb.Append("         WHEN DM.DIMENTION_ID = 'W/M' THEN");
                sb.Append("          CASE");
                sb.Append("            WHEN QTRN.EXPECTED_VOLUME > QTRN.EXPECTED_WEIGHT THEN");
                sb.Append("             ROUND(ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2)* QTRN.EXPECTED_VOLUME, 2)");
                sb.Append("            WHEN QTRN.EXPECTED_WEIGHT > QTRN.EXPECTED_VOLUME THEN");
                sb.Append("             ROUND(ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2) * QTRN.EXPECTED_WEIGHT, 2)");
                sb.Append("            ELSE");
                sb.Append("             ROUND(ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2) * QTRN.EXPECTED_WEIGHT, 2)");
                sb.Append("          END");
                sb.Append("         WHEN DM.DIMENTION_ID = 'CBM' THEN");
                sb.Append("          ROUND(ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2) * QTRN.EXPECTED_VOLUME, 2)");
                sb.Append("         WHEN DM.DIMENTION_ID = 'MT' THEN");
                sb.Append("          ROUND(ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2) * QTRN.EXPECTED_WEIGHT, 2)");
                sb.Append("         WHEN DM.DIMENTION_ID = 'UNIT' THEN");
                sb.Append("          ROUND(ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2) * QTRN.EXPECTED_BOXES, 2)");
                sb.Append("         ELSE");
                sb.Append("          ROUND(ROUND((SELECT SUM(FT.RATE)");
                sb.Append("               FROM QUOTATION_TRN_SEA_FRT_DTLS FT");
                sb.Append("              WHERE FT.QUOTE_TRN_SEA_FK = QTRN.QUOTE_TRN_SEA_PK),");
                sb.Append("             2) * QTRN.EXPECTED_BOXES, 2)");
                sb.Append("       END TOTALRATE,");
                sb.Append("       '1' AS SEL,");
                sb.Append("       ROWNUM AS CONTAINERPK,");
                sb.Append("       QCMT.COMMODITY_MST_PK AS COMMODITYPK,");
                sb.Append("       QOMT.OPERATOR_MST_PK AS OPERATORPK,");
                sb.Append("       QTRN.QUOTE_TRN_SEA_PK AS TRANSACTIONPK,");
                sb.Append("       DM.DIMENTION_UNIT_MST_PK AS BASISPK");
                sb.Append("  FROM QUOTATION_SEA_TBL         QHDR,");
                sb.Append("       QUOTATION_TRN_SEA_FCL_LCL QTRN,");
                sb.Append("       OPERATOR_MST_TBL          QOMT,");
                sb.Append("       COMMODITY_MST_TBL         QCMT,");
                sb.Append("       DIMENTION_UNIT_MST_TBL    DM,PACK_TYPE_MST_TBL PK");
                sb.Append(" WHERE (1 = 1)");
                sb.Append("   AND QTRN.OPERATOR_MST_FK = QOMT.OPERATOR_MST_PK(+)");
                sb.Append("   AND QTRN.COMMODITY_MST_FK = QCMT.COMMODITY_MST_PK(+)");
                sb.Append("   AND PK.PACK_TYPE_MST_PK(+)=QTRN.PACK_TYPE_FK");
                sb.Append("   AND QHDR.QUOTATION_SEA_PK = QTRN.QUOTATION_SEA_FK");
                sb.Append("   AND DM.DIMENTION_UNIT_MST_PK = QTRN.BASIS");

                sb.Append(" AND QHDR.QUOTATION_SEA_PK= " + intQuotationPK );

                return sb.ToString();
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
        public object funSpotRateHeader(ArrayList arrCCondition, string strCustomer, string intCommodityPK, string strPOL, string strPOD, string intSpotRatePK, string strSDate, Int16 intIsFcl, Int16 intFlag)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    if (intFlag == 1)
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, " );
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, " );
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, " );
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, " );
                        strBuilder.Append(" SRUOM.DIMENTION_ID AS BASIS," );
                        strBuilder.Append(" '' AS QTY, " );

                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" '' AS CONTAINERPK, " );
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, " );
                        strBuilder.Append(" '' AS TRANSACTIONPK, " );
                        strBuilder.Append(" SRUOM.DIMENTION_UNIT_MST_PK AS BASISPK " );
                        strBuilder.Append(" FROM " );
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, " );
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append(" DIMENTION_UNIT_MST_TBL SRUOM, " );
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT " );
                        strBuilder.Append(" WHERE(1 = 1) " );
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) " );
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " );
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append(" AND SRRSTSF.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK " );
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 " );
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1] );
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + strCustomer );

                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK );
                        strBuilder.Append(" and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK );
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME, SRCMT.COMMODITY_MST_PK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK,SRUOM.DIMENTION_ID, SRUOM.DIMENTION_UNIT_MST_PK ");
                    }
                    else
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, " );
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, " );
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, " );
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, " );
                        strBuilder.Append(" SRUOM.DIMENTION_ID AS BASIS," );
                        strBuilder.Append(" '' AS QTY, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" '' AS CONTAINERPK, " );
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, " );
                        strBuilder.Append(" '' AS TRANSACTIONPK, " );
                        strBuilder.Append(" SRUOM.DIMENTION_UNIT_MST_PK AS BASISPK " );
                        strBuilder.Append(" FROM " );
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, " );
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append(" DIMENTION_UNIT_MST_TBL SRUOM, " );
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT " );
                        strBuilder.Append(" WHERE(1 = 1) " );
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) " );
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " );
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append(" AND SRRSTSF.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK " );
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 " );
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1] );
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + strCustomer );
                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK );
                        strBuilder.Append(" and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME, SRCMT.COMMODITY_MST_PK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK,SRUOM.DIMENTION_ID, SRUOM.DIMENTION_UNIT_MST_PK ");
                    }
                }
                else
                {
                    if (intFlag == 1)
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, " );
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, " );
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, " );
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                        strBuilder.Append(" '' AS BOXES, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, " );
                        strBuilder.Append(" '' AS TRANSACTIONPK, " );
                        strBuilder.Append(" NULL AS BASISPK " );
                        strBuilder.Append(" FROM " );
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append(" CONTAINER_TYPE_MST_TBL SRCTMT, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_CONT_DET SRRST, " );
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT " );
                        strBuilder.Append(" WHERE(1 = 1) " );
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) " );
                        strBuilder.Append(" AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSF.RFQ_SPOT_SEA_TRN_PK " );
                        //Snigdharani
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " );
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append(" AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_TYPE_MST_FK " );
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 " );
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=1 " + strCustomer );
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + arrCCondition[1] );
                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK );
                        strBuilder.Append(" and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK );
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))" );
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID, SRCMT.COMMODITY_NAME, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK,SRCMT.COMMODITY_MST_PK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK ");
                    }
                    else
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, " );
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, " );
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, " );
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                        strBuilder.Append(" '' AS BOXES, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, " );
                        strBuilder.Append(" '' AS TRANSACTIONPK, " );
                        strBuilder.Append(" NULL AS BASISPK " );
                        strBuilder.Append(" FROM " );
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append(" CONTAINER_TYPE_MST_TBL SRCTMT, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_CONT_DET SRRST, " );
                        strBuilder.Append(" COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append(" COMMODITY_MST_TBL SRCMT " );
                        strBuilder.Append(" WHERE(1 = 1) " );
                        strBuilder.Append(" AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) " );
                        strBuilder.Append(" AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSF.RFQ_SPOT_SEA_TRN_PK " );
                        //Snigdharani
                        strBuilder.Append(" AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK " );
                        strBuilder.Append(" AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSF.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append(" AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_TYPE_MST_FK " );
                        strBuilder.Append(" AND SRRSRST.ACTIVE=1 " );
                        strBuilder.Append(" AND SRRSRST.CARGO_TYPE=1 " + strCustomer );
                        strBuilder.Append(" AND SRRSRST.APPROVED=1 " + arrCCondition[1] );
                        strBuilder.Append(" AND SRCOMM.COMMODITY_GROUP_PK = " + intCommodityPK );
                        strBuilder.Append(" and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append(" AND SRRSTSF.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append(" AND NVL(SRRSRST.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))" );
                        strBuilder.Append(" GROUP BY SRRSRST.RFQ_SPOT_SEA_PK,SRRSRST.RFQ_REF_NO,SROMT.OPERATOR_ID, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID, SRCMT.COMMODITY_NAME, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK,SRCMT.COMMODITY_MST_PK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK ");
                    }
                }
                return strBuilder.ToString();
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

        public object funCustContHeader(ArrayList arrCCondition, Int32 intCustomerPK, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl, string custcontRefNr = "")
        {
            try
            {
                System.Text.StringBuilder strOperatorRate = new System.Text.StringBuilder();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strOperatorRate.Append(" ( Select Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0) )                     " );
                    strOperatorRate.Append(" from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx" );
                    strOperatorRate.Append(" where mx.ACTIVE                     = 1     AND                         " );
                    strOperatorRate.Append(" mx.CONT_APPROVED              = 1  AND vx.EXCH_RATE_TYPE_FK = 1   AND                         " );
                    strOperatorRate.Append(" mx.CARGO_TYPE                 = 2     AND                         " );
                    strOperatorRate.Append(" mx.OPERATOR_MST_FK            = CCHDR.OPERATOR_MST_FK AND          " );
                    strOperatorRate.Append(" mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " );
                    strOperatorRate.Append(" TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " );
                    strOperatorRate.Append(" tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND " );
                    strOperatorRate.Append(" tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " );
                    strOperatorRate.Append(" tx.LCL_BASIS                  = CCTRN.LCL_BASIS               AND " );
                    strOperatorRate.Append(" tx.PORT_MST_POL_FK            = CCTRN.PORT_MST_POL_FK         AND " );
                    strOperatorRate.Append(" tx.PORT_MST_POD_FK            = CCTRN.PORT_MST_POD_FK         AND " );
                    strOperatorRate.Append(" tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " );
                    strOperatorRate.Append(" tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND             " );
                    strOperatorRate.Append(" sysdate between vx.FROM_DATE and vx.TO_DATE )                     ");

                    strBuilder.Append(" SELECT CCHDR.CONT_CUST_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append(" '3' AS TRNTYPESTATUS, " );
                    strBuilder.Append(" 'Cust Cont' AS CONTRACTTYPE, " );
                    strBuilder.Append(" CCHDR.CONT_REF_NO AS REFNO, " );
                    strBuilder.Append(" CCOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append(" CCUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append(" '' AS QTY, " );
                    strBuilder.Append(" CCCMT.COMMODITY_ID AS COMMODITY, " );
                    strBuilder.Append(" NVL(" + strOperatorRate.ToString() + ", NULL) AS RATE, " );
                    strBuilder.Append(" '' AS BKGRATE, " );
                    strBuilder.Append(" '' AS NET, " );
                    strBuilder.Append(" '' AS TOTALRATE, " );
                    strBuilder.Append(" '0' AS SEL, " );
                    strBuilder.Append(" '' AS CONTAINERPK, " );
                    strBuilder.Append(" CCCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                    strBuilder.Append(" CCOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append(" '' AS TRANSACTIONPK, " );
                    strBuilder.Append(" CCUOM.DIMENTION_UNIT_MST_PK AS BASISPK" );
                    strBuilder.Append(" FROM " );
                    strBuilder.Append(" CONT_CUST_SEA_TBL CCHDR, " );
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL CCTRN, " );
                    strBuilder.Append(" OPERATOR_MST_TBL CCOMT, " );
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL CCUOM, " );
                    strBuilder.Append(" COMMODITY_MST_TBL CCCMT " );
                    strBuilder.Append(" WHERE(1 = 1) " );
                    strBuilder.Append(" AND CCHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK(+) " );
                    strBuilder.Append(" AND CCTRN.COMMODITY_MST_FK= CCCMT.COMMODITY_MST_PK(+)" );
                    strBuilder.Append(" AND CCHDR.CONT_CUST_SEA_PK=CCTRN.CONT_CUST_SEA_FK " );
                    strBuilder.Append(" AND CCTRN.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append(" AND CCHDR.STATUS=2 " );
                    strBuilder.Append(" AND CCHDR.CARGO_TYPE=2 " + arrCCondition[2] );
                    strBuilder.Append(" AND CCHDR.CUSTOMER_MST_FK= " + intCustomerPK );
                    strBuilder.Append(" AND CCHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK );
                    strBuilder.Append(" AND CCTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append(" AND CCTRN.PORT_MST_POD_FK= " + strPOD );
                    if (string.IsNullOrEmpty(custcontRefNr))
                    {
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN CCHDR.VALID_FROM " );
                        strBuilder.Append(" AND NVL(CCHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                    }
                    else
                    {
                        strBuilder.Append(" and CCHDR.CONT_REF_NO = '" + custcontRefNr + "' ");
                    }

                    strBuilder.Append(" GROUP BY CCHDR.CONT_CUST_SEA_PK, CCHDR.CONT_REF_NO,CCOMT.OPERATOR_ID, " );
                    strBuilder.Append(" CCCMT.COMMODITY_ID, CCCMT.COMMODITY_MST_PK, " );
                    strBuilder.Append(" CCOMT.OPERATOR_MST_PK,CCUOM.DIMENTION_ID, CCUOM.DIMENTION_UNIT_MST_PK, " );
                    strBuilder.Append(" CCHDR.OPERATOR_MST_FK, CCTRN.LCL_BASIS, CCTRN.PORT_MST_POL_FK, CCTRN.PORT_MST_POD_FK");
                }
                else
                {
                    strOperatorRate.Append(" ( Select Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0) )                     " );
                    strOperatorRate.Append("   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx," );
                    strOperatorRate.Append("   CONT_TRN_SEA_FCL_RATES cx                                          " );
                    strOperatorRate.Append("   where mx.ACTIVE                     = 1    AND vx.EXCH_RATE_TYPE_FK = 1   AND                         " );
                    strOperatorRate.Append("   mx.CONT_APPROVED              = 1     AND                         " );
                    strOperatorRate.Append("   tx.CONT_TRN_SEA_PK = cx.CONT_TRN_SEA_FK AND                       " );
                    //Snigdharani
                    strOperatorRate.Append("   mx.CARGO_TYPE                 = 1     AND                         " );
                    strOperatorRate.Append("   mx.OPERATOR_MST_FK            = CCHDR.OPERATOR_MST_FK AND         " );
                    strOperatorRate.Append("   mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " );
                    strOperatorRate.Append("   TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strOperatorRate.Append("   tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND " );
                    strOperatorRate.Append("   tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " );
                    strOperatorRate.Append("   cx.CONTAINER_TYPE_MST_FK      = CCTRN.CONTAINER_TYPE_MST_FK   AND " );
                    strOperatorRate.Append("   tx.PORT_MST_POL_FK            = CCTRN.PORT_MST_POL_FK         AND " );
                    strOperatorRate.Append("   tx.PORT_MST_POD_FK            = CCTRN.PORT_MST_POD_FK         AND " );
                    strOperatorRate.Append("   tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " );
                    strOperatorRate.Append("   tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            " );
                    strOperatorRate.Append("   sysdate between vx.FROM_DATE and vx.TO_DATE )                     ");

                    strBuilder.Append("SELECT CCHDR.CONT_CUST_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append("'3'AS TRNTYPESTATUS, " );
                    strBuilder.Append("'Cust Cont' AS CONTRACTTYPE, " );
                    strBuilder.Append("CCHDR.CONT_REF_NO AS REFNO, " );
                    strBuilder.Append("CCOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("'' AS BOXES, " );
                    strBuilder.Append("CCCMT.COMMODITY_ID AS COMMODITY, " );
                    strBuilder.Append("NVL(" + strOperatorRate.ToString() + ", NULL) AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                    strBuilder.Append("CCCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                    strBuilder.Append("CCOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("NULL AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("CONT_CUST_SEA_TBL CCHDR, " );
                    strBuilder.Append("CONT_CUST_TRN_SEA_TBL CCTRN, " );
                    strBuilder.Append("OPERATOR_MST_TBL CCOMT, " );
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL CCCTMT, " );
                    strBuilder.Append("COMMODITY_MST_TBL CCCMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND CCHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK(+) " );
                    strBuilder.Append("AND CCHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) " );
                    strBuilder.Append("AND CCHDR.CONT_CUST_SEA_PK=CCTRN.CONT_CUST_SEA_FK " );
                    strBuilder.Append("AND CCCTMT.CONTAINER_TYPE_MST_PK = CCTRN.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append("AND CCHDR.STATUS=2 " );
                    strBuilder.Append("AND CCHDR.CARGO_TYPE=1 " + arrCCondition[2] );
                    strBuilder.Append("AND CCHDR.CUSTOMER_MST_FK= " + intCustomerPK );
                    strBuilder.Append("AND CCHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK );
                    strBuilder.Append("AND CCTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND CCTRN.PORT_MST_POD_FK= " + strPOD );

                    if (string.IsNullOrEmpty(custcontRefNr))
                    {
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN CCHDR.VALID_FROM " );
                        strBuilder.Append("AND NVL(CCHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                    }
                    else
                    {
                        strBuilder.Append(" and CCHDR.CONT_REF_NO = '" + custcontRefNr + "' ");
                    }
                    strBuilder.Append("GROUP BY CCHDR.CONT_CUST_SEA_PK, CCHDR.CONT_REF_NO,CCOMT.OPERATOR_ID, " );
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_ID, CCCMT.COMMODITY_ID, " );
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_PK, " );
                    strBuilder.Append("CCCMT.COMMODITY_MST_PK , CCOMT.OPERATOR_MST_PK, " );
                    strBuilder.Append("CCHDR.OPERATOR_MST_FK, CCTRN.CONTAINER_TYPE_MST_FK, " );
                    strBuilder.Append("CCTRN.PORT_MST_POL_FK, CCTRN.PORT_MST_POD_FK ");
                }
                return strBuilder.ToString();
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

        public object funSRRHeader(ArrayList arrCCondition, Int32 intCustomerPK, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                string strOperatorRate = null;
                System.Text.StringBuilder strSRRSBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strOperatorRate = " ( Select Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0) )                     " +  "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx" +  "   where mx.ACTIVE                     = 1     AND                         " +  "   mx.CONT_APPROVED              = 1   AND vx.EXCH_RATE_TYPE_FK = 1    AND                         " +  "   mx.CARGO_TYPE                 = 2     AND                         " +  "   mx.OPERATOR_MST_FK            = SRRHDR.OPERATOR_MST_FK AND          " +  "   mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " +  "   TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " +  "   tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)         AND " +  "   tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK            AND " +  "   tx.LCL_BASIS                  = SRRTRN.LCL_BASIS               AND " +  "   tx.PORT_MST_POL_FK            = SRRTRN.PORT_MST_POL_FK         AND " +  "   tx.PORT_MST_POD_FK            = SRRTRN.PORT_MST_POD_FK         AND " +  "   tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "   tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND             " +  "   sysdate between vx.FROM_DATE and vx.TO_DATE )                     ";

                    strSRRSBuilder.Append("SELECT SRRTRN.SRR_TRN_SEA_PK AS TRNTYPEPK, ");
                    strSRRSBuilder.Append("'5' AS TRNTYPESTATUS, ");
                    strSRRSBuilder.Append("'SRR' AS CONTRACTTYPE, ");
                    strSRRSBuilder.Append("SRRHDR.SRR_REF_NO AS REFNO, ");
                    strSRRSBuilder.Append("CCOMT.OPERATOR_ID AS OPERATOR, ");
                    strSRRSBuilder.Append("SRRUOM.DIMENTION_ID AS BASIS, ");
                    strSRRSBuilder.Append("'' AS QTY, ");
                    strSRRSBuilder.Append("CCCMT.COMMODITY_ID AS COMMODITY, ");
                    strSRRSBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, ");
                    strSRRSBuilder.Append("'' AS BKGRATE, ");
                    strSRRSBuilder.Append("'' AS NET, ");
                    strSRRSBuilder.Append("'' AS TOTALRATE, ");
                    strSRRSBuilder.Append("'0' AS SEL, ");
                    strSRRSBuilder.Append("'' AS CONTAINERPK, ");
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
                    strSRRSBuilder.Append("AND SRRHDR.CUSTOMER_MST_FK= " + intCustomerPK + " ");
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
                    strOperatorRate = " ( Select Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0) )                     " +  "   from  CONT_MAIN_SEA_TBL mx, CONT_TRN_SEA_FCL_LCL tx, V_EXCHANGE_RATE vx,";
                    // & vbCrLf & _
                    strOperatorRate = strOperatorRate + "CONT_TRN_SEA_FCL_RATES cx                                " +  "   where mx.ACTIVE                     = 1    AND vx.EXCH_RATE_TYPE_FK = 1   AND                         " +  "         mx.CONT_APPROVED              = 1     AND                         " +  "         tx.CONT_TRN_SEA_PK = cx.CONT_TRN_SEA_FK AND                       " +  "         mx.CARGO_TYPE                 = 1     AND                         " +  "         mx.OPERATOR_MST_FK            = SRRHDR.OPERATOR_MST_FK AND         " +  "         mx.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " +  "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " +  "           tx.VALID_FROM   AND   NVL(tx.VALID_TO,NULL_DATE_FORMAT)        AND " +  "         tx.CONT_MAIN_SEA_FK           = mx.CONT_MAIN_SEA_PK           AND " +  "         cx.CONTAINER_TYPE_MST_FK      = SRRTRN.CONTAINER_TYPE_MST_FK   AND " +  "         tx.PORT_MST_POL_FK            = SRRTRN.PORT_MST_POL_FK         AND " +  "         tx.PORT_MST_POD_FK            = SRRTRN.PORT_MST_POD_FK         AND " +  "         tx.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "         tx.CURRENCY_MST_FK            = vx.CURRENCY_MST_FK AND            " +  "         sysdate between vx.FROM_DATE and vx.TO_DATE )                     ";

                    strSRRSBuilder.Append("SELECT SRRTRN.SRR_TRN_SEA_PK AS TRNTYPEPK, ");
                    strSRRSBuilder.Append("'5' AS TRNTYPESTATUS, ");
                    strSRRSBuilder.Append("'SRR' AS CONTRACTTYPE, ");
                    strSRRSBuilder.Append("SRRHDR.SRR_REF_NO AS REFNO, ");
                    strSRRSBuilder.Append("CCOMT.OPERATOR_ID AS OPERATOR, ");
                    strSRRSBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_ID AS TYPE, ");
                    strSRRSBuilder.Append("'' AS BOXES, ");
                    strSRRSBuilder.Append("CCCMT.COMMODITY_ID AS COMMODITY, ");
                    strSRRSBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, ");
                    strSRRSBuilder.Append("'' AS BKGRATE, ");
                    strSRRSBuilder.Append("'' AS NET, ");
                    strSRRSBuilder.Append("'' AS TOTALRATE, ");
                    strSRRSBuilder.Append("'0' AS SEL, ");
                    strSRRSBuilder.Append("SRRTRN.CONTAINER_TYPE_MST_FK AS CONTAINERPK, ");
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
                    strSRRSBuilder.Append("AND SRRHDR.CUSTOMER_MST_FK= " + intCustomerPK + " ");
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public object funSLTariffHeader(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                string strOperatorRate = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strOperatorRate = " ( Select Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0) )                     " +  "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v  " +  "   where m.ACTIVE                     = 1  AND v.EXCH_RATE_TYPE_FK = 1     AND                         " +  "         m.CONT_APPROVED              = 1     AND                         " +  "         m.CARGO_TYPE                 = 2     AND                         " +  "         m.OPERATOR_MST_FK            = OHDR.OPERATOR_MST_FK AND          " +  "         m.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " +  "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " +  "         t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " +  "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " +  "         t.LCL_BASIS                  = OTRN.LCL_BASIS               AND " +  "         t.PORT_MST_POL_FK            = OTRN.PORT_MST_POL_FK          AND " +  "         t.PORT_MST_POD_FK            = OTRN.PORT_MST_POD_FK         AND " +  "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " +  "         sysdate between v.FROM_DATE and v.TO_DATE )                     ";

                    strBuilder.Append(" SELECT TRNTYPEPK,TRNTYPESTATUS, CONTRACTTYPE, REFNO, OPERATOR, COMMODITY,  PACK_PK, PACK_TYPE, " );
                    strBuilder.Append(" BASIS, QTY,  CARGO_WT, CARGO_VOL, CARGO_CALC, RATE, BKGRATE, NET, TOTALRATE, SEL, " );
                    strBuilder.Append("  ROWNUM CONTAINERPK, COMMODITYPK, OPERATORPK, TRANSACTIONPK,BASISPK FROM ( " );
                    //CONTAINERPK HAS TAKEN AS SLNO FOR CARGO CALCULATOR
                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append("'4' AS TRNTYPESTATUS, " );
                    strBuilder.Append("'SL Tariff' AS CONTRACTTYPE, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append(" CMT.COMMODITY_NAME AS COMMODITY, " );
                    strBuilder.Append("0 PACK_PK,'' PACK_TYPE, " );
                    ///''''''BB
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append("'' AS QTY, " );
                    strBuilder.Append(" null CARGO_WT,null CARGO_VOL,'' CARGO_CALC, " );
                    strBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("'' AS CONTAINERPK, " );
                    strBuilder.Append(" CMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                    strBuilder.Append("OOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("OUOM.DIMENTION_UNIT_MST_PK AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, " );
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM,COMMODITY_MST_TBL CMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK(+) " );
                    strBuilder.Append("AND CMT.COMMODITY_MST_PK=OTRN.COMMODITY_MST_FK " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=1 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[3] );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) " );
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, OOMT.OPERATOR_ID, " );
                    strBuilder.Append("OOMT.OPERATOR_MST_PK, OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK, " );
                    strBuilder.Append("OHDR.OPERATOR_MST_FK ,OTRN.LCL_BASIS, OTRN.PORT_MST_POL_FK, OTRN.PORT_MST_POD_FK,CMT.COMMODITY_MST_PK ,CMT.COMMODITY_NAME)");

                }
                else
                {
                    strOperatorRate = " ( Select Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0) )                     " +  "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v,  ";
                    // & vbCrLf & _
                    strOperatorRate = strOperatorRate + "CONT_TRN_SEA_FCL_RATES c                                 " +  "   where m.ACTIVE                     = 1   AND v.EXCH_RATE_TYPE_FK = 1    AND                         " +  "         m.CONT_APPROVED              = 1     AND                         " +  "         m.CARGO_TYPE                 = 1     AND                         " +  "         t.CONT_TRN_SEA_PK = c.CONT_TRN_SEA_FK AND                        " +  "         m.OPERATOR_MST_FK            = OHDR.OPERATOR_MST_FK AND         " +  "         m.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " +  "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " +  "           t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " +  "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " +  "         c.CONTAINER_TYPE_MST_FK      = OTRNCONT.CONTAINER_TYPE_MST_FK   AND " +  "         t.PORT_MST_POL_FK            = OTRN.PORT_MST_POL_FK         AND " +  "         t.PORT_MST_POD_FK            = OTRN.PORT_MST_POD_FK         AND " +  "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " +  "         V.CURRENCY_MST_BASE_FK       = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " AND " +  "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " +  "         sysdate between v.FROM_DATE and v.TO_DATE )                     ";

                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append("'4' AS TRNTYPESTATUS, " );
                    strBuilder.Append("'SL Tariff' AS CONTRACTTYPE, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("'' AS BOXES, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                    strBuilder.Append("NULL AS COMMODITYPK, " );
                    strBuilder.Append("OOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("NULL AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, " );
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, " );
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK(+) " );
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=1 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 " + arrCCondition[3] );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) " );
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, OOMT.OPERATOR_ID, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK, " );
                    strBuilder.Append("OOMT.OPERATOR_MST_PK, " );
                    strBuilder.Append("OTRNCONT.CONTAINER_TYPE_MST_FK, " );
                    strBuilder.Append("OTRN.PORT_MST_POL_FK, " );
                    strBuilder.Append("OTRN.PORT_MST_POD_FK, " );
                    strBuilder.Append("OHDR.OPERATOR_MST_FK");
                }
                return strBuilder.ToString();
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

        public object funGTariffHeader(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append("'6' AS TRNTYPESTATUS, " );
                    strBuilder.Append("'Gen Tariff' AS CONTRACTTYPE, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("'General' AS OPERATOR, " );
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append("'' AS QTY, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("NULL AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("'' AS CONTAINERPK, " );
                    strBuilder.Append("NULL AS COMMODITYPK, " );
                    strBuilder.Append("NULL AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("OUOM.DIMENTION_UNIT_MST_PK AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[3] );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) " );
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, " );
                    strBuilder.Append("OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK, " );
                    strBuilder.Append("OTRN.LCL_BASIS, OTRN.PORT_MST_POL_FK, OTRN.PORT_MST_POD_FK");
                }
                else
                {
                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append("'6' AS TRNTYPESTATUS, " );
                    strBuilder.Append("'Gen Tariff' AS CONTRACTTYPE, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("'General' AS OPERATOR, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("'' AS BOXES, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("NULL AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                    strBuilder.Append("NULL AS COMMODITYPK, " );
                    strBuilder.Append("NULL AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("NULL AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, " );
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK " );
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 " + arrCCondition[3] );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE(' " + strSDate + " ','" + dateFormat + "')) " );
                    strBuilder.Append("GROUP BY OHDR.TARIFF_MAIN_SEA_PK, OHDR.TARIFF_REF_NO, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK, " );
                    strBuilder.Append("OTRNCONT.CONTAINER_TYPE_MST_FK, " );
                    strBuilder.Append("OTRN.PORT_MST_POL_FK, " );
                    strBuilder.Append("OTRN.PORT_MST_POD_FK ");
                }
                return strBuilder.ToString();
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

        #endregion

        #region "Fetch Freight"
        public DataTable FetchFreight(Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, string strPOL = "", string strPOD = "", Int16 intCommodityPk = 0, string strSDate = "", string strContainer = "", short intSRateStatus = 0, short intCContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, Int32 intSpotRatePK = 0, string CustContRefNr = "")
        {
            string strSql = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            ArrayList arrCCondition = new ArrayList();
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
                strCustomer = " AND (SRRSRST.CUSTOMER_MST_FK= " + intCustomerPK + " OR SRRSRST.CUSTOMER_MST_FK IS NULL)";

            }

            if (!(intQuotationPK == 0))
            {
                strSql = FunMakeQuotationFreight(intQuotationPK, strPOL, strPOD, intIsFcl);

            }
            else
            {
                if (!(intSpotRatePK == 0))
                {
                    strSql = funSpotRateFreight(arrCCondition, strCustomer, Convert.ToString(intCommodityPk), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 1).ToString();
                }
                if (intSRateStatus == 1 & intSpotRatePK == 0)
                {
                    strSql = funSpotRateFreight(arrCCondition, strCustomer, Convert.ToString(intCommodityPk), strPOL, strPOD, Convert.ToString(intSpotRatePK), strSDate, intIsFcl, 2).ToString();
                }
                if (intCContractStatus == 1)
                {
                    strSql = funCustContFreight(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl, Convert.ToString(getDefault(CustContRefNr, ""))).ToString();
                }
                if (intSRRContractStatus == 1)
                {
                    strSql = funSRRFreight(arrCCondition, intCustomerPK, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl).ToString();
                }
                if (intOTariffStatus == 1)
                {
                    strSql = funSLTariffFreight(arrCCondition, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl).ToString();
                }
                if (intGTariffStatus == 1)
                {
                    strSql = funGTariffFreight(arrCCondition, Convert.ToString(intCommodityPk), strPOL, strPOD, strSDate, intIsFcl).ToString();
                }

            }
            try
            {
                dtMain = objWF.GetDataTable(strSql);
                return dtMain;
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

        public string GetTempOrNewCus(string CustomerPK)
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string GetTempOrNewCus1(string CustomerPK)
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string FunMakeQuotationFreight(Int32 intQuotationPK = 0, string strPOL = "", string strPOD = "", Int16 intIsFcl = 0)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("SELECT Q.TRNTYPEFK,");
                sb.Append("       Q.REFNO,");
                sb.Append("       '' BASIS,");
                sb.Append("       Q.COMMODITY,");
                sb.Append("       Q.POLPK PORT_MST_PK,");
                sb.Append("       Q.POL,");
                sb.Append("       Q.PODPK PORT_MST_PK,");
                sb.Append("       Q.POD,");
                sb.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                sb.Append("       Q.CHECK_FOR_ALL_IN_RT SEL,");
                sb.Append("       Q.CURRENCY_MST_PK,");
                sb.Append("       Q.CURRENCY_ID,");
                sb.Append("       CASE WHEN Q.CHARGE_BASIS = '%' THEN  Q.SURCHARGE_VALUE ELSE Q.BKGRATE END SURCHARGE_VALUE,");
                //'surcharge
                sb.Append("       (ABS(CASE WHEN Q.CHARGE_BASIS = '%' THEN Q.EXCHANGERATE ELSE Q.BKGRATE END))*(DECODE(Q.CREDIT, NULL, 1, 0, -1, 1, 1)) MIN_RATE,");
                sb.Append("       Q.RATE,");

                sb.Append("       ROUND((Q.BKGRATE1),2) BKGRATE,");
                sb.Append("       Q.BASISPK,");
                sb.Append("       Q.PYMT_TYPE,");
                sb.Append("       COMMODITY_MST_PK,");
                sb.Append("       NULL FREIGHTPK,Q.EXCHANGERATE,Q.SURCHARGE_TYPE,Q.CREDIT");
                sb.Append("  FROM (SELECT QTRN.QUOTE_TRN_SEA_PK AS TRNTYPEFK,");
                sb.Append("               QHDR.QUOTATION_REF_NO AS REFNO,");
                sb.Append("               QCMT.COMMODITY_ID AS COMMODITY,");
                sb.Append("               QPL.PORT_MST_PK POLPK,");
                sb.Append("               QPL.PORT_ID AS POL,");
                sb.Append("               QPD.PORT_MST_PK PODPK,");
                sb.Append("               QPD.PORT_ID AS POD,");
                sb.Append("               QFEMT.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("               QFEMT.FREIGHT_ELEMENT_ID,");
                sb.Append("               DECODE(QFEMT.CHARGE_BASIS,'0','','1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                sb.Append("               DECODE(QTRNCHRG.CHECK_FOR_ALL_IN_RT, 1, '1', '0') AS CHECK_FOR_ALL_IN_RT,");
                sb.Append("               QCUMT.CURRENCY_MST_PK,");
                sb.Append("               QCUMT.CURRENCY_ID,");
                sb.Append("               QTRNCHRG.ROE AS RATE,");
                sb.Append("            case WHEN  QFEMT.Charge_Basis=1 THEN  NVL(QTRNCHRG.RATE, 0) else    CASE WHEN NVL(QTRNCHRG.QUOTED_RATE,0) > NVL(QTRNCHRG.QUOTED_MIN_RATE,0) THEN ");
                sb.Append("                QTRNCHRG.QUOTED_RATE ELSE QTRNCHRG.QUOTED_MIN_RATE END end  BKGRATE,");
                sb.Append("                NVL(QTRNCHRG.Rate, 0)BKGRATE1,");

                sb.Append("               QTRN.BASIS AS BASISPK,");
                sb.Append("               DECODE(QTRNCHRG.PYMT_TYPE, 1, '1', '2') AS PYMT_TYPE,");
                sb.Append("               QCMT.COMMODITY_MST_PK,  QTRNCHRG.QUOTED_RATE EXCHANGERATE,");
                sb.Append("                 QFEMT.CREDIT,'' SURCHARGE_TYPE,TO_NUMBER(SUBSTR(QTRNCHRG.SURCHARGE,0,(INSTR(QTRNCHRG.SURCHARGE, '%', 1, 1) - 1))) SURCHARGE_VALUE ");
                //'surcharge

                sb.Append("          FROM QUOTATION_SEA_TBL          QHDR,");
                sb.Append("               QUOTATION_TRN_SEA_FCL_LCL  QTRN,");
                sb.Append("               QUOTATION_TRN_SEA_FRT_DTLS QTRNCHRG,");
                sb.Append("               OPERATOR_MST_TBL           QOMT,");
                sb.Append("               COMMODITY_MST_TBL          QCMT,");
                sb.Append("               PORT_MST_TBL               QPL,");
                sb.Append("               PORT_MST_TBL               QPD,");
                sb.Append("               FREIGHT_ELEMENT_MST_TBL    QFEMT,");
                sb.Append("               CURRENCY_TYPE_MST_TBL      QCUMT");
                sb.Append("         WHERE (1 = 1)");
                sb.Append("           AND QHDR.QUOTATION_SEA_PK = QTRN.QUOTATION_SEA_FK");
                sb.Append("           AND QTRN.QUOTE_TRN_SEA_PK = QTRNCHRG.QUOTE_TRN_SEA_FK");
                sb.Append("           AND QTRN.PORT_MST_POL_FK = QPL.PORT_MST_PK");
                sb.Append("           AND QTRN.PORT_MST_POD_FK = QPD.PORT_MST_PK");
                sb.Append("           AND QTRNCHRG.FREIGHT_ELEMENT_MST_FK =");
                sb.Append("               QFEMT.FREIGHT_ELEMENT_MST_PK");
                sb.Append("           AND QTRNCHRG.CURRENCY_MST_FK = QCUMT.CURRENCY_MST_PK");
                sb.Append("           AND QTRN.OPERATOR_MST_FK = QOMT.OPERATOR_MST_PK(+)");
                sb.Append("           AND QTRN.COMMODITY_MST_FK = QCMT.COMMODITY_MST_PK(+)");
                sb.Append("AND QHDR.QUOTATION_SEA_PK= " + intQuotationPK );
                sb.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                sb.Append(" WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK");
                sb.Append(" ORDER BY FRT.PREFERENCE ASC");
                return sb.ToString();
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

        public object funSpotRateFreight(ArrayList arrCCondition, string strCustomer, string intCommodityPK, string strPOL, string strPOD, string intSpotRatePK, string strSDate, Int16 intIsFcl, Int16 intFlag)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    if (intFlag == 1)
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, " );
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append("SRUOM.DIMENTION_ID AS BASIS, " );
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, " );
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, " );
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, " );
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_MIN_RATE AS MIN_RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS BKGRATE, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE " );
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, " );
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, " );
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append("PORT_MST_TBL SRPL, " );
                        strBuilder.Append("PORT_MST_TBL SRPD, " );
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, " );
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT, " );
                        strBuilder.Append("DIMENTION_UNIT_MST_TBL SRUOM " );
                        strBuilder.Append("WHERE (1=1) " );
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK " );
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) " );
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " );
                        strBuilder.Append("AND SRRSTSFL.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK " );
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 " + strCustomer );
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1] );
                        strBuilder.Append("AND SRRSRST.APPROVED=1 " );
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK );
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK );
                        strBuilder.Append("and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ASC");
                    }
                    else
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, " );
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append("SRUOM.DIMENTION_ID AS BASIS, " );
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, " );
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, " );
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, " );
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRSTSFL.LCL_CURRENT_MIN_RATE AS MIN_RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS BKGRATE, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE " );
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, " );
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, " );
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append("PORT_MST_TBL SRPL, " );
                        strBuilder.Append("PORT_MST_TBL SRPD, " );
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, " );
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT, " );
                        strBuilder.Append("DIMENTION_UNIT_MST_TBL SRUOM " );
                        strBuilder.Append("WHERE (1=1) " );
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK " );
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK(+) " );
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " );
                        strBuilder.Append("AND SRRSTSFL.LCL_BASIS=SRUOM.DIMENTION_UNIT_MST_PK " );
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 " + strCustomer );
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=2 " + arrCCondition[1] );
                        strBuilder.Append("AND SRRSRST.APPROVED=1 " );
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK );
                        strBuilder.Append("and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ASC");
                    }
                }
                else
                {

                    if (intFlag == 1)
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, " );
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append("SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, " );
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, " );
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, " );
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS RATE, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS BKGRATE, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE " );
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, " );
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_CONT_DET SRRST, " );
                        strBuilder.Append("CONTAINER_TYPE_MST_TBL SRCTMT, " );
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, " );
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append("PORT_MST_TBL SRPL, " );
                        strBuilder.Append("PORT_MST_TBL SRPD, " );
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, " );
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT " );
                        strBuilder.Append("WHERE (1=1) " );
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append("AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSFL.RFQ_SPOT_SEA_TRN_PK " );
                        //Snigdharani
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK " );
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK (+)" );
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " + arrCCondition[1] );
                        strBuilder.Append("AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_type_MST_FK " );
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 " );
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=1 " + strCustomer );
                        strBuilder.Append("AND SRRSRST.APPROVED=1" );
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK= " + intSpotRatePK );
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK );
                        strBuilder.Append("and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "'))" );
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ASC");
                    }
                    else
                    {
                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, " );
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append("SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, " );
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, " );
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, " );
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS RATE, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS BKGRATE, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE " );
                        strBuilder.Append("FROM RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSFL, " );
                        strBuilder.Append("OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append("RFQ_SPOT_TRN_SEA_CONT_DET SRRST, " );
                        strBuilder.Append("CONTAINER_TYPE_MST_TBL SRCTMT, " );
                        strBuilder.Append("COMMODITY_MST_TBL SRCMT, " );
                        strBuilder.Append("COMMODITY_GROUP_MST_TBL  SRCOMM, " );
                        strBuilder.Append("PORT_MST_TBL SRPL, " );
                        strBuilder.Append("PORT_MST_TBL SRPD, " );
                        strBuilder.Append("FREIGHT_ELEMENT_MST_TBL SRFEMT, " );
                        strBuilder.Append("CURRENCY_TYPE_MST_TBL SRCUMT " );
                        strBuilder.Append("WHERE (1=1) " );
                        strBuilder.Append("AND SRRSRST.RFQ_SPOT_SEA_PK=SRRSTSFL.RFQ_SPOT_SEA_FK " );
                        strBuilder.Append("AND SRRST.RFQ_SPOT_SEA_TRN_FK=SRRSTSFL.RFQ_SPOT_SEA_TRN_PK " );
                        //Snigdharani
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK=SRPL.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK=SRPD.PORT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.FREIGHT_ELEMENT_MST_FK=SRFEMT.FREIGHT_ELEMENT_MST_PK " );
                        strBuilder.Append("AND SRRSTSFL.CURRENCY_MST_FK=SRCUMT.CURRENCY_MST_PK " );
                        strBuilder.Append("AND SRRSRST.OPERATOR_MST_FK=SROMT.OPERATOR_MST_PK (+)" );
                        strBuilder.Append("AND SRRSRST.COMMODITY_MST_FK=SRCMT.COMMODITY_MST_PK(+) " + arrCCondition[1] );
                        strBuilder.Append("AND SRCTMT.CONTAINER_TYPE_MST_PK = SRRST.CONTAINER_type_MST_FK " );
                        strBuilder.Append("AND SRRSRST.ACTIVE=1 " );
                        strBuilder.Append("AND SRRSRST.CARGO_TYPE=1 " );
                        strBuilder.Append("AND SRRSRST.APPROVED=1" + strCustomer );
                        strBuilder.Append("AND SRCOMM.COMMODITY_GROUP_PK= " + intCommodityPK );
                        strBuilder.Append("and srcomm.commodity_group_pk = srrsrst.commodity_group_fk " );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POL_FK= " + strPOL );
                        strBuilder.Append("AND SRRSTSFL.PORT_MST_POD_FK= " + strPOD );
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN SRRSRST.VALID_FROM " );
                        strBuilder.Append("AND NVL(SRRSRST.VALID_TO, TO_DATE('" + strSDate + "','" + dateFormat + "'))" );
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ASC");
                    }
                }
                return strBuilder.ToString();
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

        public object funCustContFreight(ArrayList arrCCondition, Int32 intCustomerPK, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl, string CustContRefNr = "")
        {
            try
            {
                System.Text.StringBuilder strContRefNo = new System.Text.StringBuilder();
                System.Text.StringBuilder strFreightElements = new System.Text.StringBuilder();
                System.Text.StringBuilder strSurcharge = new System.Text.StringBuilder();
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strContRefNo.Append(" (   Select    DISTINCT  CONT_REF_NO " );
                    strContRefNo.Append("from  " );
                    strContRefNo.Append("CONT_CUST_SEA_TBL main7, " );
                    strContRefNo.Append("CONT_CUST_TRN_SEA_TBL tran7 " );
                    strContRefNo.Append("where " );
                    strContRefNo.Append("main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK " );
                    strContRefNo.Append("AND    main7.CARGO_TYPE            = 2 " );
                    strContRefNo.Append("AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "  " );
                    strContRefNo.Append("AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + " " );
                    strContRefNo.Append("AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK " );
                    strContRefNo.Append("AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK " );
                    strContRefNo.Append("AND    tran7.LCL_BASIS             =  tran6.LCL_BASIS  " );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strContRefNo.Append(" AND main7.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strContRefNo.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN " );
                        strContRefNo.Append("tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT) " );
                    }
                    strContRefNo.Append("AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  ) ");
                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " );
                    strFreightElements.Append(" from                                                             " );
                    strFreightElements.Append(" CONT_CUST_SEA_TBL              main8,                           " );
                    strFreightElements.Append(" CONT_CUST_TRN_SEA_TBL          tran8,                           " );
                    strFreightElements.Append(" CONT_SUR_CHRG_SEA_TBL          frtd8                            " );
                    strFreightElements.Append(" where                                                                " );
                    strFreightElements.Append(" main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " );
                    strFreightElements.Append(" AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " );
                    strFreightElements.Append(" AND    main8.CARGO_TYPE            = 2                                   " );
                    strFreightElements.Append(" AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strFreightElements.Append(" AND    main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strFreightElements.Append(" AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " );
                    strFreightElements.Append(" AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " );
                    strFreightElements.Append(" AND    tran8.LCL_BASIS             = tran6.LCL_BASIS                     " );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strFreightElements.Append(" AND main8.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strFreightElements.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                        strFreightElements.Append(" tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " );
                    }

                    strFreightElements.Append(" AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " );
                    strSurcharge.Append("    from                                                             " );
                    strSurcharge.Append("     CONT_CUST_SEA_TBL              main9,                           " );
                    strSurcharge.Append("     CONT_CUST_TRN_SEA_TBL          tran9                            " );
                    strSurcharge.Append("    where                                                                " );
                    strSurcharge.Append("            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " );
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 2                                   " );
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strSurcharge.Append("     AND    main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " );
                    strSurcharge.Append("     AND    tran9.LCL_BASIS             =  tran6.LCL_BASIS                    " );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strSurcharge.Append(" AND main9.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                        strSurcharge.Append("            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " );
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
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.MIN_RATE,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,COMMODITY_MST_PK AS COMMODITYPK ");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("    Select     " );
                    strBuilder.Append(" tran2.CONT_CUST_SEA_FK    TRNTYPEFK,  " );
                    strBuilder.Append(" main2.CONT_REF_NO     REFNO,   " );
                    strBuilder.Append(" CCUOM.DIMENTION_ID BASIS,   " );
                    strBuilder.Append(" CCCMT.COMMODITY_ID COMMODITY, " );
                    strBuilder.Append(" CCPL.PORT_MST_PK POLPK, " );
                    strBuilder.Append(" CCPL.PORT_ID POL, " );
                    strBuilder.Append(" CCPD.PORT_MST_PK PODPK, " );
                    strBuilder.Append(" CCPD.PORT_ID POD, " );
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_MST_PK, " );
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_ID, " );

                    strBuilder.Append(" 'true' SEL,  " );
                    strBuilder.Append(" curr2.CURRENCY_MST_PK, " );
                    strBuilder.Append(" curr2.CURRENCY_ID, " );
                    strBuilder.Append("  NULL  MIN_RATE, " );
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN" );
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE " );
                    strBuilder.Append(" Else " );
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE " );
                    strBuilder.Append(" END) AS RATE, " );
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN" );
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE " );
                    strBuilder.Append(" Else " );
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE " );
                    strBuilder.Append(" END) AS BKGRATE, " );
                    strBuilder.Append(" tran2.LCL_BASIS BASISPK, " );
                    strBuilder.Append(" '1' AS PYMT_TYPE ,CCCMT.COMMODITY_MST_PK  " );

                    strBuilder.Append(" from " );
                    strBuilder.Append(" CONT_CUST_SEA_TBL main2, " );
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL tran2, " );
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL frt2, " );
                    strBuilder.Append(" OPERATOR_MST_TBL CCOMT, " );
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL CCUOM, " );
                    strBuilder.Append(" COMMODITY_MST_TBL CCCMT, " );
                    strBuilder.Append(" PORT_MST_TBL CCPL, " );
                    strBuilder.Append(" PORT_MST_TBL CCPD, " );
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL curr2 " );
                    strBuilder.Append(" where " );
                    strBuilder.Append(" main2.CONT_CUST_SEA_PK = tran2.CONT_CUST_SEA_FK " );
                    strBuilder.Append(" AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF' " );
                    strBuilder.Append(" AND TRAN2.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK   " );
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK " );
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK " );
                    strBuilder.Append(" AND main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strBuilder.Append(" AND TRAN2.COMMODITY_MST_FK = CCCMT.COMMODITY_MST_PK(+) " );
                    strBuilder.Append(" AND TRAN2.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append(" AND main2.CARGO_TYPE  = 2       " );
                    strBuilder.Append(" AND main2.STATUS   = 2       " );
                    strBuilder.Append(" AND main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK );
                    strBuilder.Append(" AND main2.CUSTOMER_MST_FK  = " + intCustomerPK );
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK = " + strPOL + " " + arrCCondition[3] );
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK = " + strPOD );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN   " );
                        strBuilder.Append(" tran2.VALID_FROM AND NVL(tran2.VALID_TO,NULL_DATE_FORMAT)  " );
                    }

                    strBuilder.Append(" UNION " );
                    strBuilder.Append(" Select " );
                    strBuilder.Append("     tran2.CONT_CUST_SEA_FK                    TRNTYPEFK,            " );
                    strBuilder.Append("     main2.CONT_REF_NO                          REFNO,               " );
                    strBuilder.Append("     CCUOM.DIMENTION_ID                         BASIS,                " );
                    strBuilder.Append("     CCCMT.COMMODITY_ID                         COMMODITY,           " );
                    strBuilder.Append("     CCPL.PORT_MST_PK POLPK                            ,              " );
                    strBuilder.Append("     CCPL.PORT_ID                               POL,                 " );
                    strBuilder.Append("     CCPD.PORT_MST_PK PODPK              ,              " );
                    strBuilder.Append("     CCPD.PORT_ID                               POD,                 " );
                    strBuilder.Append("     frt2.FREIGHT_ELEMENT_MST_PK,              " );
                    strBuilder.Append("     frt2.FREIGHT_ELEMENT_ID                    ,              " );
                    strBuilder.Append("     DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,       " );
                    strBuilder.Append("     curr2.CURRENCY_MST_PK                      ,             " );
                    strBuilder.Append("     curr2.CURRENCY_ID                          ,             " );
                    strBuilder.Append("      NULL  MIN_RATE,           " );
                    strBuilder.Append("     frtd2.APP_SURCHARGE_AMT                    RATE,                " );
                    strBuilder.Append("     frtd2.APP_SURCHARGE_AMT                    BKGRATE,                " );
                    strBuilder.Append("     tran2.LCL_BASIS                            BASISPK,                   " );
                    strBuilder.Append("     '1' AS PYMT_TYPE ,CCCMT.COMMODITY_MST_PK              " );
                    strBuilder.Append("    from                                                             " );
                    strBuilder.Append("     CONT_CUST_SEA_TBL              main2,                           " );
                    strBuilder.Append("     CONT_CUST_TRN_SEA_TBL          tran2,                           " );
                    strBuilder.Append("     CONT_SUR_CHRG_SEA_TBL          frtd2,                           " );
                    strBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt2,                            " );
                    strBuilder.Append("     OPERATOR_MST_TBL               CCOMT,                           " );
                    strBuilder.Append("     DIMENTION_UNIT_MST_TBL         CCUOM,                          " );
                    strBuilder.Append("     COMMODITY_MST_TBL              CCCMT,                           " );
                    strBuilder.Append("     PORT_MST_TBL                   CCPL,                            " );
                    strBuilder.Append("     PORT_MST_TBL                   CCPD,                            " );
                    strBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr2                            " );
                    strBuilder.Append("    where                                                                " );
                    strBuilder.Append("            main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " );
                    strBuilder.Append("     AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK          " );
                    strBuilder.Append("     AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK        " );
                    strBuilder.Append("     AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK               " );
                    strBuilder.Append("     AND    tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK " );
                    strBuilder.Append("     AND    tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK " );
                    strBuilder.Append("     AND    main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strBuilder.Append("     AND    TRAN2.COMMODITY_MST_FK = CCCMT.COMMODITY_MST_PK(+)" );
                    strBuilder.Append("     AND    tran2.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append("     AND    main2.CARGO_TYPE            = 2                                   " );
                    strBuilder.Append("     AND    main2.STATUS                = 2                                   " );
                    strBuilder.Append("     AND    main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strBuilder.Append("     AND    main2.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strBuilder.Append("     AND    tran2.PORT_MST_POL_FK      = " + strPOL );
                    strBuilder.Append("     AND    tran2.PORT_MST_POD_FK      = " + strPOD + arrCCondition[3] );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                        strBuilder.Append("            tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        " );
                    }

                    strBuilder.Append("    UNION  " );
                    strBuilder.Append("   Select            " );
                    strBuilder.Append("     NULL                                       TRNTYPEFK,           " );
                    strBuilder.Append("     " + strContRefNo.ToString() + "                       REFNO,               " );
                    strBuilder.Append("     CCUOM.DIMENTION_ID                         BASIS,               " );
                    strBuilder.Append("     NULL                                       COMMODITY,           " );
                    strBuilder.Append("     COPL.PORT_MST_PK POLPK             ,                         " );
                    strBuilder.Append("     COPL.PORT_ID                               POL,                 " );
                    strBuilder.Append("     COPD.PORT_MST_PK PODPK            ,                         " );
                    strBuilder.Append("     COPD.PORT_ID                               POD,                 " );
                    strBuilder.Append("     frt6.FREIGHT_ELEMENT_MST_PK               ,                     " );
                    strBuilder.Append("     frt6.FREIGHT_ELEMENT_ID                    ,          " );
                    strBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                    strBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        " );
                    strBuilder.Append("     curr6.CURRENCY_ID                          ,        " );
                    strBuilder.Append("  NULL  MIN_RATE, " );
                    strBuilder.Append("     cont6.FCL_REQ_RATE                         RATE,                " );
                    strBuilder.Append("     cont6.FCL_REQ_RATE                         BKGRATE,             " );
                    strBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               " );
                    strBuilder.Append("     '1'  PYMT_TYPE   ,COMM.COMMODITY_MST_PK         " );
                    strBuilder.Append("    from                                                             " );
                    strBuilder.Append("     TARIFF_MAIN_SEA_TBL            main6,                           " );
                    strBuilder.Append("     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " );
                    strBuilder.Append("     TARIFF_TRN_SEA_CONT_DTL        cont6,                           " );
                    strBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt6,                            " );
                    strBuilder.Append("     OPERATOR_MST_TBL               COOMT,                           " );
                    strBuilder.Append("     DIMENTION_UNIT_MST_TBL         CCUOM,                           " );
                    strBuilder.Append("     PORT_MST_TBL                   COPL,                            " );
                    strBuilder.Append("     PORT_MST_TBL                   COPD,                            " );
                    strBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr6  ,COMMODITY_MST_TBL COMM   " );
                    strBuilder.Append("     where " + strContRefNo.ToString() + " IS NOT NULL AND                       " );
                    strBuilder.Append("            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " );
                    strBuilder.Append("     AND    cont6.TARIFF_TRN_SEA_FK = tran6.TARIFF_TRN_SEA_PK           " );
                    strBuilder.Append("     AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           " );
                    strBuilder.Append("     AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           " );
                    strBuilder.Append("     AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK (+)     " );
                    strBuilder.Append("     AND    tran6.LCL_BASIS                    = CCUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append("     AND    main6.CARGO_TYPE            = 2                                  " );
                    strBuilder.Append("     AND    main6.ACTIVE                = 1   AND COMM.COMMODITY_MST_PK=TRAN6.COMMODITY_MST_FK                                " );
                    strBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       " );
                    strBuilder.Append("     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              " );
                    strBuilder.Append("     AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK );
                    strBuilder.Append("     AND    tran6.PORT_MST_POL_FK       = " + strPOL );
                    strBuilder.Append("     AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4] );


                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                    }
                    else
                    {
                        strBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " );
                        strBuilder.Append("            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " );
                    }
                    strBuilder.Append("     AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK " );
                    strBuilder.Append("     FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK " );
                    strBuilder.Append("     WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) " );
                    strBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") " );
                    strBuilder.Append("     AND  " + strSurcharge.ToString() + " = 1 " );
                    strBuilder.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append("     ORDER BY FRT.PREFERENCE ASC");

                }
                else
                {
                    strContRefNo.Append("(   Select    DISTINCT  CONT_REF_NO " );
                    strContRefNo.Append("    from                                                             " );
                    strContRefNo.Append("     CONT_CUST_SEA_TBL              main7,                           " );
                    strContRefNo.Append("     CONT_CUST_TRN_SEA_TBL          tran7                            " );
                    strContRefNo.Append("    where                                                                " );
                    strContRefNo.Append("            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " );
                    strContRefNo.Append("     AND    main7.CARGO_TYPE            = 1                                   " );
                    strContRefNo.Append("     AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strContRefNo.Append("     AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strContRefNo.Append("     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " );
                    strContRefNo.Append("     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strContRefNo.Append(" AND main7.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strContRefNo.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                        strContRefNo.Append("            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " );
                    }
                    strContRefNo.Append(" AND    tran7.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " );
                    strContRefNo.Append(" AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK )    ");

                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " );
                    strFreightElements.Append("    from                                                             " );
                    strFreightElements.Append("     CONT_CUST_SEA_TBL              main8,                           " );
                    strFreightElements.Append("     CONT_CUST_TRN_SEA_TBL          tran8,                           " );
                    strFreightElements.Append("     CONT_SUR_CHRG_SEA_TBL          frtd8                            " );
                    strFreightElements.Append("    where                                                                " );
                    strFreightElements.Append("            main8.CONT_CUST_SEA_PK      = tran8.CONT_CUST_SEA_FK              " );
                    strFreightElements.Append("     AND    tran8.CONT_CUST_TRN_SEA_PK  = frtd8.CONT_CUST_TRN_SEA_FK          " );
                    strFreightElements.Append("     AND    main8.CARGO_TYPE            = 1                                   " );
                    strFreightElements.Append("     AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strFreightElements.Append("     AND    main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strFreightElements.Append("     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " );
                    strFreightElements.Append("     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " );
                    strFreightElements.Append("     AND    tran8.CONTAINER_TYPE_MST_FK = cont6.CONTAINER_TYPE_MST_FK         " );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strFreightElements.Append(" AND main8.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strFreightElements.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                        strFreightElements.Append("            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " );
                    }
                    strFreightElements.Append("     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " );
                    strSurcharge.Append("    from                                                             " );
                    strSurcharge.Append("     CONT_CUST_SEA_TBL              main9,                           " );
                    strSurcharge.Append("     CONT_CUST_TRN_SEA_TBL          tran9                            " );
                    strSurcharge.Append("    where                                                                " );
                    strSurcharge.Append("            main9.CONT_CUST_SEA_PK      = tran9.CONT_CUST_SEA_FK              " );
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 1                                   " );
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strSurcharge.Append("     AND    main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " );
                    strSurcharge.Append("     AND    tran9.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strSurcharge.Append(" AND main9.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                        strSurcharge.Append(" tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " );
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
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("    Select     " );
                    strBuilder.Append(" tran2.CONT_CUST_SEA_FK    TRNTYPEFK,  " );
                    strBuilder.Append(" main2.CONT_REF_NO     REFNO,   " );
                    strBuilder.Append(" CCCTMT.CONTAINER_TYPE_MST_ID Type, " );
                    strBuilder.Append(" CCCMT.COMMODITY_ID COMMODITY, " );
                    strBuilder.Append(" CCPL.PORT_MST_PK POLPK, " );
                    strBuilder.Append(" CCPL.PORT_ID POL, " );
                    strBuilder.Append(" CCPD.PORT_MST_PK PODPK, " );
                    strBuilder.Append(" CCPD.PORT_ID POD, " );
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_MST_PK, " );
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append(" 'true' SEL,  " );
                    strBuilder.Append(" curr2.CURRENCY_MST_PK, " );
                    strBuilder.Append(" curr2.CURRENCY_ID, " );
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN" );
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE " );
                    strBuilder.Append(" Else " );
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE " );
                    strBuilder.Append(" END) AS RATE, " );
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN" );
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE " );
                    strBuilder.Append(" Else " );
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE " );
                    strBuilder.Append(" END) AS BKGRATE, " );
                    strBuilder.Append(" tran2.LCL_BASIS BASISPK, " );
                    strBuilder.Append(" '1' AS PYMT_TYPE   " );
                    strBuilder.Append(" from " );
                    strBuilder.Append(" CONT_CUST_SEA_TBL main2, " );
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL tran2, " );
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL frt2, " );
                    strBuilder.Append(" OPERATOR_MST_TBL CCOMT, " );
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL CCCTMT, " );
                    strBuilder.Append(" COMMODITY_MST_TBL CCCMT, " );
                    strBuilder.Append(" PORT_MST_TBL CCPL, " );
                    strBuilder.Append(" PORT_MST_TBL CCPD, " );
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL curr2 " );
                    strBuilder.Append(" where " );
                    strBuilder.Append(" main2.CONT_CUST_SEA_PK = tran2.CONT_CUST_SEA_FK " );
                    strBuilder.Append(" AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF' " );
                    strBuilder.Append(" AND TRAN2.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK   " );
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK " );
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK " );
                    strBuilder.Append(" AND main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strBuilder.Append(" AND main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) " );
                    strBuilder.Append(" AND tran2.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK" );
                    strBuilder.Append(" AND main2.CARGO_TYPE  = 1       " );
                    strBuilder.Append(" AND main2.STATUS   = 2       " );
                    strBuilder.Append(" AND main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK );
                    strBuilder.Append(" AND main2.CUSTOMER_MST_FK  = " + intCustomerPK );
                    strBuilder.Append(" AND tran2.PORT_MST_POL_FK = " + strPOL );
                    strBuilder.Append(" AND tran2.PORT_MST_POD_FK = " + strPOD + " " + arrCCondition[3] );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strBuilder.Append(" AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN   " );
                        strBuilder.Append(" tran2.VALID_FROM AND NVL(tran2.VALID_TO,NULL_DATE_FORMAT)  " );
                    }
                    strBuilder.Append(" UNION " );
                    strBuilder.Append(" Select " );
                    strBuilder.Append(" tran2.CONT_CUST_SEA_FK                    TRNTYPEFK,            " );
                    strBuilder.Append(" main2.CONT_REF_NO                          REFNO,               " );
                    strBuilder.Append(" CCCTMT.CONTAINER_TYPE_MST_ID               TYPE,                " );
                    strBuilder.Append(" CCCMT.COMMODITY_ID                         COMMODITY,           " );
                    strBuilder.Append(" CCPL.PORT_MST_PK POLPK                     ,              " );
                    strBuilder.Append(" CCPL.PORT_ID                               POL,                 " );
                    strBuilder.Append(" CCPD.PORT_MST_PK PODPK              ,              " );
                    strBuilder.Append(" CCPD.PORT_ID                               POD,                 " );
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_MST_PK,              " );
                    strBuilder.Append(" frt2.FREIGHT_ELEMENT_ID                    ,              " );
                    strBuilder.Append(" DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,       " );
                    strBuilder.Append(" curr2.CURRENCY_MST_PK                      ,             " );
                    strBuilder.Append(" curr2.CURRENCY_ID                          ,             " );
                    strBuilder.Append(" frtd2.APP_SURCHARGE_AMT                    RATE,                " );
                    strBuilder.Append(" frtd2.APP_SURCHARGE_AMT                    BKGRATE,                " );
                    strBuilder.Append(" tran2.LCL_BASIS                            BASISPK,                   " );
                    strBuilder.Append(" '1' AS PYMT_TYPE               " );
                    strBuilder.Append(" from                                                             " );
                    strBuilder.Append(" CONT_CUST_SEA_TBL              main2,                           " );
                    strBuilder.Append(" CONT_CUST_TRN_SEA_TBL          tran2,                           " );
                    strBuilder.Append(" CONT_SUR_CHRG_SEA_TBL          frtd2,                           " );
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL        frt2,                            " );
                    strBuilder.Append(" OPERATOR_MST_TBL               CCOMT,                           " );
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL         CCCTMT,                          " );
                    strBuilder.Append(" COMMODITY_MST_TBL              CCCMT,                           " );
                    strBuilder.Append(" PORT_MST_TBL                   CCPL,                            " );
                    strBuilder.Append(" PORT_MST_TBL                   CCPD,                            " );
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL          curr2                            " );
                    strBuilder.Append(" where                                                                " );
                    strBuilder.Append(" main2.CONT_CUST_SEA_PK      = tran2.CONT_CUST_SEA_FK              " );
                    strBuilder.Append(" AND    tran2.CONT_CUST_TRN_SEA_PK  = frtd2.CONT_CUST_TRN_SEA_FK          " );
                    strBuilder.Append(" AND    frtd2.FREIGHT_ELEMENT_MST_FK = frt2.FREIGHT_ELEMENT_MST_PK        " );
                    strBuilder.Append(" AND    frtd2.CURRENCY_MST_FK       = curr2.CURRENCY_MST_PK               " );
                    strBuilder.Append(" AND    tran2.PORT_MST_POL_FK=CCPL.PORT_MST_PK " );
                    strBuilder.Append(" AND    tran2.PORT_MST_POD_FK=CCPD.PORT_MST_PK " );
                    strBuilder.Append(" AND    main2.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strBuilder.Append(" AND    main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)" );
                    strBuilder.Append(" AND    tran2.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK" );
                    strBuilder.Append(" AND    main2.CARGO_TYPE            = 1                                   " );
                    strBuilder.Append(" AND    main2.STATUS                = 2                                   " );
                    strBuilder.Append(" AND    main2.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strBuilder.Append(" AND    main2.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strBuilder.Append(" AND    tran2.PORT_MST_POL_FK      = " + strPOL );
                    strBuilder.Append(" AND    tran2.PORT_MST_POD_FK      = " + strPOD + arrCCondition[3] );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                        strBuilder.Append(" AND main2.cont_ref_no = '" + CustContRefNr + "'  " );
                    }
                    else
                    {
                        strBuilder.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                        strBuilder.Append(" tran2.VALID_FROM   AND   NVL(tran2.VALID_TO,NULL_DATE_FORMAT)        " );
                    }
                    strBuilder.Append(" UNION  " );
                    strBuilder.Append(" Select            " );
                    strBuilder.Append(" NULL                                       TRNTYPEFK,           " );
                    strBuilder.Append(" " + strContRefNo.ToString() + "                       REFNO,               " );
                    strBuilder.Append(" COCTMT.CONTAINER_TYPE_MST_ID               TYPE,                " );
                    strBuilder.Append(" NULL                                       COMMODITY,           " );
                    strBuilder.Append(" COPL.PORT_MST_PK POLPK              ,              " );
                    strBuilder.Append(" COPL.PORT_ID                               POL,                 " );
                    strBuilder.Append(" COPD.PORT_MST_PK PODPK               ,              " );
                    strBuilder.Append(" COPD.PORT_ID                               POD,                 " );
                    strBuilder.Append(" frt6.FREIGHT_ELEMENT_MST_PK               ,          " );
                    strBuilder.Append(" frt6.FREIGHT_ELEMENT_ID                    ,          " );
                    strBuilder.Append(" DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                    strBuilder.Append(" curr6.CURRENCY_MST_PK                      ,        " );
                    strBuilder.Append(" curr6.CURRENCY_ID                          ,        " );
                    strBuilder.Append(" cont6.FCL_REQ_RATE                         RATE,                " );
                    strBuilder.Append(" cont6.FCL_REQ_RATE                         BKGRATE,             " );
                    strBuilder.Append(" tran6.LCL_BASIS                            BASISPK,               " );
                    strBuilder.Append(" '1'                                        PYMT_TYPE            " );
                    strBuilder.Append(" from                                                             " );
                    strBuilder.Append(" TARIFF_MAIN_SEA_TBL            main6,                           " );
                    strBuilder.Append(" TARIFF_TRN_SEA_FCL_LCL         tran6,                           " );
                    strBuilder.Append(" TARIFF_TRN_SEA_CONT_DTL cont6,                       " );
                    strBuilder.Append(" FREIGHT_ELEMENT_MST_TBL        frt6,                            " );
                    strBuilder.Append(" OPERATOR_MST_TBL               COOMT,                           " );
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL         COCTMT,                          " );
                    strBuilder.Append(" PORT_MST_TBL                   COPL,                            " );
                    strBuilder.Append(" PORT_MST_TBL                   COPD,                            " );
                    strBuilder.Append(" CURRENCY_TYPE_MST_TBL          curr6                            " );
                    strBuilder.Append(" where " + strContRefNo.ToString() + " IS NOT NULL AND                       " );
                    strBuilder.Append(" main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " );
                    strBuilder.Append(" AND cont6.TARIFF_TRN_SEA_FK = tran6.TARIFF_TRN_SEA_PK           " );
                    //Snigdharani
                    strBuilder.Append(" AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           " );
                    strBuilder.Append(" AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           " );
                    strBuilder.Append(" AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK   (+)" );
                    strBuilder.Append(" AND    cont6.CONTAINER_TYPE_MST_FK        = COCTMT.CONTAINER_TYPE_MST_PK" );
                    strBuilder.Append(" AND    main6.CARGO_TYPE            = 1                                  " );
                    strBuilder.Append(" AND    main6.ACTIVE                = 1                                  " );
                    strBuilder.Append(" AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       " );
                    strBuilder.Append(" AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              " );
                    strBuilder.Append(" AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK );
                    strBuilder.Append(" AND    tran6.PORT_MST_POL_FK       = " + strPOL );
                    strBuilder.Append(" AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4] );

                    if (!string.IsNullOrEmpty(CustContRefNr))
                    {
                    }
                    else
                    {
                        strBuilder.Append(" AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " );
                        strBuilder.Append(" tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " );
                    }
                    strBuilder.Append(" AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK " );
                    strBuilder.Append(" FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK " );
                    strBuilder.Append(" WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) " );
                    strBuilder.Append(" AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") " );
                    strBuilder.Append(" AND  " + strSurcharge.ToString() + " = 1                  " );
                    strBuilder.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append("     ORDER BY FRT.PREFERENCE ASC");
                }
                return strBuilder.ToString();
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
                    strContRefNo.Append(" (   Select    DISTINCT  srr_ref_no " );
                    strContRefNo.Append("    from                                                             " );
                    strContRefNo.Append("     SRR_SEA_TBL              main7,                           " );
                    strContRefNo.Append("     SRR_TRN_SEA_TBL          tran7                            " );
                    strContRefNo.Append("    where                                                                " );
                    strContRefNo.Append("            tran7.srr_sea_fk=main7.srr_sea_pk              " );
                    strContRefNo.Append("     AND    main7.CARGO_TYPE            = 2                                   " );
                    strContRefNo.Append("     AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strContRefNo.Append("     AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strContRefNo.Append("     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " );
                    strContRefNo.Append("     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " );
                    strContRefNo.Append("     AND    TRAN7.LCL_BASIS=TRAN6.LCL_BASIS                                     " );
                    strContRefNo.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strContRefNo.Append("            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " );
                    strContRefNo.Append("     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ");


                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " );
                    strFreightElements.Append("    from                                                             " );
                    strFreightElements.Append("     srr_sea_tbl              main8,                           " );
                    strFreightElements.Append("     srr_trn_sea_tbl          tran8,                           " );
                    strFreightElements.Append("     srr_sur_chrg_sea_tbl          frtd8                            " );
                    strFreightElements.Append("    where                                                                " );
                    strFreightElements.Append("            tran8.srr_sea_fk=main8.srr_sea_pk                " );
                    strFreightElements.Append("     and frtd8.srr_trn_sea_fk=tran8.srr_trn_sea_pk          " );
                    strFreightElements.Append("     AND    main8.CARGO_TYPE            = 2                                   " );
                    strFreightElements.Append("     AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strFreightElements.Append("     AND    main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strFreightElements.Append("     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " );
                    strFreightElements.Append("     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " );
                    strFreightElements.Append("     AND    TRAN8.LCL_BASIS=TRAN6.LCL_BASIS         " );
                    strFreightElements.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strFreightElements.Append("            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " );
                    strFreightElements.Append("     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK          )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " );
                    strSurcharge.Append("    from                                                             " );
                    strSurcharge.Append("     srr_sea_tbl main9,                                " );
                    strSurcharge.Append("     srr_trn_sea_tbl tran9                                " );
                    strSurcharge.Append("    where                                                                " );
                    strSurcharge.Append("            tran9.srr_sea_fk=main9.srr_sea_pk             " );
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 1                                   " );
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strSurcharge.Append("     AND    main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " );
                    strSurcharge.Append("     AND    TRAN9.LCL_BASIS=TRAN6.LCL_BASIS                              " );
                    strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strSurcharge.Append("            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " );
                    strSurcharge.Append("     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK         )   ");

                    //
                    strSRRSBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strSRRSBuilder.Append("       Q.REFNO,");
                    strSRRSBuilder.Append("       Q.BASIS,");
                    strSRRSBuilder.Append("       Q.COMMODITY,");
                    strSRRSBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POL,");
                    strSRRSBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POD,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strSRRSBuilder.Append("       Q.SEL,");
                    strSRRSBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strSRRSBuilder.Append("       Q.CURRENCY_ID,");
                    strSRRSBuilder.Append("       Q.MIN_RATE,");
                    strSRRSBuilder.Append("       Q.RATE,");
                    strSRRSBuilder.Append("       Q.BKGRATE,");
                    strSRRSBuilder.Append("       Q.BASISPK,");
                    strSRRSBuilder.Append("       Q.PYMT_TYPE");
                    strSRRSBuilder.Append("       FROM  (");

                    strSRRSBuilder.Append("  Select " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK    TRNTYPEFK,   " );
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,   " );
                    strSRRSBuilder.Append("     SRRUOM.DIMENTION_ID AS BASIS,     " );
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID     COMMODITY,   " );
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK,   " );
                    strSRRSBuilder.Append("     CCPL.PORT_ID POL,    " );
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK    PODPK,   " );
                    strSRRSBuilder.Append("     CCPD.PORT_ID POD,    " );
                    strSRRSBuilder.Append("     FRT2.FREIGHT_ELEMENT_MST_PK,   " );
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID    ,   " );
                    strSRRSBuilder.Append("     'true' SEL," );
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK," );
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID     ,   " );
                    strSRRSBuilder.Append("  NULL  MIN_RATE, " );
                    strSRRSBuilder.Append("   (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN " );
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE " );
                    strSRRSBuilder.Append("     Else " );
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE " );
                    strSRRSBuilder.Append("     END) " );
                    strSRRSBuilder.Append("     AS RATE, " );
                    strSRRSBuilder.Append("     (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN " );
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE " );
                    strSRRSBuilder.Append("     ELSE " );
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE " );
                    strSRRSBuilder.Append("     END) AS BKGRATE, " );
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,    " );
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE   " );
                    strSRRSBuilder.Append("     from           " );
                    strSRRSBuilder.Append("     SRR_SEA_TBL    SRRHDR,     " );
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,        " );
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL  frt2,     " );
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL   CCOMT,     " );
                    strSRRSBuilder.Append("     DIMENTION_UNIT_MST_TBL   SRRUOM,          " );
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL   CCCMT,     " );
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPL,     " );
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPD,     " );
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL  curr2     " );
                    strSRRSBuilder.Append("     where           " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK   " );
                    strSRRSBuilder.Append("     AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF'" );
                    strSRRSBuilder.Append("     AND SRRTRN.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK  " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK " );
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)" );
                    strSRRSBuilder.Append("     AND SRRTRN.LCL_BASIS=SRRUOM.DIMENTION_UNIT_MST_PK " );
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=2       " );
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1       " );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK );
                    strSRRSBuilder.Append("     AND SRRHDR.CUSTOMER_MST_FK = " + intCustomerPK );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD );
                    strSRRSBuilder.Append("     AND TO_DATE('" + strSDate + " ','" + dateFormat + "') BETWEEN   " );
                    strSRRSBuilder.Append("     SRRHDR.VALID_FROM AND NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)  " );
                    strSRRSBuilder.Append("     UNION " );
                    strSRRSBuilder.Append("     SELECT " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK                   TRNTYPEFK,              " );
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,                  " );
                    strSRRSBuilder.Append("     SRRUOM.DIMENTION_ID AS BASIS,                   " );
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID                         COMMODITY,           " );
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK                            ,              " );
                    strSRRSBuilder.Append("     CCPL.PORT_ID                               POL,                 " );
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK PODPK            ,              " );
                    strSRRSBuilder.Append("     CCPD.PORT_ID                               POD,                 " );
                    strSRRSBuilder.Append("     SRRSUR.FREIGHT_ELEMENT_MST_FK,              " );
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID                    ,              " );
                    strSRRSBuilder.Append("     DECODE(SRRSUR.CHECK_FOR_ALL_IN_RT, 1,'true','false') SEL,         " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,                              " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID                          ,             " );
                    strSRRSBuilder.Append("  NULL  MIN_RATE, " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT RATE,               " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT AS BKGRATE,  " );
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,                 " );
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE               " );
                    strSRRSBuilder.Append("     from                                                             " );
                    strSRRSBuilder.Append("     SRR_SEA_TBL                 SRRHDR,                             " );
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,                                         " );
                    strSRRSBuilder.Append("     SRR_SUR_CHRG_SEA_TBL SRRSUR,                                    " );
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt2,                            " );
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               CCOMT,                           " );
                    strSRRSBuilder.Append("     DIMENTION_UNIT_MST_TBL              SRRUOM,                          " );
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL              CCCMT,                           " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPL,                            " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPD,                            " );
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr2                            " );
                    strSRRSBuilder.Append("     where                                                            " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK              " );
                    strSRRSBuilder.Append("     AND    SRRSUR.SRR_TRN_SEA_FK=SRRTRN.SRR_TRN_SEA_PK          " );
                    strSRRSBuilder.Append("     AND SRRSUR.FREIGHT_ELEMENT_MST_FK= frt2.FREIGHT_ELEMENT_MST_PK       " );
                    strSRRSBuilder.Append("     AND SRRSUR.CURRENCY_MST_FK = curr2.CURRENCY_MST_PK               " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK  " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK  " );
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) " );
                    strSRRSBuilder.Append("     AND SRRTRN.LCL_BASIS=SRRUOM.DIMENTION_UNIT_MST_PK " );
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=2                                   " );
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1                                        " );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK + "       " );
                    strSRRSBuilder.Append("     AND SRRHDR.CUSTOMER_MST_FK =  " + intCustomerPK + "               " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[6] );
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strSRRSBuilder.Append("            SRRHDR.VALID_FROM  AND   NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)        " );
                    strSRRSBuilder.Append("    UNION  " );
                    strSRRSBuilder.Append("   Select            " );
                    strSRRSBuilder.Append("     NULL                                       TRNTYPEFK,           " );
                    strSRRSBuilder.Append("     " + strContRefNo.ToString() + "                       REFNO,               " );
                    strSRRSBuilder.Append("     SRRUOM.DIMENTION_ID AS                     BASIS,               " );
                    strSRRSBuilder.Append("     NULL                                       COMMODITY,           " );
                    strSRRSBuilder.Append("     COPL.PORT_MST_PK POLPK             ,              " );
                    strSRRSBuilder.Append("     COPL.PORT_ID                               POL,                 " );
                    strSRRSBuilder.Append("     COPD.PORT_MST_PK PODPK              ,              " );
                    strSRRSBuilder.Append("     COPD.PORT_ID                               POD,                 " );
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_MST_PK               ,          " );
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_ID                    ,          " );
                    strSRRSBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_ID                          ,        " );
                    strSRRSBuilder.Append("  NULL  MIN_RATE, " );
                    strSRRSBuilder.Append("     TRAN6.LCL_TARIFF_RATE                      RATE,                " );
                    strSRRSBuilder.Append("     TRAN6.LCL_TARIFF_RATE                      BKGRATE,             " );
                    strSRRSBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               " );
                    strSRRSBuilder.Append("     '1'                                        PYMT_TYPE            " );
                    strSRRSBuilder.Append("    from                                                             " );
                    strSRRSBuilder.Append("     TARIFF_MAIN_SEA_TBL            main6,                           " );
                    strSRRSBuilder.Append("     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " );
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt6,                            " );
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               COOMT,                           " );
                    strSRRSBuilder.Append("     DIMENTION_UNIT_MST_TBL         SRRUOM,                                         " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPL,                            " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPD,                            " );
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr6                            " );
                    strSRRSBuilder.Append("     where " + strContRefNo.ToString() + " IS NOT NULL AND                       " );
                    strSRRSBuilder.Append("            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           " );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           " );
                    strSRRSBuilder.Append("     AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK     (+) " );
                    strSRRSBuilder.Append("     AND    TRAN6.LCL_BASIS                    = SRRUOM.DIMENTION_UNIT_MST_PK    " );
                    strSRRSBuilder.Append("     AND    main6.CARGO_TYPE            = 2                                  " );
                    strSRRSBuilder.Append("     AND    main6.ACTIVE                = 1                                  " );
                    strSRRSBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       " );
                    strSRRSBuilder.Append("     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              " );
                    strSRRSBuilder.Append("     AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK       = " + strPOL );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4] );
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " );
                    strSRRSBuilder.Append("            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " );
                    strSRRSBuilder.Append("     AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK " );
                    strSRRSBuilder.Append("     FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK " );
                    strSRRSBuilder.Append("     WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) " );
                    strSRRSBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") " );
                    strSRRSBuilder.Append("     AND  " + strSurcharge.ToString() + " = 1                  ");
                    strSRRSBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //
                    strSRRSBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strSRRSBuilder.Append(" ORDER BY FRT.PREFERENCE ASC");
                }
                else
                {
                    strContRefNo.Append(" (   Select    DISTINCT  srr_ref_no " );
                    strContRefNo.Append("    from                                                             " );
                    strContRefNo.Append("     SRR_SEA_TBL              main7,                           " );
                    strContRefNo.Append("     SRR_TRN_SEA_TBL          tran7                            " );
                    strContRefNo.Append("    where                                                                " );
                    strContRefNo.Append("            tran7.srr_sea_fk=main7.srr_sea_pk              " );
                    strContRefNo.Append("     AND    main7.CARGO_TYPE            = 1                                   " );
                    strContRefNo.Append("     AND    main7.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strContRefNo.Append("     AND    main7.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strContRefNo.Append("     AND    tran7.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " );
                    strContRefNo.Append("     AND    tran7.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " );
                    strContRefNo.Append("     AND    tran7.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " );
                    strContRefNo.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strContRefNo.Append("            tran7.VALID_FROM   AND   NVL(tran7.VALID_TO,NULL_DATE_FORMAT)        " );
                    strContRefNo.Append("     AND    main7.OPERATOR_MST_FK =  main6.OPERATOR_MST_FK  )    ");


                    strFreightElements.Append(" (   Select   DISTINCT  frtd8.FREIGHT_ELEMENT_MST_FK " );
                    strFreightElements.Append("    from                                                             " );
                    strFreightElements.Append("     srr_sea_tbl              main8,                           " );
                    strFreightElements.Append("     srr_trn_sea_tbl          tran8,                           " );
                    strFreightElements.Append("     srr_sur_chrg_sea_tbl          frtd8                            " );
                    strFreightElements.Append("    where                                                                " );
                    strFreightElements.Append("            tran8.srr_sea_fk=main8.srr_sea_pk                " );
                    strFreightElements.Append("     and frtd8.srr_trn_sea_fk=tran8.srr_trn_sea_pk          " );
                    strFreightElements.Append("     AND    main8.CARGO_TYPE            = 1                                   " );
                    strFreightElements.Append("     AND    main8.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strFreightElements.Append("     AND    main8.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strFreightElements.Append("     AND    tran8.PORT_MST_POL_FK       = tran6.PORT_MST_POL_FK               " );
                    strFreightElements.Append("     AND    tran8.PORT_MST_POD_FK       = tran6.PORT_MST_POD_FK               " );
                    strFreightElements.Append("     AND    tran8.CONTAINER_TYPE_MST_FK = cont6.CONTAINER_TYPE_MST_FK         " );
                    strFreightElements.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strFreightElements.Append("            tran8.VALID_FROM   AND   NVL(tran8.VALID_TO,NULL_DATE_FORMAT)        " );
                    strFreightElements.Append("     AND    main8.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK       )   ");

                    strSurcharge.Append(" ( Select  DISTINCT  SUBJECT_TO_SURCHG_CHG  " );
                    strSurcharge.Append("    from                                                             " );
                    strSurcharge.Append("     srr_sea_tbl main9,                                " );
                    strSurcharge.Append("     srr_trn_sea_tbl tran9                                " );
                    strSurcharge.Append("    where                                                                " );
                    strSurcharge.Append("            tran9.srr_sea_fk=main9.srr_sea_pk             " );
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 1                                   " );
                    strSurcharge.Append("     AND    main9.COMMODITY_GROUP_MST_FK = " + intCommodityPK + "       " );
                    strSurcharge.Append("     AND    main9.CUSTOMER_MST_FK       =  " + intCustomerPK + "               " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POL_FK       =  tran6.PORT_MST_POL_FK              " );
                    strSurcharge.Append("     AND    tran9.PORT_MST_POD_FK       =  tran6.PORT_MST_POD_FK              " );
                    strSurcharge.Append("     AND    tran9.CONTAINER_TYPE_MST_FK =  cont6.CONTAINER_TYPE_MST_FK        " );
                    strSurcharge.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strSurcharge.Append("            tran9.VALID_FROM   AND   NVL(tran9.VALID_TO,NULL_DATE_FORMAT)        " );
                    strSurcharge.Append("     AND    main9.OPERATOR_MST_FK       =  main6.OPERATOR_MST_FK         )   ");

                    //
                    strSRRSBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strSRRSBuilder.Append("       Q.REFNO,");
                    strSRRSBuilder.Append("       Q.TYPE,");
                    strSRRSBuilder.Append("       Q.COMMODITY,");
                    strSRRSBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POL,");
                    strSRRSBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strSRRSBuilder.Append("       Q.POD,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strSRRSBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strSRRSBuilder.Append("       Q.SEL,");
                    strSRRSBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strSRRSBuilder.Append("       Q.CURRENCY_ID,");
                    strSRRSBuilder.Append("       Q.RATE,");
                    strSRRSBuilder.Append("       Q.BKGRATE,");
                    strSRRSBuilder.Append("       Q.BASISPK,");
                    strSRRSBuilder.Append("       Q.PYMT_TYPE");
                    strSRRSBuilder.Append("       FROM  (");

                    strSRRSBuilder.Append("  Select " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK    TRNTYPEFK,   " );
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,   " );
                    strSRRSBuilder.Append("     CCCTMT.CONTAINER_TYPE_MST_ID TYPE,    " );
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID     COMMODITY,   " );
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK,   " );
                    strSRRSBuilder.Append("     CCPL.PORT_ID POL,    " );
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK    PODPK,   " );
                    strSRRSBuilder.Append("     CCPD.PORT_ID POD,    " );
                    strSRRSBuilder.Append("     FRT2.FREIGHT_ELEMENT_MST_PK,   " );
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID    ,   " );
                    //strSRRSBuilder.Append("     frt2.PREFERENCE   ,   " & vbCrLf) 'ADDED BY PURNANAND
                    strSRRSBuilder.Append("     'true' SEL," );
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK," );
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID     ,   " );
                    strSRRSBuilder.Append("     (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN " );
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE " );
                    strSRRSBuilder.Append("     Else " );
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE " );
                    strSRRSBuilder.Append("     END) " );
                    strSRRSBuilder.Append("     AS RATE, " );
                    strSRRSBuilder.Append("     (CASE WHEN SRRTRN.APPROVED_BOF_RATE IS NOT NULL THEN " );
                    strSRRSBuilder.Append("     SRRTRN.APPROVED_BOF_RATE " );
                    strSRRSBuilder.Append("     ELSE " );
                    strSRRSBuilder.Append("     SRRTRN.CURRENT_BOF_RATE " );
                    strSRRSBuilder.Append("     END) AS BKGRATE, " );
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,    " );
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE  " );
                    // strSRRSBuilder.Append("     frt2.PREFERENCE      " & vbCrLf)
                    strSRRSBuilder.Append("     from           " );
                    strSRRSBuilder.Append("     SRR_SEA_TBL    SRRHDR,     " );
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,        " );
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL  frt2,     " );
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL   CCOMT,     " );
                    strSRRSBuilder.Append("     CONTAINER_TYPE_MST_TBL  CCCTMT,     " );
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL   CCCMT,     " );
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPL,     " );
                    strSRRSBuilder.Append("     PORT_MST_TBL    CCPD,     " );
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL  curr2     " );
                    strSRRSBuilder.Append("     where           " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK   " );
                    strSRRSBuilder.Append("     AND FRT2.FREIGHT_ELEMENT_ID LIKE '%BOF'" );
                    strSRRSBuilder.Append("     AND SRRTRN.CURRENCY_MST_FK= curr2.CURRENCY_MST_PK  " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK " );
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)" );
                    strSRRSBuilder.Append("     AND SRRTRN.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK" );
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=1       " );
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1       " );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK );
                    strSRRSBuilder.Append("     AND SRRHDR.CUSTOMER_MST_FK = " + intCustomerPK );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[6] );
                    strSRRSBuilder.Append("     AND TO_DATE('" + strSDate + " ','" + dateFormat + "') BETWEEN   " );
                    strSRRSBuilder.Append("     SRRHDR.VALID_FROM AND NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)  " );
                    strSRRSBuilder.Append("     UNION " );
                    strSRRSBuilder.Append("    Select     " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_TRN_SEA_PK                   TRNTYPEFK,              " );
                    strSRRSBuilder.Append("     SRRHDR.SRR_REF_NO REFNO,                  " );
                    strSRRSBuilder.Append("     CCCTMT.CONTAINER_TYPE_MST_ID   TYPE,                   " );
                    strSRRSBuilder.Append("     CCCMT.COMMODITY_ID                         COMMODITY,           " );
                    strSRRSBuilder.Append("     CCPL.PORT_MST_PK POLPK                         ,              " );
                    strSRRSBuilder.Append("     CCPL.PORT_ID                               POL,                 " );
                    strSRRSBuilder.Append("     CCPD.PORT_MST_PK PODPK          ,              " );
                    strSRRSBuilder.Append("     CCPD.PORT_ID                               POD,                 " );
                    strSRRSBuilder.Append("     SRRSUR.FREIGHT_ELEMENT_MST_FK,              " );
                    strSRRSBuilder.Append("     frt2.FREIGHT_ELEMENT_ID                    ,              " );
                    //strSRRSBuilder.Append("     frt2.PREFERENCE   ,   " & vbCrLf) 'ADDED BY PURNANAND
                    strSRRSBuilder.Append("     DECODE(SRRSUR.CHECK_FOR_ALL_IN_RT, 1,'true','false') SEL,         " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,                              " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID                          ,             " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT RATE,               " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT AS BKGRATE,  " );
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,                 " );
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE               " );
                    //strSRRSBuilder.Append("     frt2.PREFERENCE      " & vbCrLf)
                    strSRRSBuilder.Append("     from                                                             " );
                    strSRRSBuilder.Append("     SRR_SEA_TBL                 SRRHDR,                             " );
                    strSRRSBuilder.Append("     SRR_TRN_SEA_TBL SRRTRN,                                         " );
                    strSRRSBuilder.Append("     SRR_SUR_CHRG_SEA_TBL SRRSUR,                                    " );
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt2,                            " );
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               CCOMT,                           " );
                    strSRRSBuilder.Append("     CONTAINER_TYPE_MST_TBL         CCCTMT,                          " );
                    strSRRSBuilder.Append("     COMMODITY_MST_TBL              CCCMT,                           " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPL,                            " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   CCPD,                            " );
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr2                            " );
                    strSRRSBuilder.Append("    where                                                                " );
                    strSRRSBuilder.Append("     SRRTRN.SRR_SEA_FK=SRRHDR.SRR_SEA_PK              " );
                    strSRRSBuilder.Append("     AND    SRRSUR.SRR_TRN_SEA_FK=SRRTRN.SRR_TRN_SEA_PK          " );
                    strSRRSBuilder.Append("     AND SRRSUR.FREIGHT_ELEMENT_MST_FK= frt2.FREIGHT_ELEMENT_MST_PK       " );
                    strSRRSBuilder.Append("     AND SRRSUR.CURRENCY_MST_FK = curr2.CURRENCY_MST_PK               " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK=CCPL.PORT_MST_PK  " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK=CCPD.PORT_MST_PK  " );
                    strSRRSBuilder.Append("     AND SRRHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK (+)" );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)" );
                    strSRRSBuilder.Append("     AND SRRTRN.CONTAINER_TYPE_MST_FK=CCCTMT.CONTAINER_TYPE_MST_PK" );
                    strSRRSBuilder.Append("     AND SRRHDR.CARGO_TYPE=1                                   " );
                    strSRRSBuilder.Append("     AND SRRHDR.STATUS = 1                                        " );
                    strSRRSBuilder.Append("     AND SRRHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK + "       " );
                    strSRRSBuilder.Append("     AND SRRHDR.CUSTOMER_MST_FK =  " + intCustomerPK + "               " );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POL_FK = " + strPOL );
                    strSRRSBuilder.Append("     AND SRRTRN.PORT_MST_POD_FK= " + strPOD + arrCCondition[6] );
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                  " );
                    strSRRSBuilder.Append("            SRRHDR.VALID_FROM  AND   NVL(SRRTRN.VALID_TO,NULL_DATE_FORMAT)        " );
                    strSRRSBuilder.Append("    UNION  " );
                    strSRRSBuilder.Append("   Select            " );
                    strSRRSBuilder.Append("     NULL                                       TRNTYPEFK,           " );
                    strSRRSBuilder.Append("     " + strContRefNo.ToString() + "                       REFNO,               " );
                    strSRRSBuilder.Append("     COCTMT.CONTAINER_TYPE_MST_ID               TYPE,                " );
                    strSRRSBuilder.Append("     NULL                                       COMMODITY,           " );
                    strSRRSBuilder.Append("     COPL.PORT_MST_PK POLPK           ,              " );
                    strSRRSBuilder.Append("     COPL.PORT_ID                               POL,                 " );
                    strSRRSBuilder.Append("     COPD.PORT_MST_PK PODPK            ,              " );
                    strSRRSBuilder.Append("     COPD.PORT_ID                               POD,                 " );
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_MST_PK               ,          " );
                    strSRRSBuilder.Append("     frt6.FREIGHT_ELEMENT_ID                    ,          " );
                    strSRRSBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_ID                          ,        " );
                    strSRRSBuilder.Append("     cont6.FCL_REQ_RATE                         RATE,                " );
                    strSRRSBuilder.Append("     cont6.FCL_REQ_RATE                         BKGRATE,             " );
                    strSRRSBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               " );
                    strSRRSBuilder.Append("     '1'                                        PYMT_TYPE            " );
                    strSRRSBuilder.Append("    from                                                             " );
                    strSRRSBuilder.Append("     TARIFF_MAIN_SEA_TBL            main6,                           " );
                    strSRRSBuilder.Append("     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " );
                    strSRRSBuilder.Append("     TARIFF_TRN_SEA_CONT_DTL cont6,                       " );
                    strSRRSBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt6,                            " );
                    strSRRSBuilder.Append("     OPERATOR_MST_TBL               COOMT,                           " );
                    strSRRSBuilder.Append("     CONTAINER_TYPE_MST_TBL         COCTMT,                          " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPL,                            " );
                    strSRRSBuilder.Append("     PORT_MST_TBL                   COPD,                            " );
                    strSRRSBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr6                            " );
                    strSRRSBuilder.Append("     where " + strContRefNo.ToString() + " IS NOT NULL AND                       " );
                    strSRRSBuilder.Append("            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           " );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           " );
                    strSRRSBuilder.Append("     AND    CONT6.TARIFF_TRN_SEA_FK = TRAN6.TARIFF_TRN_SEA_PK           " );
                    //Snigdharani ' modified by thiyagarajan on 21/11/08
                    strSRRSBuilder.Append("     AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK    (+)  " );
                    strSRRSBuilder.Append("     AND    cont6.CONTAINER_TYPE_MST_FK        = COCTMT.CONTAINER_TYPE_MST_PK" );
                    strSRRSBuilder.Append("     AND    main6.CARGO_TYPE            = 1                                  " );
                    strSRRSBuilder.Append("     AND    main6.ACTIVE                = 1                                  " );
                    strSRRSBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       " );
                    strSRRSBuilder.Append("     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              " );
                    strSRRSBuilder.Append("     AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POL_FK       = " + strPOL );
                    strSRRSBuilder.Append("     AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4] );
                    strSRRSBuilder.Append("     AND    TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " );
                    strSRRSBuilder.Append("            tran6.VALID_FROM   AND   NVL(tran6.VALID_TO,NULL_DATE_FORMAT)       " );
                    strSRRSBuilder.Append("     AND TRAN6.FREIGHT_ELEMENT_MST_FK NOT IN ( SELECT FRTCHECK.FREIGHT_ELEMENT_MST_PK " );
                    strSRRSBuilder.Append("     FROM FREIGHT_ELEMENT_MST_TBL FRTCHECK " );
                    strSRRSBuilder.Append("     WHERE FRTCHECK.FREIGHT_ELEMENT_ID LIKE ('%BOF') ) " );
                    strSRRSBuilder.Append("     AND tran6.FREIGHT_ELEMENT_MST_FK NOT IN (" + strFreightElements.ToString() + ") " );
                    strSRRSBuilder.Append("     AND " + strSurcharge.ToString() + " = 1                  " );
                    strSRRSBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //
                    strSRRSBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strSRRSBuilder.Append(" ORDER BY FRT.PREFERENCE ASC");
                }
                return strSRRSBuilder.ToString();
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

        public object funSLTariffFreight(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                string strOperatorRate = null;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    //
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.BASIS,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.MIN_RATE,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,Q.COMMODITYPK");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_MIN_RATE AS MIN_RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS BKGRATE, OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE,CMT.COMMODITY_MST_PK COMMODITYPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, " );
                    strBuilder.Append("PORT_MST_TBL OPL, " );
                    strBuilder.Append("PORT_MST_TBL OPD, " );
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, " );
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT, " );
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM,COMMODITY_MST_TBL CMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK " );
                    strBuilder.Append("AND CMT.COMMODITY_MST_PK=OTRN.COMMODITY_MST_FK " );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK " );
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK " );
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK " );
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK " );
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=1 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[5] );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ASC");
                }
                else
                {
                    //
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.TYPE,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS RATE, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS BKGRATE, OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, " );
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, " );
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT, " );
                    strBuilder.Append("PORT_MST_TBL OPL, " );
                    strBuilder.Append("PORT_MST_TBL OPD, " );
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, " );
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK " );
                    //Snigdharani
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK " );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK " );
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK " );
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK " );
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK (+)" + arrCCondition[5] );
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=1 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 " );
                    strBuilder.Append("AND OTRN.CHECK_FOR_ALL_IN_RT=1" );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                    strBuilder.Append(" ORDER BY OFEMT.PREFERENCE ASC");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ASC");
                }
                return strBuilder.ToString();
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

        public object funGTariffFreight(ArrayList arrCCondition, string intCommodityPK, string strPOL, string strPOD, string strSDate, Int16 intIsFcl)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    //
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.BASIS,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.MIN_RATE,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_MIN_RATE AS MIN_RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS BKGRATE, OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("PORT_MST_TBL OPL, " );
                    strBuilder.Append("PORT_MST_TBL OPD, " );
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, " );
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT, " );
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK " );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK " );
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK " );
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK " );
                    strBuilder.Append("AND OTRN.LCL_BASIS=OUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=2 " + arrCCondition[5] );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "'))");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ASC");
                }
                else
                {
                    //
                    strBuilder.Append("SELECT Q.TRNTYPEFK,");
                    strBuilder.Append("       Q.REFNO,");
                    strBuilder.Append("       Q.TYPE,");
                    strBuilder.Append("       Q.COMMODITY,");
                    strBuilder.Append("       Q.POLPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POL,");
                    strBuilder.Append("       Q.PODPK PORT_MST_PK,");
                    strBuilder.Append("       Q.POD,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_MST_PK,");
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       Q.BKGRATE,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS RATE, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS BKGRATE, OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("TARIFF_TRN_SEA_CONT_DTL OTRNCONT, " );
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL OCTMT, " );
                    strBuilder.Append("PORT_MST_TBL OPL, " );
                    strBuilder.Append("PORT_MST_TBL OPD, " );
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL OFEMT, " );
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL OCUMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.TARIFF_MAIN_SEA_PK=OTRN.TARIFF_MAIN_SEA_FK " );
                    strBuilder.Append("AND OTRNCONT.TARIFF_TRN_SEA_FK = OTRN.TARIFF_TRN_SEA_PK " );
                    //Snigdharani
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK=OPL.PORT_MST_PK " );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK=OPD.PORT_MST_PK " );
                    strBuilder.Append("AND OTRN.FREIGHT_ELEMENT_MST_FK=OFEMT.FREIGHT_ELEMENT_MST_PK " );
                    strBuilder.Append("AND OTRN.CURRENCY_MST_FK=OCUMT.CURRENCY_MST_PK " + arrCCondition[5] );
                    strBuilder.Append("AND OCTMT.CONTAINER_TYPE_MST_PK=OTRNCONT.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append("AND OHDR.ACTIVE=1 " );
                    strBuilder.Append("AND OHDR.TARIFF_TYPE=2 " );
                    strBuilder.Append("AND OHDR.CARGO_TYPE=1 " );
                    strBuilder.Append("AND OHDR.COMMODITY_GROUP_FK= " + intCommodityPK );
                    strBuilder.Append("AND OTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND OTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN OHDR.VALID_FROM " );
                    strBuilder.Append("AND NVL(OHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ASC");
                }
                return strBuilder.ToString();
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

        #endregion

        #region "FETCH CREDTDAYS AND CREDIT LIMIT"
        public int fetchCredit(object CreditDays, object CreditLimit, string Pk = "0", int type = 0, int CustPk = 0)
        {

            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                System.Text.StringBuilder strCustQuery = new System.Text.StringBuilder();
                strCustQuery.Append("SELECT c.CREDIT_DAYS," );
                strCustQuery.Append(" c.CREDIT_LIMIT" );
                strCustQuery.Append("FROM customer_mst_tbl c" );
                strCustQuery.Append("WHERE c.customer_mst_pk=" + CustPk );
                OracleDataReader dr = null;
                if (type == 1)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD" );
                    strQuery.Append("  FROM SRR_SEA_TBL     c" );
                    strQuery.Append("  WHERE s.srr_sea_pk=" + Pk );
                }
                else if (type == 2)
                {
                    strQuery.Append("SELECT Q.CREDIT_DAYS," );
                    strQuery.Append("     Q.CREDIT_LIMIT " );
                    strQuery.Append("     FROM QUOTATION_SEA_TBL Q" );
                    strQuery.Append("     WHERE Q.QUOTATION_SEA_PK=" + Pk );
                    strQuery.Append("  --   AND Q.CUSTOMER_MST_FK=" + CustPk );
                }
                else if (type == 3)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD FROM cont_cust_sea_tbl C  " );
                    strQuery.Append("WHERE C.CONT_CUST_SEA_PK=" + Pk );
                }
                else
                {
                    strQuery = strCustQuery;
                }
                DataTable dt = null;
                dt = (new WorkFlow()).GetDataTable(strQuery.ToString());
                if (dt.Rows.Count > 0)
                {
                    CreditDays = getDefault(dt.Rows[0][0], "");
                    if (dt.Columns.Count > 1)
                        CreditLimit = getDefault(dt.Rows[0][1], "");
                }
                else
                {
                    dt = (new WorkFlow()).GetDataTable(strCustQuery.ToString());
                    CreditDays = getDefault(dt.Rows[0][0], "");
                    if (dt.Columns.Count > 1)
                        CreditLimit = getDefault(dt.Rows[0][1], "");
                }
                dr = (new WorkFlow()).GetDataReader(strCustQuery.ToString());
                while (dr.Read())
                {
                    if (string.IsNullOrEmpty(Convert.ToString(CreditDays)))
                        CreditDays = getDefault(dr[0], "");
                    if (string.IsNullOrEmpty(Convert.ToString(CreditLimit)))
                        CreditLimit = getDefault(dr[1], "");
                }
                dr.Close();
                if (!string.IsNullOrEmpty(Convert.ToString(CreditDays)))
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                return 0;
            }
        }
        #endregion

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
                    if (Convert.ToInt32(Mode )== 0)
                    {
                        selectCommand.CommandText = objWF.MyUserName + ".BKG_TRN_SEA_HAZ_SPL_REQ_PKG.BKG_TRN_SEA_HAZ_SPL_REQ_UPD";
                    }
                    else
                    {
                        selectCommand.CommandText = objWF.MyUserName + ".BKG_TRN_AIR_HAZ_SPL_REQ_PKG.BKG_TRN_AIR_HAZ_SPL_REQ_UPD";
                    }
                    var _with1 = selectCommand.Parameters;
                    _with1.Clear();
                    if (Convert.ToInt32(Mode) == 1)
                    {
                        _with1.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with1.Add("BKG_TRN_AIR_HAZ_SPL_PK_IN", getDefault(strParam[14], DBNull.Value)).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with1.Add("BKG_TRN_SEA_HAZ_SPL_PK_IN", getDefault(strParam[14], DBNull.Value)).Direction = ParameterDirection.Input;
                    }



                    _with1.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[3]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;

                    _with1.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[5]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;

                    _with1.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[7]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;

                    _with1.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;

                    _with1.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;

                    _with1.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;

                    _with1.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;

                    _with1.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    selectCommand.ExecuteNonQuery();
                    strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                    return strReturn;
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

                    var _with2 = selectCommand.Parameters;
                    _with2.Clear();

                    if (Convert.ToInt32(Mode) == 1)
                    {
                        _with2.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with2.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    }
                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with2.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with2.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with2.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with2.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[3]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with2.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with2.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[5]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with2.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with2.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[7]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with2.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with2.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with2.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with2.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with2.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with2.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    selectCommand.ExecuteNonQuery();
                    strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                    return strReturn;
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
                    var _with3 = SCM;
                    _with3.CommandType = CommandType.StoredProcedure;
                    _with3.CommandText = UserName + ".BKG_TRN_SEA_HAZ_SPL_REQ_PKG.BKG_TRN_SEA_HAZ_SPL_REQ_INS";
                    var _with4 = _with3.Parameters;
                    _with4.Clear();
                    //BKG_TRN_SEA_FK_IN()
                    _with4.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with4.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with4.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with4.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with4.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with4.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with4.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with4.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with4.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with4.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with4.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with4.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with4.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with4.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with4.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with3.ExecuteNonQuery();
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
                    strQuery.Append("SELECT BKG_TRN_SEA_HAZ_SPL_PK," );
                    strQuery.Append("BOOKING_TRN_SEA_FK," );
                    strQuery.Append("OUTER_PACK_TYPE_MST_FK," );
                    strQuery.Append("INNER_PACK_TYPE_MST_FK," );
                    strQuery.Append("MIN_TEMP," );
                    strQuery.Append("MIN_TEMP_UOM," );
                    strQuery.Append("MAX_TEMP," );
                    strQuery.Append("MAX_TEMP_UOM," );
                    strQuery.Append("FLASH_PNT_TEMP," );
                    strQuery.Append("FLASH_PNT_TEMP_UOM," );
                    strQuery.Append("IMDG_CLASS_CODE," );
                    strQuery.Append("UN_NO," );
                    strQuery.Append("IMO_SURCHARGE," );
                    strQuery.Append("SURCHARGE_AMT," );
                    strQuery.Append("IS_MARINE_POLLUTANT," );
                    strQuery.Append("EMS_NUMBER FROM BKG_TRN_SEA_HAZ_SPL_REQ Q" );
                    //,BKG_TRN_SEA_FCL_LCL QT,BKG_SEA_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE " );
                    strQuery.Append("Q.BOOKING_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("QT.BKG_SEA_FK=QM.BKG_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_SEA_PK=" & strPK)
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

        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {

            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with5 = SCM;
                    _with5.CommandType = CommandType.StoredProcedure;
                    _with5.CommandText = UserName + ".BKG_TRN_SEA_REF_SPL_REQ_PKG.BKG_TRN_SEA_REF_SPL_REQ_INS";
                    var _with6 = _with5.Parameters;
                    _with6.Clear();
                    //BOOKING_TRN_SEA_FK_IN()
                    _with6.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with6.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with6.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with6.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IS_PERSHIABLE_GOODS_IN()
                    _with6.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with6.Add("MIN_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with6.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with6.Add("MAX_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with6.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with6.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with6.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    //sivachandran 26Jun2008 CR Date 04/06/2008 For Reefer special Requirment
                    _with6.Add("HAULAGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("GENSET_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("CO2_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("O2_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("REQ_SET_TEMP_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("REQ_SET_TEMP_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("AIR_VENTILATION_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("AIR_VENTILATION_UOM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("DEHUMIDIFIER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("FLOORDRAINS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with6.Add("DEFROSTING_INTERVAL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    //End
                    _with6.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with5.ExecuteNonQuery();
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
                    strQuery.Append("SELECT BKG_TRN_SEA_REF_SPL_PK," );
                    strQuery.Append("BOOKING_TRN_SEA_FK," );
                    strQuery.Append("VENTILATION," );
                    strQuery.Append("AIR_COOL_METHOD," );
                    strQuery.Append("HUMIDITY_FACTOR," );
                    strQuery.Append("IS_PERISHABLE_GOODS," );
                    strQuery.Append("MIN_TEMP," );
                    strQuery.Append("MIN_TEMP_UOM," );
                    strQuery.Append("MAX_TEMP," );
                    strQuery.Append("MAX_TEMP_UOM," );
                    strQuery.Append("PACK_TYPE_MST_FK," );
                    strQuery.Append("Q.PACK_COUNT " );
                    strQuery.Append("FROM BKG_TRN_SEA_REF_SPL_REQ Q" );
                    //,BKG_TRN_SEA_FCL_LCL QT,BKG_SEA_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE " );
                    strQuery.Append("Q.BOOKING_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("BKG_SEA_TBL QM," & vbCrLf)
                    //strQuery.Append("BKG_TRN_SEA_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.BKG_TRN_SEA_FK=QT.BKG_TRN_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.BKG_SEA_FK=QM.BKG_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_SEA_PK=" & strPK)
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
        public DataTable fetchSpclReqODC(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT " );
                    strQuery.Append("BKG_TRN_SEA_ODC_SPL_PK," );
                    strQuery.Append("BOOKING_TRN_SEA_FK," );
                    strQuery.Append("LENGTH," );
                    strQuery.Append("LENGTH_UOM_MST_FK," );
                    strQuery.Append("HEIGHT," );
                    strQuery.Append("HEIGHT_UOM_MST_FK," );
                    strQuery.Append("WIDTH," );
                    strQuery.Append("WIDTH_UOM_MST_FK," );
                    strQuery.Append("WEIGHT," );
                    strQuery.Append("WEIGHT_UOM_MST_FK," );
                    strQuery.Append("VOLUME," );
                    strQuery.Append("VOLUME_UOM_MST_FK," );
                    strQuery.Append("SLOT_LOSS," );
                    strQuery.Append("LOSS_QUANTITY," );
                    strQuery.Append("APPR_REQ, " );
                    //Snigdharani - 03/03/2009 - EBooking Integration
                    strQuery.Append("NO_OF_SLOTS " );
                    strQuery.Append("FROM BKG_TRN_SEA_ODC_SPL_REQ Q" );
                    //,BKG_TRN_SEA_FCL_LCL QT,BKG_SEA_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE " );
                    strQuery.Append("Q.BOOKING_TRN_SEA_FK=" + strPK);
                    //strQuery.Append("BKG_SEA_TBL QM," & vbCrLf)
                    //strQuery.Append("BKG_TRN_SEA_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.BKG_TRN_SEA_FK=QT.BKG_TRN_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.BKG_SEA_FK=QM.BKG_SEA_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_SEA_PK=" & strPK)
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
        private ArrayList SaveTransactionODC(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with7 = SCM;
                    _with7.CommandType = CommandType.StoredProcedure;
                    _with7.CommandText = UserName + ".BKG_TRN_SEA_ODC_SPL_REQ_PKG.BKG_TRN_SEA_ODC_SPL_REQ_INS";
                    var _with8 = _with7.Parameters;
                    _with8.Clear();
                    //BKG_TRN_SEA_FK_IN()
                    _with8.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with8.Add("LENGTH_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with8.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0]) ? 0 : Convert.ToInt32(strParam[1])), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with8.Add("HEIGHT_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with8.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with8.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with8.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with8.Add("WEIGHT_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with8.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with8.Add("VOLUME_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with8.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with8.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with8.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], DBNull.Value)).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with8.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with8.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with7.ExecuteNonQuery();
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


        #endregion


        #endregion

        #region "To assign data to Cargo Move Code"
        public object FillddCMCode(int PkValue)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            DataTable dtCM = null;
            WorkFlow objwf = new WorkFlow();
            strBuilder.Append(" select CARGO_MOVE_FK from Quotation_Sea_Tbl where Quotation_sea_pk=" + PkValue);
            try
            {
                dtCM = objwf.GetDataTable(strBuilder.ToString());
                return dtCM;
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

        #endregion

        #region "Save"


        public ArrayList SaveBooking(DataSet dsMain, string txtBookingNo, long nLocationId, long nEmpId, string Measure, string Wt, string Divfac, string strPolPk = "", string strPodPk = "", string strFreightpk = "",
        Int16 intIsLcl = 0, string strBStatus = "", string strVoyagepk = "", string PODLocfk = "", string ShipperPK = "", string Consigne = "", bool Nomination = false)
        {

            string EbkgRefno = null;
            string BookingRefNo = null;
            string BookingRef = "";
            string EBookingRef = null;
            bool IsUpdate = false;
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                if (!Nomination)
                {
                    if ((string.IsNullOrEmpty(strVoyagepk) || strVoyagepk == "0") & !string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_ID"].ToString()))
                    {
                        strVoyagepk = "0";
                        arrMessage = SaveVesselMaster(Convert.ToInt32(strVoyagepk),
                           Convert.ToString(getDefault(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_NAME"], "")),
                            Convert.ToInt32(getDefault(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"], 0)),
                            Convert.ToString(getDefault(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_ID"], "")),
                            Convert.ToString(getDefault(dsMain.Tables["tblMaster"].Rows[0]["VOYAGE"], "")),
                            objWK.MyCommand,
                            Convert.ToInt32(getDefault(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POL_FK"], 0)),
                            Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POD_FK"]),
                            DateTime.MinValue,
                            Convert.ToDateTime(getDefault(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"], null)),
                            Convert.ToDateTime(getDefault(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"], null)),
                            Convert.ToDateTime(getDefault(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"], null)), DateTime.MinValue, DateTime.MinValue);
                        dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"] = strVoyagepk;
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }
                    }
                }

                if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["booking_mst_pk"].ToString()))
                {
                    if (string.IsNullOrEmpty(txtBookingNo))
                    {
                        if (Nomination)
                        {
                            BookingRefNo = GenerateNominationNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK);
                        }
                        else
                        {
                            BookingRefNo = GenerateBookingNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK);
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
                            objWK.MyCommand.CommandText = "SELECT COUNT(*) FROM booking_mst_tbl BST WHERE BST.BOOKING_REF_NO='" + BookingRefNo + "'";
                            ifBkgRefExist = Convert.ToInt32(objWK.MyCommand.ExecuteScalar());
                            if (ifBkgRefExist == 1)
                            {
                                BookingRefNo = BookingRefNo.Substring(0, BookingRefNo.Length - 3) + (Convert.ToInt32(BookingRefNo.Substring(BookingRefNo.Length - 3, 3)) + 1).ToString().PadLeft(3, '0');
                            }
                        }
                    }
                    else
                    {
                        BookingRefNo = txtBookingNo;
                    }

                    objWK.MyCommand.Parameters.Clear();
                    var _with9 = objWK.MyCommand;
                    _with9.CommandType = CommandType.StoredProcedure;
                    _with9.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_INS";

                    if (Nomination)
                    {
                        _with9.Parameters.Add("FROM_FLAG_IN", 1).Direction = ParameterDirection.Input;
                    }

                    //Booking Ref No
                    _with9.Parameters.Add("BOOKING_REF_NO_IN", BookingRefNo).Direction = ParameterDirection.Input;
                    _with9.Parameters["BOOKING_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("BOOKING_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["BOOKING_DATE"])).Direction = ParameterDirection.Input;
                    _with9.Parameters["BOOKING_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["CONS_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                    _with9.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                    _with9.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;
                    _with9.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("OPERATOR_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with9.Parameters.Add("OPR_UPDATE_STATUS_IN", 1).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                        _with9.Parameters.Add("OPR_UPDATE_STATUS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"].ToString()))
                    {
                        _with9.Parameters.Add("SHIPMENT_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("SHIPMENT_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["SHIPMENT_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                    _with9.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"].ToString()))
                    {
                        _with9.Parameters.Add("CARGO_MOVE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("CARGO_MOVE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"].ToString()))
                    {
                        _with9.Parameters.Add("PYMT_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("PYMT_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("COL_PLACE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("COL_PLACE_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"].ToString()))
                    {
                        _with9.Parameters.Add("COL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("COL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["COL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("DEL_PLACE_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with9.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"].ToString()))
                    {
                        _with9.Parameters.Add("DEL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("DEL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("CB_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("CB_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("CL_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("CL_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("PACK_TYP_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("PACK_TYP_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["PACK_TYP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"].ToString()))
                    {
                        _with9.Parameters.Add("PACK_COUNT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("PACK_COUNT_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"].ToString()))
                    {
                        _with9.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("GROSS_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    if (intIsLcl == -1)
                    {
                        _with9.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with9.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"].ToString()))
                        {
                            _with9.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with9.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"].ToString()))
                        {
                            _with9.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("NET_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with9.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        _with9.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with9.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"].ToString()))
                    {
                        _with9.Parameters.Add("VOLUME_IN_CBM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("VOLUME_IN_CBM_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"].ToString()))
                    {
                        _with9.Parameters.Add("LINE_BKG_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("LINE_BKG_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["LINE_BKG_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("VESSEL_NAME_IN", dsMain.Tables["tblMaster"].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
                    _with9.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("VOYAGE_IN", dsMain.Tables["tblMaster"].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
                    _with9.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"].ToString()))
                    {
                        _with9.Parameters.Add("ETD_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"].ToString()))
                    {
                        _with9.Parameters.Add("CUT_OFF_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("CUT_OFF_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["CUT_OFF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"].ToString()))
                    {
                        _with9.Parameters.Add("ETA_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with9.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with9.Parameters.Add("STATUS_IN", dsMain.Tables["tblMaster"].Rows[0]["STATUS"]).Direction = ParameterDirection.Input;
                    _with9.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"].ToString()))
                    {
                        _with9.Parameters.Add("CUSTOMER_REF_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("CUSTOMER_REF_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["CUSTOMER_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"].ToString()))
                    {
                        _with9.Parameters.Add("DP_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("DP_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"].ToString()))
                    {
                        _with9.Parameters.Add("VESSEL_VOYAGE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("VESSEL_VOYAGE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with9.Parameters.Add("CREDIT_LIMIT_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_LIMIT"])).Direction = ParameterDirection.Input;
                    _with9.Parameters.Add("CREDIT_DAYS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_DAYS"])).Direction = ParameterDirection.Input;

                    _with9.Parameters["VESSEL_VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with9.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;

                    _with9.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"].ToString()))
                    {
                        _with9.Parameters.Add("BASE_CURRENCY_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("BASE_CURRENCY_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["BASE_CURRENCY_FK"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"].ToString()))
                    {
                        _with9.Parameters.Add("MARKS_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("MARKS_NUMBER_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"].ToString()))
                    {
                        _with9.Parameters.Add("GOODS_DESC_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with9.Parameters.Add("GOODS_DESC_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"])).Direction = ParameterDirection.Input;
                    }


                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["POO_FK"].ToString()))
                        {
                            _with9.Parameters.Add("POO_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("POO_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["POO_FK"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PFD_FK"].ToString()))
                        {
                            _with9.Parameters.Add("PFD_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("PFD_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["PFD_FK"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["HANDLING_INFO"].ToString()))
                        {
                            _with9.Parameters.Add("HANDLING_INFO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("HANDLING_INFO_IN", dsMain.Tables["tblMaster"].Rows[0]["HANDLING_INFO"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["REMARKS"].ToString()))
                        {
                            _with9.Parameters.Add("REMARKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("REMARKS_IN", dsMain.Tables["tblMaster"].Rows[0]["REMARKS"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["REMARKS_NEW"].ToString()))
                        {
                            _with9.Parameters.Add("REMARKS_NEW_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("REMARKS_NEW_IN", dsMain.Tables["tblMaster"].Rows[0]["REMARKS_NEW"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }


                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_DT"].ToString()))
                        {
                            _with9.Parameters.Add("LINE_BKG_DT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("LINE_BKG_DT_IN", dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_DT"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }



                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PO_NUMBER"].ToString()))
                        {
                            _with9.Parameters.Add("PO_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("PO_NUMBER_IN", dsMain.Tables["tblMaster"].Rows[0]["PO_NUMBER"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PO_DATE"].ToString()))
                        {
                            _with9.Parameters.Add("PO_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("PO_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["PO_DATE"])).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SALES_CALL_FK"].ToString()))
                        {
                            _with9.Parameters.Add("SALES_CALL_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with9.Parameters.Add("SALES_CALL_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["SALES_CALL_FK"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;


                    _with9.ExecuteNonQuery();

                }
                else
                {
                    IsUpdate = true;

                    if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                    {
                        Int16 BookingPK = default(Int16);
                        string EBKSbr = null;
                        Int16 status = default(Int16);
                        BookingPK = Convert.ToInt16(dsMain.Tables["tblMaster"].Rows[0]["booking_mst_pk"]);
                        Chk_EBK = FetchEBKN(BookingPK);
                        EBookingRef = txtBookingNo;
                        status = Convert.ToInt16(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]);
                        if (Chk_EBK == 1 & status == 2)
                        {
                            BookingRefNo = GenerateBookingNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK);
                            if (BookingRefNo == "Protocol Not Defined.")
                            {
                                arrMessage.Add("Protocol Not Defined.");
                                return arrMessage;
                            }
                        }
                        else
                        {
                            BookingRefNo = txtBookingNo;
                        }
                        txtBookingNo = BookingRefNo;
                        EbkgRefno = BookingRef;
                        BookingRef = BookingRefNo;
                    }
                    else
                    {
                        BookingRefNo = txtBookingNo;
                    }
                    var _with10 = objWK.MyCommand;
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_UPD";

                    _with10.Parameters.Clear();
                    _with10.Parameters.Add("BOOKING_SEA_PK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["booking_mst_pk"])).Direction = ParameterDirection.Input;
                    _with10.Parameters["BOOKING_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;



                    _with10.Parameters.Add("BOOKING_REF_NO_IN", BookingRefNo).Direction = ParameterDirection.Input;
                    _with10.Parameters["BOOKING_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("BOOKING_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["BOOKING_DATE"])).Direction = ParameterDirection.Input;
                    _with10.Parameters["BOOKING_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("CUST_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CUST_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["CUST_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("CONS_CUSTOMER_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CONS_CUSTOMER_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["CONS_CUSTOMER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                    _with10.Parameters["PORT_MST_POL_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                    _with10.Parameters["PORT_MST_POD_FK_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("COMMODITY_GROUP_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COMMODITY_GROUP_FK"])).Direction = ParameterDirection.Input;
                    _with10.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;


                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("OPERATOR_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with10.Parameters.Add("OPR_UPDATE_STATUS_IN", 1).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                        _with10.Parameters.Add("OPR_UPDATE_STATUS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["OPERATOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"].ToString()))
                    {
                        _with10.Parameters.Add("SHIPMENT_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("SHIPMENT_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["SHIPMENT_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["SHIPMENT_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("CARGO_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"])).Direction = ParameterDirection.Input;
                    _with10.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"].ToString()))
                    {
                        _with10.Parameters.Add("CARGO_MOVE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("CARGO_MOVE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CARGO_MOVE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"].ToString()))
                    {
                        _with10.Parameters.Add("PYMT_TYPE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("PYMT_TYPE_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PYMT_TYPE"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("COL_PLACE_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("COL_PLACE_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["COL_PLACE_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"].ToString()))
                    {
                        _with10.Parameters.Add("COL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("COL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["COL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["COL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("DEL_PLACE_MST_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_PLACE_MST_FK"]).Direction = ParameterDirection.Input;
                    _with10.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"].ToString()))
                    {
                        _with10.Parameters.Add("DEL_ADDRESS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("DEL_ADDRESS_IN", dsMain.Tables["tblMaster"].Rows[0]["DEL_ADDRESS"]).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["DEL_ADDRESS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("CB_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("CB_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CB_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["CB_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("CL_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("CL_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CL_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["CL_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("PACK_TYP_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("PACK_TYP_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["PACK_TYP_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["PACK_TYP_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"].ToString()))
                    {
                        _with10.Parameters.Add("PACK_COUNT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("PACK_COUNT_IN", Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["PACK_COUNT"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["PACK_COUNT_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"].ToString()))
                    {
                        _with10.Parameters.Add("GROSS_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("GROSS_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["GROSS_WEIGHT"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["GROSS_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                    if (intIsLcl == -1)
                    {
                        _with10.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with10.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"].ToString()))
                        {
                            _with10.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with10.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"].ToString()))
                        {
                            _with10.Parameters.Add("NET_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("NET_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["NET_WEIGHT"])).Direction = ParameterDirection.Input;
                        }
                        _with10.Parameters["NET_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        _with10.Parameters.Add("CHARGEABLE_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        _with10.Parameters["CHARGEABLE_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"].ToString()))
                    {
                        _with10.Parameters.Add("VOLUME_IN_CBM_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("VOLUME_IN_CBM_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["VOLUME_IN_CBM"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["VOLUME_IN_CBM_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"].ToString()))
                    {
                        _with10.Parameters.Add("LINE_BKG_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("LINE_BKG_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["LINE_BKG_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("VESSEL_NAME_IN", dsMain.Tables["tblMaster"].Rows[0]["VESSEL_NAME"]).Direction = ParameterDirection.Input;
                    _with10.Parameters["VESSEL_NAME_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("VOYAGE_IN", dsMain.Tables["tblMaster"].Rows[0]["VOYAGE"]).Direction = ParameterDirection.Input;
                    _with10.Parameters["VOYAGE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"].ToString()))
                    {
                        _with10.Parameters.Add("ETD_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("ETD_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETD_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["ETD_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"].ToString()))
                    {
                        _with10.Parameters.Add("CUT_OFF_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with10.Parameters.Add("CUT_OFF_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["CUT_OFF_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["CUT_OFF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"].ToString()))
                    {
                        _with10.Parameters.Add("ETA_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with10.Parameters.Add("ETA_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["ETA_DATE"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["ETA_DATE_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("STATUS_IN", dsMain.Tables["tblMaster"].Rows[0]["STATUS"]).Direction = ParameterDirection.Input;
                    _with10.Parameters["STATUS_IN"].SourceVersion = DataRowVersion.Current;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with10.Parameters.Add("SHIPPING_TERMS_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["SHIPPING_TERMS_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["SHIPPING_TERMS_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"].ToString()))
                    {
                        _with10.Parameters.Add("CUSTOMER_REF_NO_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with10.Parameters.Add("CUSTOMER_REF_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["CUSTOMER_REF_NO"]).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["CUSTOMER_REF_NO_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"].ToString()))
                    {
                        _with10.Parameters.Add("DP_AGENT_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with10.Parameters.Add("DP_AGENT_MST_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["DP_AGENT_MST_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["DP_AGENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"].ToString()))
                    {
                        _with10.Parameters.Add("VESSEL_VOYAGE_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with10.Parameters.Add("VESSEL_VOYAGE_FK_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["VESSEL_VOYAGE_FK"])).Direction = ParameterDirection.Input;
                    }
                    _with10.Parameters["VESSEL_VOYAGE_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with10.Parameters.Add("CREDIT_LIMIT_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_LIMIT"])).Direction = ParameterDirection.Input;
                    _with10.Parameters.Add("CREDIT_DAYS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["CREDIT_DAYS"])).Direction = ParameterDirection.Input;


                    _with10.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;


                    _with10.Parameters.Add("VERSION_NO_IN", dsMain.Tables["tblMaster"].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;
                    _with10.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;

                    _with10.Parameters.Add("CONFIG_PK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;

                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"].ToString()))
                    {
                        _with10.Parameters.Add("MARKS_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with10.Parameters.Add("MARKS_NUMBER_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["MARKS_NUMBER"])).Direction = ParameterDirection.Input;
                    }
                    if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"].ToString()))
                    {
                        _with10.Parameters.Add("GOODS_DESC_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    }
                    else
                    {
                        _with10.Parameters.Add("GOODS_DESC_IN", Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["GOODS_DESC"])).Direction = ParameterDirection.Input;
                    }
                    //-------------------------POO
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["POO_FK"].ToString()))
                        {
                            _with10.Parameters.Add("POO_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("POO_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["POO_FK"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    //--------------------------PFD
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PFD_FK"].ToString()))
                        {
                            _with10.Parameters.Add("PFD_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("PFD_FK_IN", dsMain.Tables["tblMaster"].Rows[0]["PFD_FK"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    //--------------------------HANDLING INFO
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["HANDLING_INFO"].ToString()))
                        {
                            _with10.Parameters.Add("HANDLING_INFO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("HANDLING_INFO_IN", dsMain.Tables["tblMaster"].Rows[0]["HANDLING_INFO"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    //--------------------------REMARKS
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["REMARKS"].ToString()))
                        {
                            _with10.Parameters.Add("REMARKS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("REMARKS_IN", dsMain.Tables["tblMaster"].Rows[0]["REMARKS"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    //-------------------------Remarks_New
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["REMARKS_NEW"].ToString()))
                        {
                            _with10.Parameters.Add("REMARKS_NEW_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("REMARKS_NEW_IN", dsMain.Tables["tblMaster"].Rows[0]["REMARKS_NEW"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }

                    //-------------------------LINE_BKG_DT
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_DT"].ToString()))
                        {
                            _with10.Parameters.Add("LINE_BKG_DT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("LINE_BKG_DT_IN", dsMain.Tables["tblMaster"].Rows[0]["LINE_BKG_DT"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    //--------------------------PO NUMBER
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PO_NUMBER"].ToString()))
                        {
                            _with10.Parameters.Add("PO_NUMBER_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("PO_NUMBER_IN", dsMain.Tables["tblMaster"].Rows[0]["PO_NUMBER"]).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    //--------------------------PO DATE
                    try
                    {
                        if (string.IsNullOrEmpty(dsMain.Tables["tblMaster"].Rows[0]["PO_DATE"].ToString()))
                        {
                            _with10.Parameters.Add("PO_DATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with10.Parameters.Add("PO_DATE_IN", Convert.ToDateTime(dsMain.Tables["tblMaster"].Rows[0]["PO_DATE"])).Direction = ParameterDirection.Input;
                        }
                    }
                    catch (Exception ex)
                    {
                    }


                    _with10.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with10.ExecuteNonQuery();
                }
                if (string.Compare(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value), "booking")>0)
                {
                    arrMessage.Add(Convert.ToString(objWK.MyCommand.Parameters["RETURN_VALUE"].Value));

                    if (!IsUpdate)
                    {
                        RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
                    }

                    TRAN.Rollback();
                    return arrMessage;
                }
                else
                {
                    _PkValueMain = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                }
                arrMessage = SaveBookingOFreight(dsMain, _PkValueMain, objWK.MyCommand, IsUpdate);
                //Save Other Freights/Flat Freights
                arrMessage = SaveBookingTRN(dsMain, _PkValueMain, objWK.MyCommand, IsUpdate);
                //Save the Transaction and Freights
                if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                {
                    return arrMessage;
                }
                string JCPks = "";
                if (Convert.ToInt32(strBStatus) == 2)
                {
                    WorkFlow objWF = new WorkFlow();
                    long JobPk = 0;
                    System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                    sb.Append("SELECT COUNT(J.JOB_CARD_TRN_PK) FROM JOB_CARD_TRN J WHERE J.booking_mst_fk =" + _PkValueMain);
                    JobPk = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
                    if (JobPk == 0)
                    {
                        arrMessage = SaveJobCard(_PkValueMain, objWK, Convert.ToString(nLocationId), PODLocfk, ShipperPK, Consigne, strVoyagepk, JCPks);
                    }
                }

                //Booking Confirmed through updation
                if (IsUpdate == true & Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]) == 2)
                {
                    arrMessage = objTrackNTrace.SaveBBTrackAndTrace(Convert.ToInt32(_PkValueMain), 2, 1, "Job Card", "JOB-INS-SEA-EXP", Convert.ToInt32(nLocationId), objWK, "UPD", M_CREATED_BY_FK, "O");
                    //New Confirmed Booking
                }
                else if (IsUpdate == false & Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]) == 2)
                {
                    arrMessage = objTrackNTrace.SaveBBTrackAndTrace(Convert.ToInt32(_PkValueMain), 2, 1, "Job Card", "JOB-INS-SEA-EXP", Convert.ToInt32(nLocationId), objWK, "INS", M_CREATED_BY_FK, "O");
                }
                OracleDataReader oRead = null;
                string EmailId = null;
                string CustBID = null;
                string statusBKG = null;
                System.Text.StringBuilder strsql = new System.Text.StringBuilder();
                Int32 chk = 0;
                if (arrMessage.Count > 0)
                {
                    if (string.Compare((Convert.ToString(arrMessage[0]).ToUpper()), "SAVED")>0)
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
                            statusBKG = Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]);
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
                                Int32 Bstatus = Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]);
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

                        if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["STATUS"]) == 3)
                        {
                            //  
                            arrMessage.Clear();
                            arrMessage.Add("Booking Cancelled and JobCard Closed Sucessfully.");

                        }

                        return arrMessage;

                    }
                    else
                    {
                        if (!IsUpdate)
                        {
                            RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
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
                    RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
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
                    RollbackProtocolKey("BOOKING (SEA)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), BookingRefNo, System.DateTime.Now);
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

        public object SendMail(string MailId, string CUSTOMERID, string BkgRefnr, string EBkgRefnr, Int32 Bstatus)
        {
            System.Web.Mail.MailMessage objMail = new System.Web.Mail.MailMessage();

            string EAttach = null;
            string dsMail = null;
            Int32 intCnt = default(Int32);
            System.Text.StringBuilder strhtml = new System.Text.StringBuilder();


            try
            {
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
                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is Canceled <br><br>");
                }
                else
                {
                    objMail.Subject = "Booking Confirmation";
                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is confirmed. Please refer your Original Booking Int32:" + BkgRefnr + "<br><br>");
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                return "All Data Saved Successfully. Due To Server Problem Mail Has Not Been Sent.";
            }
        }
        public ArrayList SaveJobCard(long PkValue, WorkFlow objWF, string LocationPK, string PodLocPk, string ShipperPK = "", string ConsignePK = "", string strVoyagePk = "", string JCPKs = "")
        {

            string strValueArgument = null;
            arrMessage.Clear();


            try
            {
                var _with11 = objWF.MyCommand;
                _with11.CommandType = CommandType.StoredProcedure;
                _with11.CommandText = objWF.MyUserName + ".BB_BOOKING_SEA_PKG.JOB_CARD_SEA_EXP_TBL_INS";
                _with11.Parameters.Clear();

                _with11.Parameters.Add("BOOKING_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                _with11.Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with11.Parameters.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input;
                _with11.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with11.Parameters.Add("POD_FK_IN", getDefault(PodLocPk, DBNull.Value)).Direction = ParameterDirection.Input;
                _with11.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with11.Parameters.Add("SHIPPER_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
                _with11.Parameters["SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with11.Parameters.Add("CONSIGNEE_MST_FK_IN", getDefault(ConsignePK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with11.Parameters["CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with11.Parameters.Add("VOYAGE_PK_IN", strVoyagePk).Direction = ParameterDirection.Input;
                _with11.Parameters["VOYAGE_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with11.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with11.ExecuteNonQuery();
                strRet = (string.IsNullOrEmpty(_with11.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with11.Parameters["RETURN_VALUE"].Value.ToString());
                JCPKs = strRet;
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
                        var _with12 = SelectCommand;
                        _with12.CommandType = CommandType.StoredProcedure;
                        _with12.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_TRN_SEA_OTH_CHRG_INS";
                        SelectCommand.Parameters.Clear();


                        _with12.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with12.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with12.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with12.ExecuteNonQuery();

                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;


                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with13 = SelectCommand;
                        _with13.CommandType = CommandType.StoredProcedure;
                        _with13.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_TRN_SEA_OTH_CHRG_UPD";
                        SelectCommand.Parameters.Clear();


                        _with13.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with13.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with13.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with13.ExecuteNonQuery();

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

        public ArrayList SaveBookingCDimension(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string Measure, string Wt, string Divfac)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            try
            {

                if (!IsUpdate)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with14 = SelectCommand;
                        _with14.CommandType = CommandType.StoredProcedure;
                        _with14.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_INS";
                        SelectCommand.Parameters.Clear();


                        _with14.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with14.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with14.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with14.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with14.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with14.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with14.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with14.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with14.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with14.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;

                        //Manoharan 03Jan07: to save Measurement, weight and Division factor 
                        _with14.Parameters.Add("CARGO_MEASURE_IN", Measure).Direction = ParameterDirection.Input;
                        _with14.Parameters.Add("CARGO_WT_IN", Wt).Direction = ParameterDirection.Input;
                        _with14.Parameters.Add("CARGO_DIVFAC_IN", Divfac).Direction = ParameterDirection.Input;

                        //Return value of the proc.
                        _with14.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
                        _with14.ExecuteNonQuery();

                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;


                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with15 = SelectCommand;
                        _with15.CommandType = CommandType.StoredProcedure;
                        _with15.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_UPD";
                        SelectCommand.Parameters.Clear();

                        //BOOKING_Sea_CARGO_CALC_PK

                        _with15.Parameters.Add("BOOKING_SEA_CARGO_CALC_PK_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["BOOKING_SEA_CARGO_CALC_PK"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["BOOKING_SEA_CARGO_CALC_PK_IN"].SourceVersion = DataRowVersion.Current;
                        //Booking Sea Fk 
                        _with15.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with15.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with15.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with15.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with15.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with15.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with15.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with15.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with15.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with15.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;


                        _with15.Parameters.Add("CARGO_MEASURE_IN", Measure).Direction = ParameterDirection.Input;
                        _with15.Parameters.Add("CARGO_WT_IN", Wt).Direction = ParameterDirection.Input;
                        _with15.Parameters.Add("CARGO_DIVFAC_IN", Divfac).Direction = ParameterDirection.Input;


                        _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with15.ExecuteNonQuery();

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

        public ArrayList SaveBookingTRN(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate)
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            string CommodityFk = null;

            DataSet DSCalculator = new DataSet();
            DSCalculator = (DataSet)HttpContext.Current.Session["dsCalc"];
            int I_nRowCnt = 0;

            arrMessage.Clear();
            try
            {

                if (!IsUpdate)
                {

                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with16 = SelectCommand;
                        _with16.CommandType = CommandType.StoredProcedure;
                        _with16.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_TRN_INS";
                        SelectCommand.Parameters.Clear();


                        _with16.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with16.Parameters.Add("TRANS_REFERED_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;
                        _with16.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;

                        _with16.Parameters.Add("TRANS_REF_NO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]).Direction = ParameterDirection.Input;
                        _with16.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                        _with16.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with16.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with16.Parameters.Add("NO_OF_BOXES_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"]).Direction = ParameterDirection.Input;
                        _with16.Parameters["NO_OF_BOXES_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
                        {
                            _with16.Parameters.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with16.Parameters.Add("BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]).Direction = ParameterDirection.Input;
                        }
                        _with16.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"].ToString()))
                        {
                            _with16.Parameters.Add("QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with16.Parameters.Add("QUANTITY_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"])).Direction = ParameterDirection.Input;
                        }
                        _with16.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;


                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_VOL"].ToString()))
                        {
                            _with16.Parameters.Add("VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with16.Parameters.Add("VOLUME_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_VOL"])).Direction = ParameterDirection.Input;
                        }
                        _with16.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;


                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_WT"].ToString()))
                        {
                            _with16.Parameters.Add("WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with16.Parameters.Add("WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_WT"])).Direction = ParameterDirection.Input;
                        }
                        _with16.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        ///'
                        _with16.Parameters.Add("PACK_TYPE_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PACK_TYPE_FK"]).Direction = ParameterDirection.Input;
                        _with16.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;
                        ///''
                        _with16.Parameters.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
                        _with16.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with16.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with16.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()))
                        {
                            _with16.Parameters.Add("ALL_IN_TARIFF_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with16.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
                        }
                        _with16.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"].ToString()))
                        {
                            _with16.Parameters.Add("BUYING_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with16.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"])).Direction = ParameterDirection.Input;
                        }
                        _with16.Parameters["BUYING_RATE_IN"].SourceVersion = DataRowVersion.Current;
                        //Return value of the proc.
                        _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
                        _with16.ExecuteNonQuery();
                        if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value), "bookingtrans")>0)
                        {
                            arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
                            return arrMessage;
                        }
                        else
                        {
                            _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
                        }
                        if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
                        {
                            strValueArgument = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]);
                        }
                        else
                        {
                            strValueArgument = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]);
                            CommodityFk = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]);
                        }
                        arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate,
                            Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 
                            strValueArgument,
                           Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]), CommodityFk);
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }

                        if ((DSCalculator != null))
                        {
                            SaveBBCargoCalculator(SelectCommand, DSCalculator, Convert.ToInt64(PkValue), _PkValueTrans,
                                Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINERPK"]));
                        }

                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
                            {
                                return arrMessage;
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
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;


                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                    {
                        I_nRowCnt = nRowCnt + 1;
                        var _with17 = SelectCommand;
                        _with17.CommandType = CommandType.StoredProcedure;
                        _with17.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_TRN_UPD";

                        SelectCommand.Parameters.Clear();


                        _with17.Parameters.Add("BOOKING_TRN_SEA_PK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BOOKING_TRN_SEA_PK"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["BOOKING_TRN_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;

                        //Booking Sea Fk 
                        _with17.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with17.Parameters.Add("TRANS_REFERED_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;

                        _with17.Parameters.Add("TRANS_REF_NO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                        _with17.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with17.Parameters.Add("NO_OF_BOXES_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["NO_OF_BOXES_IN"].SourceVersion = DataRowVersion.Current;

                        if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
                        {
                            _with17.Parameters.Add("BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with17.Parameters.Add("BASIS_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        _with17.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"].ToString()))
                        {
                            _with17.Parameters.Add("QUANTITY_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"])).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with17.Parameters.Add("QUANTITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        _with17.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_VOL"].ToString()))
                        {
                            _with17.Parameters.Add("VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with17.Parameters.Add("VOLUME_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_VOL"])).Direction = ParameterDirection.Input;
                        }
                        _with17.Parameters["VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_WT"].ToString()))
                        {
                            _with17.Parameters.Add("WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with17.Parameters.Add("WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CARGO_WT"])).Direction = ParameterDirection.Input;
                        }
                        _with17.Parameters["WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                        _with17.Parameters.Add("PACK_TYPE_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["PACK_TYPE_FK"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["PACK_TYPE_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with17.Parameters.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with17.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with17.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()))
                        {
                            _with17.Parameters.Add("ALL_IN_TARIFF_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with17.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
                        }
                        _with17.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;
                        if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"].ToString()))
                        {
                            _with17.Parameters.Add("BUYING_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with17.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"])).Direction = ParameterDirection.Input;
                        }
                        _with17.Parameters["BUYING_RATE_IN"].SourceVersion = DataRowVersion.Current;
                        //Return value of the proc.
                        _with17.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
                        _with17.ExecuteNonQuery();
                        if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value), "bookingtrans")>0)
                        {
                            arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
                            return arrMessage;
                        }
                        else
                        {
                            _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
                        }
                        if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
                        {
                            strValueArgument = Convert.ToString(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"], ""));
                        }
                        else
                        {
                            strValueArgument = Convert.ToString(getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"], ""));
                        }
                        CommodityFk = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]);

                        arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate,
                            Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 
                            strValueArgument,
                            Convert.ToString(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]), 
                            CommodityFk);
                        //Added by subhransu for BB Cargo Calculator
                        if ((DSCalculator != null))
                        {
                            SaveBBCargoCalculator(SelectCommand, DSCalculator, Convert.ToInt64(PkValue), _PkValueTrans,
                                Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINERPK"]));
                        }
                        //End
                        if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                        {
                            return arrMessage;
                        }

                        if (Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 1 | Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"] )== 2)
                        {
                            arrMessage = (ArrayList)UpdateUpStream(dsMain, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]), Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 0);

                            if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
                            {
                                arrMessage.Add("Upstream Updation failed, Please check for valid Data");
                                return arrMessage;
                            }
                        }
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

        public ArrayList SaveBookingFRT(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string strContractRefNo, string strValueArgument, string isLcl, string CommodityFk = "")
        {
            Int32 nRowCnt = default(Int32);
            WorkFlow objWK = new WorkFlow();
            DataView dv_Freight = new DataView();

            Int16 Check = default(Int16);
            dv_Freight = getDataView(dsMain.Tables["tblFreight"], strContractRefNo, strValueArgument, isLcl, CommodityFk);
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
                if (!IsUpdate)
                {
                    var _with18 = SelectCommand;
                    _with18.CommandType = CommandType.StoredProcedure;
                    _with18.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_TRN_FRT_INS";
                    for (nRowCnt = 0; nRowCnt <= dv_Freight.Table.Rows.Count - 1; nRowCnt++)
                    {
                        if (!string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"].ToString()))
                        {
                            if (dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"] == "1")
                            {
                                SelectCommand.Parameters.Clear();
                                //Booking Transaction Sea(Fk)
                                _with18.Parameters.Add("BOOKING_SEA_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                                _with18.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                                _with18.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                                _with18.Parameters.Add("CURRENCY_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                                _with18.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                                if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"].ToString()))
                                {
                                    _with18.Parameters.Add("MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with18.Parameters.Add("MIN_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])).Direction = ParameterDirection.Input;
                                }
                                _with18.Parameters["MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

                                if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"].ToString()))
                                {
                                    _with18.Parameters.Add("TARIFF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with18.Parameters.Add("TARIFF_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])).Direction = ParameterDirection.Input;
                                }
                                _with18.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;

                                //'SURCHARGE
                                if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"].ToString()))
                                {
                                    _with18.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with18.Parameters.Add("SURCHARGE_IN", (dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                                }
                                //'SURCHARGE

                                _with18.Parameters.Add("PYMT_TYPE_IN", dv_Freight.Table.Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                                _with18.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                                _with18.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"]).Direction = ParameterDirection.Input;
                                _with18.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;

                                _with18.Parameters.Add("ROE_IN", dv_Freight.Table.Rows[nRowCnt]["RATE"]).Direction = ParameterDirection.Input;
                                _with18.Parameters["ROE_IN"].SourceVersion = DataRowVersion.Current;


                                //Return value of the proc.
                                _with18.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                //Execute the command
                                _with18.ExecuteNonQuery();
                            }
                        }

                    }
                }
                else
                {
                    var _with19 = SelectCommand;
                    _with19.CommandType = CommandType.StoredProcedure;
                    _with19.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_TRN_FRT_UPD";
                    for (nRowCnt = 0; nRowCnt <= dv_Freight.Table.Rows.Count - 1; nRowCnt++)
                    {
                        SelectCommand.Parameters.Clear();
                        if (!string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"].ToString()))
                        {
                            //Contract Transaction Pk
                            _with19.Parameters.Add("BOOKING_TRN_SEA_FRT_PK_IN", dv_Freight.Table.Rows[nRowCnt]["BOOKING_TRN_SEA_FRT_PK"]).Direction = ParameterDirection.Input;
                            _with19.Parameters["BOOKING_TRN_SEA_FRT_PK_IN"].SourceVersion = DataRowVersion.Current;


                            _with19.Parameters.Add("BOOKING_SEA_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                            _with19.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                            _with19.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                            _with19.Parameters.Add("CURRENCY_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                            _with19.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


                            if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"].ToString()))
                            {
                                _with19.Parameters.Add("SURCHARGE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with19.Parameters.Add("SURCHARGE_IN", (dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
                            }
                            //'SURCHARGE

                            if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"].ToString()))
                            {
                                _with19.Parameters.Add("MIN_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with19.Parameters.Add("MIN_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])).Direction = ParameterDirection.Input;
                            }
                            _with19.Parameters["MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

                            if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"].ToString()))
                            {
                                try
                                {
                                    _with19.Parameters.Add("TARIFF_RATE_IN", DBNull.Value).Direction = ParameterDirection.Input;
                                }
                                catch (Exception ex)
                                {
                                    _with19.Parameters.Add("TARIFF_RATE_IN", 0).Direction = ParameterDirection.Input;
                                }
                            }
                            else
                            {
                                _with19.Parameters.Add("TARIFF_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])).Direction = ParameterDirection.Input;
                            }

                            _with19.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;

                            _with19.Parameters.Add("PYMT_TYPE_IN", dv_Freight.Table.Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                            _with19.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                            _with19.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"]).Direction = ParameterDirection.Input;
                            _with19.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;

                            _with19.Parameters.Add("ROE_IN", dv_Freight.Table.Rows[nRowCnt]["RATE"]).Direction = ParameterDirection.Input;
                            _with19.Parameters["ROE_IN"].SourceVersion = DataRowVersion.Current;

                            //Return value of the proc.
                            _with19.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            //Execute the command
                            _with19.ExecuteNonQuery();
                        }
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
        public Int16 FetchEBFrt(long BkgPk)
        {
            string sql = "";
            string res = "";
            Int16 check = 0;
            WorkFlow objWK = new WorkFlow();
            sql = "select BOOKING_TRN_SEA_FK from BOOKING_TRN_SEA_FRT_DTLS where BOOKING_TRN_SEA_FK='" + BkgPk + "'";
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
                var _with20 = SelectCommand;
                _with20.CommandType = CommandType.StoredProcedure;
                _with20.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_UPDATE_UPSTREAM";
                SelectCommand.Parameters.Clear();

                _with20.Parameters.Add("TRANS_REFERED_FROM_IN", Convert.ToInt64(strTranType)).Direction = ParameterDirection.Input;
                _with20.Parameters.Add("TRANS_REF_NO_IN", Convert.ToString(strContractRefNo)).Direction = ParameterDirection.Input;

                _with20.Parameters.Add("ISUPDATE", IsUpdate.ToString()).Direction = ParameterDirection.Input;

                _with20.Parameters.Add("BOOKING_STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;

                //Return value of the proc.
                _with20.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //Execute the command
                _with20.ExecuteNonQuery();
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

        private DataView getDataView(DataTable dtFreight, string strContractRefNo, string strValueArgument, string isLcl, string CommodityFk = "")
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
                        if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], "") & CommodityFk == getDefault(dtFreight.Rows[nRowCnt]["COMMODITYPK"], "") & strValueArgument == getDefault(dtFreight.Rows[nRowCnt]["BASISPK"], ""))
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
        //Filter Container or basis freights on their refno or to corresponding transaction belonging to

        public bool CheckActiveJobCard(string strABEPk)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            short intCnt = 0;
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;

            strBuilder.Append(" UPDATE JOB_CARD_TRN J ");
            strBuilder.Append(" SET J.JOB_CARD_STATUS = 2, J.JOB_CARD_CLOSED_ON = SYSDATE ");
            strBuilder.Append(" WHERE J.booking_mst_fk = " + strABEPk);

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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }


        public string GenerateBookingNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("BOOKING (SEA)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, objWK);
            return functionReturnValue;
        }

        public string GenerateNominationNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            return GenerateProtocolKey("NOMINATION SEA", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, objWK);
        }

        #endregion

        #region "Check Customer Credit Status"

        public bool funCheckCustCredit(DataSet dsMain, long lngCustomerPk, long CreditLimit)
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

                if ((Temp != null) | Convert.ToInt32(Temp) > 0)
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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

                if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
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
                if (!(Convert.ToDouble(Convert.ToDouble(dt.Rows[0][0]) - dblBookingAmt) >= 0))
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
                    if (((Convert.ToInt32(dt.Rows[0][1]) - dblBookingAmt) >= 0))
                    {
                        CreditLimit = Convert.ToInt64(dblBookingAmt);
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

        #endregion

        #region "Retrive Address for Quotation for Sea"
        public object funRAddressSea(string strQRefNo)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            strBuilder.Append("SELECT " );
            strBuilder.Append("QST.COL_PLACE_MST_FK, PMTC.PLACE_CODE, QST.COL_ADDRESS, " );
            strBuilder.Append("QST.DEL_PLACE_MST_FK, PMTD.PLACE_CODE, QST.DEL_ADDRESS " );
            strBuilder.Append("FROM QUOTATION_SEA_TBL QST, PLACE_MST_TBL PMTC,PLACE_MST_TBL PMTD " );
            strBuilder.Append("WHERE " );
            strBuilder.Append("QST.COL_PLACE_MST_FK = PMTC.PLACE_PK " );
            strBuilder.Append("AND QST.DEL_PLACE_MST_FK=PMTD.PLACE_PK " );
            strBuilder.Append("AND QST.QUOTATION_REF_NO='" + strQRefNo + "'");
            try
            {
                dt = objwf.GetDataTable(strBuilder.ToString());
                return dt;
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
        #endregion

        #region "Check for JobCard existence"
        public string FunJobExist(string strSBEPk)
        {
            try
            {
                bool boolFound = false;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                string strReturn = null;
                strBuilder.Append("SELECT JCSET.JOB_CARD_TRN_PK FROM JOB_CARD_TRN JCSET " );
                strBuilder.Append("WHERE JCSET.booking_mst_fk=" + strSBEPk);
                strReturn = objWF.ExecuteScaler(strBuilder.ToString());
                return strReturn;
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
        #endregion

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

            try
            {
                Int64 k = default(Int64);
                strHazFK = "1";
                strRType = "1";
                string strRString = "";
                for (i = 5; i <= arrCond.Length - 1; i++)
                {
                    if (!string.IsNullOrEmpty(Convert.ToString(arrCond.GetValue(i))))
                    {
                        strCommodityFk = Convert.ToString(Convert.ToString(arrCond.GetValue(i)));
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                var _with21 = objWFT.MyCommand.Parameters;
                _with21.Add("POL_PK_IN", strPolPk).Direction = ParameterDirection.Input;
                _with21.Add("POD_PK_IN", strPodPk).Direction = ParameterDirection.Input;
                _with21.Add("COMMODITY_MST_FK_IN", getDefault(strcommodityfk, DBNull.Value)).Direction = ParameterDirection.Input;
                _with21.Add("S_DATE_IN", strsdate).Direction = ParameterDirection.Input;
                _with21.Add("BUSINESS_TYPE_IN", strBType).Direction = ParameterDirection.Input;
                _with21.Add("HAZARDOUS_IN", strHazFK).Direction = ParameterDirection.Input;
                _with21.Add("RESTRICTION_TYPE_IN", strRType).Direction = ParameterDirection.Input;
                _with21.Add("RES_CURSOR_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (objWFT.ExecuteCommands() == true)
                {
                    Da.SelectCommand = objWFT.MyCommand;
                    Da.Fill(ds);
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
                                            strRestriction += FindCommodityID(strCommodityPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindCommodityID(strCommodityPK);
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
                                            strRestriction += FindPID(strPodPK);
                                            strRestriction += "," + FindPID(strPodPK);
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindPID(strPodPK);
                                            strRestriction += "," + FindPID(strPodPK);
                                        }
                                        intPLPD = 1;
                                    }
                                    break;
                            }
                        }
                    }
                }
                return strRestriction;
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

        public string FindCommodityID(string strCommodityFK = "")
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strBuilder.Append("select COMMODITY_ID from COMMODITY_MST_TBL where COMMODITY_MST_PK = " + strCommodityFK + " ");
                string ComName = null;
                ComName = objWF.ExecuteScaler(strBuilder.ToString());
                return ComName;
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

        public string FindPID(string strPFK = "")
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strBuilder.Append("select PORT_ID from PORT_MST_TBL where PORT_MST_PK= " + strPFK + " ");
                string strPName = null;
                strPName = objWF.ExecuteScaler(strBuilder.ToString());
                return strPName;
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

        public string FindCID(string strPFK = "")
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                strBuilder.Append("select COUNTRY_ID from COUNTRY_MST_TBL where COUNTRY_MST_PK= (SELECT COUNTRY_MST_FK " );
                strBuilder.Append("FROM PORT_MST_TBL WHERE PORT_MST_PK=" + strPFK + ")");
                string strCName = null;
                strCName = objWF.ExecuteScaler(strBuilder.ToString());
                return strCName;
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

        #endregion

        #region "Enhance Search Functions "

        public string FetchBookingAddressSea(string strCond)
        {

            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            string strAddress = "";
            Int32 j = default(Int32);
            strSql = "SELECT " +  "QST.COL_PLACE_MST_FK, PMTC.PLACE_CODE, QST.COL_ADDRESS, " +  "QST.DEL_PLACE_MST_FK, PMTD.PLACE_CODE, QST.DEL_ADDRESS " +  "FROM QUOTATION_SEA_TBL QST, PLACE_MST_TBL PMTC,PLACE_MST_TBL PMTD " +  "WHERE " +  "QST.COL_PLACE_MST_FK = PMTC.PLACE_PK " +  "AND QST.DEL_PLACE_MST_FK=PMTD.PLACE_PK " +  "AND QST.QUOTATION_REF_NO='" + strCond + "'";
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
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                var _with22 = SCM.Parameters;
                _with22.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with22.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with22.Add("PORT", ifDBNull(Port)).Direction = ParameterDirection.Input;
                _with22.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with22.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                var _with23 = SCM.Parameters;
                _with23.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with23.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with23.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with23.Add("PODPK_IN", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
                _with23.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
            if (arr.Length > 2)
                strBusinessType = Convert.ToString(arr.GetValue(2));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_PACKTYPE_COMMON";
                var _with24 = SCM.Parameters;
                _with24.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with24.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with24.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with24.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
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
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_QUOTATION_NO";
                var _with25 = SCM.Parameters;
                _with25.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with25.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with25.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with25.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with25.Add("POL_IN", getDefault(strPOL, DBNull.Value)).Direction = ParameterDirection.Input;
                _with25.Add("POD_IN", getDefault(strPod, DBNull.Value)).Direction = ParameterDirection.Input;
                _with25.Add("CUSTPK_IN", getDefault(strShipper, DBNull.Value)).Direction = ParameterDirection.Input;
                _with25.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;

                _with25.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = null;
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                var _with26 = SCM.Parameters;
                _with26.Add("CUSTOMER_PK_IN", ifDBNull(strCustomerPk)).Direction = ParameterDirection.Input;
                _with26.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with26.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with26.Add("COMMODITY_PK_IN", ifDBNull(strCommodityPk)).Direction = ParameterDirection.Input;
                _with26.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with26.Add("CONT_BASIS_IN", ifDBNull(strContBasis)).Direction = ParameterDirection.Input;
                _with26.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with26.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
            if (arr.Length > 5)
                strVES = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                strVOY = Convert.ToString(arr.GetValue(6));
            if (arr.Length > 7)
                strImp = Convert.ToString(arr.GetValue(7));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.VESSEL_VOYAGE_BOOKING";
                var _with27 = SCM.Parameters;
                _with27.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with27.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with27.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with27.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with27.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with27.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with27.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with27.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with27.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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
                var _with28 = SCM.Parameters;
                _with28.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with28.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with28.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
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

        #endregion


        #endregion

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
                        strQuery.Append("select distinct cargo_measurement, cargo_weight_in, cargo_division_fact " );
                        strQuery.Append("from BOOKING_CARGO_CALC where " );
                        strQuery.Append("booking_mst_fk = " + pk);
                        return objWF.GetDataSet(strQuery.ToString());
                    }
                }
                return null;
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
        #endregion

        #region "Fetch Location of Port "
        public DataSet fetch_Port_Location_Fk(string strPODfk, string JCDate = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                if (string.IsNullOrEmpty(JCDate))
                {
                    strQuery.Append(" SELECT DISTINCT A.LOCATION_MST_PK, A.LOCATION_ID " );
                    strQuery.Append(" FROM LOCATION_MST_TBL A, PORT_MST_TBL B, LOCATION_WORKING_PORTS_TRN C " );
                    strQuery.Append(" WHERE A.LOCATION_MST_PK = B.LOCATION_MST_FK" );
                    strQuery.Append(" AND B.LOCATION_MST_FK = C.LOCATION_MST_FK" );
                    strQuery.Append(" AND B.PORT_MST_PK=" + strPODfk );
                    strQuery.Append("" );
                }
                else
                {
                    strQuery.Append("SELECT DISTINCT LOC.LOCATION_MST_PK,");
                    strQuery.Append("                LOC.LOCATION_ID,");
                    strQuery.Append("                CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                GET_EX_RATE(CNT.CURRENCY_MST_FK,");
                    strQuery.Append("                            " + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",");
                    strQuery.Append("                            TO_DATE('" + JCDate + "', 'DD/MM/YYYY')) ROE");
                    strQuery.Append("  FROM LOCATION_MST_TBL           LOC,");
                    strQuery.Append("       PORT_MST_TBL               PORT,");
                    strQuery.Append("       LOCATION_WORKING_PORTS_TRN LOCP,");
                    strQuery.Append("       COUNTRY_MST_TBL            CNT");
                    strQuery.Append(" WHERE LOC.LOCATION_MST_PK = PORT.LOCATION_MST_FK");
                    strQuery.Append("   AND PORT.LOCATION_MST_FK = LOCP.LOCATION_MST_FK");
                    strQuery.Append("   AND CNT.COUNTRY_MST_PK = LOC.COUNTRY_MST_FK");
                    strQuery.Append("   AND PORT.PORT_MST_PK =" + strPODfk);
                }
                return ObjWk.GetDataSet(strQuery.ToString());
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
        #endregion

        #region "GetJobPkRegion"
        public string GetJobPk(string bPK, object refno)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            string strJobpk = "";
            Int32 j = default(Int32);
            strBuilder.Append("SELECT J.JOB_CARD_TRN_PK,J.JOBCARD_REF_NO FROM JOB_CARD_TRN J WHERE J.booking_mst_fk=" + bPK);
            try
            {
                dt = objwf.GetDataTable(strBuilder.ToString());
                if (dt.Rows.Count > 0)
                {
                    strJobpk = dt.Rows[0][0].ToString();
                    refno = dt.Rows[0][1];
                }
                return strJobpk;
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

        #endregion

        #region "Vessel/voyage saving"

        public ArrayList SaveVesselMaster(long dblVesselPK, string strVesselName, long dblOperatorFK, string strVesselID, string VoyNo, OracleCommand SelectCommand, long POLPK, string PODPK, System.DateTime POLETA, System.DateTime POLETD,
        System.DateTime POLCUT, System.DateTime PODETA, System.DateTime ATDPOL, System.DateTime ATAPOD)
        {


            WorkFlow objWK = new WorkFlow();

            int RESULT = 0;
            try
            {
                if (dblVesselPK == 0)
                {
                    OracleCommand InsCommand = new OracleCommand();
                    Int16 VER = default(Int16);
                    var _with29 = SelectCommand;
                    _with29.Parameters.Clear();
                    _with29.CommandType = CommandType.StoredProcedure;
                    _with29.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TBL_INS";
                    _with29.Parameters.Add("OPERATOR_MST_FK_IN", dblOperatorFK).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("VESSEL_NAME_IN", strVesselName).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("VESSEL_ID_IN", strVesselID).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("ACTIVE_IN", 1).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with29.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    RESULT = _with29.ExecuteNonQuery();
                    dblVesselPK = Convert.ToInt32(_with29.Parameters["RETURN_VALUE"].Value);
                    _with29.Parameters.Clear();

                }

                var _with30 = SelectCommand;
                _with30.Parameters.Clear();
                _with30.CommandType = CommandType.StoredProcedure;
                _with30.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_INS";
                _with30.Parameters.Add("VESSEL_VOYAGE_TBL_FK_IN", dblVesselPK).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("VOYAGE_IN", VoyNo).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("PORT_MST_POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("PORT_MST_POD_FK_IN", getDefault(PODPK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("POL_ETA_IN", getDefault((POLETA == DateTime.MinValue ? DateTime.MinValue : POLETA), DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("POL_ETD_IN", getDefault((POLETD == DateTime.MinValue ? DateTime.MinValue : POLETD), DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("POL_CUT_OFF_DATE_IN", getDefault((POLCUT == DateTime.MinValue ? DateTime.MinValue : POLCUT), DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("POD_ETA_IN", getDefault((PODETA == DateTime.MinValue ? DateTime.MinValue : PODETA), DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("ATD_POL_IN", getDefault((ATDPOL == DateTime.MinValue ? DateTime.MinValue : ATDPOL), DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("ATA_POD_IN", getDefault((ATAPOD == DateTime.MinValue ? DateTime.MinValue : ATAPOD), DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("CUSTOMS_CALL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("CAPTAIN_NAME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("PORT_CALL_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("NRT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("GRT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with30.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 4, "VOYAGE_TRN_PK").Direction = ParameterDirection.Output;
                RESULT = _with30.ExecuteNonQuery();
                dblVesselPK = Convert.ToInt32(_with30.Parameters["RETURN_VALUE"].Value);
                _with30.Parameters.Clear();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;

            }
            catch (Exception ex)
            {
                if (string.Compare(ex.Message, "ORA-00001")>0)
                {
                    arrMessage.Add("Vessel or Voyage Already Exist in Database.");
                }
                else
                {
                    arrMessage.Add(ex.Message);
                }
                return arrMessage;
            }
        }
        #endregion

        #region "FETCH QUOTATION PK"
        public string Fetch_Quote_Pk(string ref_nr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append(" select QMAIN.QUOTATION_SEA_PK from quotation_sea_tbl  QMAIN where QMAIN.QUOTATION_REF_NO = '" + ref_nr + "'");
                return ObjWk.ExecuteScaler(strQuery.ToString());
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

        public Int16 FetchEBKTrans(Int16 BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            Int16 IS_EBOOKING_CHK = 0;
            try
            {
                sqlStr = " SELECT distinct btrn.booking_trn_sea_pk  FROM booking_mst_tbl Bhdr,booking_trn Btrn ";
                sqlStr += "  where bhdr.booking_mst_pk = btrn.booking_mst_fk  ";
                sqlStr += "  and bhdr.booking_mst_pk = " + BookingPK;

                IS_EBOOKING_CHK = Convert.ToInt16(objWF.ExecuteScaler(sqlStr));
                return IS_EBOOKING_CHK;
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
        #endregion

        #region "COmmodity Grid Query"
        public DataSet Fetch_Commodity(int BkgPk, int QuotPk)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                if (BkgPk > 0)
                {
                    sb.Append("SELECT ROWNUM SLNO,");
                    sb.Append("       CG.COMMODITY_GROUP_PK,");
                    sb.Append("       CG.COMMODITY_GROUP_CODE,");
                    sb.Append("       CMT.COMMODITY_MST_PK,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME,");
                    sb.Append("       '1' SEL");
                    sb.Append("  FROM booking_trn BT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL CG,");
                    sb.Append("       COMMODITY_MST_TBL       CMT");
                    sb.Append(" WHERE BT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
                    sb.Append("   AND CMT.COMMODITY_GROUP_FK = CG.COMMODITY_GROUP_PK");
                    sb.Append("   AND BT.booking_mst_fk = " + BkgPk);
                }
                else if (QuotPk > 0)
                {
                    sb.Append("SELECT ROWNUM SLNO, CG.COMMODITY_GROUP_PK,");
                    sb.Append("       CG.COMMODITY_GROUP_CODE,");
                    sb.Append("       CMT.COMMODITY_MST_PK,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME,");
                    sb.Append("       '1' SEL");
                    sb.Append("  FROM QUOTATION_TRN_SEA_FCL_LCL QT,");
                    sb.Append("       COMMODITY_MST_TBL         CMT,");
                    sb.Append("       COMMODITY_GROUP_MST_TBL   CG");
                    sb.Append(" WHERE QT.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
                    sb.Append("   AND CMT.COMMODITY_GROUP_FK = CG.COMMODITY_GROUP_PK");
                    sb.Append("   AND QT.QUOTATION_SEA_FK = " + QuotPk);
                }
                else
                {
                    sb.Append("SELECT ROWNUM SLNO,CG.COMMODITY_GROUP_PK,");
                    sb.Append("       CG.COMMODITY_GROUP_CODE,");
                    sb.Append("       CMT.COMMODITY_MST_PK,");
                    sb.Append("       CMT.COMMODITY_ID,");
                    sb.Append("       CMT.COMMODITY_NAME,");
                    sb.Append("       '' SEL");
                    sb.Append("  FROM COMMODITY_GROUP_MST_TBL CG, COMMODITY_MST_TBL CMT");
                    sb.Append(" WHERE CG.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK AND 1=2");
                }

                return objWF.GetDataSet(sb.ToString());
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
        #endregion

        #region "Cargo Calculator"
        public int SaveBBCargoCalculator(OracleCommand SelectCommand, DataSet M_DataSet, Int64 BkgHeaderPK, Int64 BkgCargoPK, int slno = 0)
        {
            WorkFlow objWK = new WorkFlow();
            
            long i = 0;
            var strNull = DBNull.Value;
            double LtoMT = 0.98421;
            double FTOM = 0.3048;
            try
            {
                if (M_DataSet.Tables[0].Rows.Count > 0)
                {
                    for (int RowCnt = 0; RowCnt <= M_DataSet.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        if (Convert.ToInt16(M_DataSet.Tables[0].Rows[RowCnt]["CARGO_CALL_PK"]) <= 0)
                        {
                            if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["QUOTREF"].ToString()) == slno)
                            {
                                var _with31 = SelectCommand;
                                _with31.CommandType = CommandType.StoredProcedure;
                                _with31.Parameters.Clear();
                                _with31.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_INS";
                                _with31.Parameters.Clear();

                                _with31.Parameters.Add("BOOKING_HEADER_FK_IN", Convert.ToInt32(BkgHeaderPK)).Direction = ParameterDirection.Input;
                                _with31.Parameters.Add("BOOKING_CARGO_FK_IN", Convert.ToInt32(BkgCargoPK)).Direction = ParameterDirection.Input;
                                _with31.Parameters.Add("NO_OF_PIECES_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["NOP"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["NOP"])).Direction = ParameterDirection.Input;

                                if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"] )== 1)
                                {
                                    _with31.Parameters.Add("LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("WTUNIT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"]) * LtoMT))).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Width"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Height"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("CBM_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["Cube"]) * LtoMT))).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? 0 : (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Uom"]) * LtoMT))).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with31.Parameters.Add("LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"])).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("WTUNIT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"])).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Width"])).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Height"])).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("CBM_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Cube"])).Direction = ParameterDirection.Input;
                                    _with31.Parameters.Add("WEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Uom"])).Direction = ParameterDirection.Input;
                                }

                                _with31.Parameters.Add("Measurement_In", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"].ToString()) ? 0 : M_DataSet.Tables[0].Rows[RowCnt]["Measurement"])).Direction = ParameterDirection.Input;
                                _with31.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                _with31.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                _with31.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["QUOTREF"]) == slno)
                            {
                                var _with32 = SelectCommand;
                                _with32.CommandText = objWK.MyUserName + ".BB_BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_UPD";
                                _with32.Parameters.Clear();

                                _with32.Parameters.Add("BOOKING_SEA_CARGO_CALC_PK_IN", M_DataSet.Tables[0].Rows[RowCnt]["CARGO_CALL_PK"]).Direction = ParameterDirection.Input;
                                _with32.Parameters.Add("Booking_Trn_Sea_Fk_IN", Convert.ToInt32(BkgCargoPK)).Direction = ParameterDirection.Input;
                                _with32.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt32(BkgHeaderPK)).Direction = ParameterDirection.Input;
                                _with32.Parameters.Add("CARGO_NOP_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["NOP"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["NOP"])).Direction = ParameterDirection.Input;
                                if (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"]) == 1)
                                {
                                    _with32.Parameters.Add("CARGO_LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? 0 : (Convert.ToDouble(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"].ToString()) ? 0 : (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? 0 : (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Width"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? 0 : (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Height"]) * FTOM))).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_ACTUAL_WT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? 0 : (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Cube"]) * LtoMT))).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_CUBE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? 0 : (Convert.ToInt32(M_DataSet.Tables[0].Rows[RowCnt]["Uom"]) * LtoMT))).Direction = ParameterDirection.Input;
                                }
                                else
                                {
                                    _with32.Parameters.Add("CARGO_LENGTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["LENGTH"])).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["WTUNIT"])).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_WIDTH_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Width"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Width"])).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_HEIGHT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Height"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Height"])).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_ACTUAL_WT_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Cube"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Cube"])).Direction = ParameterDirection.Input;
                                    _with32.Parameters.Add("CARGO_CUBE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Uom"].ToString()) ? "" : M_DataSet.Tables[0].Rows[RowCnt]["Uom"])).Direction = ParameterDirection.Input;
                                }
                                _with32.Parameters.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[RowCnt]["Measurement"].ToString()) ? 0 : M_DataSet.Tables[0].Rows[RowCnt]["Measurement"])).Direction = ParameterDirection.Input;
                                //.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input
                                //.Parameters.Add("LAST_MODIFIED_BY_IN", HttpContext.Current.Session("USER_PK")).Direction = ParameterDirection.Input
                                _with32.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                                _with32.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                                _with32.ExecuteNonQuery();
                            }
                        }
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return 1;
                }
                else
                {
                    return 0;
                }

            }
            catch (OracleException oraexp)
            {
                throw oraexp;
                arrMessage.Add(oraexp.Message);
            }
            catch (Exception ex)
            {
                throw ex;
                arrMessage.Add(ex.Message);
            }
        }
        #endregion

        #region "Space Request"
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
                    strCondition +=  " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(SpaceReqNr))
                {
                    strCondition +=  " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (SrStatus != "0")
                {
                    strCondition +=  "   AND BST.SR_STATUS=" + SrStatus;
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
                sb.Append("           AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
                sb.Append("           AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("           AND CTMT.CONTAINER_TYPE_MST_PK = BTSFL.CONTAINER_TYPE_MST_FK");
                sb.Append("           AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                sb.Append("           AND POL.LOCATION_MST_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
                sb.Append("              NVL(BST.VOLUME_IN_CBM, 0)");
                sb.Append("            END");
                sb.Append("         ELSE");
                sb.Append("              NVL(BST.VOLUME_IN_CBM, 0)");
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
                sb.Append("   AND POL.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
                    strCondition +=  " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(SpaceReqNr))
                {
                    strCondition +=  " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (SrStatus != "0")
                {
                    strCondition +=  "   AND BST.SR_STATUS=" + SrStatus;
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
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
                sb.Append("   AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK = BTSFL.BASIS");
                sb.Append("   AND UMT.USER_MST_PK = BST.CREATED_BY_FK");
                sb.Append("   AND BST.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                sb.Append("   AND POL.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
                sb.Append("   AND POL.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
                    strCondition +=  " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (!string.IsNullOrEmpty(SpaceReqNr))
                {
                    strCondition +=  " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
                }
                if (SrStatus != "0")
                {
                    strCondition +=  "   AND BST.SR_STATUS=" + SrStatus;
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
                sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
                sb.Append("   AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
                sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK = BTSFL.BASIS");
                sb.Append("   AND UMT.USER_MST_PK = BST.CREATED_BY_FK");
                sb.Append("   AND POL.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
                sb.Append("   AND POL.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
                strCondition = strCondition + " AND BST.FLIGHT_NO =  '" + FlightNr + "'";
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
                strCondition +=  " AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + bkgRefNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition +=  " AND UPPER(BST.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition +=  "   AND BST.SR_STATUS=" + SrStatus;
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
            sb.Append("       BST.CHARGEABLE_WEIGHT GROSSWT,");
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
            sb.Append("   AND CGMT.COMMODITY_GROUP_PK = BST.COMMODITY_GROUP_FK");
            sb.Append("   AND BST.BOOKING_MST_PK = BTSFL.BOOKING_MST_FK");
            sb.Append("   AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("   AND POL.LOCATION_MST_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
            sqlstr2.Append( "  ( Select ROWNUM SLNR, q.* from ");
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
        #endregion

        #region "Get ContainerID"
        public object GetContainerID()
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
        #endregion

        #region "Generate Space Request Int32"
        public ArrayList GenerateSpaceRequestNr(string BKGPK, string SpaceReqNr, string BizType)
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
                var _with33 = objWK.MyCommand;
                _with33.Connection = TRAN.Connection;
                _with33.CommandType = CommandType.StoredProcedure;
                if (Convert.ToInt32(BizType) == 2)
                {
                    _with33.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BKG_SPACE_REQUEST_NO_INS";
                }
                else
                {
                    _with33.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.BKG_SPACE_REQUEST_NO_INS";
                }
                _with33.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(BKGPK)).Direction = ParameterDirection.Input;
                _with33.Parameters.Add("SPACE_REQUEST_NO_IN", SpaceReqNr).Direction = ParameterDirection.Input;
                _with33.Parameters.Add("SPACE_REQUEST_STATUS_IN", 1).Direction = ParameterDirection.Input;
                _with33.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                Afct = _with33.ExecuteNonQuery();
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
        #endregion

        #region "Generete SRNUMBER"
        public string GenerateKey(string strName, long nLocPK, long nEmpPK, System.DateTime dtDate, long nUserID)
        {
            return GenerateProtocolKey(strName, nLocPK, nEmpPK, dtDate, "", "", "", nUserID);
        }
        #endregion

        #region "Space Request Report FCL"
        public DataSet FetchSpaceRequestFCL(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.OPERATOR_MST_FK=" + lblCarrierPK;
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
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BOOK.booking_mst_pk IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition +=  " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition +=  "   AND BOOK.SR_STATUS=" + SrStatus;
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
            sb.Append("               BOOK.VESSEL_NAME || '/' || BOOK.VOYAGE VESSEL_NAME,");
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
            sb.Append("               AND BTCD.BOOKING_TRN_SEA_FK(+) = BKGTRN.BOOKING_TRN_SEA_PK) ");
            sb.Append("               ELSE NVL(BOOK.GROSS_WEIGHT, 0) END GROSS_WEIGHT,");

            sb.Append("               CASE WHEN BOOK.CARGO_TYPE = 1 THEN CASE WHEN (SELECT SUM(NVL(BCD.VOLUME_IN_CBM, 0)) ");
            sb.Append("               FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD ");
            sb.Append("               WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK ");
            sb.Append("               AND BTCD.BOOKING_TRN_SEA_FK(+) = BKGTRN.BOOKING_TRN_SEA_PK) > 0 THEN ");
            sb.Append("               (SELECT SUM(NVL(BCD.VOLUME_IN_CBM, 0)) FROM BOOKING_COMMODITY_DTL BCD, BOOKING_TRN_CARGO_DTL BTCD ");
            sb.Append("               WHERE BCD.BOOKING_CARGO_DTL_FK(+) = BTCD.BOOKING_TRN_CARGO_PK ");
            sb.Append("               AND BTCD.BOOKING_TRN_SEA_FK(+) = BKGTRN.BOOKING_TRN_SEA_PK) ELSE ");
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
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK");
            sb.Append("           AND BOOK.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
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

        #endregion

        #region "Get CommodityName"
        public DataSet GetCommodityName(string BOOKINGPK)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append(" SELECT BOOK.booking_mst_pk, CMT.COMMODITY_NAME");
            sb.Append("          FROM booking_mst_tbl         BOOK,");
            sb.Append("               booking_trn BKGTRN,");
            sb.Append("                COMMODITY_MST_TBL       CMT");
            sb.Append("         WHERE BOOK.booking_mst_pk = BKGTRN.booking_mst_fk");
            sb.Append("          AND BKGTRN.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");
            sb.Append("          AND BOOK.booking_mst_pk IN (" + BOOKINGPK + ") ");
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
        #endregion

        #region "Space Request Report LCL"
        public DataSet FetchSpaceRequestLCL(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.OPERATOR_MST_FK=" + lblCarrierPK;
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
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BOOK.booking_mst_pk IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition +=  " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition +=  "   AND BOOK.SR_STATUS=" + SrStatus;
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
            sb.Append("               BOOK.VESSEL_NAME || '/' || BOOK.VOYAGE VESSEL_NAME,");
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
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK");
            sb.Append("           AND BOOK.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
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
        #endregion

        #region "Space Request Report BBC"
        public DataSet FetchSpaceRequestBBC(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string VslVoyPK = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.OPERATOR_MST_FK=" + lblCarrierPK;
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
            if (Convert.ToInt32(CommGroupfk) > 0)
            {
                strCondition = strCondition + " AND BOOK.booking_mst_pk IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition +=  " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition +=  "   AND BOOK.SR_STATUS=" + SrStatus;
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
            sb.Append("               BOOK.VESSEL_NAME || '/' || BOOK.VOYAGE VESSEL_NAME,");
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
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK");
            sb.Append("           AND BOOK.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
            sb.Append("           AND BOOK.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("           AND LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
            sb.Append("           AND BOOK.STATUS = 1");
            sb.Append("           AND BOOK.CARGO_TYPE = 4");
            sb.Append("           AND UMT.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
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
        #endregion

        #region "Space Request Report AIR"
        public DataSet FetchSpaceRequestAIR(string BOOKINGPK, string lblCarrierPK = "", string POLPK = "", string PODPK = "", string CustomerPK = "", string FlightNr = "", string ShipmentDate = "", string bkgRefNr = "", Int16 intXBkg = 0, Int16 intCLAgt = 0,
        string SpaceReqNr = "", string CommGroupfk = "", string SrStatus = "")
        {

            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            string strCondition = null;

            if (Convert.ToInt32(lblCarrierPK) > 0)
            {
                strCondition = strCondition + " AND BOOK.AIRLINE_MST_FK=" + lblCarrierPK;
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
                strCondition = strCondition + " AND BOOK.FLIGHT_NO =  '" + FlightNr + "'";
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
                strCondition = strCondition + " AND BOOK.BOOKING_AIR_PK IN (" + BOOKINGPK + " )";
            }
            if (!string.IsNullOrEmpty(SpaceReqNr))
            {
                strCondition +=  " AND UPPER(BOOK.SPACE_REQUEST_NR) LIKE '%" + SpaceReqNr.Trim().ToUpper().Replace("'", "''") + "%'";
            }
            if (SrStatus != "0")
            {
                strCondition +=  "   AND BOOK.SR_STATUS=" + SrStatus;
            }
            if (!string.IsNullOrEmpty(ShipmentDate))
            {
                strCondition = strCondition + "  AND BOOK.SHIPMENT_DATE = TO_DATE('" + ShipmentDate + "','" + dateFormat + "')   ";
            }
            sb.Append("SELECT Q.BOOKING_AIR_PK,");
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
            sb.Append("  FROM (SELECT BOOK.BOOKING_AIR_PK,");
            sb.Append("               BOOK.SPACE_REQUEST_PK,");
            sb.Append("               BOOK.SPACE_REQUEST_NR,");
            sb.Append("               BOOK.SR_STATUS,");
            sb.Append("               BOOK.SHIPMENT_DATE,");
            sb.Append("               POL.PORT_ID POL,");
            sb.Append("               POD.PORT_ID POD,");
            sb.Append("               CGMT.COMMODITY_GROUP_CODE,");
            sb.Append("               BOOK.FLIGHT_NO VESSEL_NAME,");
            sb.Append("               0 CARGO_TYPE,");
            sb.Append("               LMT.OFFICE_NAME,");
            sb.Append("               BOOK.CHARGEABLE_WEIGHT,");
            sb.Append("               BOOK.VOLUME_IN_CBM,");
            sb.Append("               AMT.AIRLINE_NAME OPERATOR_NAME");
            sb.Append("          FROM booking_mst_tbl         BOOK,");
            sb.Append("               BOOKING_TRN_AIR         BKGTRN,");
            sb.Append("               PORT_MST_TBL            POL,");
            sb.Append("               PORT_MST_TBL            POD,");
            sb.Append("               COMMODITY_GROUP_MST_TBL CGMT,");
            sb.Append("               AIRLINE_MST_TBL         AMT,");
            sb.Append("               USER_MST_TBL            UMT,");
            sb.Append("               LOCATION_MST_TBL        LMT");
            sb.Append("         WHERE BOOK.BOOKING_AIR_PK = BKGTRN.BOOKING_AIR_FK");
            sb.Append("           AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("           AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("           AND BOOK.COMMODITY_GROUP_FK = CGMT.COMMODITY_GROUP_PK");
            sb.Append("           AND BOOK.AIRLINE_MST_FK = AMT.AIRLINE_MST_PK");
            sb.Append("           AND BOOK.CREATED_BY_FK = UMT.USER_MST_PK");
            sb.Append("           AND LMT.LOCATION_MST_PK = UMT.DEFAULT_LOCATION_FK");
            sb.Append("           AND BOOK.STATUS = 1");
            sb.Append("   AND UMT.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
            sb.Append(" " + strCondition + "");
            sb.Append("   ) Q");
            sb.Append(" GROUP BY Q.BOOKING_AIR_PK,");
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
        #endregion

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
        #endregion

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

                var _with34 = updCmdUser;
                _with34.Connection = objWK.MyConnection;
                _with34.Transaction = TRAN;
                _with34.CommandType = CommandType.StoredProcedure;
                if (Convert.ToInt32(BizType) == 2)
                {
                    _with34.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.SAVE_SPACE_REQUEST_STATUS";
                }
                var _with35 = _with34.Parameters;
                _with35.Add("BOOKING_FK_IN", BooikgPK).Direction = ParameterDirection.Input;
                _with35.Add("SPACE_REQUEST_STATUS_IN", Status).Direction = ParameterDirection.Input;
                intIns = Convert.ToInt16(_with34.ExecuteNonQuery());
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
        #endregion

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

                var _with36 = updCmdUser;
                _with36.Connection = objWK.MyConnection;
                _with36.Transaction = TRAN;
                _with36.CommandType = CommandType.StoredProcedure;
                if (Convert.ToInt32(BizType) == 2)
                {
                    _with36.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.SAVE_SPACE_REQUEST_FLAG";
                    _with36.Parameters.Add("CONTAINER_TYPE_FK_IN", ContainerPK).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with36.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.SAVE_SPACE_REQUEST_FLAG";
                }
                var _with37 = _with36.Parameters;
                _with37.Add("BOOKING_FK_IN", BooikgPK).Direction = ParameterDirection.Input;
                _with37.Add("FLAG", "1").Direction = ParameterDirection.Input;
                intIns = Convert.ToInt16(_with36.ExecuteNonQuery());
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
        #endregion

        #region "Cancel Space Request Int32"
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

                var _with38 = updCmdUser;
                _with38.Connection = objWK.MyConnection;
                _with38.Transaction = TRAN;
                _with38.CommandType = CommandType.StoredProcedure;
                if (Convert.ToInt32(BizType) == 2)
                {
                    _with38.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.CANCEL_SPACE_REQUEST";
                }
                else
                {
                    _with38.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.CANCEL_SPACE_REQUEST";
                }
                _with38.Parameters.Add("BOOKING_FK_IN", BooikgPK).Direction = ParameterDirection.Input;
                intIns = Convert.ToInt16(_with38.ExecuteNonQuery());
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
        #endregion

        #endregion

        #region "Credit Control"
        public object FetchCreditDetails(string BookingPk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow ObjWk = new WorkFlow();

            strSQL.Append(" SELECT BKG.booking_mst_pk,");
            strSQL.Append(" BKG.CUST_CUSTOMER_MST_FK,");
            strSQL.Append(" BKG.CREDIT_LIMIT,");
            strSQL.Append("  BKG.BOOKING_REF_NO,");
            strSQL.Append(" SUM(BTS.ALL_IN_TARIFF) AS TOTAL");

            strSQL.Append("  FROM BOOKING_TRN_SEA_FRT_DTLS BFD,");
            strSQL.Append(" CUSTOMER_MST_TBL         CMT,");
            strSQL.Append("  booking_mst_tbl          BKG,");
            strSQL.Append("  booking_trn  BTS");

            strSQL.Append(" WHERE BKG.booking_mst_pk=BTS.booking_mst_fk(+)");
            strSQL.Append("  AND BTS.BOOKING_TRN_SEA_PK=BFD.BOOKING_TRN_SEA_FK(+)");
            strSQL.Append(" AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strSQL.Append(" AND BKG.booking_mst_pk=" + BookingPk);
            strSQL.Append(" GROUP BY BKG.booking_mst_pk,BKG.CUST_CUSTOMER_MST_FK, BKG.CREDIT_LIMIT,BKG.BOOKING_REF_NO");

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
        #endregion

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
        #endregion

    }
}