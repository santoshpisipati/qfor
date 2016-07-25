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
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class cls_SeaBookingEntry : CommonFeatures
    {

        #region "Private Variables"
        private long _PkValueMain;
        private long _PkValueTrans;
        cls_CargoDetails objCargoDetails = new cls_CargoDetails();
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
        string _Containers;
        public string Containers
        {
            get { return _Containers; }
            set { _Containers = value; }
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
                strSQL = "select booking_trn_sea_pk from booking_trn_sea_fcl_lcl where booking_sea_fk='" + PKVal + "'";
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            sb.Append("  FROM BOOKING_SEA_TBL MAIN, BOOKING_TRN_SEA_FCL_LCL TRN");
            sb.Append(" WHERE MAIN.BOOKING_SEA_PK = TRN.BOOKING_SEA_FK");
            if (Int_Bookingpk > 0)
            {
                sb.Append(" AND MAIN.BOOKING_SEA_PK=" + Int_Bookingpk);
            }

            try
            {
                return (objwf.GetDataSet(sb.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sb.Append("   FROM BOOKING_SEA_TBL          MAIN,");
                sb.Append("       BOOKING_TRN_SEA_FCL_LCL  TRN,");
                sb.Append("       BOOKING_TRN_SEA_FRT_DTLS FD");
                sb.Append("    WHERE MAIN.BOOKING_SEA_PK= TRN.BOOKING_SEA_FK");
                sb.Append("   AND TRN.BOOKING_TRN_SEA_PK = FD.BOOKING_TRN_SEA_FK");
                sb.Append("   AND MAIN.BOOKING_SEA_PK = " + EBkgpk);

                DsFreightCnt = objwf.GetDataSet(sb.ToString());

                //'for the Ebooking 
                sb.Remove(0, sb.Length);
                sb.Append("  SELECT MAIN.IS_EBOOKING FROM BOOKING_SEA_TBL MAIN");
                sb.Append("  WHERE MAIN.BOOKING_SEA_PK = " + EBkgpk);
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //If FCL then intIsLcl is 0- (//FCL=1 AND LCL=2//)
                if (Convert.ToInt32(dtMain.Rows[0]["CARGO_TYPE"]) == 1)
                {
                    intIsLcl = 0;
                    //LCL FALSE

                }
                else
                {
                    intIsLcl = 1;
                    //LCL TRUE

                }
                dtTrans = (DataTable)FetchSBEntryTrans(dtTrans, lngSBEPk, intIsLcl);
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (EBkg == 1 & Bsts == 1)
                    {
                        dtFreight = (DataTable)FetchEBKGEntryFreight(dtFreight, lngSBEPk, intIsLcl);
                        //it will work only for FCL
                    }
                    else
                    {
                        dtFreight = (DataTable)FetchSBEntryFreight(dtFreight, lngSBEPk, intIsLcl, EBkg);
                    }
                }
                else
                {
                    dtFreight = (DataTable)FetchSBEntryFreight(dtFreight, lngSBEPk, intIsLcl);
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            strBuilder.Append(" BOOKING_SEA_TBL BST, ");
            strBuilder.Append(" BOOKING_TRN_SEA_OTH_CHRG BTSOC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" BTSOC.BOOKING_SEA_FK = BST.BOOKING_SEA_PK ");
            strBuilder.Append(" AND BTSOC.BOOKING_SEA_FK= " + lngSBEPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        public Int32 FetchBkgPk(Int32 jobpk)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            //Fetch the Cargo Dimesion for the existing booking 
            strSql = "select sea.booking_sea_fk from job_card_sea_exp_tbl sea where sea.job_card_sea_exp_pk=" + jobpk;
            try
            {
                return Convert.ToInt32(objwf.ExecuteScaler(strSql));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //End Snigdharani

        public object FetchComm(long SeaBkgPK, int status)
        {
            string strSqlCDimension = "";
            WorkFlow objwf = new WorkFlow();
            try
            {
                if (status == 1)
                {
                    strSqlCDimension = " select c1.commodity_name,(select bb.pack_count  from booking_sea_tbl bb where bb.booking_sea_pk=" + SeaBkgPK + ") pack_count, " + " (select pack.pack_type_desc from PACK_TYPE_MST_TBL pack where pack.pack_type_mst_pk in (select bb.pack_typ_mst_fk from booking_sea_tbl bb where bb.booking_sea_pk=" + SeaBkgPK + ")) pack_type_desc " + " from commodity_mst_tbl c1 where c1.commodity_mst_pk in ( " + " select bb.commodity_mst_fk from BOOKING_TRN_SEA_FCL_LCL bb where bb.booking_sea_fk=" + SeaBkgPK + ") ";

                    //ElseIf status = 2 Or status = 3 Then
                }
                else
                {
                    strSqlCDimension = "select distinct(commodity_name) ," + "BST.pack_count," + "PTMT.PACK_TYPE_DESC " + "from commodity_mst_tbl CST," + "job_card_sea_exp_tbl JCSE, " + "job_trn_sea_exp_cont JTSE,BOOKING_SEA_TBL BST, " + "PACK_TYPE_MST_TBL PTMT" + "where JTSE.commodity_mst_fk=CST.commodity_mst_pk" + "and PTMT.PACK_TYPE_MST_PK=BST.PACK_TYP_MST_FK " + "and JTSE.job_card_sea_exp_fk=JCSE.job_card_sea_exp_pk  " + "and BST.booking_sea_pk=JCSE.booking_sea_fk" + "and BOOKING_SEA_PK= " + SeaBkgPK;
                }
                return objwf.GetDataSet(strSqlCDimension);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING 
            strSqlHeader.Append(" SELECT " );
            strSqlHeader.Append(" BST.BOOKING_SEA_PK, " );
            strSqlHeader.Append(" BST.BOOKING_REF_NO, " );
            strSqlHeader.Append(" BST.BOOKING_DATE, " );
            strSqlHeader.Append(" BST.CUST_CUSTOMER_MST_FK, " );

            strSqlHeader.Append(" (case when CMTCUST.CUSTOMER_ID is null then " );
            strSqlHeader.Append(" ( select customer_id from temp_customer_tbl where customer_mst_pk=BST.CUST_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   else CMTCUST.CUSTOMER_ID end ) CUSTOMERID," );

            strSqlHeader.Append(" (case when CMTCUST.CUSTOMER_NAME is null then " );
            strSqlHeader.Append(" ( select CUSTOMER_NAME from temp_customer_tbl where customer_mst_pk=BST.CUST_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   else CMTCUST.CUSTOMER_NAME end ) CUSTOMERNAME," );

            // strSqlHeader.Append(" CMTCUST.CUSTOMER_ID AS CUSTOMERID, " & vbCrLf)
            strSqlHeader.Append(" BST.CONS_CUSTOMER_MST_FK, " );

            strSqlHeader.Append(" (case when CMTCONS.CUSTOMER_ID is null then " );
            strSqlHeader.Append(" ( select customer_id from temp_customer_tbl where customer_mst_pk=BST.CONS_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   else CMTCONS.CUSTOMER_ID end ) CONSIGNEEID," );

            strSqlHeader.Append(" (case when CMTCONS.CUSTOMER_NAME is null then " );
            strSqlHeader.Append(" ( select CUSTOMER_NAME from temp_customer_tbl where customer_mst_pk=BST.CONS_CUSTOMER_MST_FK)" );
            strSqlHeader.Append("   else CMTCONS.CUSTOMER_NAME end ) CONSIGNEENAME," );

            //strSqlHeader.Append(" CMTCONS.CUSTOMER_ID AS CONSIGNEEID, " & vbCrLf)
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
            //strSqlHeader.Append(" BST.ETA_DATE, " & vbCrLf)
            //strSqlHeader.Append(" BST.ETD_DATE, " & vbCrLf)
            //strSqlHeader.Append(" BST.CUT_OFF_DATE, " & vbCrLf)
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
            strSqlHeader.Append(" BST.PO_NUMBER,  " );
            strSqlHeader.Append(" BST.PO_DATE,  " );
            strSqlHeader.Append(" BST.NOMINATION_REF_NO,  " );
            strSqlHeader.Append(" BST.SALES_CALL_FK,  " );

            strSqlHeader.Append(" EMP.EMPLOYEE_MST_PK EXECUTIVE_MST_FK,  " );
            strSqlHeader.Append(" EMP.EMPLOYEE_ID EXECUTIVE_MST_ID,  " );
            strSqlHeader.Append(" EMP.EMPLOYEE_NAME EXECUTIVE_MST_NAME,  " );
            strSqlHeader.Append(" EMP_SHP.EMPLOYEE_MST_PK SHP_EXE_MST_FK,  " );
            strSqlHeader.Append(" EMP_SHP.EMPLOYEE_ID SHP_EXE_MST_ID,  " );
            strSqlHeader.Append(" EMP_SHP.EMPLOYEE_NAME SHP_EXE_MST_NAME,  " );
            strSqlHeader.Append(" EMP_CON.EMPLOYEE_MST_PK CON_EXE_MST_FK,  " );
            strSqlHeader.Append(" EMP_CON.EMPLOYEE_ID CON_EXE_MST_ID,  " );
            strSqlHeader.Append(" EMP_CON.EMPLOYEE_NAME CON_EXE_MST_NAME,  " );
            strSqlHeader.Append(" BST.LINE_BKG_DT LINE_BKG_DT  " );

            strSqlHeader.Append(" FROM " );
            strSqlHeader.Append(" BOOKING_SEA_TBL BST, " );
            strSqlHeader.Append(" CUSTOMER_MST_TBL CMTCUST, " );
            strSqlHeader.Append(" CUSTOMER_MST_TBL CMTCONS, " );
            strSqlHeader.Append(" EMPLOYEE_MST_TBL EMP, " );
            strSqlHeader.Append(" EMPLOYEE_MST_TBL EMP_SHP, " );
            strSqlHeader.Append(" EMPLOYEE_MST_TBL EMP_CON, " );
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
            strSqlHeader.Append(" AND BST.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) " );
            strSqlHeader.Append(" AND CMTCUST.REP_EMP_MST_FK=EMP_SHP.EMPLOYEE_MST_PK(+) " );
            strSqlHeader.Append(" AND CMTCONS.REP_EMP_MST_FK=EMP_CON.EMPLOYEE_MST_PK(+) " );
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
            strSqlHeader.Append(" AND BST.BOOKING_SEA_PK= " + lngSBEPK);

            try
            {
                dtMain = objwf.GetDataTable(strSqlHeader.ToString());
                return dtMain;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public object FetchSBEntryTrans(DataTable dtTrans, long lngSBEPK, Int16 intIsLcl)
        {
            System.Text.StringBuilder strSqlGridHeader = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();

            //SELECT DATA FROM TRANSACTION TABLE FOR EXISTING BOOKING
            if (intIsLcl == 1)
            {
                strSqlGridHeader.Append("SELECT DISTINCT " );
                strSqlGridHeader.Append(" NULL AS TRNTYPEPK, " );
                strSqlGridHeader.Append(" BTSFL.TRANS_REFERED_FROM AS TRNTYPESTATUS, " );
                strSqlGridHeader.Append(" DECODE(BTSFL.TRANS_REFERED_FROM,1,'Quote',2,'Sp Rate',3,'Cust Cont',4,'SL Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') AS CONTRACTTYPE, " );
                strSqlGridHeader.Append(" BTSFL.TRANS_REF_NO AS REFNO, " );
                strSqlGridHeader.Append(" OMT.OPERATOR_ID AS OPERATOR, " );
                strSqlGridHeader.Append(" UOM.DIMENTION_ID AS BASIS, " );
                strSqlGridHeader.Append(" BTSFL.QUANTITY AS QTY, " );
                strSqlGridHeader.Append(" '' AS CARGO, " );
                //strSqlGridHeader.Append(" CMT.COMMODITY_NAME AS COMM, " & vbCrLf)
                strSqlGridHeader.Append(" '' AS COMMODITY, " );
                strSqlGridHeader.Append(" BTSFL.BUYING_RATE AS RATE, " );
                strSqlGridHeader.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_SEA_PK,2," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)AS BKGRATE, " );
                strSqlGridHeader.Append(" NULL AS NET, " );
                strSqlGridHeader.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_SEA_PK,1," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",1)AS TOTALRATE, " );
                strSqlGridHeader.Append(" '1' AS SEL, " );
                strSqlGridHeader.Append(" NULL AS CONTAINERPK, " );
                strSqlGridHeader.Append(" BTC.BOOKING_TRN_CARGO_PK AS CARGOPK, " );
                strSqlGridHeader.Append(" BTSFL.COMMODITY_MST_FKS AS COMMODITYPK, " );
                strSqlGridHeader.Append(" BST.OPERATOR_MST_FK AS OPERATORPK, " );
                strSqlGridHeader.Append(" BTSFL.BOOKING_TRN_SEA_PK AS TRANSACTIONPK, " );
                strSqlGridHeader.Append(" UOM.DIMENTION_UNIT_MST_PK AS BASISPK " );
                strSqlGridHeader.Append(" FROM " );
                strSqlGridHeader.Append(" BOOKING_SEA_TBL BST, " );
                strSqlGridHeader.Append(" BOOKING_TRN_SEA_FCL_LCL BTSFL, " );
                strSqlGridHeader.Append(" DIMENTION_UNIT_MST_TBL UOM, " );
                strSqlGridHeader.Append(" COMMODITY_GROUP_MST_TBL CGMT, " );
                strSqlGridHeader.Append(" COMMODITY_MST_TBL CMT, " );
                strSqlGridHeader.Append(" OPERATOR_MST_TBL OMT, " );
                strSqlGridHeader.Append(" BOOKING_TRN_CARGO_DTL BTC " );
                strSqlGridHeader.Append(" WHERE(1 = 1) " );
                strSqlGridHeader.Append(" AND BTSFL.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK (+)" );
                strSqlGridHeader.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) " );
                strSqlGridHeader.Append(" AND BTSFL.BOOKING_SEA_FK=BST.BOOKING_SEA_PK " );
                strSqlGridHeader.Append(" AND BST.OPERATOR_MST_FK=OMT.OPERATOR_MST_PK (+)" );
                strSqlGridHeader.Append(" AND BTSFL.BASIS=UOM.DIMENTION_UNIT_MST_PK (+)" );
                strSqlGridHeader.Append(" AND BTSFL.BOOKING_TRN_SEA_PK=BTC.BOOKING_TRN_SEA_FK(+)" );
                strSqlGridHeader.Append(" AND BST.BOOKING_SEA_PK= " + lngSBEPK);
            }
            else
            {
                strSqlGridHeader.Append("SELECT  TRNTYPEPK,  TRNTYPESTATUS,  CONTRACTTYPE,  REFNO, " );
                strSqlGridHeader.Append("OPERATOR,  TYPE,  BOXES, CARGO, COMMODITY,  RATE,  BKGRATE,  NET,  TOTALRATE, " );
                strSqlGridHeader.Append("SEL,CONTAINERPK, CARGOPK,  COMMODITYPK,  OPERATORPK, TRANSACTIONPK,  BASISPK FROM( " );
                strSqlGridHeader.Append("SELECT DISTINCT " );
                strSqlGridHeader.Append(" NULL AS TRNTYPEPK, " );
                strSqlGridHeader.Append(" BTSFL.TRANS_REFERED_FROM AS TRNTYPESTATUS, " );
                strSqlGridHeader.Append(" DECODE(BTSFL.TRANS_REFERED_FROM,1,'Quote',2,'Sp Rate',3,'Cust Cont',4,'SL Tariff',5,'SRR',6,'Gen Tariff',7,'Manual') AS CONTRACTTYPE, " );
                strSqlGridHeader.Append(" BTSFL.TRANS_REF_NO AS REFNO, " );
                strSqlGridHeader.Append(" OMT.OPERATOR_ID AS OPERATOR, " );
                strSqlGridHeader.Append(" CTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                strSqlGridHeader.Append(" BTSFL.NO_OF_BOXES AS BOXES, " );
                //strSqlGridHeader.Append("  '' AS CARGO, " & vbCrLf)
                //--------COMMENTED BY ASHISH
                //strSqlGridHeader.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||")
                //strSqlGridHeader.Append("                NVL(BTSFL.COMMODITY_MST_FKS, 0) || ')') CARGO,")
                //---------
                strSqlGridHeader.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN ");
                strSqlGridHeader.Append("       (SELECT BCD.COMMODITY_MST_FK FROM BOOKING_COMMODITY_DTL BCD WHERE BCD.BOOKING_CARGO_DTL_FK IN ");
                strSqlGridHeader.Append("       (SELECT BTCD.BOOKING_TRN_CARGO_PK FROM BOOKING_TRN_CARGO_DTL BTCD WHERE BTCD.BOOKING_TRN_SEA_FK='|| ");
                strSqlGridHeader.Append("       NVL(BTSFL.BOOKING_TRN_SEA_PK, 0) || '))') CARGO,");

                //strSqlGridHeader.Append(" CMT.COMMODITY_NAME AS COMM, " & vbCrLf)
                strSqlGridHeader.Append(" '' AS COMMODITY, " );
                strSqlGridHeader.Append(" BTSFL.BUYING_RATE AS RATE, " );
                strSqlGridHeader.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_SEA_PK,2," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)AS BKGRATE, " );
                strSqlGridHeader.Append(" NULL AS NET, " );
                strSqlGridHeader.Append(" FETCH_FCL_LCL_TOTALBKGFRT(BTSFL.BOOKING_TRN_SEA_PK,1," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",2)AS TOTALRATE, " );
                strSqlGridHeader.Append(" '1' AS SEL, " );
                strSqlGridHeader.Append(" BTSFL.CONTAINER_TYPE_MST_FK AS CONTAINERPK, " );
                //strSqlGridHeader.Append(" BTC.BOOKING_TRN_CARGO_PK AS CARGOPK, " & vbCrLf)
                strSqlGridHeader.Append(" 0 AS CARGOPK, " );

                //strSqlGridHeader.Append(" BTSFL.COMMODITY_MST_FKS AS COMMODITYFK, " & vbCrLf)
                strSqlGridHeader.Append("       ROWTOCOL('SELECT BCD.COMMODITY_MST_FK FROM BOOKING_COMMODITY_DTL BCD WHERE BCD.BOOKING_CARGO_DTL_FK IN ");
                strSqlGridHeader.Append("       (SELECT BTCD.BOOKING_TRN_CARGO_PK FROM BOOKING_TRN_CARGO_DTL BTCD WHERE BTCD.BOOKING_TRN_SEA_FK='|| ");
                strSqlGridHeader.Append("       NVL(BTSFL.BOOKING_TRN_SEA_PK, 0) || ')') COMMODITYPK,");

                strSqlGridHeader.Append(" BST.OPERATOR_MST_FK AS OPERATORPK, " );
                strSqlGridHeader.Append(" BTSFL.BOOKING_TRN_SEA_PK AS TRANSACTIONPK, " );
                strSqlGridHeader.Append(" NULL AS BASISPK , CTMT.Preferences" );
                strSqlGridHeader.Append(" FROM " );
                strSqlGridHeader.Append(" BOOKING_SEA_TBL BST, " );
                strSqlGridHeader.Append(" BOOKING_TRN_SEA_FCL_LCL BTSFL, " );
                strSqlGridHeader.Append(" CONTAINER_TYPE_MST_TBL CTMT, " );
                strSqlGridHeader.Append(" COMMODITY_GROUP_MST_TBL CGMT, " );
                strSqlGridHeader.Append(" COMMODITY_MST_TBL CMT, " );
                strSqlGridHeader.Append(" OPERATOR_MST_TBL OMT, " );
                strSqlGridHeader.Append(" BOOKING_TRN_CARGO_DTL BTC " );
                strSqlGridHeader.Append(" WHERE(1 = 1) " );
                strSqlGridHeader.Append(" AND BTSFL.CONTAINER_TYPE_MST_FK=CTMT.CONTAINER_TYPE_MST_PK (+)" );
                strSqlGridHeader.Append(" AND BTSFL.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK (+)" );
                strSqlGridHeader.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) " );
                strSqlGridHeader.Append(" AND BTSFL.BOOKING_SEA_FK=BST.BOOKING_SEA_PK " );
                strSqlGridHeader.Append(" AND BST.OPERATOR_MST_FK=OMT.OPERATOR_MST_PK (+)" );
                strSqlGridHeader.Append(" AND BTSFL.BOOKING_TRN_SEA_PK=BTC.BOOKING_TRN_SEA_FK(+)" );
                strSqlGridHeader.Append(" AND BST.BOOKING_SEA_PK= " + lngSBEPK);
                strSqlGridHeader.Append(" ORDER BY  CTMT.Preferences)");
            }
            try
            {
                dtTrans = objwf.GetDataTable(strSqlGridHeader.ToString());
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
            //Added by Faheem
            string strStatus = "SELECT B.STATUS FROM BOOKING_SEA_TBL B WHERE B.BOOKING_SEA_PK= " + lngSBEPK;
            BkStatus = Convert.ToInt32(objwf.ExecuteScaler(strStatus));
            //End
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
                strSqlGridChild.Append("       abs(BKGRATE)BKGRATE,");
                strSqlGridChild.Append("       TOTAL,");
                strSqlGridChild.Append("       BASIS_PK,");
                strSqlGridChild.Append("       PYMT_TYPE,");
                strSqlGridChild.Append("       Credit,CHECK_ADVATOS,BOOKING_TRN_SEA_FRT_PK ");
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
                strSqlGridChild.Append("               MIN_RATE AS MIN_RATE,");
                strSqlGridChild.Append("               NULL AS RATE,");
                strSqlGridChild.Append("               BTSFD.TARIFF_RATE AS BKGRATE,");
                strSqlGridChild.Append("               BTSFD.MIN_RATE TOTAL,");
                strSqlGridChild.Append("               BTSFL.BASIS AS BASIS_PK,");
                strSqlGridChild.Append("               DECODE(BTSFD.PYMT_TYPE, 1, 'PrePaid', 'Collect') AS PYMT_TYPE,");
                strSqlGridChild.Append("               BTSFD.BOOKING_TRN_SEA_FRT_PK,");
                strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,BTSFD.CHECK_ADVATOS ");
                strSqlGridChild.Append("          FROM BOOKING_SEA_TBL          BST,");
                strSqlGridChild.Append("               BOOKING_TRN_SEA_FCL_LCL  BTSFL,");
                strSqlGridChild.Append("               BOOKING_TRN_SEA_FRT_DTLS BTSFD,");
                strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL    CTMT,");
                strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
                strSqlGridChild.Append("               COMMODITY_MST_TBL        CMT,");
                strSqlGridChild.Append("               PORT_MST_TBL             PL,");
                strSqlGridChild.Append("               PORT_MST_TBL             PD,");
                strSqlGridChild.Append("               DIMENTION_UNIT_MST_TBL   UOM");
                strSqlGridChild.Append("         WHERE (1 = 1)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_TRN_SEA_PK = BTSFD.BOOKING_TRN_SEA_FK");
                strSqlGridChild.Append("           AND BTSFD.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFD.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
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
                strSqlGridChild.Append("           AND BST.BOOKING_SEA_PK = " + lngSBEPK);
                if (BkStatus == 1)
                {
                    strSqlGridChild.Append("        UNION");
                    strSqlGridChild.Append("        SELECT NULL AS TRNTYPEFK,");
                    strSqlGridChild.Append("               BTSFL.TRANS_REF_NO AS REFNO,");
                    strSqlGridChild.Append("               UOM.DIMENTION_ID AS BASIS,");
                    strSqlGridChild.Append("               BTSFL.COMMODITY_MST_FK AS COMMODITYFK,");
                    //strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS POLPK,")''COMMENTED BY ASHISH
                    strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS PORT_MST_PK,");
                    strSqlGridChild.Append("               PL.PORT_ID AS POL,");
                    //strSqlGridChild.Append("               BST.PORT_MST_POD_FK AS PODPK,")''COMMENTED BY ASHISH
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
                    strSqlGridChild.Append("               NULL BOOKING_TRN_SEA_FRT_PK,");
                    strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,0 CHECK_ADVATOS ");
                    strSqlGridChild.Append("          FROM BOOKING_SEA_TBL         BST,");
                    strSqlGridChild.Append("               BOOKING_TRN_SEA_FCL_LCL BTSFL,");
                    strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL   CTMT,");
                    strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL FEMT,");
                    strSqlGridChild.Append("               COMMODITY_MST_TBL       CMT,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PL,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PD,");
                    strSqlGridChild.Append("               DIMENTION_UNIT_MST_TBL  UOM");
                    strSqlGridChild.Append("         WHERE (1 = 1)");
                    strSqlGridChild.Append("           AND FEMT.FREIGHT_ELEMENT_MST_PK NOT IN");
                    strSqlGridChild.Append("               (SELECT BF.FREIGHT_ELEMENT_MST_FK");
                    strSqlGridChild.Append("                  FROM BOOKING_TRN_SEA_FCL_LCL  BT,");
                    strSqlGridChild.Append("                       BOOKING_TRN_SEA_FRT_DTLS BF");
                    strSqlGridChild.Append("                 WHERE BF.BOOKING_TRN_SEA_FK = BT.BOOKING_TRN_SEA_PK");
                    strSqlGridChild.Append("                   AND BT.BOOKING_SEA_FK = " + lngSBEPK + ")");
                    strSqlGridChild.Append("           AND FEMT.ACTIVE_FLAG = 1");
                    strSqlGridChild.Append("           AND FEMT.BUSINESS_TYPE = 2");
                    strSqlGridChild.Append("           AND NVL(FEMT.CHARGE_TYPE, 0) <> 3");
                    strSqlGridChild.Append("           AND CTMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    strSqlGridChild.Append("           AND BTSFL.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
                    strSqlGridChild.Append("           AND BTSFL.BASIS = UOM.DIMENTION_UNIT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.BOOKING_SEA_PK = " + lngSBEPK);
                }
                strSqlGridChild.Append(" ) Q ORDER BY PREFERENCE");
            }
            else
            {
                strSqlGridChild.Append("SELECT Q.TRNTYPEFK,");
                strSqlGridChild.Append("       REFNO,");
                strSqlGridChild.Append("       TYPE,");
                strSqlGridChild.Append("       COMMODITYFK,");
                strSqlGridChild.Append("       POLPK AS PORT_MST_PK,");
                //MODIFIED BY ASHISH
                strSqlGridChild.Append("       POL,");
                strSqlGridChild.Append("       PODPK AS PORT_MST_PK1,");
                //MODIFIED BY ASHISH
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
                strSqlGridChild.Append("       Credit,CHECK_ADVATOS,BOOKING_TRN_SEA_FRT_PK ");
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
                strSqlGridChild.Append("               BTSFD.MIN_RATE TOTAL,");
                strSqlGridChild.Append("               BTSFL.BASIS AS BASIS,");
                strSqlGridChild.Append("               DECODE(BTSFD.PYMT_TYPE, 1, 'PrePaid', 'Collect') AS PYMT_TYPE,");
                strSqlGridChild.Append("               BTSFD.BOOKING_TRN_SEA_FRT_PK,");
                strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,BTSFD.CHECK_ADVATOS ");
                strSqlGridChild.Append("          FROM BOOKING_SEA_TBL          BST,");
                strSqlGridChild.Append("               BOOKING_TRN_SEA_FCL_LCL  BTSFL,");
                strSqlGridChild.Append("               BOOKING_TRN_SEA_FRT_DTLS BTSFD,");
                strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL    CTMT,");
                strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL  FEMT,");
                strSqlGridChild.Append("               CONTAINER_TYPE_MST_TBL   CTMT,");
                strSqlGridChild.Append("               COMMODITY_MST_TBL        CMT,");
                strSqlGridChild.Append("               PORT_MST_TBL             PL,");
                strSqlGridChild.Append("               PORT_MST_TBL             PD");
                strSqlGridChild.Append("         WHERE (1 = 1)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_TRN_SEA_PK = BTSFD.BOOKING_TRN_SEA_FK");
                strSqlGridChild.Append("           AND BTSFD.CURRENCY_MST_FK = CTMT.CURRENCY_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFD.FREIGHT_ELEMENT_MST_FK = FEMT.FREIGHT_ELEMENT_MST_PK(+)");
                strSqlGridChild.Append("           AND BTSFL.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
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
                strSqlGridChild.Append("           AND BST.BOOKING_SEA_PK = " + lngSBEPK);
                if (BkStatus == 1)
                {
                    strSqlGridChild.Append("        UNION");
                    strSqlGridChild.Append("        SELECT NULL AS TRNTYPEFK,");
                    strSqlGridChild.Append("               BTSFL.TRANS_REF_NO AS REFNO,");
                    strSqlGridChild.Append("               CTMT.CONTAINER_TYPE_MST_ID AS TYPE,");
                    strSqlGridChild.Append("               BTSFL.COMMODITY_MST_FK AS COMMODITYFK,");
                    //strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS POLPK,")''COMMENTED BY ASHISH
                    strSqlGridChild.Append("               BST.PORT_MST_POL_FK AS PORT_MST_PK,");
                    strSqlGridChild.Append("               PL.PORT_ID AS POL,");
                    //strSqlGridChild.Append("               BST.PORT_MST_POD_FK AS PODPK,")''COMMENTED BY ASHISH
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
                    strSqlGridChild.Append("               NULL BOOKING_TRN_SEA_FRT_PK,");
                    strSqlGridChild.Append("               FEMT.PREFERENCE,FEMT.Credit,0 CHECK_ADVATOS ");
                    strSqlGridChild.Append("          FROM BOOKING_SEA_TBL         BST,");
                    strSqlGridChild.Append("               BOOKING_TRN_SEA_FCL_LCL BTSFL,");
                    strSqlGridChild.Append("               CURRENCY_TYPE_MST_TBL   CTMT,");
                    strSqlGridChild.Append("               FREIGHT_ELEMENT_MST_TBL FEMT,");
                    strSqlGridChild.Append("               CONTAINER_TYPE_MST_TBL  CTMT,");
                    strSqlGridChild.Append("               COMMODITY_MST_TBL       CMT,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PL,");
                    strSqlGridChild.Append("               PORT_MST_TBL            PD");
                    strSqlGridChild.Append("         WHERE (1 = 1)");
                    strSqlGridChild.Append("           AND FEMT.FREIGHT_ELEMENT_MST_PK NOT IN");
                    strSqlGridChild.Append("               (SELECT BF.FREIGHT_ELEMENT_MST_FK");
                    strSqlGridChild.Append("                  FROM BOOKING_TRN_SEA_FCL_LCL  BT,");
                    strSqlGridChild.Append("                       BOOKING_TRN_SEA_FRT_DTLS BF");
                    strSqlGridChild.Append("                 WHERE BF.BOOKING_TRN_SEA_FK = BT.BOOKING_TRN_SEA_PK");
                    strSqlGridChild.Append("                   AND BT.BOOKING_SEA_FK = " + lngSBEPK + ")");
                    strSqlGridChild.Append("           AND FEMT.ACTIVE_FLAG = 1");
                    strSqlGridChild.Append("           AND FEMT.BUSINESS_TYPE = 2");
                    strSqlGridChild.Append("           AND NVL(FEMT.CHARGE_TYPE, 0) <> 3");
                    strSqlGridChild.Append("           AND BTSFL.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
                    strSqlGridChild.Append("           AND BTSFL.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POL_FK = PL.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BST.PORT_MST_POD_FK = PD.PORT_MST_PK(+)");
                    strSqlGridChild.Append("           AND BTSFL.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                    strSqlGridChild.Append("           AND CTMT.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    strSqlGridChild.Append("           AND BST.BOOKING_SEA_PK = " + lngSBEPK + "");
                }
                strSqlGridChild.Append(" ) Q, container_type_mst_tbl ctmt WHERE q.type = ctmt.container_type_mst_id");
                strSqlGridChild.Append("  ORDER BY ctmt.preferences,Q.CHECK_FOR_ALL_IN_RT DESC, Q.PREFERENCE");

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
            strSqlCDimension.Append(" BACC.BOOKING_SEA_CARGO_CALC_PK AS PK, " );
            strSqlCDimension.Append(" BACC.BOOKING_SEA_FK " );
            strSqlCDimension.Append(" FROM " );
            strSqlCDimension.Append(" BOOKING_SEA_CARGO_CALC BACC " );
            strSqlCDimension.Append(" WHERE " );
            strSqlCDimension.Append(" BACC.BOOKING_SEA_FK= " + lngSBEPK);
            try
            {
                dtMain = objwf.GetDataTable(strSqlCDimension.ToString());
                return dtMain;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        //Fetch the cargo Dimension details

        #endregion

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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sqlStr = "select booking_air_pk from booking_air_tbl where booking_ref_no='" + BookingId + "'";
                BookingPk = Convert.ToInt32(objWF.ExecuteScaler(sqlStr));
                return BookingPk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sqlStr = "select booking_sea_pk from booking_sea_tbl where booking_ref_no='" + BookingId + "'";
                BookingPk = Convert.ToInt32(objWF.ExecuteScaler(sqlStr));
                return BookingPk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sqlStr = "select IS_EBOOKING from booking_sea_tbl where booking_sea_pk='" + BookingPK + "'";
                IS_EBOOKING = Convert.ToInt16(objWF.ExecuteScaler(sqlStr));
                return IS_EBOOKING;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sqlStr = "select BOOKING_REF_NO from booking_sea_tbl where booking_sea_pk='" + BookingPK + "' and BOOKING_REF_NO like 'BKG%'";
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sqlStr = "select BOOKING_DATE from booking_sea_tbl where BOOKING_REF_NO='" + BOOKING_REF_NO + "' ";
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sqlStr = "select BOOKING_REF_NO from booking_sea_tbl where booking_sea_pk='" + BookingPK + "' ";
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        //End Snigdharani
        #endregion

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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            //Manoharan 27June2007: to fill Volume and Chargable weight in the respective Textboxes : DTS 2981
            //strBuilder.Append(" TO_CHAR(QST.EXPECTED_SHIPMENT_DT,'" & dateFormat & "') AS EXP_SHIPMENT_DATE ")
            strBuilder.Append(" TO_CHAR(QST.EXPECTED_SHIPMENT_DT,'" + dateFormat + "') AS EXP_SHIPMENT_DATE, ");
            strBuilder.Append(" QTSFL.EXPECTED_VOLUME AS VOLUME,  CASE WHEN DMT.DIMENTION_ID = 'MT' THEN  QTSFL.EXPECTED_WEIGHT * 1000 ELSE QTSFL.EXPECTED_WEIGHT END WEIGHT, ");
            //end
            strBuilder.Append(" QST.shipping_terms_mst_pk ");
            strBuilder.Append(" FROM QUOTATION_SEA_TBL QST, ");
            strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL QTSFL, ");
            strBuilder.Append(" CUSTOMER_MST_TBL CMT, ");
            strBuilder.Append(" PLACE_MST_TBL CPMT, ");
            strBuilder.Append(" PLACE_MST_TBL DPMT, ");
            strBuilder.Append(" AGENT_MST_TBL CBAMT, ");
            strBuilder.Append(" PORT_MST_TBL POL, ");
            strBuilder.Append(" PORT_MST_TBL POD, ");
            strBuilder.Append(" AGENT_MST_TBL DPAMT, ");
            strBuilder.Append(" CUSTOMER_CATEGORY_MST_TBL CCMT, ");
            strBuilder.Append(" DIMENTION_UNIT_MST_TBL    DMT ");
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
            strBuilder.Append(" AND QTSFL.BASIS = DMT.DIMENTION_UNIT_MST_PK(+) ");
            strBuilder.Append(" AND QST.QUOTATION_SEA_PK= " + lngSQEPK);
            strBuilder.Append(" AND QTSFL.PORT_MST_POL_FK= " + strQuotationPOLPK);
            strBuilder.Append(" AND QTSFL.PORT_MST_POD_FK= " + strQuotationPODPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL QTRN, ");
            strBuilder.Append(" QUOTATION_TRN_SEA_OTH_CHRG QTSOC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" QTSOC.QUOTATION_TRN_SEA_FK= QTRN.QUOTE_TRN_SEA_PK ");
            strBuilder.Append(" AND QTRN.QUOTATION_SEA_FK=QHDR.QUOTATION_SEA_PK ");
            strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + lngPolPk);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + lngPodPk);
            strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK= " + lngSQEPK);

            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            strBuilder.Append(" NULL AS FK ");
            strBuilder.Append(" FROM ");
            strBuilder.Append(" QUOTATION_SEA_TBL QHDR,");
            strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL QTRN, ");
            strBuilder.Append(" QUOTATION_SEA_CARGO_CALC QCALC ");
            strBuilder.Append(" WHERE ");
            strBuilder.Append(" QTRN.QUOTATION_SEA_FK=QHDR.QUOTATION_SEA_PK ");
            strBuilder.Append(" AND QCALC.QUOTE_TRN_SEA_FK=QTRN.QUOTE_TRN_SEA_PK ");
            strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + lngPOLPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + lngPODPK);
            strBuilder.Append(" AND QTRN.QUOTATION_SEA_FK= " + lngQPK);
            try
            {
                dtMain = objwf.GetDataTable(strBuilder.ToString());
                return dtMain;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            DataTable dtMain = new DataTable();
            FetchSpotRateEntryHeader(dtMain, lngSpotRatePk);
            //Data set ds contains the Master Table details
            dsSpotRateEntry.Tables.Add(dtMain);
        }

        public void FetchSpotRateEntryHeader(DataTable dtMain, long lngSpotRateEntryPK)
        {
            System.Text.StringBuilder strBuilderSpotRate = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING Spot Rate
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING 
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strSql = "select booking_sea_fk from BOOKING_TRN_SEA_FCL_LCL where booking_sea_fk=" + BkgPk;
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                dtMain = FetchHeaderManual(intIsFcl, strContainer, Fetch);
                dtChild = FetchFreightManual(intIsFcl, strPOL, strPOD, strContainer, BaseCurrency, Fetch);
                dsGrid.Tables.Add(dtMain);
                dsGrid.Tables.Add(dtChild);
                string relCol = "CONTAINERPK";
                if (intIsFcl == 2)
                    relCol = "BASISPK";
                DataRelation rel = new DataRelation("rl_HEAD_TRAN", new DataColumn[] { dsGrid.Tables[0].Columns[relCol] }, new DataColumn[] { dsGrid.Tables[1].Columns[relCol] });
                dsGrid.Relations.Clear();
                dsGrid.Relations.Add(rel);
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
                if (intIsFcl != 2)
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEPK," );
                    strQuery.Append("       '7' AS TRNTYPESTATUS," );
                    strQuery.Append("       'Manual' AS CONTRACTTYPE," );
                    strQuery.Append("       NULL AS REFNO," );
                    strQuery.Append("       NULL AS OPERATOR," );
                    strQuery.Append("       CONTAINER_TYPE_MST_ID AS TYPE," );
                    strQuery.Append("       '' AS BOXES," );
                    strQuery.Append("       NULL AS CARGO," );
                    strQuery.Append("       NULL AS COMMODITY," );
                    strQuery.Append("       NULL AS RATE," );
                    strQuery.Append("       '' AS BKGRATE," );
                    strQuery.Append("       '' AS NET," );
                    strQuery.Append("       '' AS TOTALRATE," );
                    strQuery.Append("       '0' AS SEL," );
                    strQuery.Append("       CONTAINER_TYPE_MST_PK AS CONTAINERPK," );
                    strQuery.Append("       NULL AS CARGOPK," );
                    strQuery.Append("       NULL AS COMMODITYPK," );
                    strQuery.Append("       NULL AS OPERATORPK," );
                    strQuery.Append("       '' AS TRANSACTIONPK," );
                    strQuery.Append("       NULL AS BASISPK" );
                    strQuery.Append("  FROM(SELECT OCTMT.CONTAINER_TYPE_MST_ID,OCTMT.CONTAINER_TYPE_MST_PK,OCTMT.Preferences" );
                    strQuery.Append("  FROM CONTAINER_TYPE_MST_TBL OCTMT" );
                    strQuery.Append(" WHERE (1 = " + intFetch + ")" );
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN" );
                        strQuery.Append("       (" + strContainer + ")" );
                    }
                    strQuery.Append(" AND OCTMT.ACTIVE_FLAG = 1 " );
                    strQuery.Append(" GROUP BY OCTMT.CONTAINER_TYPE_MST_ID, OCTMT.CONTAINER_TYPE_MST_PK,OCTMT.Preferences ORDER BY OCTMT.Preferences)" );
                }
                else
                {
                    strQuery.Append("SELECT NULL AS TRNTYPEPK," );
                    strQuery.Append("       '7' AS TRNTYPESTATUS," );
                    strQuery.Append("       'Manual' AS CONTRACTTYPE," );
                    strQuery.Append("       NULL AS REFNO," );
                    strQuery.Append("       NULL AS OPERATOR," );
                    strQuery.Append("       OUOM.DIMENTION_ID AS BASIS," );
                    strQuery.Append("       '' AS QTY," );
                    strQuery.Append("       NULL AS CARGO," );
                    strQuery.Append("       NULL AS COMMODITY," );
                    strQuery.Append("       NULL AS RATE," );
                    strQuery.Append("       '' AS BKGRATE," );
                    strQuery.Append("       '' AS NET," );
                    strQuery.Append("       '' AS TOTALRATE," );
                    strQuery.Append("       '0' AS SEL," );
                    strQuery.Append("       '' AS CONTAINERPK," );
                    strQuery.Append("       NULL AS CARGOPK," );
                    strQuery.Append("       NULL AS COMMODITYPK," );
                    strQuery.Append("       NULL AS OPERATORPK," );
                    strQuery.Append("       '' AS TRANSACTIONPK," );
                    strQuery.Append("       OUOM.DIMENTION_UNIT_MST_PK AS BASISPK" );
                    strQuery.Append("  FROM DIMENTION_UNIT_MST_TBL OUOM" );
                    strQuery.Append(" WHERE (1 = 1)" );
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OUOM.DIMENTION_UNIT_MST_PK IN  (" + strContainer + ")" );
                    }
                    strQuery.Append("   AND OUOM.ACTIVE = 1" );

                    strQuery.Append(" GROUP BY OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK" );
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
                // FCL
                if (intIsFcl != 2)
                {
                    strQuery.Append("SELECT  NULL AS TRNTYPEFK," );
                    strQuery.Append("                NULL AS REFNO," );
                    strQuery.Append("                NULL AS TYPE," );
                    //strQuery.Append("       NULL AS CARGO," & vbCrLf)
                    strQuery.Append("                NULL AS COMMODITY," );
                    strQuery.Append("                OPL.PORT_MST_PK," );
                    strQuery.Append("                OPL.PORT_ID AS POL," );
                    strQuery.Append("                OPD.PORT_MST_PK," );
                    strQuery.Append("                OPD.PORT_ID AS POD," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_ID," );
                    strQuery.Append("                DECODE(OFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS," );
                    strQuery.Append("                DECODE(0, 1, 'true', 'false') SEL," );
                    strQuery.Append("                OCUMT.CURRENCY_MST_PK," );
                    strQuery.Append("                OCUMT.CURRENCY_ID," );
                    strQuery.Append("                '' AS RATE," );
                    strQuery.Append("                '' AS BKGRATE,0 TOTAL," );
                    //strQuery.Append("                0 AS RATE," & vbCrLf)
                    //strQuery.Append("                0 AS BKGRATE," & vbCrLf)
                    strQuery.Append("                '' AS BASISPK," );
                    strQuery.Append("                '1' AS PYMT_TYPE," );
                    strQuery.Append("                OFEMT.Credit,0 CHECK_ADVATOS,CONTAINER_TYPE_MST_PK AS CONTAINERPK " );
                    //strQuery.Append("                OFEMT.PREFERENCE" & vbCrLf)
                    strQuery.Append("  FROM CONTAINER_TYPE_MST_TBL  OCTMT," );
                    strQuery.Append("       PORT_MST_TBL            OPL," );
                    strQuery.Append("       PORT_MST_TBL            OPD," );
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   OCUMT" );
                    strQuery.Append(" WHERE (1 = " + intFetch + ")" );
                    strQuery.Append("   AND OPL.PORT_MST_PK = " + strPOL );
                    strQuery.Append("   AND OPD.PORT_MST_PK = " + strPOD );
                    strQuery.Append("   AND OCUMT.CURRENCY_MST_PK = " + BaseCurrency );
                    //strQuery.Append("   AND OFEMT.BUSINESS_TYPE IN (2, 3)" & vbCrLf)
                    strQuery.Append("   AND OFEMT.BUSINESS_TYPE =2" );
                    //strQuery.Append("   AND OFEMT.ACTIVE_FLAG IN(0,1)" & vbCrLf)
                    strQuery.Append("   AND OFEMT.ACTIVE_FLAG = 1" );
                    strQuery.Append("   AND OCTMT.ACTIVE_FLAG = 1" );
                    //OFEMT.CHARGE_BASIS
                    strQuery.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3" );
                    //OFEMT.CHARGE_BASIS
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN" );
                        strQuery.Append("       (" + strContainer + ")" );
                        //ACTIVE_FLAG
                    }
                    strQuery.Append("   ORDER BY OFEMT.PREFERENCE" );
                    //LCL
                }
                else
                {

                    strQuery.Append("SELECT NULL AS TRNTYPEFK," );
                    strQuery.Append("                NULL AS REFNO," );
                    strQuery.Append("                NULL AS TYPE," );
                    strQuery.Append("                NULL AS COMMODITY," );
                    strQuery.Append("                OPL.PORT_MST_PK," );
                    strQuery.Append("                OPL.PORT_ID AS POL," );
                    strQuery.Append("                OPD.PORT_MST_PK," );
                    strQuery.Append("                OPD.PORT_ID AS POD," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_MST_PK," );
                    strQuery.Append("                OFEMT.FREIGHT_ELEMENT_ID," );
                    strQuery.Append("                DECODE(OFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS," );
                    strQuery.Append("                DECODE(0, 1, 'true', 'false') SEL," );
                    strQuery.Append("                OCUMT.CURRENCY_MST_PK," );
                    strQuery.Append("                OCUMT.CURRENCY_ID," );
                    strQuery.Append("                0 AS MIN_RATE," );
                    strQuery.Append("                0 AS RATE," );
                    strQuery.Append("                0 AS BKGRATE,0 TOTAL," );
                    strQuery.Append("                OUOM.DIMENTION_UNIT_MST_PK AS BASISPK," );
                    strQuery.Append("                '1' AS PYMT_TYPE," );
                    strQuery.Append("                 OFEMT.Credit,0 CHECK_ADVATOS,OUOM.DIMENTION_UNIT_MST_PK AS BASISPK1 " );
                    strQuery.Append("" );
                    strQuery.Append("  FROM PORT_MST_TBL            OPL," );
                    strQuery.Append("       PORT_MST_TBL            OPD," );
                    strQuery.Append("       FREIGHT_ELEMENT_MST_TBL OFEMT," );
                    strQuery.Append("       CURRENCY_TYPE_MST_TBL   OCUMT," );
                    strQuery.Append("       DIMENTION_UNIT_MST_TBL  OUOM" );
                    strQuery.Append(" WHERE (1 = 1)" );
                    strQuery.Append("   AND OPL.PORT_MST_PK = " + strPOL );
                    strQuery.Append("   AND OPD.PORT_MST_PK = " + strPOD );
                    strQuery.Append("   AND OCUMT.CURRENCY_MST_PK = " + BaseCurrency );
                    strQuery.Append("   AND OFEMT.BUSINESS_TYPE IN (2, 3)" );
                    strQuery.Append("   AND OFEMT.ACTIVE_FLAG = 1" );
                    strQuery.Append("   AND nvl(OFEMT.CHARGE_TYPE,0) <> 3 " );
                    //OFEMT.CHARGE_BASIS
                    strQuery.Append("   AND OUOM.ACTIVE = 1" );
                    if (strContainer.Length > 0)
                    {
                        strQuery.Append("   AND DIMENTION_UNIT_MST_PK IN (" + strContainer + ")" );
                    }
                    strQuery.Append("   ORDER BY OFEMT.PREFERENCE" );
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
                //sb.Append(" BST.PORT_MST_POL_FK AS POLPK, ")'' ADDED AN COMMENTED BY ASHISH
                sb.Append(" BST.PORT_MST_POL_FK AS PORT_MST_PK, ");
                sb.Append(" PL.PORT_ID AS POL, ");
                //sb.Append(" BST.PORT_MST_POD_FK AS PODPK, ")'' ADDED AN COMMENTED BY ASHISH
                sb.Append(" BST.PORT_MST_POD_FK AS PORT_MST_PK1, ");
                sb.Append(" PD.PORT_ID AS POD, ");
                sb.Append(" FEMT.FREIGHT_ELEMENT_MST_PK,");
                sb.Append(" FEMT.FREIGHT_ELEMENT_ID, ");
                sb.Append("  DECODE(FEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS,");
                //'added by subhransu
                sb.Append(" 'FALSE' AS CHECK_FOR_ALL_IN_RT, ");
                sb.Append(" '" + HttpContext.Current.Session["CURRENCY_MST_PK"] + "' as CURRENCY_MST_FK,");
                sb.Append(" '" + HttpContext.Current.Session["CURRENCY_ID"] + "' as CURRENCY_ID,");
                sb.Append(" NULL AS MIN_RATE, ");
                sb.Append(" NULL AS RATE, ");
                sb.Append(" NULL AS BKGRATE, ");
                sb.Append(" 0 AS TOTAL, ");
                //'added by subhransu
                sb.Append(" NULL AS BASIS, ");
                sb.Append(" 'PrePaid' AS PYMT_TYPE, ");
                sb.Append(" '' as BOOKING_TRN_SEA_FRT_PK,FEMT.Credit ");
                sb.Append(" FROM");
                sb.Append(" BOOKING_SEA_TBL BST, ");
                sb.Append(" BOOKING_TRN_SEA_FCL_LCL BTSFL, ");
                sb.Append(" CURRENCY_TYPE_MST_TBL CTMT, ");
                sb.Append(" FREIGHT_ELEMENT_MST_TBL FEMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" PORT_MST_TBL PL, ");
                sb.Append(" PORT_MST_TBL PD, ");
                sb.Append(" DIMENTION_UNIT_MST_TBL UOM");
                sb.Append(" WHERE(1 = 1)");
                sb.Append(" AND BTSFL.BOOKING_SEA_FK=BST.BOOKING_SEA_PK");
                sb.Append(" AND BTSFL.BOOKING_SEA_FK=BST.BOOKING_SEA_PK ");
                sb.Append(" AND BTSFL.BASIS=UOM.DIMENTION_UNIT_MST_PK(+) ");
                sb.Append(" AND BST.PORT_MST_POL_FK=PL.PORT_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POD_FK=PD.PORT_MST_PK (+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BST.BOOKING_SEA_PK= " + lngSBEPK);
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
                //sb.Append(" BST.PORT_MST_POL_FK AS POLPK, ")'' ADDED AN COMMENTED BY ASHISH
                sb.Append(" BST.PORT_MST_POL_FK AS PORT_MST_PK, ");
                sb.Append(" PL.PORT_ID AS POL, ");
                //sb.Append(" BST.PORT_MST_POD_FK AS PODPK, ")'' ADDED AN COMMENTED BY ASHISH
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
                sb.Append(" '' as BOOKING_TRN_SEA_FRT_PK,FEMT.Credit");
                sb.Append(" FROM ");
                sb.Append(" BOOKING_SEA_TBL BST, ");
                sb.Append(" BOOKING_TRN_SEA_FCL_LCL BTSFL, ");
                sb.Append(" CURRENCY_TYPE_MST_TBL CTMT, ");
                sb.Append(" FREIGHT_ELEMENT_MST_TBL FEMT, ");
                sb.Append(" CONTAINER_TYPE_MST_TBL CTMT, ");
                sb.Append(" COMMODITY_MST_TBL CMT, ");
                sb.Append(" PORT_MST_TBL PL, ");
                sb.Append(" PORT_MST_TBL PD ");
                sb.Append(" WHERE(1 = 1) ");
                sb.Append(" AND BTSFL.BOOKING_SEA_FK=BST.BOOKING_SEA_PK ");
                sb.Append(" AND BTSFL.CONTAINER_TYPE_MST_FK=CTMT.CONTAINER_TYPE_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POL_FK=PL.PORT_MST_PK (+)");
                sb.Append(" AND BST.PORT_MST_POD_FK=PD.PORT_MST_PK (+)");
                sb.Append(" AND BTSFL.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) ");
                sb.Append(" AND BST.BOOKING_SEA_PK= " + lngSBEPK);
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Grid Values"
        public DataSet FetchGridValues(DataSet dsGrid, Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, short intSRateStatus = 0, short intCContractStatus = 0, short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, string strPOL = "",
        string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", string strContainer = "", Int32 intSpotRatePk = 0, string CustContRefNr = "", string hdnQuotFetch = "", int EBKGSTATUS = 0, int BookingPK = 0)
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
                            sb.Append("    FROM BOOKING_TRN_SEA_FCL_LCL BT ");
                            sb.Append("   WHERE BT.BOOKING_SEA_FK = " + dsGrid.Tables[0].Rows[0]["BOOKING_SEA_PK"] + " ");
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
                dtMain = FetchHeader(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, intSRateStatus, intCContractStatus,
                intOTariffStatus, intGTariffStatus, intSRRContractStatus, 0, CustContRefNr, EBKGSTATUS, BookingPK);
                dtChild = FetchFreight(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, intSRateStatus, intCContractStatus,
                intOTariffStatus, intGTariffStatus, intSRRContractStatus, 0, CustContRefNr, EBKGSTATUS, BookingPK);
            }
            else if (intSpotRatePk == 0)
            {
                dtMain = FetchHeader(intQuotationPK, intIsFcl, 0, strPOL, strPOD, 0, "", strContainer, 0, 0, 0

                , 0, 0, 0, "", EBKGSTATUS, BookingPK);
                dtChild = FetchFreight(intQuotationPK, intIsFcl, 0, strPOL, strPOD, 0,"", strContainer, 0, 0, 0

                , 0, 0, 0, "", EBKGSTATUS, BookingPK);
            }
            else
            {
                dtMain = FetchHeader(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, 0, 0,
                0, 0, 0, intSpotRatePk);
                dtChild = FetchFreight(0, intIsFcl, intCustomerPK, strPOL, strPOD, intCommodityPK, strSDate, strContainer, 0, 0,
                0, 0, 0, intSpotRatePk);
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
                            //rel.Nested = True
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
                            //rel.Nested = True
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
                            //rel.Nested = True
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
                            //rel.Nested = True
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Header"
        //Search for data if any exist for the customer or Agent with the given details
        //Cargo Type,Port Pair,Commodity Group, 
        //Port Pair wise Container Types
        public DataTable FetchHeader(Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, string strPOL = "", string strPOD = "", Int16 intCommodityPK = 0, string strSDate = "", string strContainer = "", short intSRateStatus = 0, short intCContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, Int32 intSpotRatePK = 0, string CustContRefNr = "", int EBKGSTATUS = 0, int BookingPK = 0)
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
                //QUOTATION
                if (!(intQuotationPK == 0))
                {
                    strSql = funQuotationHeader(arrCCondition, strPOL, strPOD, intQuotationPK, intIsFcl, strContainer, EBKGSTATUS, BookingPK).ToString();
                }
                else
                {
                    //SPOT RATE
                    if (!(intSpotRatePK == 0))
                    {
                        strSql = funSpotRateHeader(arrCCondition, strCustomer, intCommodityPK.ToString(), strPOL, strPOD, intSpotRatePK.ToString(), strSDate, intIsFcl, 1).ToString();
                    }
                    if (intSRateStatus == 1 & intSpotRatePK == 0)
                    {
                        strSql = funSpotRateHeader(arrCCondition, strCustomer, intCommodityPK.ToString(), strPOL, strPOD, intSpotRatePK.ToString(), strSDate, intIsFcl, 2).ToString();
                    }
                    //CUSTOMER(CONTRACT)
                    if (intCContractStatus == 1)
                    {
                        strSql = funCustContHeader(arrCCondition, intCustomerPK, intCommodityPK.ToString(), strPOL, strPOD, strSDate, intIsFcl, getDefault(CustContRefNr, "").ToString(), EBKGSTATUS, BookingPK).ToString();
                    }
                    //SPECIAL RATE REQUEST CONTRACT
                    if (intSRRContractStatus == 1)
                    {
                        strSql = funSRRHeader(arrCCondition, intCustomerPK, intCommodityPK.ToString(), strPOL, strPOD, strSDate, intIsFcl).ToString();
                    }
                    //OPERATOR TARIFF
                    if (intOTariffStatus == 1)
                    {
                        strSql = funSLTariffHeader(arrCCondition, intCommodityPK.ToString(), strPOL, strPOD, strSDate, intIsFcl).ToString();
                    }
                    //GENERAL TARIFF
                    if (intGTariffStatus == 1)
                    {
                        strSql = funGTariffHeader(arrCCondition, intCommodityPK.ToString(), strPOL, strPOD, strSDate, intIsFcl).ToString();
                    }
                }
                return (new WorkFlow()).GetDataTable(strSql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public object funQuotationHeader(ArrayList arrCCondition, string strPOL, string strPOD, Int32 intQuotationPK, Int16 intIsFcl, string strContainer = "", int EBKGSTATUS = 0, int BookingPK = 0)
        {
            try
            {
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                if (intIsFcl == 2)
                {
                    strBuilder.Append(" SELECT DISTINCT QHDR.QUOTATION_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append(" '1' AS TRNTYPESTATUS, " );
                    strBuilder.Append(" 'Quote' AS CONTRACTTYPE, " );
                    strBuilder.Append(" QHDR.QUOTATION_REF_NO AS REFNO, " );
                    strBuilder.Append(" QOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append(" QUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append(" case WHEN QUOM.DIMENTION_ID = 'KGS' THEN QTRN.EXPECTED_WEIGHT " );
                    strBuilder.Append(" when QUOM.DIMENTION_ID = 'CBM' THEN QTRN.EXPECTED_VOLUME " );
                    strBuilder.Append(" when QUOM.DIMENTION_ID = 'W/M' then (case when QTRN.EXPECTED_WEIGHT > QTRN.EXPECTED_VOLUME " );
                    strBuilder.Append(" then QTRN.EXPECTED_WEIGHT else QTRN.EXPECTED_VOLUME end) end as QTY," );
                    strBuilder.Append("'' AS CARGO, " );
                    strBuilder.Append(" NULL AS COMMODITY, " );
                    strBuilder.Append(" NULL AS RATE, " );
                    strBuilder.Append(" '' AS BKGRATE, " );
                    strBuilder.Append(" '' AS NET, " );
                    strBuilder.Append(" '' AS TOTALRATE, " );
                    strBuilder.Append(" '1' AS SEL, " );
                    strBuilder.Append(" '' AS CONTAINERPK, " );
                    strBuilder.Append("'' AS CARGOPK, " );
                    strBuilder.Append(" CASE WHEN QTRN.COMMODITY_MST_FKS IS NULL THEN TO_CHAR(QTRN.COMMODITY_MST_FK) ELSE QTRN.COMMODITY_MST_FKS END AS COMMODITYPK, " );
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append(" '' AS TRANSACTIONPK, " );
                    strBuilder.Append(" QUOM.DIMENTION_UNIT_MST_PK AS BASISPK " );
                    strBuilder.Append(" FROM " );
                    strBuilder.Append(" QUOTATION_SEA_TBL QHDR, " );
                    strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL QTRN, " );
                    strBuilder.Append(" QUOTATION_TRN_SEA_FRT_DTLS QTRNCHRG, " );
                    strBuilder.Append(" OPERATOR_MST_TBL QOMT, " );
                    strBuilder.Append(" COMMODITY_MST_TBL QCMT, " );
                    strBuilder.Append(" DIMENTION_UNIT_MST_TBL QUOM " );
                    strBuilder.Append(" WHERE(1 = 1) " );
                    strBuilder.Append(" AND QTRN.OPERATOR_MST_FK=QOMT.OPERATOR_MST_PK(+)" );
                    strBuilder.Append(" AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK(+) " );
                    strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK=QTRN.QUOTATION_SEA_FK " );
                    strBuilder.Append(" AND QTRN.QUOTE_TRN_SEA_PK=QTRNCHRG.QUOTE_TRN_SEA_FK " );
                    strBuilder.Append(" AND QTRN.BASIS=QUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK= " + intQuotationPK );
                    strBuilder.Append("  " + arrCCondition[0] + " " );
                    strBuilder.Append(" GROUP BY QHDR.QUOTATION_SEA_PK, QHDR.QUOTATION_REF_NO,QOMT.OPERATOR_ID, " );
                    strBuilder.Append(" QCMT.COMMODITY_NAME, QCMT.COMMODITY_MST_PK, " );
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK, QUOM.DIMENTION_ID, QUOM.DIMENTION_UNIT_MST_PK, QTRN.EXPECTED_WEIGHT, QTRN.EXPECTED_VOLUME,QTRN.COMMODITY_MST_FKS,QTRN.COMMODITY_MST_FK ");
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
                        strBuilder.Append("       OUOM.DIMENTION_UNIT_MST_PK AS BASISPK");
                        strBuilder.Append("  FROM DIMENTION_UNIT_MST_TBL OUOM");
                        strBuilder.Append(" WHERE (1 = 1)");
                        strBuilder.Append("   AND OUOM.DIMENTION_UNIT_MST_PK IN (" + strContainer + ")");
                        strBuilder.Append("   AND OUOM.DIMENTION_UNIT_MST_PK NOT IN");
                        strBuilder.Append("       (SELECT QT.BASIS");
                        strBuilder.Append("          FROM QUOTATION_SEA_TBL Q, QUOTATION_TRN_SEA_FCL_LCL QT");
                        strBuilder.Append("         WHERE Q.QUOTATION_SEA_PK = QT.QUOTATION_SEA_FK");
                        strBuilder.Append("           AND Q.QUOTATION_SEA_PK = " + intQuotationPK + ")");
                        strBuilder.Append("   AND OUOM.ACTIVE = 1");
                        strBuilder.Append(" GROUP BY OUOM.DIMENTION_ID, OUOM.DIMENTION_UNIT_MST_PK");
                    }
                    //'
                }
                else
                {
                    strBuilder.Append(" SELECT QHDR.QUOTATION_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append(" '1' AS TRNTYPESTATUS, " );
                    strBuilder.Append(" 'Quote' AS CONTRACTTYPE, " );
                    strBuilder.Append(" QHDR.QUOTATION_REF_NO AS REFNO, " );
                    strBuilder.Append(" QOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append(" QTRN.EXPECTED_BOXES AS BOXES, " );
                    //strBuilder.Append("'' AS CARGO, " & vbCrLf)
                    strBuilder.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
                    strBuilder.Append("                NVL(QTRN.COMMODITY_MST_FKS, 0) || ')') AS CARGO,");
                    //'End
                    strBuilder.Append(" NULL AS COMMODITY, " );
                    strBuilder.Append(" NULL AS RATE, " );
                    strBuilder.Append(" qtrn.all_in_quoted_tariff AS BKGRATE," );
                    strBuilder.Append(" '' AS NET, " );
                    strBuilder.Append(" qtrn.all_in_quoted_tariff * QTRN.EXPECTED_BOXES AS TOTALRATE, " );
                    strBuilder.Append(" '1' AS SEL, " );
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                    strBuilder.Append("'' AS CARGOPK, " );
                    strBuilder.Append(" CASE WHEN QTRN.COMMODITY_MST_FKS IS NULL THEN TO_CHAR(QTRN.COMMODITY_MST_FK) ELSE QTRN.COMMODITY_MST_FKS END AS COMMODITYPK, " );
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append(" '' AS TRANSACTIONPK, " );
                    strBuilder.Append(" NULL AS BASISPK " );
                    strBuilder.Append(" FROM " );
                    strBuilder.Append(" QUOTATION_SEA_TBL QHDR, " );
                    strBuilder.Append(" QUOTATION_TRN_SEA_FCL_LCL QTRN, " );
                    strBuilder.Append(" QUOTATION_TRN_SEA_FRT_DTLS QTRNCHRG, " );
                    strBuilder.Append(" OPERATOR_MST_TBL QOMT, " );
                    strBuilder.Append(" CONTAINER_TYPE_MST_TBL QCTMT, " );
                    strBuilder.Append(" COMMODITY_MST_TBL QCMT " );
                    strBuilder.Append(" WHERE(1 = 1) " );
                    strBuilder.Append(" AND QTRN.OPERATOR_MST_FK=QOMT.OPERATOR_MST_PK(+) " );
                    strBuilder.Append(" AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK(+) " );
                    strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK=QTRN.QUOTATION_SEA_FK " );
                    strBuilder.Append(" AND QTRN.QUOTE_TRN_SEA_PK=QTRNCHRG.QUOTE_TRN_SEA_FK " );
                    strBuilder.Append(" AND QCTMT.CONTAINER_TYPE_MST_PK=QTRN.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append("  " + arrCCondition[0] + " " );
                    strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK= " + intQuotationPK );
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CONTAINER_TYPE_MST_FK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN_SEA_FCL_LCL BTRN WHERE BTRN.BOOKING_SEA_FK =" + BookingPK + ")");
                    }
                    strBuilder.Append(" GROUP BY QHDR.QUOTATION_SEA_PK, qtrn.all_in_tariff, all_in_quoted_tariff,QHDR.QUOTATION_REF_NO,QOMT.OPERATOR_ID, " );
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_ID, QCMT.COMMODITY_NAME, " );
                    strBuilder.Append(" QCTMT.CONTAINER_TYPE_MST_PK, " );
                    strBuilder.Append(" QCMT.COMMODITY_MST_PK, " );
                    strBuilder.Append(" QOMT.OPERATOR_MST_PK,QTRN.EXPECTED_BOXES,QTRN.COMMODITY_MST_FKS,QTRN.COMMODITY_MST_FK");
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
                        strBuilder.Append("       '' AS TRANSACTIONPK,");
                        strBuilder.Append("       NULL AS BASISPK");
                        strBuilder.Append("  FROM CONTAINER_TYPE_MST_TBL OCTMT");
                        strBuilder.Append(" WHERE 1 = 1");
                        strBuilder.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK NOT IN");
                        strBuilder.Append("       (SELECT QT.CONTAINER_TYPE_MST_FK");
                        strBuilder.Append("          FROM QUOTATION_SEA_TBL Q, QUOTATION_TRN_SEA_FCL_LCL QT");
                        strBuilder.Append("         WHERE Q.QUOTATION_SEA_PK = QT.QUOTATION_SEA_FK");
                        strBuilder.Append("           AND Q.QUOTATION_SEA_PK = " + intQuotationPK + ")");
                        strBuilder.Append("   AND OCTMT.CONTAINER_TYPE_MST_PK IN (" + strContainer + ")");
                        strBuilder.Append("   AND OCTMT.ACTIVE_FLAG = 1");
                        strBuilder.Append(" GROUP BY OCTMT.CONTAINER_TYPE_MST_ID, OCTMT.CONTAINER_TYPE_MST_PK");
                    }
                }
                return strBuilder.ToString();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                        strBuilder.Append(" '' AS CARGO, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" '' AS CONTAINERPK, " );
                        strBuilder.Append(" '' AS CARGOPK, " );
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
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK= " & intCommodityPK & vbCrLf)
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
                        strBuilder.Append(" '' AS CARGO, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" '' AS CONTAINERPK, " );
                        strBuilder.Append(" '' AS CARGOPK, " );
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
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK= " & intCommodityPK & vbCrLf)
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
                    //modifying by thiyagarajan on 23/10/08 to make commodity optional for FCL as per prabhu sugges.
                    if (intFlag == 1)
                    {
                        strBuilder.Append(" SELECT SRRSRST.RFQ_SPOT_SEA_PK AS TRNTYPEPK, " );
                        strBuilder.Append(" '2' AS TRNTYPESTATUS, " );
                        strBuilder.Append(" 'Sp Rate' AS CONTRACTTYPE, " );
                        strBuilder.Append(" SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append(" SROMT.OPERATOR_ID AS OPERATOR, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                        strBuilder.Append(" '' AS BOXES, " );
                        strBuilder.Append(" '' AS CARGO, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                        strBuilder.Append(" '' AS CARGOPK, " );
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, " );
                        strBuilder.Append(" '' AS TRANSACTIONPK, " );
                        strBuilder.Append(" NULL AS BASISPK " );
                        strBuilder.Append(" FROM " );
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append(" CONTAINER_TYPE_MST_TBL SRCTMT, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, " );
                        //Snigdharani - 04/11/2008 - Removing v-array
                        //strBuilder.Append(" TABLE(SRRSTSF.CONTAINER_DTL_FCL) (+) SRRST, " & vbCrLf)
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
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK(+)= " & intCommodityPK & vbCrLf)
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
                        strBuilder.Append(" '' AS CARGO, " );
                        strBuilder.Append(" SRCMT.COMMODITY_NAME AS COMMODITY, " );
                        strBuilder.Append(" NULL AS RATE, " );
                        strBuilder.Append(" '' AS BKGRATE, " );
                        strBuilder.Append(" '' AS NET, " );
                        strBuilder.Append(" '' AS TOTALRATE, " );
                        strBuilder.Append(" '0' AS SEL, " );
                        strBuilder.Append(" SRCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                        strBuilder.Append(" '' AS CARGOPK, " );
                        strBuilder.Append(" SRCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                        strBuilder.Append(" SROMT.OPERATOR_MST_PK AS OPERATORPK, " );
                        strBuilder.Append(" '' AS TRANSACTIONPK, " );
                        strBuilder.Append(" NULL AS BASISPK " );
                        strBuilder.Append(" FROM " );
                        strBuilder.Append(" RFQ_SPOT_RATE_SEA_TBL SRRSRST, " );
                        strBuilder.Append(" OPERATOR_MST_TBL SROMT, " );
                        strBuilder.Append(" CONTAINER_TYPE_MST_TBL SRCTMT, " );
                        strBuilder.Append(" RFQ_SPOT_TRN_SEA_FCL_LCL SRRSTSF, " );
                        //Snigdharani - 04/11/2008 - Removing v-array
                        //strBuilder.Append(" TABLE(SRRSTSF.CONTAINER_DTL_FCL) (+) SRRST, " & vbCrLf)
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
                        //strBuilder.Append(" AND SRCMT.COMMODITY_GROUP_FK= " & intCommodityPK & vbCrLf)
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strBuilder.Append(" '' AS CARGO, " );
                    strBuilder.Append(" CCCMT.COMMODITY_ID AS COMMODITY, " );
                    strBuilder.Append(" NVL(" + strOperatorRate.ToString() + ", NULL) AS RATE, " );
                    strBuilder.Append(" '' AS BKGRATE, " );
                    strBuilder.Append(" '' AS NET, " );
                    strBuilder.Append(" '' AS TOTALRATE, " );
                    strBuilder.Append(" '0' AS SEL, " );
                    strBuilder.Append(" '' AS CONTAINERPK, " );
                    strBuilder.Append(" '' AS CARGOPK, " );
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
                    strBuilder.Append(" AND CCHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)" );
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
                    //Snigdharani - 05/11/2008 - Removing v-array
                    //strOperatorRate.Append("   TABLE(tx.CONTAINER_DTL_FCL) (+) cx                                " & vbCrLf)
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
                    strBuilder.Append(" '' AS CARGO, " );
                    strBuilder.Append("CCCMT.COMMODITY_ID AS COMMODITY, " );
                    strBuilder.Append("NVL(" + strOperatorRate.ToString() + ", NULL) AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("CCCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                    strBuilder.Append(" '' AS CARGOPK, " );
                    strBuilder.Append("CCCMT.COMMODITY_MST_PK AS COMMODITYPK, " );
                    strBuilder.Append("CCOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("NULL AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("CONT_CUST_SEA_TBL CCHDR, " );
                    strBuilder.Append("CONT_CUST_TRN_SEA_TBL CCTRN, " );
                    //strBuilder.Append("CONT_SUR_CHRG_SEA_TBL CCTRNCHRG, " & vbCrLf)
                    strBuilder.Append("OPERATOR_MST_TBL CCOMT, " );
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL CCCTMT, " );
                    strBuilder.Append("COMMODITY_MST_TBL CCCMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND CCHDR.OPERATOR_MST_FK=CCOMT.OPERATOR_MST_PK(+) " );
                    strBuilder.Append("AND CCHDR.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) " );
                    strBuilder.Append("AND CCHDR.CONT_CUST_SEA_PK=CCTRN.CONT_CUST_SEA_FK " );
                    //strBuilder.Append("AND CCTRN.CONT_CUST_TRN_SEA_PK=CCTRNCHRG.CONT_CUST_TRN_SEA_FK " & vbCrLf)
                    strBuilder.Append("AND CCCTMT.CONTAINER_TYPE_MST_PK = CCTRN.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append("AND CCHDR.STATUS=2 " );
                    strBuilder.Append("AND CCHDR.CARGO_TYPE=1 " + arrCCondition[2] );
                    strBuilder.Append("AND CCHDR.CUSTOMER_MST_FK= " + intCustomerPK );
                    strBuilder.Append("AND CCHDR.COMMODITY_GROUP_MST_FK= " + intCommodityPK );
                    strBuilder.Append("AND CCTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND CCTRN.PORT_MST_POD_FK= " + strPOD );

                    //strBuilder.Append("AND TO_DATE('" & strSDate & "','" & dateFormat & "') BETWEEN CCHDR.VALID_FROM " & vbCrLf)
                    //strBuilder.Append("AND NVL(CCHDR.VALID_TO,TO_DATE('" & strSDate & "','" & dateFormat & "')) " & vbCrLf)
                    if (string.IsNullOrEmpty(custcontRefNr))
                    {
                        strBuilder.Append("AND TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN CCHDR.VALID_FROM " );
                        strBuilder.Append("AND NVL(CCHDR.VALID_TO,TO_DATE('" + strSDate + "','" + dateFormat + "')) " );
                    }
                    else
                    {
                        strBuilder.Append(" and CCHDR.CONT_REF_NO = '" + custcontRefNr + "' ");
                    }
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND CCCTMT.CONTAINER_TYPE_MST_PK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN_SEA_FCL_LCL BTRN WHERE BTRN.BOOKING_SEA_FK =" + BookingPK + ")");
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strOperatorRate = " ( Select Sum(NVL(LCL_APPROVED_RATE * EXCHANGE_RATE,0) )                     " + "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v  " + "   where m.ACTIVE                     = 1  AND v.EXCH_RATE_TYPE_FK = 1     AND                         " + "         m.CONT_APPROVED              = 1     AND                         " + "         m.CARGO_TYPE                 = 2     AND                         " + "         m.OPERATOR_MST_FK            = OHDR.OPERATOR_MST_FK AND          " + "         m.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " + "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " + "         t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " + "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " + "         t.LCL_BASIS                  = OTRN.LCL_BASIS               AND " + "         t.PORT_MST_POL_FK            = OTRN.PORT_MST_POL_FK          AND " + "         t.PORT_MST_POD_FK            = OTRN.PORT_MST_POD_FK         AND " + "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " + "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " + "         sysdate between v.FROM_DATE and v.TO_DATE )                     ";

                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append("'4' AS TRNTYPESTATUS, " );
                    strBuilder.Append("'SL Tariff' AS CONTRACTTYPE, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append("'' AS QTY, " );
                    strBuilder.Append("'' AS CARGO, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("'' AS CONTAINERPK, " );
                    strBuilder.Append("'' AS CARGOPK, " );
                    strBuilder.Append("NULL AS COMMODITYPK, " );
                    strBuilder.Append("OOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("OUOM.DIMENTION_UNIT_MST_PK AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, " );
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL OUOM " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND OHDR.OPERATOR_MST_FK=OOMT.OPERATOR_MST_PK(+) " );
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
                    strBuilder.Append("OHDR.OPERATOR_MST_FK ,OTRN.LCL_BASIS, OTRN.PORT_MST_POL_FK, OTRN.PORT_MST_POD_FK");
                }
                else
                {
                    strOperatorRate = " ( Select Sum(NVL(FCL_APP_RATE * EXCHANGE_RATE,0) )                     " + "   from  CONT_MAIN_SEA_TBL m, CONT_TRN_SEA_FCL_LCL t, V_EXCHANGE_RATE v,  ";
                    // & vbCrLf & _
                    //Snigdharani - 05/11/2008 - Removing v-array
                    strOperatorRate = strOperatorRate + "CONT_TRN_SEA_FCL_RATES c                                 " + "   where m.ACTIVE                     = 1   AND v.EXCH_RATE_TYPE_FK = 1    AND                         " + "         m.CONT_APPROVED              = 1     AND                         " + "         m.CARGO_TYPE                 = 1     AND                         " + "         t.CONT_TRN_SEA_PK = c.CONT_TRN_SEA_FK AND                        " + "         m.OPERATOR_MST_FK            = OHDR.OPERATOR_MST_FK AND         " + "         m.COMMODITY_GROUP_FK         =  " + intCommodityPK + " AND " + "         TO_DATE('" + strSDate + "','" + dateFormat + "') BETWEEN                 " + "           t.VALID_FROM   AND   NVL(t.VALID_TO,NULL_DATE_FORMAT)         AND " + "         t.CONT_MAIN_SEA_FK           = m.CONT_MAIN_SEA_PK            AND " + "         c.CONTAINER_TYPE_MST_FK      = OTRNCONT.CONTAINER_TYPE_MST_FK   AND " + "         t.PORT_MST_POL_FK            = OTRN.PORT_MST_POL_FK         AND " + "         t.PORT_MST_POD_FK            = OTRN.PORT_MST_POD_FK         AND " + "         t.CHECK_FOR_ALL_IN_RT        = 1                             AND " + "         V.CURRENCY_MST_BASE_FK       = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " AND " + "         t.CURRENCY_MST_FK            = v.CURRENCY_MST_FK AND             " + "         sysdate between v.FROM_DATE and v.TO_DATE )                     ";

                    strBuilder.Append("SELECT OHDR.TARIFF_MAIN_SEA_PK AS TRNTYPEPK, " );
                    strBuilder.Append("'4' AS TRNTYPESTATUS, " );
                    strBuilder.Append("'SL Tariff' AS CONTRACTTYPE, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OOMT.OPERATOR_ID AS OPERATOR, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("'' AS BOXES, " );
                    strBuilder.Append("'' AS CARGO, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("NVL(" + strOperatorRate + ", NULL) AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                    strBuilder.Append("'' AS CARGOPK, " );
                    strBuilder.Append("NULL AS COMMODITYPK, " );
                    strBuilder.Append("OOMT.OPERATOR_MST_PK AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("NULL AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //strBuilder.Append("TABLE(OTRN.CONTAINER_DTL_FCL) (+) OTRNCONT, " & vbCrLf)
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strBuilder.Append("'' AS CARGO, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("NULL AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("'' AS CONTAINERPK, " );
                    strBuilder.Append("'' AS CARGOPK, " );
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
                    strBuilder.Append("'' AS CARGO, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("NULL AS RATE, " );
                    strBuilder.Append("'' AS BKGRATE, " );
                    strBuilder.Append("'' AS NET, " );
                    strBuilder.Append("'' AS TOTALRATE, " );
                    strBuilder.Append("'0' AS SEL, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_PK AS CONTAINERPK, " );
                    strBuilder.Append("'' AS CARGOPK, " );
                    strBuilder.Append("NULL AS COMMODITYPK, " );
                    strBuilder.Append("NULL AS OPERATORPK, " );
                    strBuilder.Append("'' AS TRANSACTIONPK, " );
                    strBuilder.Append("NULL AS BASISPK " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    //Modified by Snigdharani - 29/10/2008 - Removing v-array
                    //strBuilder.Append("TABLE(OTRN.CONTAINER_DTL_FCL) (+) OTRNCONT, " & vbCrLf)
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Fetch Freight"
        public DataTable FetchFreight(Int32 intQuotationPK = 0, Int16 intIsFcl = 0, Int32 intCustomerPK = 0, string strPOL = "", string strPOD = "", Int16 intCommodityPk = 0, string strSDate = "", string strContainer = "", short intSRateStatus = 0, short intCContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, short intSRRContractStatus = 0, Int32 intSpotRatePK = 0, string CustContRefNr = "", int EBKGSTATUS = 0, int BookingPK = 0)
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
                strCustomer = " AND (SRRSRST.CUSTOMER_MST_FK= " + intCustomerPK + " OR SRRSRST.CUSTOMER_MST_FK IS NULL)";

            }

            //QUOTATION
            if (!(intQuotationPK == 0))
            {
                strSql = FunMakeQuotationFreight(arrCCondition, intQuotationPK, strPOL, strPOD, intIsFcl, strContainer, EBKGSTATUS, BookingPK);
            }
            else
            {
                //SPOT RATE
                if (!(intSpotRatePK == 0))
                {
                    strSql = funSpotRateFreight(arrCCondition, strCustomer, intCommodityPk.ToString(), strPOL, strPOD, intSpotRatePK.ToString(), strSDate, intIsFcl, 1).ToString();
                }
                if (intSRateStatus == 1 & intSpotRatePK == 0)
                {
                    strSql = funSpotRateFreight(arrCCondition, strCustomer, intCommodityPk.ToString(), strPOL, strPOD, intSpotRatePK.ToString(), strSDate, intIsFcl, 2).ToString();
                }
                //CUSTOMER(CONTRACT)
                if (intCContractStatus == 1)
                {
                    strSql = funCustContFreight(arrCCondition, intCustomerPK, intCommodityPk.ToString(), strPOL, strPOD, strSDate, intIsFcl, getDefault(CustContRefNr, "").ToString(), EBKGSTATUS, BookingPK).ToString();
                }
                //SPECIAL RATE REQUEST CONTRACT
                if (intSRRContractStatus == 1)
                {
                    strSql = funSRRFreight(arrCCondition, intCustomerPK, intCommodityPk.ToString(), strPOL, strPOD, strSDate, intIsFcl).ToString();
                }
                //OPEARATOR TARIFF
                if (intOTariffStatus == 1)
                {
                    strSql = funSLTariffFreight(arrCCondition, intCommodityPk.ToString(), strPOL, strPOD, strSDate, intIsFcl).ToString();
                }
                //GENERAL TARIFF
                if (intGTariffStatus == 1)
                {
                    strSql = funGTariffFreight(arrCCondition, intCommodityPk.ToString(), strPOL, strPOD, strSDate, intIsFcl).ToString();
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string FunMakeQuotationFreight(ArrayList arrCCondition, Int32 intQuotationPK = 0, string strPOL = "", string strPOD = "", Int16 intIsFcl = 0, string strContainer = "", int EBKGSTATUS = 0, int BookingPK = 0)
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
                    strBuilder.Append("SELECT QTRN.QUOTE_TRN_SEA_PK AS TRNTYPEFK,  ");
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
                    strBuilder.Append("DECODE(QTRNCHRG.PYMT_TYPE, 1,'1','2') AS PYMT_TYPE,QFEMT.CREDIT,QTRNCHRG.CHECK_ADVATOS ");
                    strBuilder.Append("FROM ");
                    strBuilder.Append("QUOTATION_SEA_TBL QHDR, ");
                    strBuilder.Append("QUOTATION_TRN_SEA_FCL_LCL QTRN, ");
                    strBuilder.Append("QUOTATION_TRN_SEA_FRT_DTLS QTRNCHRG, ");
                    strBuilder.Append("OPERATOR_MST_TBL QOMT, ");
                    strBuilder.Append("COMMODITY_MST_TBL QCMT, ");
                    strBuilder.Append("PORT_MST_TBL QPL, ");
                    strBuilder.Append("PORT_MST_TBL QPD, ");
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL QFEMT, ");
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL QCUMT, ");
                    strBuilder.Append("DIMENTION_UNIT_MST_TBL QUOM ");
                    strBuilder.Append("WHERE(1 = 1) ");
                    strBuilder.Append("AND QHDR.QUOTATION_SEA_PK=QTRN.QUOTATION_SEA_FK ");
                    strBuilder.Append("AND QTRN.QUOTE_TRN_SEA_PK=QTRNCHRG.QUOTE_TRN_SEA_FK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK=QPL.PORT_MST_PK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK=QPD.PORT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.FREIGHT_ELEMENT_MST_FK=QFEMT.FREIGHT_ELEMENT_MST_PK ");
                    strBuilder.Append("AND QTRNCHRG.CURRENCY_MST_FK=QCUMT.CURRENCY_MST_PK ");
                    strBuilder.Append("AND QTRN.OPERATOR_MST_FK=QOMT.OPERATOR_MST_PK (+)");
                    strBuilder.Append("AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK (+) ");
                    strBuilder.Append("AND QTRN.BASIS=QUOM.DIMENTION_UNIT_MST_PK ");
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK= " + strPOL);
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK= " + strPOD);
                    strBuilder.Append(" " + arrCCondition[0] + "");
                    strBuilder.Append("AND QHDR.QUOTATION_SEA_PK= " + intQuotationPK);
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
                        //strBuilder.Append("       OUOM.DIMENTION_UNIT_MST_PK AS BASISPK1,")
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
                        strBuilder.Append("          FROM QUOTATION_SEA_TBL Q, QUOTATION_TRN_SEA_FCL_LCL QT");
                        strBuilder.Append("         WHERE Q.QUOTATION_SEA_PK = QT.QUOTATION_SEA_FK");
                        strBuilder.Append("           AND Q.QUOTATION_SEA_PK = " + intQuotationPK + ")");
                    }
                    //'
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                else
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
                    strBuilder.Append("SELECT QTRN.QUOTE_TRN_SEA_PK AS TRNTYPEFK,  " );
                    strBuilder.Append("QHDR.QUOTATION_REF_NO AS REFNO, " );
                    strBuilder.Append("QCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("QCMT.COMMODITY_ID AS COMMODITY, " );
                    strBuilder.Append("QPL.PORT_MST_PK POLPK,QPL.PORT_ID AS POL, " );
                    strBuilder.Append("QPD.PORT_MST_PK PODPK,QPD.PORT_ID AS POD, " );
                    strBuilder.Append("QFEMT.FREIGHT_ELEMENT_MST_PK, QFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(QFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, ");
                    strBuilder.Append("DECODE(QTRNCHRG.CHECK_FOR_ALL_IN_RT, 1,'1','0') AS CHECK_FOR_ALL_IN_RT, " );
                    strBuilder.Append("QCUMT.CURRENCY_MST_PK,QCUMT.CURRENCY_ID, " );
                    strBuilder.Append("QTRNCHRG.QUOTED_RATE AS RATE, " );
                    strBuilder.Append("QTRNCHRG.QUOTED_RATE AS BKGRATE,QTRNCHRG.Tariff_Rate AS TOTAL, QTRN.BASIS AS BASISPK, " );
                    strBuilder.Append("DECODE(QTRNCHRG.PYMT_TYPE, 1,'1','2') AS PYMT_TYPE,QFEMT.CREDIT,QTRNCHRG.CHECK_ADVATOS " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("QUOTATION_SEA_TBL QHDR, " );
                    strBuilder.Append("QUOTATION_TRN_SEA_FCL_LCL QTRN, " );
                    strBuilder.Append("QUOTATION_TRN_SEA_FRT_DTLS QTRNCHRG, " );
                    strBuilder.Append("OPERATOR_MST_TBL QOMT,  " );
                    strBuilder.Append("CONTAINER_TYPE_MST_TBL QCTMT,  " );
                    strBuilder.Append("COMMODITY_MST_TBL QCMT,  " );
                    strBuilder.Append("PORT_MST_TBL QPL,  " );
                    strBuilder.Append("PORT_MST_TBL QPD,  " );
                    strBuilder.Append("FREIGHT_ELEMENT_MST_TBL QFEMT,  " );
                    strBuilder.Append("CURRENCY_TYPE_MST_TBL QCUMT " );
                    strBuilder.Append("WHERE(1 = 1) " );
                    strBuilder.Append("AND QHDR.QUOTATION_SEA_PK=QTRN.QUOTATION_SEA_FK " );
                    strBuilder.Append("AND QTRN.QUOTE_TRN_SEA_PK=QTRNCHRG.QUOTE_TRN_SEA_FK " );
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK=QPL.PORT_MST_PK " );
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK=QPD.PORT_MST_PK " );
                    strBuilder.Append("AND QTRNCHRG.FREIGHT_ELEMENT_MST_FK=QFEMT.FREIGHT_ELEMENT_MST_PK " );
                    strBuilder.Append("AND QTRNCHRG.CURRENCY_MST_FK=QCUMT.CURRENCY_MST_PK " );
                    strBuilder.Append("AND QTRN.OPERATOR_MST_FK=QOMT.OPERATOR_MST_PK (+)" );
                    strBuilder.Append("AND QTRN.COMMODITY_MST_FK=QCMT.COMMODITY_MST_PK (+)" );
                    strBuilder.Append("AND QCTMT.CONTAINER_TYPE_MST_PK=QTRN.CONTAINER_TYPE_MST_FK " );
                    strBuilder.Append("AND QTRN.PORT_MST_POL_FK= " + strPOL );
                    strBuilder.Append("AND QTRN.PORT_MST_POD_FK= " + strPOD );
                    strBuilder.Append(" " + arrCCondition[0] + "");
                    strBuilder.Append(" AND QHDR.QUOTATION_SEA_PK= " + intQuotationPK );
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND QTRN.CONTAINER_TYPE_MST_FK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN_SEA_FCL_LCL BTRN WHERE BTRN.BOOKING_SEA_FK =" + BookingPK + ")");
                    }
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
                        strBuilder.Append("                  FROM QUOTATION_SEA_TBL Q, QUOTATION_TRN_SEA_FCL_LCL QT");
                        strBuilder.Append("                 WHERE Q.QUOTATION_SEA_PK = QT.QUOTATION_SEA_FK");
                        strBuilder.Append("                   AND Q.QUOTATION_SEA_PK = " + intQuotationPK + ")");
                    }
                    //'
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strBuilder.ToString();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_MIN_RATE AS MIN_RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS BKGRATE,SRRSTSFL.LCL_APPROVED_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE,SRFEMT.CREDIT  " );
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
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
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
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRSTSFL.LCL_CURRENT_MIN_RATE AS MIN_RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS RATE, " );
                        strBuilder.Append("SRRSTSFL.LCL_APPROVED_RATE AS BKGRATE,SRRSTSFL.LCL_APPROVED_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE, SRFEMT.CREDIT " );
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
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
                    }
                }
                else
                {
                    //modifying by thiyagarajan on 23/10/08 to make commodity optional for FCL as per prabhu sugges.
                    if (intFlag == 1)
                    {

                        strBuilder.Append("SELECT SRRSTSFL.RFQ_SPOT_SEA_FK AS TRNTYPEFK, " );
                        strBuilder.Append("SRRSRST.RFQ_REF_NO AS REFNO, " );
                        strBuilder.Append("SRCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                        strBuilder.Append("SRCMT.COMMODITY_ID AS COMMODITY, " );
                        strBuilder.Append("SRPL.PORT_MST_PK,SRPL.PORT_ID AS POL, " );
                        strBuilder.Append("SRPD.PORT_MST_PK,SRPD.PORT_ID AS POD, " );
                        strBuilder.Append("SRFEMT.FREIGHT_ELEMENT_MST_PK, SRFEMT.FREIGHT_ELEMENT_ID, " );
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS RATE, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS BKGRATE,SRRST.FCL_APP_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE,SRFEMT.Credit " );
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
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
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
                        strBuilder.Append(" DECODE(SRFEMT.CHARGE_BASIS,0,'',1,'%',2,'Flat Rate',3,'Unit')CHARGE_BASIS, " );
                        strBuilder.Append("DECODE(SRRSTSFL.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                        strBuilder.Append("SRCUMT.CURRENCY_MST_PK,SRCUMT.CURRENCY_ID, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS RATE, " );
                        strBuilder.Append("SRRST.FCL_APP_RATE AS BKGRATE,SRRST.FCL_APP_RATE AS TOTAL, SRRSTSFL.LCL_BASIS AS BASISPK, " );
                        strBuilder.Append("'1' AS PYMT_TYPE, SRFEMT.CREDIT  " );
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
                        strBuilder.Append(" ORDER BY SRFEMT.PREFERENCE ");
                    }
                }
                return strBuilder.ToString();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strContRefNo.Append(" (   Select    DISTINCT  CONT_REF_NO " );
                    strContRefNo.Append("from  " );
                    strContRefNo.Append("CONT_CUST_SEA_TBL main7, " );
                    strContRefNo.Append("CONT_CUST_TRN_SEA_TBL tran7 " );
                    strContRefNo.Append("where " );
                    strContRefNo.Append("main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK " );
                    strContRefNo.Append("AND    main7.CARGO_TYPE            = 2 AND MAIN7.Active=1" );
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
                    strFreightElements.Append(" AND    main8.CARGO_TYPE            = 2 AND main8.Active=1                                  " );
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
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 2  and MAIN9.Active=1               " );
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
                    strBuilder.Append(" DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " );
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
                    strBuilder.Append(" '1' AS PYMT_TYPE,FRT2.Credit   " );
                    // strBuilder.Append(" frt2.PREFERENCE " & vbCrLf)
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
                    strBuilder.Append(" AND main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+) " );
                    strBuilder.Append(" AND TRAN2.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append(" AND main2.CARGO_TYPE  = 2  and main2.Active=1     " );
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
                    strBuilder.Append("     DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,              " );
                    strBuilder.Append("     DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,       " );
                    strBuilder.Append("     curr2.CURRENCY_MST_PK                      ,             " );
                    strBuilder.Append("     curr2.CURRENCY_ID                          ,             " );
                    strBuilder.Append("      NULL  MIN_RATE,           " );
                    strBuilder.Append("     frtd2.APP_SURCHARGE_AMT                    RATE,                " );
                    strBuilder.Append("     frtd2.APP_SURCHARGE_AMT                    BKGRATE,                " );
                    strBuilder.Append("     tran2.LCL_BASIS                            BASISPK,                   " );
                    strBuilder.Append("     '1' AS PYMT_TYPE,FRT2.Credit               " );
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
                    strBuilder.Append("     AND    main2.COMMODITY_MST_FK=CCCMT.COMMODITY_MST_PK (+)" );
                    strBuilder.Append("     AND    tran2.LCL_BASIS=CCUOM.DIMENTION_UNIT_MST_PK " );
                    strBuilder.Append("     AND    main2.CARGO_TYPE            = 2  and main2.Active=1              " );
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
                    if (EBKGSTATUS == 0)
                    {
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
                        strBuilder.Append("    DECODE(frt6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,         " );
                        strBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                        strBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        " );
                        strBuilder.Append("     curr6.CURRENCY_ID                          ,        " );
                        strBuilder.Append("  NULL  MIN_RATE, " );
                        strBuilder.Append("     cont6.FCL_REQ_RATE                         RATE,                " );
                        strBuilder.Append("     cont6.FCL_REQ_RATE                         BKGRATE,             " );
                        strBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               " );
                        strBuilder.Append("     '1'                                        PYMT_TYPE,FRT6.Credit  " );
                        strBuilder.Append("    from                                                             " );
                        strBuilder.Append("     TARIFF_MAIN_SEA_TBL            main6,                           " );
                        strBuilder.Append("     TARIFF_TRN_SEA_FCL_LCL         tran6,                           " );
                        strBuilder.Append("     TARIFF_TRN_SEA_CONT_DTL        cont6,                           " );
                        strBuilder.Append("     FREIGHT_ELEMENT_MST_TBL        frt6,                            " );
                        strBuilder.Append("     OPERATOR_MST_TBL               COOMT,                           " );
                        strBuilder.Append("     DIMENTION_UNIT_MST_TBL         CCUOM,                           " );
                        strBuilder.Append("     PORT_MST_TBL                   COPL,                            " );
                        strBuilder.Append("     PORT_MST_TBL                   COPD,                            " );
                        strBuilder.Append("     CURRENCY_TYPE_MST_TBL          curr6                            " );
                        strBuilder.Append("     where " + strContRefNo.ToString() + " IS NOT NULL AND                       " );
                        strBuilder.Append("            main6.TARIFF_MAIN_SEA_PK    = tran6.TARIFF_MAIN_SEA_FK           " );
                        strBuilder.Append("     AND    cont6.TARIFF_TRN_SEA_FK = tran6.TARIFF_TRN_SEA_PK           " );
                        strBuilder.Append("     AND    tran6.PORT_MST_POL_FK              = COPL.PORT_MST_PK           " );
                        strBuilder.Append("     AND    tran6.PORT_MST_POD_FK              = COPD.PORT_MST_PK           " );
                        strBuilder.Append("     AND    main6.OPERATOR_MST_FK              = COOMT.OPERATOR_MST_PK (+)     " );
                        strBuilder.Append("     AND    tran6.LCL_BASIS                    = CCUOM.DIMENTION_UNIT_MST_PK " );
                        strBuilder.Append("     AND    main6.CARGO_TYPE            = 2 and  main6.Active=1               " );
                        strBuilder.Append("     AND    main6.ACTIVE                = 1                                  " );
                        strBuilder.Append("     AND    tran6.FREIGHT_ELEMENT_MST_FK = frt6.FREIGHT_ELEMENT_MST_PK       " );
                        strBuilder.Append("     AND    tran6.CURRENCY_MST_FK       = curr6.CURRENCY_MST_PK              " );
                        strBuilder.Append("     AND    main6.COMMODITY_GROUP_FK    = " + intCommodityPK );
                        strBuilder.Append("     AND    tran6.PORT_MST_POL_FK       = " + strPOL );
                        strBuilder.Append("     AND    tran6.PORT_MST_POD_FK       = " + strPOD + arrCCondition[4] );

                        if (!string.IsNullOrEmpty(CustContRefNr))
                        {
                            //strBuilder.Append(" AND main6.cont_ref_no = '" & CustContRefNr & "'  " & vbCrLf)
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
                    }
                    strBuilder.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //Snigdharani - 23/12/2008 - order by preference
                    strBuilder.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append("     ORDER BY FRT.PREFERENCE ");

                }
                else
                {
                    strContRefNo.Append("(   Select    DISTINCT  CONT_REF_NO " );
                    strContRefNo.Append("    from                                                             " );
                    strContRefNo.Append("     CONT_CUST_SEA_TBL              main7,                           " );
                    strContRefNo.Append("     CONT_CUST_TRN_SEA_TBL          tran7                            " );
                    strContRefNo.Append("    where                                                                " );
                    strContRefNo.Append("            main7.CONT_CUST_SEA_PK      = tran7.CONT_CUST_SEA_FK              " );
                    strContRefNo.Append("     AND    main7.CARGO_TYPE            = 1  and Main7.Active=1              " );
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
                    strFreightElements.Append("     AND    main8.CARGO_TYPE            = 1 and  main8.Active=1                                  " );
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
                    strSurcharge.Append("     AND    main9.CARGO_TYPE            = 1  and  main9.Active=1                                 " );
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
                    strBuilder.Append("       Q.FREIGHT_ELEMENT_ID,Q.CHARGE_BASIS,");
                    strBuilder.Append("       Q.SEL,");
                    strBuilder.Append("       Q.CURRENCY_MST_PK,");
                    strBuilder.Append("       Q.CURRENCY_ID,");
                    strBuilder.Append("       Q.RATE,");
                    strBuilder.Append("       abs(Q.BKGRATE),Q.BKGRATE TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,q.Credit");
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
                    strBuilder.Append("  DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " );
                    strBuilder.Append(" 'true' SEL,  " );
                    strBuilder.Append(" curr2.CURRENCY_MST_PK, " );
                    strBuilder.Append(" curr2.CURRENCY_ID, " );
                    strBuilder.Append(" (CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN" );
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE " );
                    strBuilder.Append(" Else " );
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE " );
                    strBuilder.Append(" END) AS RATE, " );
                    strBuilder.Append(" (ABS(CASE WHEN TRAN2.APPROVED_BOF_RATE IS NOT NULL THEN" );
                    strBuilder.Append(" TRAN2.APPROVED_BOF_RATE " );
                    strBuilder.Append(" Else " );
                    strBuilder.Append(" TRAN2.CURRENT_BOF_RATE " );
                    strBuilder.Append(" END)*DECODE( FRT2.CREDIT ,NULL,1,0,-1,1,1)) AS BKGRATE, " );
                    strBuilder.Append(" tran2.LCL_BASIS BASISPK, " );
                    strBuilder.Append(" '1' AS PYMT_TYPE, FRT2.Credit   " );
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
                    strBuilder.Append(" AND main2.CARGO_TYPE  = 1 and main2.active=1       " );
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
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND CCCTMT.CONTAINER_TYPE_MST_PK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN_SEA_FCL_LCL BTRN WHERE BTRN.BOOKING_SEA_FK =" + BookingPK + ")");
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
                    strBuilder.Append("  DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " );
                    strBuilder.Append(" DECODE(frtd2.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,       " );
                    strBuilder.Append(" curr2.CURRENCY_MST_PK                      ,             " );
                    strBuilder.Append(" curr2.CURRENCY_ID                          ,             " );
                    strBuilder.Append(" frtd2.APP_SURCHARGE_AMT                    RATE,                " );
                    strBuilder.Append(" (ABS(frtd2.APP_SURCHARGE_AMT)*DECODE( FRT2.CREDIT ,NULL,1,0,-1,1,1))                    BKGRATE,                " );
                    strBuilder.Append(" tran2.LCL_BASIS                            BASISPK,                   " );
                    strBuilder.Append(" '1' AS PYMT_TYPE , FRT2.Credit             " );
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
                    strBuilder.Append(" AND    main2.CARGO_TYPE            = 1  and  main2.Active=1              " );
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
                    if (EBKGSTATUS == 1 & BookingPK > 0)
                    {
                        strBuilder.Append(" AND CCCTMT.CONTAINER_TYPE_MST_PK IN (SELECT BTRN.CONTAINER_TYPE_MST_FK ");
                        strBuilder.Append(" FROM BOOKING_TRN_SEA_FCL_LCL BTRN WHERE BTRN.BOOKING_SEA_FK =" + BookingPK + ")");
                    }
                    if (EBKGSTATUS == 0)
                    {
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
                        strBuilder.Append("  DECODE(frt6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS, " );
                        strBuilder.Append(" DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                        strBuilder.Append(" curr6.CURRENCY_MST_PK                      ,        " );
                        strBuilder.Append(" curr6.CURRENCY_ID                          ,        " );
                        strBuilder.Append(" cont6.FCL_REQ_RATE                         RATE,                " );
                        strBuilder.Append(" (ABS(cont6.FCL_REQ_RATE)*DECODE(FRT6.CREDIT ,NULL,1,0,-1,1,1))     BKGRATE,             " );
                        strBuilder.Append(" tran6.LCL_BASIS                            BASISPK,               " );
                        strBuilder.Append(" '1'                                        PYMT_TYPE,FRT6.Credit           " );
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
                            // strBuilder.Append(" AND main6.cont_ref_no = '" & CustContRefNr & "'  " & vbCrLf)
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   " );
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
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE , FRT2.Credit  " );
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
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   " );
                    strSRRSBuilder.Append("     DECODE(SRRSUR.CHECK_FOR_ALL_IN_RT, 1,'true','false') SEL,         " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,                              " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID                          ,             " );
                    strSRRSBuilder.Append("  NULL  MIN_RATE, " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT RATE,               " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT AS BKGRATE,  " );
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,                 " );
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE , FRT2.Credit              " );
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
                    strSRRSBuilder.Append("    DECODE(FRT6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   " );
                    strSRRSBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_ID                          ,        " );
                    strSRRSBuilder.Append("  NULL  MIN_RATE, " );
                    strSRRSBuilder.Append("     TRAN6.LCL_TARIFF_RATE                      RATE,                " );
                    strSRRSBuilder.Append("     TRAN6.LCL_TARIFF_RATE                      BKGRATE,             " );
                    strSRRSBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               " );
                    strSRRSBuilder.Append("     '1'                                        PYMT_TYPE ,FRT6.Credit           " );
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
                    //Snigdharani - 23/12/2008 - order by preference'
                    strSRRSBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strSRRSBuilder.Append(" ORDER BY FRT.PREFERENCE ");
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
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   " );
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
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE , FRT2.Credit " );
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
                    strSRRSBuilder.Append("    DECODE(FRT2.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   " );
                    strSRRSBuilder.Append("     DECODE(SRRSUR.CHECK_FOR_ALL_IN_RT, 1,'true','false') SEL,         " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_MST_PK,                              " );
                    strSRRSBuilder.Append("     curr2.CURRENCY_ID                          ,             " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT RATE,               " );
                    strSRRSBuilder.Append("     SRRSUR.APP_SURCHARGE_AMT AS BKGRATE,  " );
                    strSRRSBuilder.Append("     SRRTRN.LCL_BASIS AS BASISPK,                 " );
                    strSRRSBuilder.Append("     '1' AS PYMT_TYPE,FRT2.Credit              " );
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
                    strSRRSBuilder.Append("    DECODE(FRT6.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit')CHARGE_BASIS,   " );
                    strSRRSBuilder.Append("     DECODE(tran6.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL,                 " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_MST_PK                      ,        " );
                    strSRRSBuilder.Append("     curr6.CURRENCY_ID                          ,        " );
                    strSRRSBuilder.Append("     cont6.FCL_REQ_RATE                         RATE,                " );
                    strSRRSBuilder.Append("     cont6.FCL_REQ_RATE                         BKGRATE,             " );
                    strSRRSBuilder.Append("     tran6.LCL_BASIS                            BASISPK,               " );
                    strSRRSBuilder.Append("     '1'                                        PYMT_TYPE,FRT6.Credit            " );
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
                    //Snigdharani - 23/12/2008 - order by preference'
                    strSRRSBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strSRRSBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strSRRSBuilder.ToString();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strBuilder.Append("       abs(Q.BKGRATE)BKGRATE,(abs(TOTAL)*DECODE(Q.Credit,NULL,1,0,-1,1,1))TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    strBuilder.Append("       Q.PYMT_TYPE,q.credit");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_MIN_RATE AS MIN_RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS BKGRATE,OTRN.LCL_TARIFF_RATE TOTAL, OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE,OFEMT.credit " );
                    strBuilder.Append("FROM " );
                    strBuilder.Append("TARIFF_MAIN_SEA_TBL OHDR, " );
                    strBuilder.Append("TARIFF_TRN_SEA_FCL_LCL OTRN, " );
                    strBuilder.Append("OPERATOR_MST_TBL OOMT, " );
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
                    //Snigdharani - 23/12/2008 - order by preference'
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
                    strBuilder.Append("       Q.BKGRATE, (abs(TOTAL)*DECODE(Q.Credit,NULL,1,0,-1,1,1))TOTAL,");
                    strBuilder.Append("       Q.BASISPK,");
                    //strBuilder.Append("       Q.PYMT_TYPE, ''FrtTransPk,q.Credit")
                    strBuilder.Append("       Q.PYMT_TYPE,q.Credit");
                    strBuilder.Append("       FROM  (");

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS RATE, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS BKGRATE,OTRNCONT.FCL_REQ_RATE TOTAL, OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE, OFEMT.Credit " );
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
                    strBuilder.Append(" ORDER BY OFEMT.PREFERENCE ");
                    strBuilder.Append(") Q, FREIGHT_ELEMENT_MST_TBL FRT  ");
                    //Snigdharani - 23/12/2008 - order by preference'
                    strBuilder.Append("WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FREIGHT_ELEMENT_MST_PK  ");
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strBuilder.ToString();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OUOM.DIMENTION_ID AS BASIS, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false') AS SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_MIN_RATE AS MIN_RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS RATE, " );
                    strBuilder.Append("OTRN.LCL_TARIFF_RATE AS BKGRATE,OTRN.LCL_TARIFF_RATE AS TOTAL, OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE,OFEMT.credit " );
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

                    strBuilder.Append("SELECT OTRN.TARIFF_MAIN_SEA_FK AS TRNTYPEFK, " );
                    strBuilder.Append("OHDR.TARIFF_REF_NO AS REFNO, " );
                    strBuilder.Append("OCTMT.CONTAINER_TYPE_MST_ID AS TYPE, " );
                    strBuilder.Append("NULL AS COMMODITY, " );
                    strBuilder.Append("OPL.PORT_MST_PK POLPK,OPL.PORT_ID AS POL, " );
                    strBuilder.Append("OPD.PORT_MST_PK PODPK,OPD.PORT_ID AS POD, " );
                    strBuilder.Append("OFEMT.FREIGHT_ELEMENT_MST_PK, OFEMT.FREIGHT_ELEMENT_ID, " );
                    strBuilder.Append("DECODE(OTRN.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') CHARGE_BASIS, " );
                    strBuilder.Append("DECODE(OTRN.CHECK_FOR_ALL_IN_RT, 1,'true','false')  SEL, " );
                    strBuilder.Append("OCUMT.CURRENCY_MST_PK,OCUMT.CURRENCY_ID, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS RATE, " );
                    strBuilder.Append("OTRNCONT.FCL_REQ_RATE AS BKGRATE,OTRNCONT.FCL_REQ_RATE AS TOTAL,  OTRN.LCL_BASIS AS BASISPK, " );
                    strBuilder.Append("'1' AS PYMT_TYPE,OFEMT.Credit " );
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
                    strBuilder.Append(" ORDER BY FRT.PREFERENCE ");
                }
                return strBuilder.ToString();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

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
                strCustQuery.Append("SELECT C.SEA_CREDIT_DAYS," );
                strCustQuery.Append(" C.SEA_CREDIT_LIMIT" );
                strCustQuery.Append("FROM customer_mst_tbl c" );
                strCustQuery.Append("WHERE c.customer_mst_pk=" + CustPk );
                OracleDataReader dr = default(OracleDataReader);
                if (type == 1)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD" );
                    strQuery.Append("  FROM SRR_SEA_TBL     C" );
                    strQuery.Append("  WHERE C.SRR_SEA_PK=" + Pk );
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
                        CreditDays = getDefault(dr[0], "").ToString();
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            OracleCommand SCD = default(OracleCommand);
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
                    var _with1 = selectCommand.Parameters;
                    _with1.Clear();
                    //BKG_TRN_SEA_FK_IN()
                    if (Convert.ToInt32(Mode) == 1)
                    {
                        _with1.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with1.Add("BKG_TRN_AIR_HAZ_SPL_PK_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with1.Add("BOOKING_TRN_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        _with1.Add("BKG_TRN_SEA_HAZ_SPL_PK_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
                    }


                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with1.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with1.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with1.Add("MIN_TEMP_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with1.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[3]) ? "" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with1.Add("MAX_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with1.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[5]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with1.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with1.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[7]) ? "": strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with1.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with1.Add("UN_NO_IN", getDefault(strParam[9], "")).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with1.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with1.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with1.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with1.Add("EMS_NUMBER_IN", getDefault(strParam[13], "")).Direction = ParameterDirection.Input;
                    _with1.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
                    _with1.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], "")).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    _with1.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    selectCommand.ExecuteNonQuery();
                    strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                    return strReturn;
                }
                catch (OracleException OraExp)
                {
                    throw OraExp;
                    //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    _with4.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with4.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with4.Add("MIN_TEMP_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with4.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with4.Add("MAX_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with4.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with4.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with4.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with4.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with4.Add("UN_NO_IN", getDefault(strParam[9], "")).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with4.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with4.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with4.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with4.Add("EMS_NUMBER_IN", getDefault(strParam[13], "")).Direction = ParameterDirection.Input;
                    _with4.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
                    _with4.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], "")).Direction = ParameterDirection.Input;
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
                    strQuery.Append("EMS_NUMBER," );
                    strQuery.Append("PROPER_SHIPPING_NAME," );
                    strQuery.Append("PACK_CLASS_TYPE FROM BKG_TRN_SEA_HAZ_SPL_REQ Q" );
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    _with6.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //IS_PERSHIABLE_GOODS_IN()
                    _with6.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with6.Add("MIN_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with6.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with6.Add("MAX_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with6.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with6.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with6.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    _with6.Add("REF_VENTILATION_IN", getDefault(strParam[10], "")).Direction = ParameterDirection.Input;
                    //RETURN_VALUE()
                    //sivachandran 26Jun2008 CR Date 04/06/2008 For Reefer special Requirment
                    _with6.Add("HAULAGE_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("GENSET_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("CO2_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("O2_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("REQ_SET_TEMP_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("REQ_SET_TEMP_UOM_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("AIR_VENTILATION_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("AIR_VENTILATION_UOM_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("DEHUMIDIFIER_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("FLOORDRAINS_IN", "").Direction = ParameterDirection.Input;
                    _with6.Add("DEFROSTING_INTERVAL_IN", "").Direction = ParameterDirection.Input;
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
                    strQuery.Append("Q.PACK_COUNT, " );
                    strQuery.Append("Q.REF_VENTILATION " );
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    strQuery.Append("NO_OF_SLOTS, " );
                    strQuery.Append("STOWAGE, " );
                    strQuery.Append("HANDLING_INSTR, " );
                    strQuery.Append("LASHING_INSTR " );
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                    _with8.Add("LENGTH_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with8.Add("LENGTH_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with8.Add("HEIGHT_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with8.Add("HEIGHT_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with8.Add("WIDTH_IN", getDefault(strParam[1], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with8.Add("WIDTH_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with8.Add("WEIGHT_IN", getDefault(strParam[3], "")).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with8.Add("WEIGHT_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with8.Add("VOLUME_IN", "").Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with8.Add("VOLUME_UOM_MST_FK_IN", "").Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with8.Add("SLOT_LOSS_IN", "").Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with8.Add("LOSS_QUANTITY_IN", "").Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with8.Add("APPR_REQ_IN", "").Direction = ParameterDirection.Input;
                    if (Convert.ToBoolean(strParam[4]) == true)
                    {
                        _with8.Add("STOWAGE_IN", 1).Direction = ParameterDirection.Input;
                    }
                    else
                    {
                        _with8.Add("STOWAGE_IN", 2).Direction = ParameterDirection.Input;
                    }
                    _with8.Add("HAND_INST_IN", (string.IsNullOrEmpty(strParam[6]) ? "" : strParam[6])).Direction = ParameterDirection.Input;
                    _with8.Add("LASH_INST_IN", (string.IsNullOrEmpty(strParam[7]) ? "" : strParam[7])).Direction = ParameterDirection.Input;
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
                //dtMain = objwf.GetDataTable(strSqlCDimension.ToString)
                //Return dtMain
                return dtCM;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region "Save"

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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                //Throw ex
                return "All Data Saved Successfully. Due To Server Problem Mail Has Not Been Sent.";
            }
        }
        public ArrayList SaveJobCard(long PkValue, WorkFlow objWF, string LocationPK, string PodLocPk, string ShipperPK = "", string ConsignePK = "", string strVoyagePk = "", string JCPks = "")
        {
            // Dim objWK As New WorkFlow
            string strValueArgument = null;
            int JobFlag = 0;

            arrMessage.Clear();
            JobFlag = CheckJobcard(Convert.ToInt16(PkValue));
            try
            {
                if (JobFlag == 0)
                {
                    var _with9 = objWF.MyCommand;
                    _with9.CommandType = CommandType.StoredProcedure;
                    _with9.CommandText = objWF.MyUserName + ".BOOKING_SEA_PKG.JOB_CARD_SEA_EXP_TBL_INS";
                    _with9.Parameters.Clear();

                    _with9.Parameters.Add("BOOKING_SEA_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with9.Parameters["BOOKING_SEA_FK_IN"].SourceVersion = DataRowVersion.Current;
                    // Amit Singh 18June07: To insert Location and Freight Payer in Job Card
                    _with9.Parameters.Add("LOCATION_MST_FK_IN", LocationPK).Direction = ParameterDirection.Input;
                    _with9.Parameters["LOCATION_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with9.Parameters.Add("POD_FK_IN", getDefault(PodLocPk, "")).Direction = ParameterDirection.Input;
                    _with9.Parameters["POD_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with9.Parameters.Add("SHIPPER_MST_FK_IN", ShipperPK).Direction = ParameterDirection.Input;
                    _with9.Parameters["SHIPPER_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    _with9.Parameters.Add("CONSIGNEE_MST_FK_IN", getDefault(ConsignePK, "")).Direction = ParameterDirection.Input;
                    _with9.Parameters["CONSIGNEE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
                    // End
                    _with9.Parameters.Add("VOYAGE_PK_IN", strVoyagePk).Direction = ParameterDirection.Input;
                    _with9.Parameters["VOYAGE_PK_IN"].SourceVersion = DataRowVersion.Current;

                    //Return value of the proc.
                    _with9.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    //Execute the command
                    _with9.ExecuteNonQuery();
                    //Manoharan 16May07: to get JobCard Pk for insert BuyingCost to JCPIA
                    strRet = (!string.IsNullOrEmpty(_with9.Parameters["RETURN_VALUE"].Value.ToString()) ? "" : _with9.Parameters["RETURN_VALUE"].Value.ToString());
                    JCPks = strRet;
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
        
        public object GetQuotationDetails(string PrevQuotaion)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow Objwk = new WorkFlow();
            try
            {
                sb.Append("SELECT QST.QUOTATION_SEA_PK,");
                sb.Append("       UPPER(QST.QUOTATION_REF_NO) AS QUOTATION_REF_NO");
                sb.Append("  FROM QUOTATION_SEA_TBL    QST");
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
        public new string GenerateQuoteNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
        {
            return GenerateProtocolKey("QUOTATION (SEA)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, ObjWK);
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
                sb.Append("      FROM JOB_CARD_SEA_EXP_TBL JC ");
                sb.Append("     WHERE JC.BOOKING_SEA_FK = " + BookingPK);
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
                        var _with12 = SelectCommand;
                        _with12.CommandType = CommandType.StoredProcedure;
                        _with12.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_TRN_SEA_OTH_CHRG_INS";
                        SelectCommand.Parameters.Clear();

                        //Booking Sea Fk 
                        _with12.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with12.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with12.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                        //Return value of the proc.
                        _with12.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
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
                        _with13.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_TRN_SEA_OTH_CHRG_UPD";
                        SelectCommand.Parameters.Clear();

                        //Booking Sea Fk 
                        _with13.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with13.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("AMOUNT_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                        _with13.Parameters.Add("FREIGHT_TYPE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["FREIGHT_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                        //Return value of the proc.
                        _with13.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
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
        //Save and update the Flat rates 

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
                        _with14.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_INS";
                        SelectCommand.Parameters.Clear();

                        //Booking Sea Fk 
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
                        _with15.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_CARGO_CALC_UPD";
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

                        //Manoharan 03Jan07: to save Measurement, weight and Division factor 
                        _with15.Parameters.Add("CARGO_MEASURE_IN", Measure).Direction = ParameterDirection.Input;
                        _with15.Parameters.Add("CARGO_WT_IN", Wt).Direction = ParameterDirection.Input;
                        _with15.Parameters.Add("CARGO_DIVFAC_IN", Divfac).Direction = ParameterDirection.Input;

                        //Return value of the proc.
                        _with15.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
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
        //Save the cargo calc details 

        //public ArrayList SaveBookingTRN(DataSet dsMain, DataSet dsCtrDetails, long PkValue, OracleCommand SelectCommand, bool IsUpdate)
        //{
        //    Int32 nRowCnt = default(Int32);
        //    WorkFlow objWK = new WorkFlow();
        //    string strValueArgument = null;
        //    DataSet dsGrid = new DataSet();
        //    if ((HttpContext.Current.Session["ctrdetails"] != null))
        //    {
        //        dsGrid = (DataSet)HttpContext.Current.Session["ctrdetails"];
        //    }

        //    arrMessage.Clear();
        //    try
        //    {

        //        if (!IsUpdate)
        //        {

        //            for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
        //            {
        //                var _with16 = SelectCommand;
        //                _with16.CommandType = CommandType.StoredProcedure;
        //                _with16.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_INS";
        //                SelectCommand.Parameters.Clear();

        //                //BOOKING_SEA_FK_IN 
        //                _with16.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

        //                //TRANS_REFERED_FROM_IN
        //                _with16.Parameters.Add("TRANS_REFERED_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;
        //                _with16.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;

        //                //TRANS_REF_NO_IN
        //                _with16.Parameters.Add("TRANS_REF_NO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]).Direction = ParameterDirection.Input;
        //                _with16.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

        //                //CONTAINER_TYPE_MST_FK_IN
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
        //                {
        //                    _with16.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with16.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
        //                }
        //                _with16.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                //NO_OF_BOXES_IN
        //                _with16.Parameters.Add("NO_OF_BOXES_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"]).Direction = ParameterDirection.Input;
        //                _with16.Parameters["NO_OF_BOXES_IN"].SourceVersion = DataRowVersion.Current;

        //                //BASIS_IN
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
        //                {
        //                    _with16.Parameters.Add("BASIS_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with16.Parameters.Add("BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]).Direction = ParameterDirection.Input;
        //                }
        //                _with16.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

        //                //QUANTITY_IN
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"].ToString()))
        //                {
        //                    _with16.Parameters.Add("QUANTITY_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with16.Parameters.Add("QUANTITY_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"])).Direction = ParameterDirection.Input;
        //                }
        //                _with16.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

        //                //COMMODITY_GROUP_FK_IN
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"].ToString()))
        //                {
        //                    _with16.Parameters.Add("COMMODITY_GROUP_FK_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with16.Parameters.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
        //                }
        //                _with16.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                //COMMODITY_MST_FK
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"].ToString()))
        //                {
        //                    _with16.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with16.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
        //                }
        //                _with16.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                //ALL_IN_TARIFF
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()))
        //                {
        //                    _with16.Parameters.Add("ALL_IN_TARIFF_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with16.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
        //                }
        //                _with16.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;

        //                //BUYING_RATE
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"].ToString()))
        //                {
        //                    _with16.Parameters.Add("BUYING_RATE_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with16.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"])).Direction = ParameterDirection.Input;
        //                }
        //                _with16.Parameters["BUYING_RATE_IN"].SourceVersion = DataRowVersion.Current;

        //                _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                _with16.ExecuteNonQuery();
        //                if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value), "bookingtrans")>0)
        //                {
        //                    arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
        //                    return arrMessage;
        //                }
        //                else
        //                {
        //                    _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
        //                    if ((HttpContext.Current.Session["CtrDetails"] != null))
        //                    {
        //                        try
        //                        {
        //                            dsGrid = (DataSet)HttpContext.Current.Session["CtrDetails"];
        //                            foreach (DataRow _row in dsGrid.Tables[nRowCnt].Rows)
        //                            {
        //                                _row["BOOKING_TRN_SEA_PK"] = _PkValueTrans;
        //                            }
        //                            HttpContext.Current.Session["CtrDetails"] = dsGrid;
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                        }
        //                    }
        //                }
        //                if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
        //                {
        //                    if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
        //                    {
        //                        strValueArgument = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]);
        //                    }
        //                    else
        //                    {
        //                        strValueArgument = "";
        //                    }
        //                }
        //                else
        //                {
        //                    if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
        //                    {
        //                        strValueArgument = Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]);
        //                    }
        //                    else
        //                    {
        //                        strValueArgument = "";
        //                    }
        //                }
        //                //If dsGrid.Tables.Count >= nRowCnt + 1 Then
        //                //    arrMessage = objCargoDetails.SaveCargo(dsGrid.Tables(nRowCnt), SelectCommand)
        //                //End If

        //                arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate, dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"].ToString(), strValueArgument, dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"].ToString());
        //                if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
        //                {
        //                    return arrMessage;
        //                }
        //                // added by vimlesh kumar 26/08/2006 for adding special request
        //                int CntTypePK = 0;
        //                CntTypePK = Convert.ToInt32(!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()) ? "" : dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString());
        //                int i = 0;
        //                string strSql = null;
        //                string drCntKind = null;
        //                strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= '" + CntTypePK + "'";
        //                drCntKind = objWK.ExecuteScaler(strSql);
        //                if (CommodityGroup == HAZARDOUS)
        //                {
        //                    if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                    {
        //                        arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                    }
        //                    else
        //                    {
        //                        arrMessage = SaveTransactionHZSpcl(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                    }

        //                }
        //                else if (CommodityGroup == REEFER)
        //                {
        //                    if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                    {
        //                        arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                    }
        //                    else
        //                    {
        //                        arrMessage = SaveTransactionReefer(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                    }

        //                }
        //                else if (CommodityGroup == ODC)
        //                {
        //                    if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                    {
        //                        arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                    }
        //                }
        //                else
        //                {
        //                    if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                    {
        //                        arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                    }
        //                }

        //                if (arrMessage.Count > 0)
        //                {
        //                    if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
        //                    {
        //                        return arrMessage;
        //                    }
        //                }
        //                if (Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 1 | Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 2)
        //                {
        //                    arrMessage = (ArrayList)UpdateUpStream(dsMain, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]), Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 0);

        //                    if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
        //                    {
        //                        arrMessage.Add("Upstream Updation failed, Please check for valid Data");
        //                        return arrMessage;
        //                    }
        //                }
        //            }
        //            if (dsMain.Tables["tblTransaction"].Rows.Count > 0)
        //            {
        //                arrMessage.Add("All data saved successfully");
        //                return arrMessage;
        //            }
        //            else
        //            {
        //                arrMessage.Add("No Record selected to save!");
        //                return arrMessage;
        //            }


        //        }
        //        else
        //        {

        //            for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
        //            {
        //                var _with17 = SelectCommand;
        //                _with17.CommandType = CommandType.StoredProcedure;
        //                _with17.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_UPD";

        //                SelectCommand.Parameters.Clear();

        //                //Booking Sea Transaction Pk
        //                _with17.Parameters.Add("BOOKING_TRN_SEA_PK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BOOKING_TRN_SEA_PK"]).Direction = ParameterDirection.Input;
        //                _with17.Parameters["BOOKING_TRN_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;

        //                //Booking Sea Fk 
        //                _with17.Parameters.Add("BOOKING_SEA_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

        //                _with17.Parameters.Add("TRANS_REFERED_FROM_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]).Direction = ParameterDirection.Input;
        //                _with17.Parameters["TRANS_REFERED_FROM_IN"].SourceVersion = DataRowVersion.Current;

        //                _with17.Parameters.Add("TRANS_REF_NO_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]).Direction = ParameterDirection.Input;
        //                _with17.Parameters["TRANS_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
        //                {
        //                    _with17.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with17.Parameters.Add("CONTAINER_TYPE_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]).Direction = ParameterDirection.Input;
        //                }
        //                _with17.Parameters["CONTAINER_TYPE_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                _with17.Parameters.Add("NO_OF_BOXES_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["NO_OF_BOXES"]).Direction = ParameterDirection.Input;
        //                _with17.Parameters["NO_OF_BOXES_IN"].SourceVersion = DataRowVersion.Current;

        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
        //                {
        //                    _with17.Parameters.Add("BASIS_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"]).Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with17.Parameters.Add("BASIS_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                _with17.Parameters["BASIS_IN"].SourceVersion = DataRowVersion.Current;

        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"].ToString()))
        //                {
        //                    _with17.Parameters.Add("QUANTITY_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["QUANTITY"])).Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with17.Parameters.Add("QUANTITY_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                _with17.Parameters["QUANTITY_IN"].SourceVersion = DataRowVersion.Current;

        //                _with17.Parameters.Add("COMMODITY_GROUP_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_GROUP_FK"]).Direction = ParameterDirection.Input;
        //                _with17.Parameters["COMMODITY_GROUP_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                _with17.Parameters.Add("COMMODITY_MST_FK_IN", dsMain.Tables["tblTransaction"].Rows[nRowCnt]["COMMODITY_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with17.Parameters["COMMODITY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"].ToString()))
        //                {
        //                    _with17.Parameters.Add("ALL_IN_TARIFF_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with17.Parameters.Add("ALL_IN_TARIFF_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"])).Direction = ParameterDirection.Input;
        //                }
        //                _with17.Parameters["ALL_IN_TARIFF_IN"].SourceVersion = DataRowVersion.Current;
        //                if (string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"].ToString()))
        //                {
        //                    _with17.Parameters.Add("BUYING_RATE_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with17.Parameters.Add("BUYING_RATE_IN", Convert.ToDouble(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BUYING_RATE"])).Direction = ParameterDirection.Input;
        //                }
        //                _with17.Parameters["BUYING_RATE_IN"].SourceVersion = DataRowVersion.Current;
        //                _with17.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                _with17.ExecuteNonQuery();
        //                if (string.Compare(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value), "bookingtrans")>0)
        //                {
        //                    arrMessage.Add(Convert.ToString(SelectCommand.Parameters["RETURN_VALUE"].Value));
        //                    return arrMessage;
        //                }
        //                else
        //                {
        //                    _PkValueTrans = Convert.ToInt64(SelectCommand.Parameters["RETURN_VALUE"].Value);
        //                    if ((HttpContext.Current.Session["CtrDetails"] != null))
        //                    {
        //                        try
        //                        {
        //                            dsGrid = (DataSet)HttpContext.Current.Session["CtrDetails"];
        //                            foreach (DataRow _row in dsGrid.Tables[nRowCnt].Rows)
        //                            {
        //                                _row["BOOKING_TRN_SEA_PK"] = _PkValueTrans;
        //                            }
        //                            HttpContext.Current.Session["CtrDetails"] = dsGrid;
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                        }
        //                    }
        //                }
        //                if (Convert.ToInt32(dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"]) == 1)
        //                {
        //                    if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
        //                    {
        //                        strValueArgument = getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString(), "").ToString();
        //                    }
        //                    else
        //                    {
        //                        strValueArgument = "";
        //                    }
        //                }
        //                else
        //                {
        //                    if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"].ToString()))
        //                    {
        //                        strValueArgument = getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["BASIS"], "").ToString();
        //                    }
        //                    else
        //                    {
        //                        strValueArgument = "";
        //                    }
        //                }
        //                //If dsGrid.Tables.Count >= nRowCnt + 1 Then
        //                //    arrMessage = objCargoDetails.SaveCargo(dsGrid.Tables(nRowCnt), SelectCommand)
        //                //End If
        //                arrMessage = SaveBookingFRT(dsMain, _PkValueTrans, SelectCommand, IsUpdate, dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"].ToString(), strValueArgument, dsMain.Tables["tblMaster"].Rows[0]["CARGO_TYPE"].ToString());
        //                if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
        //                {
        //                    return arrMessage;
        //                }
        //                //============spacial requirment=============
        //                int CntTypePK = 0;
        //                if (!string.IsNullOrEmpty(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"].ToString()))
        //                {
        //                    CntTypePK = Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"]);
        //                    int i = 0;
        //                    string strSql = null;
        //                    string drCntKind = null;
        //                    strSql = "SELECT C.CONTAINER_KIND FROM CONTAINER_TYPE_MST_TBL C WHERE C.CONTAINER_TYPE_MST_PK= '" + CntTypePK + "'";
        //                    drCntKind = objWK.ExecuteScaler(strSql);
        //                    if (dsMain.Tables["tblTransaction"].Columns.Contains("strSpclReq"))
        //                    {
        //                        if (CommodityGroup == HAZARDOUS)
        //                        {
        //                            if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                            {
        //                                arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                            }
        //                            else
        //                            {
        //                                arrMessage = SaveTransactionHZSpcl(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                            }

        //                        }
        //                        else if (CommodityGroup == REEFER)
        //                        {
        //                            if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                            {
        //                                arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                            }
        //                            else
        //                            {
        //                                arrMessage = SaveTransactionReefer(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                            }

        //                        }
        //                        else if (CommodityGroup == ODC)
        //                        {
        //                            if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                            {
        //                                arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                            }
        //                        }
        //                        else
        //                        {
        //                            if (drCntKind == "3" | drCntKind == "4" | drCntKind == "5")
        //                            {
        //                                arrMessage = SaveTransactionODC(SelectCommand, objWK.MyUserName, getDefault(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["strSpclReq"], "").ToString(), _PkValueTrans);
        //                            }
        //                        }
        //                    }
        //                }
        //                if (arrMessage.Count > 0)
        //                {
        //                    if (string.Compare(Convert.ToString(arrMessage[0]).ToUpper(), "SAVED") == 0)
        //                    {
        //                        return arrMessage;
        //                    }
        //                }
        //                //========================end============================
        //                if (Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 1 | Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]) == 2)
        //                {
        //                    arrMessage = (ArrayList)UpdateUpStream(dsMain, SelectCommand, IsUpdate, Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REFERED_FROM"]), Convert.ToString(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["TRANS_REF_NO"]), 0);

        //                    if (!(string.Compare(arrMessage[0].ToString(), "saved") > 0))
        //                    {
        //                        arrMessage.Add("Upstream Updation failed, Please check for valid Data");
        //                        return arrMessage;
        //                    }
        //                }
        //            }
        //            if (dsMain.Tables["tblTransaction"].Rows.Count > 0)
        //            {
        //                arrMessage.Add("All data saved successfully");
        //                return arrMessage;
        //            }
        //            else
        //            {
        //                arrMessage.Add("No Record selected to save!");
        //                return arrMessage;
        //            }
        //        }
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        //public ArrayList SaveBookingFRT(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string strContractRefNo, string strValueArgument, string isLcl)
        //{
        //    Int32 nRowCnt = default(Int32);
        //    WorkFlow objWK = new WorkFlow();
        //    DataView dv_Freight = new DataView();
        //    //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        //    Int16 Check = default(Int16);
        //    dv_Freight = getDataView(dsMain.Tables["tblFreight"], strContractRefNo, strValueArgument, isLcl);
        //    if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
        //    {
        //        Check = FetchEBFrt(PkValue);
        //        if (Chk_EBK == 1 & Check == 0)
        //        {
        //            IsUpdate = false;
        //        }
        //    }
        //    arrMessage.Clear();
        //    try
        //    {
        //        if (!IsUpdate)
        //        {
        //            var _with18 = SelectCommand;
        //            _with18.CommandType = CommandType.StoredProcedure;
        //            _with18.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_FRT_INS";

        //            for (nRowCnt = 0; nRowCnt <= dv_Freight.Table.Rows.Count - 1; nRowCnt++)
        //            {
        //                SelectCommand.Parameters.Clear();

        //                //Booking Transaction Sea(Fk)
        //                _with18.Parameters.Add("BOOKING_SEA_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

        //                _with18.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with18.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                _with18.Parameters.Add("CURRENCY_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
        //                _with18.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //                //Added by Faheem
        //                _with18.Parameters.Add("CHARGE_BASIS_IN", dv_Freight.Table.Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;
        //                _with18.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;
        //                //End
        //                //'SURCHARGE
        //                if (string.IsNullOrEmpty(Convert.ToString(dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])))
        //                {
        //                    _with18.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with18.Parameters.Add("SURCHARGE_IN", (dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
        //                }
        //                //'SURCHARGE
        //                if (string.IsNullOrEmpty(Convert.ToString(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])))
        //                {
        //                    _with18.Parameters.Add("MIN_RATE_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with18.Parameters.Add("MIN_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])).Direction = ParameterDirection.Input;
        //                }
        //                _with18.Parameters["MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;

        //                if (string.IsNullOrEmpty(Convert.ToString(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])))
        //                {
        //                    _with18.Parameters.Add("TARIFF_RATE_IN", "").Direction = ParameterDirection.Input;
        //                }
        //                else
        //                {
        //                    _with18.Parameters.Add("TARIFF_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])).Direction = ParameterDirection.Input;
        //                }
        //                _with18.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;

        //                _with18.Parameters.Add("PYMT_TYPE_IN", dv_Freight.Table.Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
        //                _with18.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

        //                _with18.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"]).Direction = ParameterDirection.Input;
        //                _with18.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;

        //                _with18.Parameters.Add("CHECK_ADVATOS_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_ADVATOS"]).Direction = ParameterDirection.Input;
        //                _with18.Parameters["CHECK_ADVATOS_IN"].SourceVersion = DataRowVersion.Current;

        //                //Return value of the proc.
        //                _with18.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                //Execute the command
        //                _with18.ExecuteNonQuery();
        //            }
        //        }
        //        else
        //        {
        //            var _with19 = SelectCommand;
        //            _with19.CommandType = CommandType.StoredProcedure;
        //            _with19.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_TRN_FRT_UPD";
        //            for (nRowCnt = 0; nRowCnt <= dv_Freight.Table.Rows.Count - 1; nRowCnt++)
        //            {
        //                SelectCommand.Parameters.Clear();
        //                if (!string.IsNullOrEmpty(Convert.ToString(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])))
        //                {
        //                    //Contract Transaction Pk
        //                    _with19.Parameters.Add("BOOKING_TRN_SEA_FRT_PK_IN", dv_Freight.Table.Rows[nRowCnt]["BOOKING_TRN_SEA_FRT_PK"]).Direction = ParameterDirection.Input;
        //                    _with19.Parameters["BOOKING_TRN_SEA_FRT_PK_IN"].SourceVersion = DataRowVersion.Current;

        //                    //Booking Transaction Sea(Fk)
        //                    _with19.Parameters.Add("BOOKING_SEA_TRN_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

        //                    _with19.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
        //                    _with19.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

        //                    _with19.Parameters.Add("CURRENCY_MST_FK_IN", dv_Freight.Table.Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
        //                    //'SURCHARGE
        //                    if (string.IsNullOrEmpty(Convert.ToString(dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])))
        //                    {
        //                        _with19.Parameters.Add("SURCHARGE_IN", "").Direction = ParameterDirection.Input;
        //                    }
        //                    else
        //                    {
        //                        _with19.Parameters.Add("SURCHARGE_IN", (dv_Freight.Table.Rows[nRowCnt]["SURCHARGE"])).Direction = ParameterDirection.Input;
        //                    }
        //                    //'SURCHARGE
        //                    //Added by Faheem
        //                    _with19.Parameters.Add("CHARGE_BASIS_IN", dv_Freight.Table.Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;
        //                    _with19.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;
        //                    //End
        //                    _with19.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;
        //                    if (string.IsNullOrEmpty(Convert.ToString(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])))
        //                    {
        //                        _with19.Parameters.Add("MIN_RATE_IN", "").Direction = ParameterDirection.Input;
        //                    }
        //                    else
        //                    {
        //                        _with19.Parameters.Add("MIN_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["MIN_RATE"])).Direction = ParameterDirection.Input;
        //                    }
        //                    _with19.Parameters["MIN_RATE_IN"].SourceVersion = DataRowVersion.Current;
        //                    //Manoharan 11July2007: some times this field have Null value
        //                    if (string.IsNullOrEmpty(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"]))
        //                    {
        //                        _with19.Parameters.Add("TARIFF_RATE_IN", "").Direction = ParameterDirection.Input;
        //                    }
        //                    else
        //                    {
        //                        _with19.Parameters.Add("TARIFF_RATE_IN", Convert.ToDouble(dv_Freight.Table.Rows[nRowCnt]["TARIFF_RATE"])).Direction = ParameterDirection.Input;
        //                    }
        //                    //.Parameters.Add("TARIFF_RATE_IN", _
        //                    //        dv_Freight.Table.Rows(nRowCnt).Item("TARIFF_RATE")).Direction = _
        //                    //                                   ParameterDirection.Input
        //                    _with19.Parameters["TARIFF_RATE_IN"].SourceVersion = DataRowVersion.Current;

        //                    _with19.Parameters.Add("PYMT_TYPE_IN", dv_Freight.Table.Rows[nRowCnt]["PYMT_TYPE"]).Direction = ParameterDirection.Input;
        //                    _with19.Parameters["PYMT_TYPE_IN"].SourceVersion = DataRowVersion.Current;

        //                    _with19.Parameters.Add("CHECK_FOR_ALL_IN_RT_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_FOR_ALL_IN_RT"]).Direction = ParameterDirection.Input;
        //                    _with19.Parameters["CHECK_FOR_ALL_IN_RT_IN"].SourceVersion = DataRowVersion.Current;

        //                    _with19.Parameters.Add("CHECK_ADVATOS_IN", dv_Freight.Table.Rows[nRowCnt]["CHECK_ADVATOS"]).Direction = ParameterDirection.Input;
        //                    _with19.Parameters["CHECK_ADVATOS_IN"].SourceVersion = DataRowVersion.Current;

        //                    //Return value of the proc.
        //                    _with19.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //                    //Execute the command
        //                    _with19.ExecuteNonQuery();
        //                }
        //            }
        //        }
        //        arrMessage.Add("All data saved successfully");
        //        return arrMessage;
        //    }
        //    catch (OracleException oraexp)
        //    {
        //        throw oraexp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //Freight level Save and Updation filtered on container and basis 
        //dataview(getDataView) from dataset freight belonging to different transaction
        //public Int16 FetchEBFrt(long BkgPk)
        //{
        //    string sql = "";
        //    string res = "";
        //    Int16 check = 0;
        //    WorkFlow objWK = new WorkFlow();
        //    sql = "select BOOKING_TRN_SEA_FK from BOOKING_TRN_SEA_FRT_DTLS where BOOKING_TRN_SEA_FK='" + BkgPk + "'";
        //    res = objWK.ExecuteScaler(sql);
        //    if (res > 0)
        //    {
        //        check = 1;
        //    }
        //    else
        //    {
        //        check = 0;
        //    }
        //    return check;
        //}
        //public object UpdateUpStream(DataSet dsMain, OracleCommand SelectCommand, bool IsUpdate, string strTranType, string strContractRefNo, long PkValue)
        //{

        //    WorkFlow objWK = new WorkFlow();
        //    string strValueArgument = null;
        //    arrMessage.Clear();
        //    try
        //    {
        //        var _with20 = SelectCommand;
        //        _with20.CommandType = CommandType.StoredProcedure;
        //        _with20.CommandText = objWK.MyUserName + ".BOOKING_SEA_PKG.BOOKING_SEA_UPDATE_UPSTREAM";
        //        SelectCommand.Parameters.Clear();

        //        _with20.Parameters.Add("TRANS_REFERED_FROM_IN", Convert.ToInt64(strTranType)).Direction = ParameterDirection.Input;
        //        _with20.Parameters.Add("TRANS_REF_NO_IN", Convert.ToString(strContractRefNo)).Direction = ParameterDirection.Input;

        //        _with20.Parameters.Add("ISUPDATE", IsUpdate.ToString()).Direction = ParameterDirection.Input;

        //        _with20.Parameters.Add("BOOKING_STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;

        //        //Return value of the proc.
        //        _with20.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
        //        //Execute the command
        //        _with20.ExecuteNonQuery();
        //        arrMessage.Add("All data saved successfully");
        //        return arrMessage;

        //    }
        //    catch (OracleException oraexp)
        //    {
        //        arrMessage.Add(oraexp.Message);
        //        return arrMessage;
        //    }
        //    catch (Exception ex)
        //    {
        //        arrMessage.Add(ex.Message);
        //        return arrMessage;
        //    }
        //}

        //private DataView getDataView(DataTable dtFreight, string strContractRefNo, string strValueArgument, string isLcl)
        //{
        //    try
        //    {
        //        DataTable dstemp = new DataTable();
        //        DataRow dr = null;
        //        Int32 nRowCnt = default(Int32);
        //        Int32 nColCnt = default(Int32);
        //        ArrayList arrValueCondition = new ArrayList();
        //        string strValueCondition = "";
        //        dstemp = dtFreight.Clone();
        //        if (isLcl == "1")
        //        {
        //            for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
        //            {
        //                if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], "") & strValueArgument == getDefault(dtFreight.Rows[nRowCnt]["CONTAINER_TYPE_MST_FK"], ""))
        //                {
        //                    dr = dstemp.NewRow();
        //                    for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
        //                    {
        //                        dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
        //                    }
        //                    dstemp.Rows.Add(dr);
        //                }
        //            }
        //        }
        //        else
        //        {
        //            for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
        //            {
        //                if (strContractRefNo == getDefault(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"], "") & strValueArgument == getDefault(dtFreight.Rows[nRowCnt]["BASISPK"], ""))
        //                {
        //                    dr = dstemp.NewRow();
        //                    for (nColCnt = 0; nColCnt <= dtFreight.Columns.Count - 1; nColCnt++)
        //                    {
        //                        dr[nColCnt] = dtFreight.Rows[nRowCnt][nColCnt];
        //                    }
        //                    dstemp.Rows.Add(dr);
        //                }
        //            }
        //        }
        //        return dstemp.DefaultView;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}
        //Filter Container or basis freights on their refno or to corresponding transaction belonging to

        //public bool CheckActiveJobCard(strABEPk)
        //{
        //    System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
        //    short intCnt = 0;
        //    WorkFlow objWF = new WorkFlow();
        //    string strReturn = null;
        //    //strBuilder.Append("SELECT JCSET.JOB_CARD_SEA_EXP_PK FROM JOB_CARD_SEA_EXP_TBL JCSET ")
        //    //strBuilder.Append("WHERE JCSET.JOB_CARD_STATUS=1 ")
        //    //strBuilder.Append("AND JCSET.BOOKING_SEA_FK= " & strABEPk)
        //    strBuilder.Append(" UPDATE JOB_CARD_SEA_EXP_TBL J ");
        //    strBuilder.Append(" SET J.JOB_CARD_STATUS = 2, J.JOB_CARD_CLOSED_ON = SYSDATE ");
        //    strBuilder.Append(" WHERE J.BOOKING_SEA_FK = " + strABEPk);

        //    try
        //    {
        //        intCnt = objWF.ExecuteScaler(strBuilder.ToString());
        //        if (intCnt == 0)
        //        {
        //            return true;
        //        }
        //        else
        //        {
        //            return false;
        //        }
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //        //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }

        //}
        //Check for active Job Card

        //This function generates the Booking Referrence no. as per the protocol saved by the user.
        //public string GenerateBookingNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        //{
        //    string functionReturnValue = null;
        //    functionReturnValue = GenerateProtocolKey("BOOKING (SEA)", nLocationId, nEmployeeId, DateTime.Now, , , , nCreatedBy, objWK);
        //    return functionReturnValue;
        //    return functionReturnValue;
        //}
        ////This function generates the Nomination Reference no. as per the protocol saved by the user.
        //public string GenerateNominationNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        //{
        //    return GenerateProtocolKey("NOMINATION SEA", nLocationId, nEmployeeId, DateTime.Now, , , , nCreatedBy, objWK);
        //}
        #endregion

        #region "Vessel/voyage saving"
        //'This function will be used to Insert/update the Vessel Master Details to "vessel_voyage_tbl"
        //' If inserted/Updated then the PK Value will be Returned
        //' If the Insrtion of Updation Failed, then 0 will be passed
        public ArrayList SaveVesselMaster(long dblVesselPK, string strVesselName, long dblOperatorFK, string strVesselID, string VoyNo, OracleCommand SelectCommand, long POLPK, string PODPK, 
            DateTime POLETA, DateTime POLETD,DateTime POLCUT, DateTime PODETA, DateTime ATDPOL, DateTime ATAPOD)
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
                    var _with1 = SelectCommand;
                    _with1.Parameters.Clear();
                    _with1.CommandType = CommandType.StoredProcedure;
                    _with1.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TBL_INS";
                    _with1.Parameters.Add("OPERATOR_MST_FK_IN", dblOperatorFK).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("VESSEL_NAME_IN", strVesselName).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("VESSEL_ID_IN", strVesselID).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("ACTIVE_IN", 1).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with1.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    RESULT = _with1.ExecuteNonQuery();
                    dblVesselPK = Convert.ToInt64(_with1.Parameters["RETURN_VALUE"].Value);
                    _with1.Parameters.Clear();

                }
                //Update the Details Table
                var _with2 = SelectCommand;
                _with2.Parameters.Clear();
                _with2.CommandType = CommandType.StoredProcedure;
                _with2.CommandText = objWK.MyUserName + ".VESSEL_VOYAGE_TBL_PKG.VESSEL_VOYAGE_TRN_INS";
                _with2.Parameters.Add("VESSEL_VOYAGE_TBL_FK_IN", dblVesselPK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("VOYAGE_IN", VoyNo).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("PORT_MST_POL_FK_IN", POLPK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("PORT_MST_POD_FK_IN", getDefault(PODPK, DBNull.Value)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("POL_ETA_IN", getDefault((POLETA == DateTime.MinValue ? DateTime.MinValue : POLETA), DBNull.Value)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("POL_ETD_IN", getDefault((POLETD == DateTime.MinValue ? DateTime.MinValue : POLETD), DBNull.Value)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("POL_CUT_OFF_DATE_IN", getDefault((POLCUT == DateTime.MinValue ? DateTime.MinValue : POLCUT), DBNull.Value)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("POD_ETA_IN", getDefault((PODETA == DateTime.MinValue ? DateTime.MinValue : PODETA), DBNull.Value)).Direction = ParameterDirection.Input;
                //ADD BY LATHA ON APRIL 2
                _with2.Parameters.Add("ATD_POL_IN", getDefault((ATDPOL == DateTime.MinValue ? DateTime.MinValue : ATDPOL), DBNull.Value)).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("ATA_POD_IN", getDefault((ATAPOD == DateTime.MinValue ? DateTime.MinValue : ATAPOD), DBNull.Value)).Direction = ParameterDirection.Input;
                //END BY LATHA
                _with2.Parameters.Add("CUSTOMS_CALL_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CAPTAIN_NAME_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("PORT_CALL_MST_FK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("NRT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("GRT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with2.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 4, "VOYAGE_TRN_PK").Direction = ParameterDirection.Output;
                RESULT = _with2.ExecuteNonQuery();
                dblVesselPK = Convert.ToInt64(_with2.Parameters["RETURN_VALUE"].Value);
                _with2.Parameters.Clear();
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
        #endregion


        #region "Delete Booking transactions"
        public string DeleteBkgTrans(string PkValues)
        {
            WorkFlow objWF = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            objWF.OpenConnection();
            TRAN = objWF.MyConnection.BeginTransaction();
            objWF.MyCommand.Transaction = TRAN;
            try
            {
                var _with21 = objWF.MyCommand;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWF.MyUserName + ".BOOKING_SEA_PKG.BOOKING_TRN_SEA_DELETE";
                _with21.Parameters.Clear();

                _with21.Parameters.Add("BOOKING_TRN_SEA_PK_IN", PkValues).Direction = ParameterDirection.Input;
                _with21.Parameters["BOOKING_TRN_SEA_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with21.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with21.ExecuteNonQuery();
                strRet = (string.IsNullOrEmpty(Convert.ToString(_with21.Parameters["RETURN_VALUE"].Value)) ? "" : Convert.ToString(_with21.Parameters["RETURN_VALUE"].Value));
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
        #endregion

        #region "Check Customer Credit Status"         'Returns True if credit balance exist

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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
            try
            {
                if (string.IsNullOrEmpty(Convert.ToString(dt.Rows[0][0])))
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
            //SELECT ADDRESS FROM THE QUOTATION TABLE 
            strBuilder.Append("SELECT ");
            strBuilder.Append("QST.COL_PLACE_MST_FK, PMTC.PLACE_CODE, QST.COL_ADDRESS, ");
            strBuilder.Append("QST.DEL_PLACE_MST_FK, PMTD.PLACE_CODE, QST.DEL_ADDRESS ");
            strBuilder.Append("FROM QUOTATION_SEA_TBL QST, PLACE_MST_TBL PMTC,PLACE_MST_TBL PMTD ");
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Check for JobCard existence"
        public string FunJobExist(int strSBEPk)
        {
            try
            {
                bool boolFound = false;
                System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
                WorkFlow objWF = new WorkFlow();
                string strReturn = null;
                strBuilder.Append("SELECT JCSET.JOB_CARD_SEA_EXP_PK FROM JOB_CARD_SEA_EXP_TBL JCSET ");
                strBuilder.Append("WHERE JCSET.BOOKING_SEA_FK=" + strSBEPk);
                strReturn = objWF.ExecuteScaler(strBuilder.ToString());
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            //strHazFK 0 for non hazardous and 1 for hazardous
            //strRType 1 for Commodity wise restriction and 2 for General restriction

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
                var _with22 = objWFT.MyCommand.Parameters;
                _with22.Add("POL_PK_IN", strPolPk).Direction = ParameterDirection.Input;
                _with22.Add("POD_PK_IN", strPodPk).Direction = ParameterDirection.Input;
                //Changed By Snigdharani
                _with22.Add("COMMODITY_MST_FK_IN", getDefault(strcommodityfk, 0)).Direction = ParameterDirection.Input;
                _with22.Add("S_DATE_IN", strsdate).Direction = ParameterDirection.Input;
                _with22.Add("BUSINESS_TYPE_IN", strBType).Direction = ParameterDirection.Input;
                _with22.Add("HAZARDOUS_IN", strHazFK).Direction = ParameterDirection.Input;
                _with22.Add("RESTRICTION_TYPE_IN", strRType).Direction = ParameterDirection.Input;
                _with22.Add("RES_CURSOR_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (objWFT.ExecuteCommands() == true)
                {
                    Da.SelectCommand = objWFT.MyCommand;
                    Da.Fill(ds);
                }
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                                            strRestriction += FindCommodityID(Convert.ToString(ds.Tables[0].Rows[intRowCnt]["COMMODITY_MST_FK"]));
                                            intVStatus = 1;
                                        }
                                        else
                                        {
                                            strRestriction += "," + FindCommodityID(Convert.ToString(ds.Tables[0].Rows[intRowCnt]["COMMODITY_MST_FK"]));
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                        strAddress += '~' + Convert.ToString(dt.Rows[0][j]);
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with23 = SCM.Parameters;
                _with23.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with23.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with23.Add("PORT", ifDBNull(Port)).Direction = ParameterDirection.Input;
                _with23.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with23.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-0
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
                var _with24 = SCM.Parameters;
                _with24.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with24.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input
                _with24.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with24.Add("PODPK_IN", (string.IsNullOrEmpty(PODPK) ? "" : PODPK)).Direction = ParameterDirection.Input;
                _with24.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with25 = SCM.Parameters;
                _with25.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with25.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input
                _with25.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with25.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_QUOTATION_NO";
                var _with26 = SCM.Parameters;
                _with26.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with26.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with26.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with26.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with26.Add("POL_IN", getDefault(strPOL, "")).Direction = ParameterDirection.Input;
                _with26.Add("POD_IN", getDefault(strPod, "")).Direction = ParameterDirection.Input;
                _with26.Add("CUSTPK_IN", getDefault(strShipper, "")).Direction = ParameterDirection.Input;
                //Manoharan 11June2007: to get Logged in Based Quotations
                _with26.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with26.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with27 = SCM.Parameters;
                _with27.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with27.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with27.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with27.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with27.Add("POL_IN", getDefault(strPOL, "")).Direction = ParameterDirection.Input;
                _with27.Add("POD_IN", getDefault(strPod, "")).Direction = ParameterDirection.Input;
                _with27.Add("CUSTPK_IN", getDefault(strShipper, "")).Direction = ParameterDirection.Input;
                //Manoharan 11June2007: to get Logged in Based Quotations
                _with27.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with27.Add("CONTAINER_TYPE_FK_IN", getDefault(strConrainer, "")).Direction = ParameterDirection.Input;
                _with27.Add("COMMODITY_GROUP_IN", getDefault(strCommodity, "")).Direction = ParameterDirection.Input;
                _with27.Add("QUOT_TYPE", getDefault(intQuot, 0)).Direction = ParameterDirection.Input;
                _with27.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with28 = SCM.Parameters;
                _with28.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with28.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with28.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with28.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with28.Add("POL_IN", getDefault(strPOL, "")).Direction = ParameterDirection.Input;
                _with28.Add("POD_IN", getDefault(strPod, "")).Direction = ParameterDirection.Input;
                _with28.Add("CUSTPK_IN", getDefault(strShipper, "")).Direction = ParameterDirection.Input;
                _with28.Add("FROM_DATE_IN", getDefault(FROM_DATE_IN, "")).Direction = ParameterDirection.Input;
                _with28.Add("TO_DATE_IN", getDefault(TO_DATE_IN, "")).Direction = ParameterDirection.Input;
                //Manoharan 11June2007: to get Logged in Based Quotations
                _with28.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;

                _with28.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            dynamic strNull = "";
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

                var _with29 = selectCommand.Parameters;
                _with29.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with29.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with29.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input
                _with29.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect 
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with29.Add("CONTAINER_PK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with29.Add("RETURN_VALUE", OracleDbType.NVarchar2, 3000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
            dynamic strNull = "";
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

                var _with30 = selectCommand.Parameters;
                _with30.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with30.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with30.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input
                _with30.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect 
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with30.Add("BASIS_PK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with30.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
            dynamic strNull = "";
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

                var _with31 = selectCommand.Parameters;
                _with31.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : strNull)).Direction = ParameterDirection.Input;
                _with31.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with31.Add("SEARCH_FLAG_IN", (string.IsNullOrEmpty(SEARCH_FLAG_IN) ? strNull : SEARCH_FLAG_IN)).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", getDefault(strLOC_MST_IN, 0)).Direction = ParameterDirection.Input
                _with31.Add("BIZ_TYPE_IN", strBizType).Direction = ParameterDirection.Input;
                //.Add("ALLWORKINGPORT", getDefault(allWrkPort, "0")).Direction = ParameterDirection.Input
                //modified by Thiyagarajan on 6/5/08 : To display POL which are related to Logged in location :DTS defect 
                //.Add("IMPORT_IN", ifDBNull(Import)).Direction = ParameterDirection.Input
                //.Add("IMPORT_IN", getDefault(Import, 0)).Direction = ParameterDirection.Input
                //end
                _with31.Add("SLAB_PK_IN", getDefault(pod, 0)).Direction = ParameterDirection.Input;
                _with31.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
                var _with32 = SCM.Parameters;
                _with32.Add("CUSTOMER_PK_IN", ifDBNull(strCustomerPk)).Direction = ParameterDirection.Input;
                _with32.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with32.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with32.Add("COMMODITY_PK_IN", ifDBNull(strCommodityPk)).Direction = ParameterDirection.Input;
                _with32.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with32.Add("CONT_BASIS_IN", ifDBNull(strContBasis)).Direction = ParameterDirection.Input;
                _with32.Add("CARGO_TYPE_IN", ifDBNull(strCargoType)).Direction = ParameterDirection.Input;
                _with32.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with33 = SCM.Parameters;
                _with33.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with33.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with33.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with33.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with33.Add("FROM_DATE_IN", ifDBNull(fromDate)).Direction = ParameterDirection.Input;
                _with33.Add("TO_DATE_IN", ifDBNull(toDate)).Direction = ParameterDirection.Input;
                _with33.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with33.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with33.Add("LOC_IN", ifDBNull(strLOC)).Direction = ParameterDirection.Input;
                _with33.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with33.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with34 = SCM.Parameters;
                _with34.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with34.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with34.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with34.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with34.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with34.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with34.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with34.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with34.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                //strReturn = CStr(SCM.Parameters["RETURN_VALUE"].Value).Trim
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();

                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with35 = SCM.Parameters;
                _with35.Add("LOOKUP_VALUE_IN", ifDBNull(strReq)).Direction = ParameterDirection.Input;
                _with35.Add("OPR_IN", ifDBNull(strOpr)).Direction = ParameterDirection.Input;
                _with35.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with35.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with35.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with35.Add("VES_IN", ifDBNull(strVES)).Direction = ParameterDirection.Input;
                _with35.Add("VOY_IN", ifDBNull(strVOY)).Direction = ParameterDirection.Input;
                _with35.Add("IMPORT_IN", ((strImp == null) ? "" : strImp)).Direction = ParameterDirection.Input;
                _with35.Add("LOCATION", (string.IsNullOrEmpty(LOc) ? "" : LOc)).Direction = ParameterDirection.Input;
                _with35.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                var _with36 = SCM.Parameters;
                _with36.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with36.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with36.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                        strQuery.Append("select distinct cargo_measurement, cargo_weight_in, cargo_division_fact ");
                        strQuery.Append("from BOOKING_SEA_CARGO_CALC where ");
                        strQuery.Append("booking_sea_fk = " + pk);
                        return objWF.GetDataSet(strQuery.ToString());
                    }
                }
                return null;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
            strBuilder.Append("SELECT J.JOB_CARD_SEA_EXP_PK,J.JOBCARD_REF_NO FROM JOB_CARD_SEA_EXP_TBL J WHERE J.BOOKING_SEA_FK=" + bPK);
            try
            {
                dt = objwf.GetDataTable(strBuilder.ToString());
                if (dt.Rows.Count > 0)
                {
                    strJobpk = dt.Rows[0][0].ToString();
                    refno = dt.Rows[0][1].ToString();
                }
                return strJobpk;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
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
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public string Fetch_Quote_Status(int QuotePK)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append(" select QMAIN.Status from quotation_sea_tbl  QMAIN where QMAIN.QUOTATION_SEA_PK = " + QuotePK);
                return ObjWk.ExecuteScaler(strQuery.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
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
                sqlStr = " SELECT distinct btrn.booking_trn_sea_pk  FROM BOOKING_SEA_TBL Bhdr,Booking_Trn_Sea_Fcl_Lcl Btrn ";
                sqlStr += "  where bhdr.booking_sea_pk = btrn.booking_sea_fk  ";
                sqlStr += "  and bhdr.booking_sea_pk = " + BookingPK;

                IS_EBOOKING_CHK = Convert.ToInt16(objWF.ExecuteScaler(sqlStr));
                return IS_EBOOKING_CHK;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "Fetch Sales Executive"
        public DataSet FetchSalesExecutive(Int16 CustPk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            sb.Append("SELECT NVL(C.REP_EMP_MST_FK, 0) REPPK, NVL(E.EMPLOYEE_NAME, 'CSR') SALESREP");
            sb.Append("  FROM CUSTOMER_MST_TBL C, EMPLOYEE_MST_TBL E");
            sb.Append(" WHERE C.REP_EMP_MST_FK = E.EMPLOYEE_MST_PK(+)");
            sb.Append("   AND C.CUSTOMER_MST_PK = " + CustPk);
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
        #endregion

        #region "Credit Control"
        public object FetchCreditDetails(string BookingPk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow ObjWk = new WorkFlow();

            strSQL.Append(" SELECT BKG.BOOKING_SEA_PK,");
            strSQL.Append(" BKG.CUST_CUSTOMER_MST_FK,");
            strSQL.Append(" BKG.CREDIT_LIMIT,");
            strSQL.Append("  BKG.BOOKING_REF_NO,");
            strSQL.Append(" SUM(BTS.ALL_IN_TARIFF) AS TOTAL");

            strSQL.Append("  FROM BOOKING_TRN_SEA_FRT_DTLS BFD,");
            strSQL.Append(" CUSTOMER_MST_TBL         CMT,");
            strSQL.Append("  BOOKING_SEA_TBL          BKG,");
            strSQL.Append("  BOOKING_TRN_SEA_FCL_LCL  BTS");

            strSQL.Append(" WHERE BKG.BOOKING_SEA_PK=BTS.BOOKING_SEA_FK(+)");
            strSQL.Append("  AND BTS.BOOKING_TRN_SEA_PK=BFD.BOOKING_TRN_SEA_FK(+)");
            strSQL.Append(" AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strSQL.Append(" AND BKG.BOOKING_SEA_PK=" + BookingPk);
            strSQL.Append(" GROUP BY BKG.BOOKING_SEA_PK,BKG.CUST_CUSTOMER_MST_FK, BKG.CREDIT_LIMIT,BKG.BOOKING_REF_NO");

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
        #endregion

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
        #endregion

        #region " CRO "
        public DataSet LoadCRODetails(Int32 BkgPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append(" SELECT B.CRO_PK, TO_CHAR(B.CONT_REQ_DATE,DATEFORMAT) CONT_REQ_DATE, B.CONT_MOVE_BY,");
            strBuilder.Append(" B.CUST_TRANS_DTLS, B.VENDOR_MST_FK, VMT.VENDOR_ID, B.CRO_RECEIVED, B.CRO_NR,");
            strBuilder.Append(" TO_CHAR(B.CRO_DATE,DATEFORMAT) CRO_DATE, TO_CHAR(B.CRO_VALID_TILL,DATEFORMAT) CRO_VALID_TILL,");
            strBuilder.Append(" B.YARD_DETAILS, B.VERSION_NO FROM BOOKING_CRO_TBL B, VENDOR_MST_TBL VMT");
            strBuilder.Append(" WHERE B.VENDOR_MST_FK = VMT.VENDOR_MST_PK(+) AND B.BOOKING_SEA_FK=" + BkgPK);
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
            strBuilder.Append("  FROM BOOKING_SEA_TBL BOOK, CUSTOMER_MST_TBL CMT, BOOKING_CRO_TBL BCRO,ATTACH_FILE_DTL_TBL AFDT, CUSTOMER_CONTACT_DTLS CCD");
            strBuilder.Append("  WHERE BOOK.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strBuilder.Append("  AND BOOK.BOOKING_SEA_PK = BCRO.BOOKING_SEA_FK");
            strBuilder.Append("  AND BCRO.CRO_PK = AFDT.CRO_FK(+)");
            strBuilder.Append("  AND CMT.CUSTOMER_MST_PK=CCD.CUSTOMER_MST_FK");
            strBuilder.Append("  AND BOOK.BOOKING_SEA_PK =" + BKGPK);
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
            sb.Append("SELECT BOOK.BOOKING_SEA_PK,");
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
            sb.Append("  FROM BOOKING_SEA_TBL         BOOK,");
            sb.Append("       BOOKING_TRN_SEA_FCL_LCL BKGTRN,");
            sb.Append("       OPERATOR_MST_TBL        OPR,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       COMMODITY_GROUP_MST_TBL COMM,");
            sb.Append("       CONTAINER_TYPE_MST_TBL  CTMT,");
            sb.Append("       OPERATOR_CONTACT_DTLS   OCD,");
            sb.Append("       BOOKING_CRO_TBL BCRO");
            sb.Append(" WHERE BOOK.BOOKING_SEA_PK = BKGTRN.BOOKING_SEA_FK");
            sb.Append("   AND BOOK.OPERATOR_MST_FK = OPR.OPERATOR_MST_PK");
            sb.Append("   AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND BOOK.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
            sb.Append("   AND BKGTRN.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND OPR.OPERATOR_MST_PK = OCD.OPERATOR_MST_FK");
            sb.Append("    AND BOOK.BOOKING_SEA_PK=BCRO.BOOKING_SEA_FK(+)");
            sb.Append("   AND BOOK.BOOKING_SEA_PK = " + BKGPK);
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
            sb.Append("SELECT BOOK.BOOKING_SEA_PK,");
            sb.Append("       BCRO.CONT_REQ_DATE,");
            sb.Append("       AFDT.FILE_NAME,");
            sb.Append("       AFDT.FILE_PATH");
            sb.Append("  FROM BOOKING_SEA_TBL         BOOK,");
            sb.Append("       BOOKING_CRO_TBL         BCRO,");
            sb.Append("       ATTACH_FILE_DTL_TBL     AFDT");
            sb.Append("  WHERE ");
            sb.Append("   BOOK.BOOKING_SEA_PK = BCRO.BOOKING_SEA_FK");
            sb.Append("   AND BCRO.CRO_PK = AFDT.CRO_FK");
            sb.Append("   AND BOOK.BOOKING_SEA_PK = " + BKGPK);
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
                    var _with39 = objWK.MyCommand;
                    _with39.CommandType = CommandType.StoredProcedure;
                    _with39.CommandText = objWK.MyUserName + ".BOOKING_CRO_TBL_PKG.BOOKING_CRO_TBL_INS";
                    _with39.Parameters.Clear();
                    _with39.Parameters.Add("CONT_REQ_DATE_IN", (ContReqDate == DateTime.MinValue ? DateTime.MinValue : ContReqDate)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CONT_MOVE_BY_IN", getDefault(ContMoveBy, 0)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CUST_TRANS_DTLS_IN", (string.IsNullOrEmpty(CustTransDtls) ? "" : CustTransDtls)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("VENDOR_MST_FK_IN", getDefault(VendorFk, 0)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CRO_RECEIVED_IN", getDefault(CROReceived, 0)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CRO_NR_IN", (string.IsNullOrEmpty(CRONr) ? "" : CRONr)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CRO_DATE_IN", (CRODate == DateTime.MinValue ? DateTime.MinValue : CRODate)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CRO_VALID_TILL_IN", (CROValidTill == DateTime.MinValue ? DateTime.MinValue : CROValidTill)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("BOOKING_SEA_FK_IN", BkgFk).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("YARD_DTLS_IN", (string.IsNullOrEmpty(YardDtls) ? "" : YardDtls)).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with39.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with39.ExecuteNonQuery();
                }
                else
                {
                    var _with40 = objWK.MyCommand;
                    _with40.CommandType = CommandType.StoredProcedure;
                    _with40.CommandText = objWK.MyUserName + ".BOOKING_CRO_TBL_PKG.BOOKING_CRO_TBL_UPD";
                    _with40.Parameters.Clear();
                    _with40.Parameters.Add("CRO_PK_IN", CROPk).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CONT_REQ_DATE_IN", (ContReqDate == DateTime.MinValue ? DateTime.MinValue : ContReqDate)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CONT_MOVE_BY_IN", getDefault(ContMoveBy, 0)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CUST_TRANS_DTLS_IN", (string.IsNullOrEmpty(CustTransDtls) ? "" : CustTransDtls)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("VENDOR_MST_FK_IN", getDefault(VendorFk, 0)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CRO_RECEIVED_IN", getDefault(CROReceived, 0)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CRO_NR_IN", (string.IsNullOrEmpty(CRONr) ? "" : CRONr)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CRO_DATE_IN", (CRODate == DateTime.MinValue ? DateTime.MinValue : CRODate)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CRO_VALID_TILL_IN", (CROValidTill == DateTime.MinValue ? DateTime.Now : CROValidTill)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("VERSION_NO_IN", getDefault(versionNr, 0)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("BOOKING_SEA_FK_IN", BkgFk).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("YARD_DTLS_IN", (string.IsNullOrEmpty(YardDtls) ? "" : YardDtls)).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                    _with40.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                    _with40.ExecuteNonQuery();
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
                sb.Append("                         BOOKING_SEA_TBL         BST,");
                sb.Append("                         BOOKING_TRN_SEA_FCL_LCL BTS");
                sb.Append("                         WHERE BTS.BOOKING_SEA_FK = BST.BOOKING_SEA_PK");
                sb.Append("                           AND BTS.CONTAINER_TYPE_MST_FK =");
                sb.Append("                               CTMT.CONTAINER_TYPE_MST_PK");
                sb.Append("                           AND BST.BOOKING_SEA_PK = " + intBkgPk);
                sb.Append("                         ORDER BY CTMT.CONTAINER_TYPE_MST_ID");
            }
            else
            {
                sb.Append("SELECT CTMT.CONTAINER_TYPE_MST_PK PK, CTMT.CONTAINER_TYPE_MST_ID CONTAINERTYPE, ");
                sb.Append(" BCT.CONTAINER_NO CONTAINERNR, BCT.SEAL_NO SEALNR, BTS.NO_OF_BOXES NOOFBOXES, BCT.CRO_TRN_PK TRNPK ");
                sb.Append("                         FROM CONTAINER_TYPE_MST_TBL  CTMT,");
                sb.Append("                         BOOKING_CRO_TRN              BCT,");
                sb.Append("                         BOOKING_CRO_TBL              BTC,");
                sb.Append("                         BOOKING_SEA_TBL              BST,");
                sb.Append("                         BOOKING_TRN_SEA_FCL_LCL      BTS");
                sb.Append("                         WHERE BTC.CRO_PK = BCT.CRO_FK");
                sb.Append("                           AND BCT.CONTAINER_TYPE_MST_FK =");
                sb.Append("                               CTMT.CONTAINER_TYPE_MST_PK");
                sb.Append("                           AND BST.BOOKING_SEA_PK = BTS.BOOKING_SEA_FK");
                sb.Append("                           AND BST.BOOKING_SEA_PK = BTC.BOOKING_SEA_FK");
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
                            var _with41 = SCM.Parameters;
                            _with41.Clear();
                            _with41.Add("CRO_FK_IN", intCROPk).Direction = ParameterDirection.Input;
                            _with41.Add("CONTAINER_TYPE_MST_FK_IN", dr["PK"]).Direction = ParameterDirection.Input;
                            _with41.Add("CONTAINER_NO_IN", dr["CONTAINERNR"]).Direction = ParameterDirection.Input;
                            _with41.Add("SEAL_NO_IN", dr["SEALNR"]).Direction = ParameterDirection.Input;
                            _with41.Add("CREATED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                            _with41.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with41.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                        }
                        else
                        {
                            SCM.CommandText = objWF.MyUserName + ".BOOKING_CRO_TRN_PKG.BOOKING_CRO_TRN_UPD";
                            var _with42 = SCM.Parameters;
                            _with42.Clear();

                            _with42.Add("CRO_TRN_PK_IN", dr["TRNPK"]).Direction = ParameterDirection.Input;
                            _with42.Add("CRO_FK_IN", intCROPk).Direction = ParameterDirection.Input;
                            _with42.Add("CONTAINER_TYPE_MST_FK_IN", dr["PK"]).Direction = ParameterDirection.Input;
                            _with42.Add("CONTAINER_NO_IN", dr["CONTAINERNR"]).Direction = ParameterDirection.Input;
                            _with42.Add("SEAL_NO_IN", dr["SEALNR"]).Direction = ParameterDirection.Input;
                            _with42.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                            _with42.Add("VERSION_NO_IN", getDefault(versionNr - 1, 0)).Direction = ParameterDirection.Input;
                            _with42.Add("CONFIG_MST_FK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                            _with42.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion

        #region " Send Email "

        #endregion

        #region "GET JOBCARD CONT PKS"
        public DataSet GetJobContPks(string BKGPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append(" SELECT JOBTRN.JOB_TRN_SEA_EXP_CONT_PK");
            sb.Append("  FROM BOOKING_SEA_TBL  BKG,JOB_CARD_SEA_EXP_TBL JOB,JOB_TRN_SEA_EXP_CONT JOBTRN");
            sb.Append("  WHERE BKG.BOOKING_SEA_PK = JOB.BOOKING_SEA_FK");
            sb.Append("  AND JOB.JOB_CARD_SEA_EXP_PK = JOBTRN.JOB_CARD_SEA_EXP_FK ");
            sb.Append(" AND JOB.BOOKING_SEA_FK=" + BKGPK);
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
        #endregion

        #region "Update Container & Seal Number"
        public ArrayList UpdateContNumber(string JobContPK, string ContainerNr = "", string SealNumber = "")
        {
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            objWK.OpenConnection();
            OracleTransaction TRAN = default(OracleTransaction);
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
                str = "  UPDATE JOB_TRN_SEA_EXP_CONT JCONT ";
                str += "  SET JCONT.CONTAINER_NUMBER = '" + ContainerNr + "',";
                str += "  JCONT.SEAL_NUMBER = '" + SealNumber + "'";
                str += "  WHERE JCONT.JOB_TRN_SEA_EXP_CONT_PK = " + JobContPK;
                var _with43 = updCmdUser;
                _with43.Connection = objWK.MyConnection;
                _with43.Transaction = TRAN;
                _with43.CommandType = CommandType.Text;
                _with43.CommandText = str;
                intIns = _with43.ExecuteNonQuery();
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
            strqry.Append("(SELECT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT START WITH LMT.LOCATION_MST_PK = " + HttpContext.Current.Session["LOGED_IN_LOC_FK" + " CONNECT BY PRIOR LMT.LOCATION_MST_PK = LMT.REPORTING_TO_FK)"]);
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
        #endregion
    }
}
