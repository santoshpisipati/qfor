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

using Oracle.DataAccess.Client;
using System;
using System.Collections;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_AirBookingEntry : CommonFeatures
    {
        #region "Private Variables"

        /// <summary>
        /// The _ pk value main
        /// </summary>
        private long _PkValueMain;

        /// <summary>
        /// The _ pk value trans
        /// </summary>
        private long _PkValueTrans;

        /// <summary>
        /// The object track n trace
        /// </summary>
        private cls_TrackAndTrace objTrackNTrace = new cls_TrackAndTrace();

        /// <summary>
        /// The string ret
        /// </summary>
        public string strRet;

        #endregion "Private Variables"

        /// <summary>
        /// The CHK_ ebk
        /// </summary>
        private short Chk_EBK = 0;

        #region "Property"

        /// <summary>
        /// Gets the pk value main.
        /// </summary>
        /// <value>
        /// The pk value main.
        /// </value>
        public long PkValueMain
        {
            get { return _PkValueMain; }
        }

        /// <summary>
        /// Gets the pk value trans.
        /// </summary>
        /// <value>
        /// The pk value trans.
        /// </value>
        public long PkValueTrans
        {
            get { return _PkValueTrans; }
        }

        /// <summary>
        /// The COM GRP
        /// </summary>
        private int ComGrp;

        /// <summary>
        /// Gets or sets the commodity group.
        /// </summary>
        /// <value>
        /// The commodity group.
        /// </value>
        public int CommodityGroup
        {
            get { return ComGrp; }
            set { ComGrp = value; }
        }

        #endregion "Property"

        #region "Property_save"

        /// <summary>
        /// The LNG credit note pk
        /// </summary>
        private long lngCreditNotePk;

        /// <summary>
        /// Gets or sets the return save pk.
        /// </summary>
        /// <value>
        /// The return save pk.
        /// </value>
        public long ReturnSavePk
        {
            get { return lngCreditNotePk; }
            set { lngCreditNotePk = value; }
        }

        #endregion "Property_save"

        #region "Charge Basis Toggle"

        /// <summary>
        /// Charges the basis.
        /// </summary>
        /// <param name="Value">The value.</param>
        /// <returns></returns>
        private new string ChargeBasis(short Value)
        {
            switch ((Value))
            {
                case 1:
                    return "%";

                case 2:
                    return "Flat";

                case 3:
                    return "Kgs.";

                default:
                    return "Flat";
            }
        }

        #endregion "Charge Basis Toggle"

        #region "Fetch Air Existing Booking Entry"

        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        /// <summary>
        /// Fetches the BKG status.
        /// </summary>
        /// <param name="PKVal">The pk value.</param>
        /// <returns></returns>
        public short fetchBkgStatus(long PKVal)
        {
            try
            {
                string strSQL = "";
                string Status = "";
                WorkFlow objwf = new WorkFlow();
                strSQL = "select booking_trn_air_pk from booking_trn_air where booking_air_fk='" + PKVal + "'";
                Status = objwf.ExecuteScaler(strSQL);
                if (!string.IsNullOrEmpty(Status))
                {
                    strSQL = "select booking_trn_air_frt_pk from booking_trn_air_frt_dtls where booking_trn_air_fk='" + Status + "'";
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

        /// <summary>
        /// Fetches the b entry.
        /// </summary>
        /// <param name="dsBEntry">The ds b entry.</param>
        /// <param name="lngSBEPk">The LNG sbe pk.</param>
        /// <param name="EBkg">The e BKG.</param>
        /// <param name="Bstatus">The bstatus.</param>
        /// <returns></returns>
        public DataSet FetchBEntry(DataSet dsBEntry, long lngSBEPk, short EBkg = 0, short Bstatus = 1)
        {
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtTrans = new DataTable();
                DataTable dtFreight = new DataTable();
                string QuotPk = null;
                dtMain = FetchSBEntryHeader(dtMain, lngSBEPk);
                dtTrans = FetchSBEntryTrans(dtTrans, lngSBEPk);
                //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
                if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
                {
                    if (EBkg == 1 & Bstatus == 1)
                    {
                        dtFreight = FetchEBKGEntryFreight(dtFreight, lngSBEPk);
                    }
                    else
                    {
                        dtFreight = FetchSBEntryFreight(dtFreight, lngSBEPk, EBkg);
                    }
                }
                else
                {
                    dtFreight = FetchSBEntryFreight(dtFreight, lngSBEPk);
                }
                FetchXABEOFreights(dtTrans);
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
                                QuotPk = Fetch_Quote_Pk(Convert.ToString(dtTrans.Rows[0]["REFNO"]));
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetch the existing booking entry

        /// <summary>
        /// Fetches the sb entry header.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="lngSBEPK">The LNG sbepk.</param>
        /// <returns></returns>
        public DataTable FetchSBEntryHeader(DataTable dtMain, long lngSBEPK)
        {
            string strSqlHeader = "";
            WorkFlow objwf = new WorkFlow();
            //Changed by Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING
            //Basis included by Snigdharani - 30/12/2008
            strSqlHeader = "SELECT " + "BAT.BOOKING_AIR_PK, " + "BAT.BOOKING_REF_NO, " + "BAT.BOOKING_DATE, " + "BAT.CUST_CUSTOMER_MST_FK, " + " (case " + "when CMTCUST.CUSTOMER_ID is null then " + " ( select Customer_Name from temp_customer_tbl where customer_mst_pk=BAT.CUST_CUSTOMER_MST_FK)" + " else " + "CMTCUST.CUSTOMER_NAME " + " end ) CUSTOMERID," + "BAT.CONS_CUSTOMER_MST_FK, " + " (case " + "when CMTCONS.CUSTOMER_ID is null then " + " ( select Customer_Name from temp_customer_tbl where customer_mst_pk=BAT.CONS_CUSTOMER_MST_FK)" + " else " + "CMTCONS.CUSTOMER_NAME " + " end ) CONSIGNEEID," + "BAT.PORT_MST_POL_FK, " + "PL.PORT_ID AS POL, " + "PL.PORT_NAME AS POLNAME, " + "BAT.PORT_MST_POD_FK, " + "PD.PORT_ID AS POD, " + "PD.PORT_NAME AS PODNAME, " + "BAT.COMMODITY_GROUP_FK, " + "BAT.AIRLINE_MST_FK, " + "AMT.AIRLINE_ID, " + "BAT.SHIPMENT_DATE, " + "BAT.CARGO_MOVE_FK, " + "CMMT.CARGO_MOVE_CODE, " + "BAT.PYMT_TYPE, " + "BAT.COL_PLACE_MST_FK, " + "PMTPC.PLACE_CODE AS CPLACE, " + "BAT.COL_ADDRESS, " + "BAT.DEL_PLACE_MST_FK, " + "PMTPD.PLACE_CODE AS DPLACE, " + "BAT.DEL_ADDRESS, " + "BAT.CB_AGENT_MST_FK, " + "AMTCB.AGENT_ID AS CBAGENT, " + "BAT.CL_AGENT_MST_FK, " + "AMTCL.AGENT_NAME AS CLAGENT, " + "BAT.PACK_TYPE_MST_FK, " + "PTMT.PACK_TYPE_ID, " + "BAT.PACK_COUNT, " + "BAT.GROSS_WEIGHT, " + "BAT.CHARGEABLE_WEIGHT, " + "BAT.VOLUME_IN_CBM, " + "BAT.LINE_BKG_NO, " + "BAT.FLIGHT_NO, " + "BAT.ETA_DATE, " + "BAT.ETD_DATE, " + "BAT.CUT_OFF_DATE, " + "BAT.STATUS, " + "BAT.CUSTOMER_REF_NO, " + "BAT.CUSTOMS_CODE_MST_FK, " + "CSMT.CUSTOMS_STATUS_CODE, " + "CSMT.CUSTOMS_STATUS_DESC, " + "BAT.DP_AGENT_MST_FK, " + "DPAMT.AGENT_ID AS DPAGENT, " + "DPAMT.AGENT_NAME AS DPAGENTNAME, " + "BAT.VOLUME_WEIGHT, " + "BAT.DENSITY, " + "BAT.NO_OF_BOXES, " + "BAT.CARGO_TYPE, " + "BAT.SHIPPING_TERMS_MST_FK, " + "BAT.VERSION_NO, " + "BAT.CREDIT_LIMIT, " + "BAT.CREDIT_DAYS, BTA.BASIS, " + "BTA.TRANS_REF_NO REF_NO, " + "CURR.CURRENCY_MST_PK BASE_CURRENCY_FK, " + "CURR.CURRENCY_ID BASE_CURRENCY_ID, " + " BAT.FROM_FLAG, " + " BAT.POO_FK, " + " POO.PLACE_CODE POO_ID, " + " POO.PLACE_NAME POO_NAME, " + " BAT.PFD_FK, " + " PFD.PLACE_CODE PFD_ID, " + " PFD.PLACE_NAME PFD_NAME, " + " BAT.HANDLING_INFO, " + " BAT.REMARKS,  " + " BAT.REMARKS_NEW,  " + " BAT.LINE_BKG_DT,  " + " BAT.PO_NUMBER,  " + " BAT.PO_DATE,  " + " BAT.NOMINATION_REF_NO,  " + " BAT.SALES_CALL_FK,  " + " EMP.EMPLOYEE_MST_PK EXECUTIVE_MST_FK,  " + " EMP.EMPLOYEE_ID EXECUTIVE_MST_ID,  " + " EMP.EMPLOYEE_NAME EXECUTIVE_MST_NAME,  " + " EMP_SHP.EMPLOYEE_MST_PK SHP_EXE_MST_FK,  " + " EMP_SHP.EMPLOYEE_ID SHP_EXE_MST_ID,  " + " EMP_SHP.EMPLOYEE_NAME SHP_EXE_MST_NAME,  " + " EMP_CON.EMPLOYEE_MST_PK CON_EXE_MST_FK,  " + " EMP_CON.EMPLOYEE_ID CON_EXE_MST_ID,  " + " EMP_CON.EMPLOYEE_NAME CON_EXE_MST_NAME,BAT.AIRLINE_SCHEDULE_TRN_FK  " + "FROM " + "BOOKING_AIR_TBL BAT, " + "BOOKING_TRN_AIR BTA, " + "CUSTOMER_MST_TBL CMTCUST, " + "CUSTOMER_MST_TBL CMTCONS, " + " EMPLOYEE_MST_TBL EMP, " + " EMPLOYEE_MST_TBL EMP_SHP, " + " EMPLOYEE_MST_TBL EMP_CON, " + "PORT_MST_TBL PL, " + "PORT_MST_TBL PD, " + "AIRLINE_MST_TBL AMT, " + "CARGO_MOVE_MST_TBL CMMT, " + "PLACE_MST_TBL PMTPC, " + "PLACE_MST_TBL PMTPD, " + " PLACE_MST_TBL POO, " + " PLACE_MST_TBL PFD, " + "AGENT_MST_TBL AMTCB, " + "AGENT_MST_TBL AMTCL, " + "PACK_TYPE_MST_TBL PTMT, " + "CUSTOMS_STATUS_MST_TBL CSMT, " + "AGENT_MST_TBL DPAMT," + "CURRENCY_TYPE_MST_TBL  CURR " + "WHERE(1 = 1) " + "AND BAT.BOOKING_AIR_PK=BTA.BOOKING_AIR_FK(+) " + "AND BAT.CUST_CUSTOMER_MST_FK=CMTCUST.CUSTOMER_MST_PK(+) " + "AND BAT.CONS_CUSTOMER_MST_FK=CMTCONS.CUSTOMER_MST_PK(+) " + " AND BAT.EXECUTIVE_MST_FK=EMP.EMPLOYEE_MST_PK(+) " + " AND CMTCUST.REP_EMP_MST_FK=EMP_SHP.EMPLOYEE_MST_PK(+) " + " AND CMTCONS.REP_EMP_MST_FK=EMP_CON.EMPLOYEE_MST_PK(+) " + "AND BAT.PORT_MST_POL_FK=PL.PORT_MST_PK(+) " + "AND BAT.PORT_MST_POD_FK=PD.PORT_MST_PK(+) " + "AND BAT.AIRLINE_MST_FK=AMT.AIRLINE_MST_PK(+) " + "AND BAT.CARGO_MOVE_FK=CMMT.CARGO_MOVE_PK(+) " + "AND BAT.COL_PLACE_MST_FK=PMTPC.PLACE_PK(+) " + "AND BAT.DEL_PLACE_MST_FK=PMTPD.PLACE_PK(+) " + " AND BAT.POO_FK=POO.PLACE_PK(+) " + " AND BAT.PFD_FK=PFD.PLACE_PK(+) " + "AND BAT.CB_AGENT_MST_FK=AMTCB.AGENT_MST_PK(+) " + "AND BAT.CL_AGENT_MST_FK=AMTCL.AGENT_MST_PK(+) " + "AND BAT.PACK_TYPE_MST_FK=PTMT.PACK_TYPE_MST_PK(+) " + "AND BAT.CUSTOMS_CODE_MST_FK=CSMT.CUSTOMS_CODE_MST_PK(+) " + "AND BAT.DP_AGENT_MST_FK=DPAMT.AGENT_MST_PK(+) " + "AND CURR.CURRENCY_MST_PK(+) = BAT.BASE_CURRENCY_FK " + "AND BAT.BOOKING_AIR_PK= " + lngSBEPK;
            try
            {
                dtMain = objwf.GetDataTable(strSqlHeader);
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

        /// <summary>
        /// Fetches the xabeo freights.
        /// </summary>
        /// <param name="dtTrans">The dt trans.</param>
        private void FetchXABEOFreights(DataTable dtTrans)
        {
            //TRNTYPEPK
            int rowCnt = default(int);
            DataTable dtOFreights = new DataTable();
            for (rowCnt = 0; rowCnt <= dtTrans.Rows.Count - 1; rowCnt++)
            {
                FetchSBOFreight(dtOFreights, Convert.ToString(dtTrans.Rows[rowCnt]["TRANPK"]));
                if (dtOFreights.Rows.Count > 0)
                {
                    //dtTrans.Rows[rowCnt]["OCHARGES"] = Cls_FlatRateFreights.GetOTHstring(dtOFreights, 0, 1, 2, 3, 4, "-1");
                }
            }
        }

        //Function 1.1 Fetch the other freights for existing booking

        /// <summary>
        /// Fetches the sbo freight.
        /// </summary>
        /// <param name="dtOFreights">The dt o freights.</param>
        /// <param name="strXABEPk">The string xabe pk.</param>
        private void FetchSBOFreight(DataTable dtOFreights, string strXABEPk)
        {
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                strBuilder1.Append("SELECT ");
                strBuilder1.Append("BTAOC.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder1.Append("BTAOC.CURRENCY_MST_FK, ");
                strBuilder1.Append("BTAOC.CHARGE_BASIS, ");
                strBuilder1.Append("BTAOC.BASIS_RATE, ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("FREIGHT_TYPE ");
                strBuilder1.Append("FROM ");
                strBuilder1.Append("BOOKING_TRN_AIR_OTH_CHRG BTAOC ");
                strBuilder1.Append("WHERE BTAOC.BOOKING_TRN_AIR_FK= ");
                strBuilder1.Append(strXABEPk);
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        //Function 1.2 Fetch the other freights for existing booking

        /// <summary>
        /// Fetches the sb entry trans.
        /// </summary>
        /// <param name="dtTrans">The dt trans.</param>
        /// <param name="lngSBEPK">The LNG sbepk.</param>
        /// <returns></returns>
        public DataTable FetchSBEntryTrans(DataTable dtTrans, long lngSBEPK)
        {
            string strSqlGridHeader = "";
            WorkFlow objwf = new WorkFlow();

            //SELECT DATA FROM TRANSACTION TABLE FOR EXISTING BOOKING
            strSqlGridHeader = "SELECT DISTINCT " + " NULL AS TRNTYPEPK, " + "BTA.TRANS_REFERED_FROM AS TRNTYPESTATUS, " + "DECODE(BTA.TRANS_REFERED_FROM,1,'Quote',2,'SpRate',3,'Cont',4,'AirTar',5,'GenTar',6,'SRR',7,'Manual') AS TRNTYPE, " + "BTA.TRANS_REF_NO AS REFNO, " + "AMT.AIRLINE_ID AS AIRID, " + "NVL(AST.BREAKPOINT_ID,'') AS BASIS, " + "BTA.QUANTITY AS QTY, " + "CMT.COMMODITY_ID AS COMM, " + "BTA.BUYING_RATE AS RATE, " + "BTA.ALL_IN_TARIFF AS BKGRATE, " + "NULL AS NET, " + "NULL AS TOTALRATE, " + "NULL AS OCBUTTON, " + "NULL AS OCHARGES, " + "'1' AS SEL, " + "BTA.COMMODITY_MST_FKS AS COMMODITYFK, " + "NVL(BAT.AIRLINE_MST_FK,0) AS AIRLINEFK, " + "BTA.BOOKING_TRN_AIR_PK AS TRANPK, " + "NVL(AST.AIRFREIGHT_SLABS_TBL_PK,0) AS BASISPK " + "FROM " + "BOOKING_AIR_TBL BAT, " + "BOOKING_TRN_AIR BTA, " + "AIRFREIGHT_SLABS_TBL AST, " + "COMMODITY_GROUP_MST_TBL CGMT, " + "COMMODITY_MST_TBL CMT, " + "AIRLINE_MST_TBL AMT " + "WHERE(1 = 1) " + "AND BTA.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK " + "AND BTA.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) " + "AND BTA.BOOKING_AIR_FK=BAT.BOOKING_AIR_PK " + "AND BAT.AIRLINE_MST_FK=AMT.AIRLINE_MST_PK(+) " + "AND BTA.BASIS=AST.AIRFREIGHT_SLABS_TBL_PK(+)" + "AND BAT.BOOKING_AIR_PK= " + lngSBEPK;
            try
            {
                dtTrans = objwf.GetDataTable(strSqlGridHeader);
                return dtTrans;
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

        //Fetch the transaction records
        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        /// <summary>
        /// Fetches the ebkg entry freight.
        /// </summary>
        /// <param name="dtFreight">The dt freight.</param>
        /// <param name="lngSBEPK">The LNG sbepk.</param>
        /// <returns></returns>
        public DataTable FetchEBKGEntryFreight(DataTable dtFreight, long lngSBEPK)
        {
            string strSqlGridChild = "";
            WorkFlow objwf = new WorkFlow();
            strSqlGridChild = "SELECT " + "NULL AS TRNTYPEFK, " + "BTA.TRANS_REF_NO AS REFNO, " + "NVL(AST.BREAKPOINT_ID,'') AS BASIS, " + "BTA.COMMODITY_MST_FK AS COMMODITYFK, " + "BAT.PORT_MST_POL_FK AS POLPK, " + "PL.PORT_ID AS POL, " + "BAT.PORT_MST_POD_FK AS PODPK, " + "PD.PORT_ID AS POD, " + "FEMT.FREIGHT_ELEMENT_MST_PK," + "FEMT.FREIGHT_ELEMENT_ID, " + "'FALSE' AS CHECK_FOR_ALL_IN_RT, " + "'' as  CURRENCY_MST_FK, " + "'' as CURRENCY_ID, " + " DECODE(FEMT.CHARGE_BASIS,'1','%','2','Flat Rate','3','Kgs','4','Unit') as CHARGE_BASIS, " + "NULL AS BASIS_RATE, " + "NULL AS RATE, " + "NULL AS BKGRATE, " + "NULL AS BASISPK, " + "'PrePaid' AS PYMT_TYPE, " + "'' as BOOKING_TRN_AIR_FRT_PK " + "FROM " + "BOOKING_AIR_TBL BAT, " + "BOOKING_TRN_AIR BTA, " + "CURRENCY_TYPE_MST_TBL CTMT, " + "FREIGHT_ELEMENT_MST_TBL FEMT, " + "COMMODITY_MST_TBL CMT, " + "PORT_MST_TBL PL, " + "PORT_MST_TBL PD, " + "AIRFREIGHT_SLABS_TBL AST " + "WHERE(1 = 1) " + "AND BTA.BOOKING_AIR_FK=BAT.BOOKING_AIR_PK " + "AND BTA.BASIS=AST.AIRFREIGHT_SLABS_TBL_PK(+) " + "AND BAT.PORT_MST_POL_FK=PL.PORT_MST_PK " + "AND BAT.PORT_MST_POD_FK=PD.PORT_MST_PK " + "AND BTA.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) " + "AND BAT.BOOKING_AIR_PK=" + lngSBEPK + " " + "and ctmt.currency_mst_pk =" + HttpContext.Current.Session["CURRENCY_MST_PK"] + " " + "and femt.business_type = 1" + "AND nvl(FEMT.FREIGHT_TYPE,0) <> 0" + "ORDER BY FEMT.PREFERENCE";
            try
            {
                dtFreight = objwf.GetDataTable(strSqlGridChild);
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

        /// <summary>
        /// Fetches the sb entry freight.
        /// </summary>
        /// <param name="dtFreight">The dt freight.</param>
        /// <param name="lngSBEPK">The LNG sbepk.</param>
        /// <param name="Ebkg">The ebkg.</param>
        /// <returns></returns>
        public DataTable FetchSBEntryFreight(DataTable dtFreight, long lngSBEPK, short Ebkg = 0)
        {
            string strSqlGridChild = "";
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM FREIGHT ELEMENTS TABLE FOR EXISTING BOOKING
            strSqlGridChild = "SELECT " + "NULL AS TRNTYPEFK, " + "BTA.TRANS_REF_NO AS REFNO, " + "AST.BREAKPOINT_ID AS BASIS, " + "BTA.COMMODITY_MST_FK AS COMMODITYFK, " + "BAT.PORT_MST_POL_FK AS POLPK, " + "PL.PORT_ID AS POL, " + "BAT.PORT_MST_POD_FK AS PODPK, " + "PD.PORT_ID AS POD, " + "BTAFD.FREIGHT_ELEMENT_MST_FK," + "FEMT.FREIGHT_ELEMENT_ID, " + "DECODE(BTAFD.CHECK_FOR_ALL_IN_RT,1,'TRUE','FALSE') AS CHECK_FOR_ALL_IN_RT, " + "BTAFD.CURRENCY_MST_FK, " + "CTMT.CURRENCY_ID, " + "DECODE(BTAFD.CHARGE_BASIS,1,'%',2,'Flat',3,'Kgs',4,'Unit'), " + "BTAFD.BASIS_RATE, " + "NULL AS RATE, " + "BTAFD.TARIFF_RATE AS BKGRATE, " + "BTA.BASIS AS BASIS, " + "DECODE(BTAFD.PYMT_TYPE,1,'PrePaid','Collect') AS PYMT_TYPE, " + "BTAFD.BOOKING_TRN_AIR_FRT_PK " + "FROM " + "BOOKING_AIR_TBL BAT, " + "BOOKING_TRN_AIR BTA, " + "BOOKING_TRN_AIR_FRT_DTLS BTAFD, " + "CURRENCY_TYPE_MST_TBL CTMT, " + "FREIGHT_ELEMENT_MST_TBL FEMT, " + "COMMODITY_MST_TBL CMT, " + "PORT_MST_TBL PL, " + "PORT_MST_TBL PD, " + "AIRFREIGHT_SLABS_TBL AST " + "WHERE(1 = 1) " + "AND BTA.BOOKING_TRN_AIR_PK=BTAFD.BOOKING_TRN_AIR_FK " + "AND BTAFD.CURRENCY_MST_FK=CTMT.CURRENCY_MST_PK " + "AND BTAFD.FREIGHT_ELEMENT_MST_FK=FEMT.FREIGHT_ELEMENT_MST_PK " + "AND BTA.BOOKING_AIR_FK=BAT.BOOKING_AIR_PK " + "AND BTA.BASIS=AST.AIRFREIGHT_SLABS_TBL_PK(+) " + "AND BAT.PORT_MST_POL_FK=PL.PORT_MST_PK " + "AND BAT.PORT_MST_POD_FK=PD.PORT_MST_PK " + "AND BTA.COMMODITY_MST_FK=CMT.COMMODITY_MST_PK(+) " + "AND BAT.BOOKING_AIR_PK=" + lngSBEPK + " " + "ORDER BY FEMT.PREFERENCE";

            try
            {
                dtFreight = objwf.GetDataTable(strSqlGridChild);
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

        //Fetch the freight details

        /// <summary>
        /// Fetches the c dimension.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="lngABEPK">The LNG abepk.</param>
        /// <returns></returns>
        public DataTable FetchCDimension(DataTable dtMain, long lngABEPK)
        {
            string strSqlCDimension = "";
            WorkFlow objwf = new WorkFlow();
            //Fetch the Cargo Dimesion for the existing booking
            strSqlCDimension = "SELECT " + "ROWNUM AS SNO, " + "BACC.CARGO_NOP AS NOP, " + "BACC.CARGO_LENGTH AS LENGTH, " + "BACC.CARGO_WIDTH AS WIDTH, " + "BACC.CARGO_HEIGHT AS HEIGHT, " + "BACC.CARGO_CUBE AS CUBE, " + "BACC.CARGO_VOLUME_WT AS VOLWEIGHT, " + "BACC.CARGO_ACTUAL_WT AS ACTWEIGHT, " + "BACC.CARGO_DENSITY AS DENSITY, " + "BACC.BOOKING_AIR_CARGO_CALC_PK AS PK, " + "BACC.CARGO_MEASUREMENT AS MEASURE ," + "BACC.BOOKING_AIR_FK " + "FROM " + "BOOKING_AIR_CARGO_CALC BACC " + "WHERE " + "BACC.BOOKING_AIR_FK= " + lngABEPK;
            try
            {
                dtMain = objwf.GetDataTable(strSqlCDimension);
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

        #endregion "Fetch Air Existing Booking Entry"

        #region "Booking PK"

        /// <summary>
        /// Fetches the BKG pk.
        /// </summary>
        /// <param name="jobpk">The jobpk.</param>
        /// <returns></returns>
        public int FetchBkgPk(int jobpk)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            strSql = "select air.booking_air_fk from job_card_air_exp_tbl air where air.job_card_air_exp_pk=" + jobpk;
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

        //public long FetchCDim(long lngABEPK)
        //{
        //    string strSqlCDimension = "";
        //    WorkFlow objwf = new WorkFlow();
        //    strSqlCDimension = "SELECT BACC.CARGO_NOP AS NOP," + "BACC.CARGO_LENGTH AS LENGTH, " + "BACC.CARGO_WIDTH AS WIDTH, " + "BACC.CARGO_HEIGHT AS HEIGHT, " + "BACC.CARGO_MEASUREMENT AS MEASURE " + "FROM " + "BOOKING_AIR_CARGO_CALC BACC " + "WHERE " + "BACC.BOOKING_AIR_FK= " + lngABEPK;
        //    try
        //    {
        //        return objwf.GetDataSet(strSqlCDimension);
        //    }
        //    catch (OracleException OraExp)
        //    {
        //        throw OraExp;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }
        //}

        #endregion "Booking PK"

        #region "Fetch Air Quotation Entry Details"

        /// <summary>
        /// Fetches the q entry.
        /// </summary>
        /// <param name="dsQEntry">The ds q entry.</param>
        /// <param name="lngSQEPk">The LNG sqe pk.</param>
        /// <param name="strQuotationPOLPK">The string quotation polpk.</param>
        /// <param name="strQuotationPODPK">The string quotation podpk.</param>
        public void FetchQEntry(DataSet dsQEntry, long lngSQEPk, string strQuotationPOLPK, string strQuotationPODPK)
        {
            try
            {
                DataTable dtMain = new DataTable();
                fetchQuoteHeader(dtMain, lngSQEPk, strQuotationPOLPK, strQuotationPODPK);
                //FetchSQEntryHeader(dtMain, lngSQEPk, strQuotationPOLPK, strQuotationPODPK)
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

        /// <summary>
        /// Fetches the quote header.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="lngAQEPK">The LNG aqepk.</param>
        /// <param name="strQuotationPOLPK">The string quotation polpk.</param>
        /// <param name="strQuotationPODPK">The string quotation podpk.</param>
        private void fetchQuoteHeader(DataTable dtMain, long lngAQEPK, string strQuotationPOLPK, string strQuotationPODPK)
        {
            WorkFlow objWFT = new WorkFlow();
            objWFT.MyCommand.CommandType = CommandType.StoredProcedure;
            try
            {
                objWFT.MyCommand.Parameters.Clear();
                var _with1 = objWFT.MyCommand.Parameters;
                _with1.Add("QUOT_PK_IN", lngAQEPK).Direction = ParameterDirection.Input;
                _with1.Add("POLIN", strQuotationPOLPK).Direction = ParameterDirection.Input;
                _with1.Add("POD_IN", strQuotationPODPK).Direction = ParameterDirection.Input;
                _with1.Add("RES_RefCursor_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dtMain = objWFT.GetDataTable("BOOKING_AIR_FETCH_PKG", "FETCH_QUOTE_DATA");
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

        /// <summary>
        /// Fetches the sq entry header.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="lngAQEPK">The LNG aqepk.</param>
        /// <param name="strQuotationPOLPK">The string quotation polpk.</param>
        /// <param name="strQuotationPODPK">The string quotation podpk.</param>
        public void FetchSQEntryHeader(DataTable dtMain, long lngAQEPK, string strQuotationPOLPK, string strQuotationPODPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING QUOTATION
            strBuilder.Append("SELECT DISTINCT ");
            strBuilder.Append("QAT.CUSTOMER_MST_FK, ");
            strBuilder.Append("CMT.CUSTOMER_ID, ");
            strBuilder.Append("QAT.CUSTOMER_CATEGORY_MST_FK, ");
            strBuilder.Append("CCMT.CUSTOMER_CATEGORY_ID, ");
            strBuilder.Append("POL.PORT_MST_PK POLPK, ");
            strBuilder.Append("POL.PORT_ID POLID, ");
            strBuilder.Append("POL.PORT_NAME POLNAME, ");
            strBuilder.Append("POD.PORT_MST_PK PODPK, ");
            strBuilder.Append("POD.PORT_ID PODID, ");
            strBuilder.Append("POD.PORT_NAME PODNAME, ");
            strBuilder.Append("QTA.COMMODITY_GROUP_FK, ");
            strBuilder.Append("QAT.COL_PLACE_MST_FK, ");
            strBuilder.Append("CPMT.PLACE_CODE CPLACECODE, ");
            strBuilder.Append("(CASE WHEN QAT.COL_ADDRESS IS NULL THEN ");
            strBuilder.Append("(CASE WHEN QAT.CUSTOMER_MST_FK IS NOT NULL THEN ");
            strBuilder.Append("(SELECT CTRN.COL_ADDRESS FROM CUSTOMER_MST_TBL CTRN WHERE ");
            strBuilder.Append("CTRN.CUSTOMER_MST_PK = (SELECT DISTINCT QHDR.CUSTOMER_MST_FK FROM ");
            strBuilder.Append("QUOTATION_AIR_TBL QHDR, QUOTATION_TRN_AIR QTRN WHERE ");
            strBuilder.Append("QTRN.QUOTATION_AIR_FK = QHDR.QUOTATION_AIR_PK ");
            strBuilder.Append(" AND QHDR.CUST_TYPE= 0 ");
            strBuilder.Append(" AND QHDR.QUOTATION_AIR_PK = " + lngAQEPK + " AND QTRN.PORT_MST_POL_FK = " + strQuotationPOLPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK = " + strQuotationPODPK + " )) ");
            strBuilder.Append("ELSE ");
            strBuilder.Append("NULL ");
            strBuilder.Append("END) ");
            strBuilder.Append("ELSE ");
            strBuilder.Append("QAT.COL_ADDRESS ");
            strBuilder.Append("END) AS COL_ADDRESS, ");
            strBuilder.Append("QAT.DEL_PLACE_MST_FK, ");
            strBuilder.Append("DPMT.PLACE_CODE DPLACECODE, ");
            strBuilder.Append("(CASE WHEN QAT.DEL_ADDRESS IS NULL THEN ");
            strBuilder.Append("(CASE WHEN QAT.CUSTOMER_MST_FK IS NOT NULL THEN ");
            strBuilder.Append("(SELECT CTRN.DEL_ADDRESS FROM CUSTOMER_MST_TBL CTRN WHERE ");
            strBuilder.Append("CTRN.CUSTOMER_MST_PK = (SELECT DISTINCT QHDR.CUSTOMER_MST_FK FROM ");
            strBuilder.Append("QUOTATION_AIR_TBL QHDR, QUOTATION_TRN_AIR QTRN WHERE ");
            strBuilder.Append("QTRN.QUOTATION_AIR_FK = QHDR.QUOTATION_AIR_PK ");
            strBuilder.Append(" AND QHDR.CUST_TYPE= 0 ");
            strBuilder.Append(" AND QHDR.QUOTATION_AIR_PK= " + lngAQEPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POL_FK= " + strQuotationPOLPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + strQuotationPODPK + " ))");
            strBuilder.Append(" ELSE ");
            strBuilder.Append("NULL ");
            strBuilder.Append("END) ");
            strBuilder.Append("ELSE ");
            strBuilder.Append("QAT.DEL_ADDRESS ");
            strBuilder.Append("END) AS DEL_ADDRESS, ");
            strBuilder.Append("QAT.PYMT_TYPE, ");
            strBuilder.Append("QAT.AGENT_MST_FK, ");
            strBuilder.Append("QAT.STATUS, ");
            strBuilder.Append("CBAMT.AGENT_ID, ");
            strBuilder.Append("QTA.PACK_COUNT, ");
            strBuilder.Append("QTA.ACTUAL_WEIGHT, ");
            strBuilder.Append("QTA.ACTUAL_VOLUME, ");
            strBuilder.Append("QTA.CHARGEABLE_WEIGHT, ");
            strBuilder.Append("QTA.VOLUME_WEIGHT, ");
            strBuilder.Append("QTA.DENSITY, ");
            strBuilder.Append("QTA.QUANTITY, ");
            strBuilder.Append("DPAMT.AGENT_MST_PK DPAGENTPK, ");
            strBuilder.Append("DPAMT.AGENT_ID DPAGENT, ");
            strBuilder.Append("DPAMT.AGENT_NAME DPAGENTNAME, ");
            strBuilder.Append("TO_CHAR(QAT.EXPECTED_SHIPMENT,'" + dateFormat + "') AS EXP_SHIPMENT_DATE ");
            strBuilder.Append("FROM QUOTATION_AIR_TBL QAT, ");
            strBuilder.Append("QUOTATION_TRN_AIR QTA, ");
            strBuilder.Append("CUSTOMER_MST_TBL CMT, ");
            strBuilder.Append("PLACE_MST_TBL CPMT, ");
            strBuilder.Append("PLACE_MST_TBL DPMT, ");
            strBuilder.Append("AGENT_MST_TBL CBAMT, ");
            strBuilder.Append("PORT_MST_TBL POL, ");
            strBuilder.Append("PORT_MST_TBL POD, ");
            strBuilder.Append("AGENT_MST_TBL DPAMT, ");
            strBuilder.Append("CUSTOMER_CATEGORY_MST_TBL CCMT ");
            strBuilder.Append("WHERE ");
            strBuilder.Append("QAT.QUOTATION_AIR_PK=QTA.QUOTATION_AIR_FK ");
            strBuilder.Append("AND QAT.CUSTOMER_MST_FK=CMT.CUSTOMER_MST_PK(+) ");
            strBuilder.Append("AND CMT.DP_AGENT_MST_FK=DPAMT.AGENT_MST_PK(+) ");
            strBuilder.Append("AND QAT.COL_PLACE_MST_FK=CPMT.PLACE_PK(+) ");
            strBuilder.Append("AND QAT.DEL_PLACE_MST_FK=DPMT.PLACE_PK(+) ");
            strBuilder.Append("AND QAT.AGENT_MST_FK=CBAMT.AGENT_MST_PK(+)");
            strBuilder.Append("AND QAT.CUSTOMER_CATEGORY_MST_FK=CCMT.CUSTOMER_CATEGORY_MST_PK(+) ");
            strBuilder.Append("AND QTA.PORT_MST_POL_FK=POL.PORT_MST_PK(+) ");
            strBuilder.Append("AND QTA.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ");
            strBuilder.Append("AND QAT.QUOTATION_AIR_PK= " + lngAQEPK);
            strBuilder.Append("AND QTA.PORT_MST_POL_FK= " + strQuotationPOLPK);
            strBuilder.Append("AND QTA.PORT_MST_POD_FK= " + strQuotationPODPK);
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

        /// <summary>
        /// Fetches the qc dimension.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="lngQPK">The LNG QPK.</param>
        /// <param name="lngPOLPK">The LNG polpk.</param>
        /// <param name="lngPODPK">The LNG podpk.</param>
        /// <returns></returns>
        public object FetchQCDimension(DataTable dtMain, long lngQPK, long lngPOLPK, long lngPODPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING
            strBuilder.Append("SELECT ");
            strBuilder.Append("ROWNUM AS SNO, ");
            strBuilder.Append("QCALC.CARGO_NOP AS NOP, ");
            strBuilder.Append("QCALC.CARGO_LENGTH AS LENGTH, ");
            strBuilder.Append("QCALC.CARGO_WIDTH AS WIDTH, ");
            strBuilder.Append("QCALC.CARGO_HEIGHT AS HEIGHT,  ");
            strBuilder.Append("QCALC.CARGO_CUBE AS CUBE,");
            strBuilder.Append("QCALC.CARGO_VOLUME_WT AS VOLWEIGHT, ");
            strBuilder.Append("QCALC.CARGO_ACTUAL_WT AS ACTWEIGHT, ");
            strBuilder.Append("QCALC.CARGO_DENSITY AS DENSITY, ");
            strBuilder.Append("NULL AS PK, ");
            strBuilder.Append("NULL AS FK ");
            strBuilder.Append("FROM ");
            strBuilder.Append("QUOTATION_AIR_TBL QHDR,");
            strBuilder.Append("QUOTATION_TRN_AIR QTRN, ");
            strBuilder.Append("QUOTATION_AIR_CARGO_CALC QCALC ");
            strBuilder.Append("WHERE ");
            strBuilder.Append("QTRN.QUOTATION_AIR_FK=QHDR.QUOTATION_AIR_PK ");
            strBuilder.Append("AND QCALC.QUOTATION_TRN_AIR_FK=QTRN.QUOTE_TRN_AIR_PK ");
            strBuilder.Append("AND QTRN.PORT_MST_POL_FK= " + lngPOLPK);
            strBuilder.Append(" AND QTRN.PORT_MST_POD_FK= " + lngPODPK);
            strBuilder.Append(" AND QTRN.QUOTATION_AIR_FK= " + lngQPK);
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

        #endregion "Fetch Air Quotation Entry Details"

        #region "Fetch Air Spot Rate Entry Details"

        /// <summary>
        /// Fetches the spot rate entry.
        /// </summary>
        /// <param name="dsSpotRateEntry">The ds spot rate entry.</param>
        /// <param name="lngSpotRatePk">The LNG spot rate pk.</param>
        /// <param name="strPOLPK">The string polpk.</param>
        /// <param name="strPODPK">The string podpk.</param>
        public void FetchSpotRateEntry(DataSet dsSpotRateEntry, long lngSpotRatePk, string strPOLPK, string strPODPK)
        {
            DataTable dtMain = new DataTable();
            FetchSpotRateEntryHeader(dtMain, lngSpotRatePk);
            //Data set ds contains the Master Table details
            dsSpotRateEntry.Tables.Add(dtMain);
        }

        /// <summary>
        /// Fetches the spot rate entry header.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="lngSpotRateEntryPK">The LNG spot rate entry pk.</param>
        public void FetchSpotRateEntryHeader(DataTable dtMain, long lngSpotRateEntryPK)
        {
            System.Text.StringBuilder strBuilderSpotRate = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING Spot Rate
            strBuilderSpotRate.Append("SELECT ");
            strBuilderSpotRate.Append("HDR.CUSTOMER_MST_FK, ");
            //strBuilderSpotRate.Append("CMT.CUSTOMER_ID, ")
            strBuilderSpotRate.Append("CMT.CUSTOMER_NAME CUSTOMER_ID, ");
            strBuilderSpotRate.Append("HDR.COL_ADDRESS, ");
            strBuilderSpotRate.Append("HDR.AIRLINE_REFERENCE_NO, ");
            strBuilderSpotRate.Append("TRN.MAWB_REF_NO ");
            //Commented by Manoharan 11June2007: Agent should not be taken from Customer Profile information - by Balaji
            //strBuilderSpotRate.Append("DPAMT.AGENT_MST_PK DPAGENTPK, ")
            //strBuilderSpotRate.Append("DPAMT.AGENT_ID DPAGENT, ")
            //strBuilderSpotRate.Append("DPAMT.AGENT_NAME DPAGENTNAME ")
            //end
            //strBuilderSpotRate.Append("TRN.PACK_COUNT, ")
            //strBuilderSpotRate.Append("TRN.GROSS_WEIGHT, ")
            //strBuilderSpotRate.Append("TRN.CHARGEABLE_WEIGHT, ")
            //strBuilderSpotRate.Append("TRN.VOLUME_IN_CBM, ")
            //strBuilderSpotRate.Append("TRN.VOLUME_WEIGHT, ")
            //strBuilderSpotRate.Append("TRN.DENSITY ")
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

        /// <summary>
        /// Fetches the spot rate c dimension.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="lngSpotRateEntryPK">The LNG spot rate entry pk.</param>
        /// <returns></returns>
        public object FetchSpotRateCDimension(DataTable dtMain, long lngSpotRateEntryPK)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objwf = new WorkFlow();
            //SELECT DATA FROM THE MASTER TABLE AND TRANSACTION TO FILL HEADER DETAILS FOR EXISTING BOOKING
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

        #endregion "Fetch Air Spot Rate Entry Details"

        #region "Fetch Grid Values"

        /// <summary>
        /// Fetches the grid values.
        /// </summary>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="intCustomerPK">The int customer pk.</param>
        /// <param name="intQuotationPK">The int quotation pk.</param>
        /// <param name="intIsKGS">The int is KGS.</param>
        /// <param name="intSRateStatus">The int s rate status.</param>
        /// <param name="intCContractStatus">The int c contract status.</param>
        /// <param name="intSRRContractStatus">The int SRR contract status.</param>
        /// <param name="intOTariffStatus">The int o tariff status.</param>
        /// <param name="intGTariffStatus">The int g tariff status.</param>
        /// <param name="strPOL">The string pol.</param>
        /// <param name="strPOD">The string pod.</param>
        /// <param name="intCommodityPK">The int commodity pk.</param>
        /// <param name="strSDate">The string s date.</param>
        /// <param name="strChargeWeight">The string charge weight.</param>
        /// <param name="intContainerPk">The int container pk.</param>
        /// <param name="intSpotRatePk">The int spot rate pk.</param>
        /// <param name="intNoOfContainers">The int no of containers.</param>
        /// <param name="CustContractPK">The customer contract pk.</param>
        /// <param name="SplitBooking">The split booking.</param>
        /// <returns></returns>
        public DataSet FetchGridValues(DataSet dsGrid, int intCustomerPK = 0, int intQuotationPK = 0, short intIsKGS = 0, short intSRateStatus = 0, short intCContractStatus = 0, short intSRRContractStatus = 0, short intOTariffStatus = 0, short intGTariffStatus = 0, string strPOL = "",
        string strPOD = "", short intCommodityPK = 0, string strSDate = "", string strChargeWeight = "0", int intContainerPk = 0, int intSpotRatePk = 0, int intNoOfContainers = 0, int CustContractPK = 0, int SplitBooking = 0)
        {
            try
            {
                DataTable dtMain = new DataTable();
                DataTable dtOCharge = new DataTable();
                DataTable dtChild = new DataTable();
                int Int_I = 0;
                DataRow[] drRefNo = null;
                strChargeWeight = strChargeWeight.Replace(",", "");
                if (intQuotationPK == 0 & intSpotRatePk == 0)
                {
                    if (intIsKGS == 1)
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 1, 0, 0, 0, CustContractPK, SplitBooking);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 2, 0, 0, 0, CustContractPK, SplitBooking);
                    }
                    else
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 1, intContainerPk, 0, intNoOfContainers, 0, SplitBooking);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, intSRateStatus, intCContractStatus, intSRRContractStatus,
                        intOTariffStatus, intGTariffStatus, strChargeWeight, 2, intContainerPk, 0, intNoOfContainers, 0, SplitBooking);
                    }
                }
                else if (intSpotRatePk == 0)
                {
                    if (intIsKGS == 1)
                    {
                        dtMain = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, 0, 0, 0, 0, SplitBooking);
                        dtChild = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 2, 0, 0, 0, 0, SplitBooking);
                    }
                    else
                    {
                        dtMain = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, intContainerPk, 0, intNoOfContainers, 0, SplitBooking);
                        dtChild = FunRetriveData(intCustomerPK, intQuotationPK, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 2, intContainerPk, 0, intNoOfContainers, 0, SplitBooking);
                    }
                }
                else
                {
                    if (intIsKGS == 1)
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, 0, intSpotRatePk, 0, 0, SplitBooking);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 2, 0, intSpotRatePk, 0, 0, SplitBooking);
                    }
                    else
                    {
                        dtMain = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0, 0

                        , 0, strChargeWeight, 1, intContainerPk, intSpotRatePk, intNoOfContainers, 0, SplitBooking);
                        dtChild = FunRetriveData(intCustomerPK, 0, intIsKGS, strPOL, strPOD, intCommodityPK, strSDate, 0, 0, 0,

                       0, 0, strChargeWeight, 2, intContainerPk, intSpotRatePk, intNoOfContainers, 0, SplitBooking);
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Grid Values"

        #region "Procedure Invoke to Retrive Header and Freight Details"

        /// <summary>
        /// Funs the retrive data.
        /// </summary>
        /// <param name="intCustomerPK">The int customer pk.</param>
        /// <param name="intQuotationPK">The int quotation pk.</param>
        /// <param name="intIsKGS">The int is KGS.</param>
        /// <param name="strPOL">The string pol.</param>
        /// <param name="strPOD">The string pod.</param>
        /// <param name="intCommodityPK">The int commodity pk.</param>
        /// <param name="strSDate">The string s date.</param>
        /// <param name="intSRateStatus">The int s rate status.</param>
        /// <param name="intCContractStatus">The int c contract status.</param>
        /// <param name="intSRRContractStatus">The int SRR contract status.</param>
        /// <param name="intOTariffStatus">The int o tariff status.</param>
        /// <param name="intGTariffStatus">The int g tariff status.</param>
        /// <param name="strChargeWeightNo">The string charge weight no.</param>
        /// <param name="intFetchStatus">The int fetch status.</param>
        /// <param name="intContainerPk">The int container pk.</param>
        /// <param name="intSpotRatePk">The int spot rate pk.</param>
        /// <param name="intNoOfContainers">The int no of containers.</param>
        /// <param name="CustContractPk">The customer contract pk.</param>
        /// <param name="SplitBooking">The split booking.</param>
        /// <returns></returns>
        public DataTable FunRetriveData(int intCustomerPK = 0, int intQuotationPK = 0, short intIsKGS = 0, string strPOL = "", string strPOD = "", short intCommodityPK = 0, string strSDate = "", short intSRateStatus = 0, short intCContractStatus = 0, short intSRRContractStatus = 0,
        short intOTariffStatus = 0, short intGTariffStatus = 0, string strChargeWeightNo = "", short intFetchStatus = 0, int intContainerPk = 0, int intSpotRatePk = 0, int intNoOfContainers = 0, int CustContractPk = 0, int SplitBooking = 0)
        {
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
                var _with2 = objWFT.MyCommand.Parameters;
                _with2.Add("CUSTOMER_PK_IN", intCustomerPK).Direction = ParameterDirection.Input;
                _with2.Add("QUOTATION_PK_IN", intQuotationPK).Direction = ParameterDirection.Input;
                _with2.Add("IS_KGS_IN", intIsKGS).Direction = ParameterDirection.Input;
                _with2.Add("POL_IN", strPOL).Direction = ParameterDirection.Input;
                _with2.Add("POD_IN", strPOD).Direction = ParameterDirection.Input;
                _with2.Add("COMMODITY_PK_IN", intCommodityPK).Direction = ParameterDirection.Input;
                _with2.Add("S_DATE_IN", strSDate).Direction = ParameterDirection.Input;
                _with2.Add("SPOT_RATE_STATUS_IN", intSRateStatus).Direction = ParameterDirection.Input;
                _with2.Add("CUST_CONTRACT_STATUS_IN", intCContractStatus).Direction = ParameterDirection.Input;
                _with2.Add("SRR_CONTRACT_STATUS_IN", intSRRContractStatus).Direction = ParameterDirection.Input;
                _with2.Add("OPR_TARIFF_STATUS_IN", intOTariffStatus).Direction = ParameterDirection.Input;
                _with2.Add("GEN_TARIFF_STATUS_IN", intGTariffStatus).Direction = ParameterDirection.Input;
                _with2.Add("CHARGE_WT_NO_IN", (string.IsNullOrEmpty(strChargeWeightNo) ? "0" : (string.IsNullOrEmpty(strChargeWeightNo) ? strChargeWeightNo : "0"))).Direction = ParameterDirection.Input;
                _with2.Add("CONTAINER_PK_IN", intContainerPk).Direction = ParameterDirection.Input;
                _with2.Add("SPOT_RATE_PK_IN", intSpotRatePk).Direction = ParameterDirection.Input;
                _with2.Add("NO_OF_CONTAINERS", intNoOfContainers).Direction = ParameterDirection.Input;
                if (intFetchStatus == 1)
                {
                    _with2.Add("CURRPK", Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                    _with2.Add("SPLIT_CARGO_FLAG", SplitBkgFlag).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with2.Add("CURRPK", Convert.ToInt32(HttpContext.Current.Session["CURRENCY_MST_PK"])).Direction = ParameterDirection.Input;
                    _with2.Add("SPLIT_CARGO_FLAG", SplitBkgFlag).Direction = ParameterDirection.Input;
                }
                _with2.Add("RES_RefCursor_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                //.Add("CURRPK", HttpContext.Current.Session("CURRENCY_MST_PK")).Direction = ParameterDirection.Input
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

        /// <summary>
        /// Fetches the o freights.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        /// <param name="intQuotationPK">The int quotation pk.</param>
        /// <param name="intSRateStatus">The int s rate status.</param>
        /// <param name="intCContractStatus">The int c contract status.</param>
        /// <param name="intSRRContractStatus">The int SRR contract status.</param>
        /// <param name="intOTariffStatus">The int o tariff status.</param>
        /// <param name="intGTariffStatus">The int g tariff status.</param>
        /// <param name="intSpotRatePk">The int spot rate pk.</param>
        private void FetchOFreights(DataTable dtMain, int intQuotationPK = 0, short intSRateStatus = 0, short intCContractStatus = 0, short intSRRContractStatus = 0, short intOTariffStatus = 0, short intGTariffStatus = 0, int intSpotRatePk = 0)
        {
            //TRNTYPEPK
            try
            {
                int rowCnt = default(int);
                DataTable dtOFreights = new DataTable();
                for (rowCnt = 0; rowCnt <= dtMain.Rows.Count - 1; rowCnt++)
                {
                    if (!(intQuotationPK == 0))
                    {
                        FetchDTQOFreights(dtOFreights, Convert.ToString(dtMain.Rows[rowCnt]["TRNTYPEPK"]));
                    }
                    else if (intSRateStatus == 1 | !(intSpotRatePk == 0) & intQuotationPK == 0)
                    {
                        FetchDTSOFreights(dtOFreights, Convert.ToString(dtMain.Rows[rowCnt]["TRNTYPEPK"]));
                    }
                    else if (intCContractStatus == 1 & intQuotationPK == 0)
                    {
                        FetchDTCCOFreights(dtOFreights, Convert.ToString(dtMain.Rows[rowCnt]["TRNTYPEPK"]));
                    }
                    else if (intSRRContractStatus == 1 & intQuotationPK == 0)
                    {
                        FetchDTSRROFreights(dtOFreights, Convert.ToString(dtMain.Rows[rowCnt]["TRNTYPEPK"]));
                    }
                    else if (intOTariffStatus == 1 | intGTariffStatus == 1 & intQuotationPK == 0)
                    {
                        FetchDTAGOFreights(dtOFreights, Convert.ToString(dtMain.Rows[rowCnt]["TRNTYPEPK"]));
                    }
                    if (dtOFreights.Rows.Count > 0)
                    {
                        //dtMain.Rows[rowCnt]["OCHARGES"] = Cls_FlatRateFreights.GetOTHstring(dtOFreights, 0, 1, 2, 3, 4, "-1");
                    }
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

        /// <summary>
        /// Fetches the dtqo freights.
        /// </summary>
        /// <param name="dtOFreights">The dt o freights.</param>
        /// <param name="strQPk">The string q pk.</param>
        private void FetchDTQOFreights(DataTable dtOFreights, string strQPk)
        {
            try
            {
                System.Text.StringBuilder strBuilder1 = new System.Text.StringBuilder();
                System.Text.StringBuilder strSql = new System.Text.StringBuilder();
                short QType = default(short);
                WorkFlow objF = new WorkFlow();
                strSql.Append("select a.quotation_type from quotation_air_tbl a where a.quotation_air_pk=");
                //Manoharan 18July2007: strQPK is transaction pk. so i used in subquery
                strSql.Append("( select t.quotation_air_fk from QUOTATION_TRN_AIR t where t.quote_trn_air_pk=" + strQPk + ")");
                QType = Convert.ToInt16(objF.ExecuteScaler(strSql.ToString()));

                strBuilder1.Append("SELECT ");
                strBuilder1.Append("QTAOC.FREIGHT_ELEMENT_MST_FK, ");
                strBuilder1.Append("QTAOC.CURRENCY_MST_FK, ");
                strBuilder1.Append("QTAOC.CHARGE_BASIS, ");
                strBuilder1.Append("QTAOC.BASIS_RATE, ");
                strBuilder1.Append("'-1' PK, ");
                strBuilder1.Append("QTAOC.FREIGHT_TYPE PYMT_TYPE ");
                strBuilder1.Append("FROM ");

                if (QType == 0)
                {
                    strBuilder1.Append("QUOT_AIR_OTH_CHRG  QTAOC ");
                }
                else
                {
                    strBuilder1.Append("QUOTATION_TRN_AIR_OTH_CHRG QTAOC ");
                }

                strBuilder1.Append("WHERE QTAOC.quotation_trn_air_fk= " + strQPk);
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        //Fetch Quotation Other Freights

        /// <summary>
        /// Fetches the dtso freights.
        /// </summary>
        /// <param name="dtOFreights">The dt o freights.</param>
        /// <param name="strSRatePk">The string s rate pk.</param>
        private void FetchDTSOFreights(DataTable dtOFreights, string strSRatePk)
        {
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetch Spot Rate other Freights

        /// <summary>
        /// Fetches the dtcco freights.
        /// </summary>
        /// <param name="dtOFreights">The dt o freights.</param>
        /// <param name="strCCPk">The string cc pk.</param>
        private void FetchDTCCOFreights(DataTable dtOFreights, string strCCPk)
        {
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetch Customer Contract Other Freights

        /// <summary>
        /// Fetches the dtsrro freights.
        /// </summary>
        /// <param name="dtOFreights">The dt o freights.</param>
        /// <param name="strSRRPk">The string SRR pk.</param>
        private void FetchDTSRROFreights(DataTable dtOFreights, string strSRRPk)
        {
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetch Customer Contract Other Freights

        /// <summary>
        /// Fetches the dtago freights.
        /// </summary>
        /// <param name="dtOFreights">The dt o freights.</param>
        /// <param name="strAGPk">The string ag pk.</param>
        private void FetchDTAGOFreights(DataTable dtOFreights, string strAGPk)
        {
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //Fetch Air Tariff and Gen Tariff Other Freights

        #endregion "Main Fetch OFreights"

        #region "Compute the other freight charges based on AFC for % type Freights"

        /// <summary>
        /// Computes the o freight charge.
        /// </summary>
        /// <param name="dtChild">The dt child.</param>
        /// <param name="strSDate">The string s date.</param>
        private void ComputeOFreightCharge(DataTable dtChild, string strSDate)
        {
            try
            {
                int i = default(int);
                int j = default(int);
                string strFCurrPk = null;
                string strTCurrPk = null;
                double dblExRate = 0;
                double dblAFCBkgRate = 0;
                double dblBasisRate = 0;
                double dblBkgRate = 0;
                if (dtChild.Rows.Count > 0)
                {
                    for (i = 0; i <= dtChild.Rows.Count - 1; i++)
                    {
                        if (getDefault(dtChild.Rows[i][13], "") == "1" | getDefault(dtChild.Rows[i][13], "") == "%")
                        {
                            for (j = 0; j <= dtChild.Rows.Count - 1; j++)
                            {
                                if (Convert.ToString(dtChild.Rows[j][9]).ToUpper() == "AFC")
                                {
                                    if ((dtChild.Rows[j][9] != null) & !string.IsNullOrEmpty(Convert.ToString(dtChild.Rows[j][9])) & !string.IsNullOrEmpty(dtChild.Rows[j][9].ToString()) & (dtChild.Rows[i][13] != null) & !string.IsNullOrEmpty(Convert.ToString(dtChild.Rows[j][13])) & !string.IsNullOrEmpty(dtChild.Rows[i][13].ToString()))
                                    {
                                        strFCurrPk = Convert.ToString(dtChild.Rows[j][11]);
                                        strTCurrPk = Convert.ToString(dtChild.Rows[i][11]);
                                        dblExRate = getExRate(strFCurrPk, strTCurrPk, strSDate);
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

        /// <summary>
        /// Gets the ex rate.
        /// </summary>
        /// <param name="strFCurrPk">The string f curr pk.</param>
        /// <param name="strTCurrPk">The string t curr pk.</param>
        /// <param name="strSDate">The string s date.</param>
        /// <returns></returns>
        private double getExRate(string strFCurrPk, string strTCurrPk, string strSDate)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            strBuilder.Append("SELECT ");
            strBuilder.Append("GET_EX_RATE('" + strFCurrPk + "','");
            strBuilder.Append(strTCurrPk + "',to_date('" + strSDate + "','" + dateFormat + "')) from dual ");
            try
            {
                return Convert.ToDouble(objWF.ExecuteScaler(strBuilder.ToString()));
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

        #endregion "Compute the other freight charges based on AFC for % type Freights"

        #region "Save"

        /// <summary>
        /// Saves the quotation.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="objWF">The object wf.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="ShipperPK">The shipper pk.</param>
        /// <param name="Cust_Type">Type of the cust_.</param>
        /// <param name="Nomination">if set to <c>true</c> [nomination].</param>
        /// <returns></returns>
        public ArrayList SaveQuotation(long PkValue, WorkFlow objWF, OracleTransaction TRAN, string ShipperPK = "", int Cust_Type = 0, bool Nomination = false)
        {
            string QuoteNo = null;
            int CustCatgoryPK = 0;
            string QuotePKs = null;
            DataSet dsCust = null;
            Array QuoteArry = null;
            short i = default(short);
            arrMessage.Clear();

            try
            {
                QuoteNo = GenerateQuoteNo(Convert.ToInt64(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt64(HttpContext.Current.Session["EMP_PK"]), M_CREATED_BY_FK, objWF);
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
                var _with6 = objWF.MyCommand;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWF.MyUserName + ".BOOKING_AIR_PKG.AUTO_CREATE_QUOTATION";
                _with6.Parameters.Clear();
                _with6.Parameters.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                _with6.Parameters["BOOKING_AIR_FK_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("QUOTATION_NO_IN", QuoteNo).Direction = ParameterDirection.Input;
                _with6.Parameters["QUOTATION_NO_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("CUST_CATEGORY_PK_IN", CustCatgoryPK).Direction = ParameterDirection.Input;
                _with6.Parameters["CUST_CATEGORY_PK_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("CUST_TYPE_IN", Cust_Type).Direction = ParameterDirection.Input;
                _with6.Parameters["CUST_TYPE_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("FROM_FLAG_IN", (Nomination == true ? 1 : 0)).Direction = ParameterDirection.Input;
                _with6.Parameters["FROM_FLAG_IN"].SourceVersion = DataRowVersion.Current;
                _with6.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with6.ExecuteNonQuery();
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

        /// <summary>
        /// Generates the quote no.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="nCreatedBy">The n created by.</param>
        /// <param name="ObjWK">The object wk.</param>
        /// <returns></returns>
        public new string GenerateQuoteNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null)
        {
            return GenerateProtocolKey("QUOTATION (AIR)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy);
        }

        #region "Generate Protocol Key"

        /// <summary>
        /// Generates the protocol key.
        /// </summary>
        /// <param name="sProtocolName">Name of the s protocol.</param>
        /// <param name="ILocationId">The i location identifier.</param>
        /// <param name="IEmployeeId">The i employee identifier.</param>
        /// <param name="ProtocolDate">The protocol date.</param>
        /// <param name="sVSL">The s VSL.</param>
        /// <param name="sVOY">The s voy.</param>
        /// <param name="sPOL">The s pol.</param>
        /// <param name="UserId">The user identifier.</param>
        /// <param name="objWS">The object ws.</param>
        /// <param name="SID">The sid.</param>
        /// <param name="PODID">The podid.</param>
        /// <returns></returns>
        protected string GenerateProtocolKey(string sProtocolName, Int64 ILocationId, Int64 IEmployeeId, System.DateTime ProtocolDate, string sVSL = "", string sVOY = "", string sPOL = "", Int64 UserId = 0, WorkFlow objWS = null, string SID = "",
        string PODID = "")
        {
            //Added Optional ByVal Userid As Int64 = 0 as parameter for EDI as there is no scope for the user
            //so when using this function , call  HttpContext.Current.Session("USER_PK") as parameter
            bool bNewObject = false;
            if (objWS == null)
            {
                bNewObject = true;
                objWS = new WorkFlow();
            }
            try
            {
                objWS.MyCommand.CommandType = CommandType.StoredProcedure;
                objWS.MyCommand.CommandText = objWS.MyUserName + ".GENERATE_PROTOCOL_KEY";
                objWS.MyCommand.Parameters.Clear();
                var _with1 = objWS.MyCommand.Parameters;
                _with1.Add("PROTOCOL_NAME_IN", sProtocolName).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ILocationId).Direction = ParameterDirection.Input;
                _with1.Add("EMPLOYEE_MST_FK_IN", IEmployeeId).Direction = ParameterDirection.Input;
                _with1.Add("USER_MST_FK_IN", UserId).Direction = ParameterDirection.Input;
                _with1.Add("DATE_IN", ProtocolDate).Direction = ParameterDirection.Input;
                _with1.Add("VSL_IN", (string.IsNullOrEmpty(sVSL) ? "" : sVSL)).Direction = ParameterDirection.Input;
                _with1.Add("VOY_IN", (string.IsNullOrEmpty(sVOY) ? "" : sVOY)).Direction = ParameterDirection.Input;
                _with1.Add("POL_IN", (string.IsNullOrEmpty(sPOL) ? "" : sPOL)).Direction = ParameterDirection.Input;
                _with1.Add("SL_IN", (string.IsNullOrEmpty(sPOL) ? "" : SID)).Direction = ParameterDirection.Input;
                _with1.Add("POD_IN", (string.IsNullOrEmpty(sPOL) ? "" : PODID)).Direction = ParameterDirection.Input;
                objWS.MyCommand.Parameters["VSL_IN"].Size = 20;
                objWS.MyCommand.Parameters["VOY_IN"].Size = 20;
                objWS.MyCommand.Parameters["POL_IN"].Size = 20;
                _with1.Add("RETURN_VALUE", OracleDbType.Varchar2, 30).Direction = ParameterDirection.Output;

                if (!bNewObject)
                {
                    objWS.MyCommand.ExecuteNonQuery();
                    return objWS.MyCommand.Parameters["RETURN_VALUE"].Value.ToString();
                }

                if (objWS.ExecuteCommands())
                {
                    return objWS.MyCommand.Parameters["RETURN_VALUE"].Value.ToString();
                }
                else
                {
                    return "Protocol Not Defined.";
                }
            }
            catch (Exception ex)
            {
                return "Protocol Not Defined.";
                //if (string.Compare(ex.Message, "-20997"))
                //{
                //    return "Protocol Not Defined.";
                //}
                //else
                //{
                //    throw ex;
                //}
            }
        }

        /// <summary>
        /// Gets the barcode flag.
        /// </summary>
        /// <param name="protocolId">The protocol identifier.</param>
        /// <returns></returns>
        public int GetBarcodeFlag(string protocolId)
        {
            try
            {
                string strQuery = "SELECT Get_Barcode_flage('" + protocolId + "')FROM dual";
                WorkFlow objWF = new WorkFlow();
                return Convert.ToInt32(objWF.ExecuteScaler(strQuery));
            }
            catch (Exception ex)
            {
                return 0;
            }
        }

        #endregion "Generate Protocol Key"

        /// <summary>
        /// Gets the quotation details.
        /// </summary>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="CheargableWt">The cheargable wt.</param>
        /// <param name="ShipperPK">The shipper pk.</param>
        /// <returns></returns>
        public object GetQuotationDetails(string POLPK, string PODPK, double CheargableWt, string ShipperPK)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow Objwk = new WorkFlow();
            try
            {
                sb.Append("  SELECT QAT.QUOTATION_AIR_PK, QAT.QUOTATION_REF_NO");
                sb.Append("  FROM QUOTATION_AIR_TBL       QAT,");
                sb.Append("       QUOT_GEN_TRN_AIR_TBL    QTA,");
                sb.Append("       PORT_MST_TBL            POL,");
                sb.Append("       PORT_MST_TBL            POD,");
                sb.Append("       CUSTOMER_MST_TBL        CMT,");
                sb.Append("       QUOTE_AIR_BREAKPOINTS   QBP,");
                sb.Append("       AIRFREIGHT_SLABS_TBL    AST,");
                sb.Append("       COMMODITY_GROUP_MST_TBL COM");
                sb.Append("  WHERE QAT.QUOTATION_AIR_PK = QTA.QUOT_GEN_AIR_FK");
                sb.Append("   AND QAT.STATUS IN (2, 4)");
                sb.Append("   AND QTA.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                sb.Append("   AND QTA.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                sb.Append("   AND COM.COMMODITY_GROUP_PK = QAT.COMMODITY_GROUP_MST_FK");
                sb.Append("   AND QAT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND QBP.AIRFREIGHT_SLABS_TBL_FK = AST.AIRFREIGHT_SLABS_TBL_PK");
                sb.Append("   AND TO_DATE(SYSDATE, DATEFORMAT) >=");
                sb.Append("       TO_DATE(QAT.QUOTATION_DATE, DATEFORMAT)");
                sb.Append("   AND TO_DATE(SYSDATE, DATEFORMAT) <=");
                sb.Append("       TO_DATE(NVL2(QAT.VALID_FOR,");
                sb.Append("                    QAT.QUOTATION_DATE + QAT.VALID_FOR,");
                sb.Append("                    TO_DATE('1/1/9999', 'DD/MM/YYYY')),");
                sb.Append("               DATEFORMAT)");
                sb.Append("   AND QTA.PORT_MST_POL_FK = " + POLPK);
                sb.Append("   AND QTA.PORT_MST_POD_FK = " + PODPK);
                sb.Append("   AND QAT.CUSTOMER_MST_FK = " + ShipperPK);
                sb.Append("   AND AST.AIRFREIGHT_SLABS_TBL_PK =");
                sb.Append("       (SELECT AIST.AIRFREIGHT_SLABS_TBL_PK");
                sb.Append("          FROM AIRFREIGHT_SLABS_TBL AIST");
                sb.Append("         WHERE BREAKPOINT_RANGE =");
                sb.Append("               (SELECT MAX(BREAKPOINT_RANGE)");
                sb.Append("                  FROM AIRFREIGHT_SLABS_TBL");
                sb.Append("                 WHERE BREAKPOINT_RANGE <= " + CheargableWt + "))");
                sb.Append(" ORDER BY QUOTATION_AIR_PK DESC");

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

        /// <summary>
        /// Gets the customer category pk.
        /// </summary>
        /// <param name="Custpk">The custpk.</param>
        /// <param name="Nomination">if set to <c>true</c> [nomination].</param>
        /// <returns></returns>
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

        //Snigdharani - 03/03/2009 - PTS - Ebooking Integration With Current QFOR Build with flag
        /// <summary>
        /// Sends the mail.
        /// </summary>
        /// <param name="MailId">The mail identifier.</param>
        /// <param name="CUSTOMERID">The customerid.</param>
        /// <param name="BkgRefnr">The BKG refnr.</param>
        /// <param name="EBkgRefnr">The e BKG refnr.</param>
        /// <param name="Bstatus">The bstatus.</param>
        /// <returns></returns>
        public object SendMail(string MailId, string CUSTOMERID, string BkgRefnr, string EBkgRefnr, int Bstatus)
        {
            System.Web.Mail.MailMessage objMail = new System.Web.Mail.MailMessage();
            //Dim Mailsend As String = ConfigurationManager.AppSettings("MailServer")
            string EAttach = null;
            string dsMail = null;
            int intCnt = default(int);
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
                    //objMail.Body &= "Your Request for the E-Booking  is Canceled "
                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is Canceled <br><br>");
                }
                else
                {
                    objMail.Subject = "Booking Confirmation";
                    //objMail.Body = "Dear " & CUSTOMERID & "," & vbCrLf & vbCrLf
                    //objMail.Body &= "Your Request for the E-Booking " & EBkgRefnr & " is confirmed. Please refer your Original Booking Int32:" & BkgRefnr
                    strhtml.Append("<html><body>");
                    strhtml.Append("<p><b>Dear " + CUSTOMERID + " <br>");
                    strhtml.Append("Your Request for the E-Booking " + EBkgRefnr + " is confirmed. Please refer your Original Booking Int32:" + BkgRefnr + "<br><br>");
                }
                strhtml.Append("This is an Auto Generated Mail. Please do not reply to this Mail-ID.<br>");
                strhtml.Append("</b></p>");
                strhtml.Append("</body></html>");
                objMail.Body = strhtml.ToString();

                //objMail.Body = "Dear " & CUSTOMERID & "," & vbCrLf & vbCrLf
                //objMail.Body &= "Your Request for the E-Booking " & EBkgRefnr & " is confirmed. Please refer your Original Booking Int32:" & BkgRefnr
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

        /// <summary>
        /// Fetches the ebk.
        /// </summary>
        /// <param name="BookingPK">The booking pk.</param>
        /// <returns></returns>
        public short FetchEBK(short BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            short IS_EBOOKING_CHK = 0;
            try
            {
                sqlStr = "select IS_EBOOKING from booking_air_tbl where booking_air_pk='" + BookingPK + "'";
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

        /// <summary>
        /// Fetches the ebk trans.
        /// </summary>
        /// <param name="BookingPK">The booking pk.</param>
        /// <returns></returns>
        public short FetchEBKTrans(short BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            short IS_EBOOKING_CHK = 0;
            try
            {
                //sqlStr = "SELECT distinct btrn.booking_air_fk  FROM BOOKING_AIR_TBL Bhdr,Booking_Trn_Air Btrn,booking_trn_air_frt_dtls bft "
                //sqlStr &= " where bhdr.booking_air_pk = btrn.booking_air_fk  and bft.booking_trn_air_fk = btrn.booking_air_fk"
                //sqlStr &= " and bhdr.booking_air_pk = " & BookingPK

                sqlStr = "SELECT distinct btrn.booking_trn_air_pk  FROM BOOKING_AIR_TBL Bhdr,Booking_Trn_Air Btrn ";
                sqlStr += " where bhdr.booking_air_pk = btrn.booking_air_fk  ";
                sqlStr += " and bhdr.booking_air_pk = " + BookingPK;

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

        /// <summary>
        /// Fetches the ebk reference.
        /// </summary>
        /// <param name="BookingPK">The booking pk.</param>
        /// <returns></returns>
        public string FetchEBKRef(short BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGRef = null;
            try
            {
                sqlStr = "select BOOKING_REF_NO from booking_air_tbl where booking_air_pk='" + BookingPK + "' and BOOKING_REF_NO like 'BKG%'";
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

        //Added By Anand To Back Update EBooking Table 08-10-2008
        /// <summary>
        /// Backs the update e booking.
        /// </summary>
        /// <param name="BookRef">The book reference.</param>
        /// <param name="Status">The status.</param>
        /// <param name="BkgDate">The BKG date.</param>
        /// <param name="EBookRef">The e book reference.</param>
        /// <returns></returns>
        public ArrayList BackUpdateEBooking(string BookRef, string Status, string BkgDate, string EBookRef)
        {
            OracleCommand updCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int j = default(int);
            string sql = null;
            string sqlstr = null;
            string str = null;
            string SQPRO = null;
            OracleTransaction TRAN = null;
            arrMessage.Clear();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = TRAN;
                objWK.MyCommand.CommandType = CommandType.Text;
                if (Status == "3")
                {
                    sqlstr = "update syn_ebk_m_booking set QBSO_BKG_REF_NR='', STATUS=3, QBSO_BKG_DATE='' where QBSO_BKG_REF_NR='" + BookRef + "'";
                    objWK.MyCommand.CommandText = sqlstr;
                    objWK.MyCommand.ExecuteNonQuery();

                    sqlstr = "";
                    sqlstr = "update syn_ebk_m_booking set QBSO_BKG_REF_NR='', STATUS=3, QBSO_BKG_DATE='' where E_BKG_ORDER_REF_NR='" + BookRef + "'";
                    objWK.MyCommand.CommandText = sqlstr;
                    objWK.MyCommand.ExecuteNonQuery();
                }
                if (Status == "2")
                {
                    sqlstr = "update syn_ebk_m_booking set QBSO_BKG_REF_NR='" + BookRef + "', STATUS=2, QBSO_BKG_DATE='" + BkgDate + "' where E_BKG_ORDER_REF_NR='" + EBookRef + "'";
                    objWK.MyCommand.CommandText = sqlstr;
                    objWK.MyCommand.ExecuteNonQuery();
                }
                arrMessage.Add("All Data Saved Successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        //Added By Anand To Back Update EBooking Table 08-10-2008
        /// <summary>
        /// Reconciles the e customer.
        /// </summary>
        /// <param name="BookRef">The book reference.</param>
        /// <param name="Status">The status.</param>
        /// <param name="BkgDate">The BKG date.</param>
        /// <param name="EBookRef">The e book reference.</param>
        /// <returns></returns>
        public ArrayList ReconcileECustomer(string BookRef, string Status, string BkgDate, string EBookRef)
        {
            OracleCommand updCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            int j = default(int);
            string sql = null;
            string sqlstr = null;
            string str = null;
            string SQPRO = null;
            OracleTransaction TRAN = null;
            arrMessage.Clear();
            try
            {
                objWK.OpenConnection();
                TRAN = objWK.MyConnection.BeginTransaction();
                if (Status != "3")
                {
                    sqlstr = "update syn_ebk_m_booking set QBSO_BKG_REF_NR='', STATUS='" + Status + "', E_BKG_STATUS='" + Status + "',QBSO_BKG_DATE='' where QBSO_BKG_REF_NR='" + BookRef + "'";
                }
                else
                {
                    sqlstr = "update syn_ebk_m_booking set QBSO_BKG_REF_NR='" + BookRef + "', STATUS='" + Status + "', E_BKG_STATUS='" + Status + "',QBSO_BKG_DATE='" + BkgDate + "' where E_BKG_ORDER_REF_NR='" + EBookRef + "'";
                }
                objWK.ExecuteCommands(sqlstr);
                arrMessage.Add("All Data Saved Successfully");
                TRAN.Commit();
                return arrMessage;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
            }
            finally
            {
                objWK.MyConnection.Close();
            }
        }

        /// <summary>
        /// Fetches the BKG date.
        /// </summary>
        /// <param name="BOOKING_REF_NO">The bookin g_ re f_ no.</param>
        /// <returns></returns>
        public string FetchBkgDate(string BOOKING_REF_NO)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGDate = null;
            try
            {
                sqlStr = "select BOOKING_DATE from booking_air_tbl where BOOKING_REF_NO='" + BOOKING_REF_NO + "' ";
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

        /// <summary>
        /// Fetches the ebk ref1.
        /// </summary>
        /// <param name="BookingPK">The booking pk.</param>
        /// <returns></returns>
        public string FetchEBKRef1(short BookingPK)
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            string BOOKINGRef = null;
            try
            {
                sqlStr = "select BOOKING_REF_NO from booking_air_tbl where booking_air_pk='" + BookingPK + "' ";
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

        /// <summary>
        /// Saves the booking c dimension.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="SelectCommand">The select command.</param>
        /// <param name="IsUpdate">if set to <c>true</c> [is update].</param>
        /// <param name="Measure">The measure.</param>
        /// <param name="Wt">The wt.</param>
        /// <param name="Divfac">The divfac.</param>
        /// <returns></returns>
        public ArrayList SaveBookingCDimension(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate, string Measure, string Wt, string Divfac)
        {
            int nRowCnt = default(int);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            try
            {
                if (!IsUpdate)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with7 = SelectCommand;
                        _with7.CommandType = CommandType.StoredProcedure;
                        _with7.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.BOOKING_AIR_CARGO_CALC_INS";
                        SelectCommand.Parameters.Clear();

                        //Booking AIR Fk
                        _with7.Parameters.Add("BOOKING_AIR_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with7.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with7.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with7.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with7.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with7.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with7.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with7.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with7.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with7.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;

                        //Manoharan 04Jan07: to save Measurement, weight and Division factor
                        _with7.Parameters.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                        _with7.Parameters.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(Divfac) ? "0" : Divfac)).Direction = ParameterDirection.Input;

                        //Return value of the proc.
                        _with7.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
                        _with7.ExecuteNonQuery();
                    }
                    arrMessage.Add("All data saved successfully");
                    return arrMessage;
                }
                else
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblCDimension"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with8 = SelectCommand;
                        _with8.CommandType = CommandType.StoredProcedure;
                        _with8.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.BOOKING_AIR_CARGO_CALC_UPD";
                        SelectCommand.Parameters.Clear();

                        //BOOKING_AIR_CARGO_CALC_PK

                        _with8.Parameters.Add("BOOKING_AIR_CARGO_CALC_PK_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["BOOKING_AIR_CARGO_CALC_PK"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["BOOKING_AIR_CARGO_CALC_PK_IN"].SourceVersion = DataRowVersion.Current;
                        //Booking AIR Fk
                        _with8.Parameters.Add("BOOKING_AIR_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with8.Parameters.Add("CARGO_NOP_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_NOP"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_NOP_IN"].SourceVersion = DataRowVersion.Current;

                        _with8.Parameters.Add("CARGO_LENGTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_LENGTH"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_LENGTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with8.Parameters.Add("CARGO_WIDTH_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_WIDTH"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_WIDTH_IN"].SourceVersion = DataRowVersion.Current;

                        _with8.Parameters.Add("CARGO_HEIGHT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_HEIGHT"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_HEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                        _with8.Parameters.Add("CARGO_CUBE_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_CUBE"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_CUBE_IN"].SourceVersion = DataRowVersion.Current;

                        _with8.Parameters.Add("CARGO_VOLUME_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_VOLUME_WT"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_VOLUME_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with8.Parameters.Add("CARGO_ACTUAL_WT_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_ACTUAL_WT"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_ACTUAL_WT_IN"].SourceVersion = DataRowVersion.Current;

                        _with8.Parameters.Add("CARGO_DENSITY_IN", dsMain.Tables["tblCDimension"].Rows[nRowCnt]["CARGO_DENSITY"]).Direction = ParameterDirection.Input;
                        _with8.Parameters["CARGO_DENSITY_IN"].SourceVersion = DataRowVersion.Current;

                        //Manoharan 04Jan07: to save Measurement, weight and Division factor
                        _with8.Parameters.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                        _with8.Parameters.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                        _with8.Parameters.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(Divfac) ? "0" : Divfac)).Direction = ParameterDirection.Input;

                        //Return value of the proc.
                        _with8.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //Execute the command
                        _with8.ExecuteNonQuery();
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

        //jobcard save

        /// <summary>
        /// Saves the booking o freight.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <param name="SelectCommand">The select command.</param>
        /// <param name="IsUpdate">if set to <c>true</c> [is update].</param>
        /// <returns></returns>
        public ArrayList SaveBookingOFreight(DataSet dsMain, long PkValue, OracleCommand SelectCommand, bool IsUpdate)
        {
            int nRowCnt = default(int);
            WorkFlow objWK = new WorkFlow();
            arrMessage.Clear();
            try
            {
                if (!IsUpdate)
                {
                    for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                    {
                        var _with12 = SelectCommand;
                        _with12.CommandType = CommandType.StoredProcedure;
                        _with12.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.BOOKING_TRN_AIR_OTH_CHRG_INS";
                        SelectCommand.Parameters.Clear();

                        //Booking AIR Fk
                        _with12.Parameters.Add("BOOKING_TRN_AIR_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with12.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("CHARGE_BASIS_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        _with12.Parameters.Add("BASIS_RATE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["BASIS_RATE"]).Direction = ParameterDirection.Input;
                        _with12.Parameters["BASIS_RATE_IN"].SourceVersion = DataRowVersion.Current;

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
                        _with13.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.BOOKING_TRN_AIR_OTH_CHRG_UPD";
                        SelectCommand.Parameters.Clear();

                        //Booking AIR Fk
                        _with13.Parameters.Add("BOOKING_TRN_AIR_FK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;

                        _with13.Parameters.Add("FREIGHT_ELEMENT_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["FREIGHT_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("CURRENCY_MST_FK_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("CHARGE_BASIS_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["CHARGE_BASIS"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["CHARGE_BASIS_IN"].SourceVersion = DataRowVersion.Current;

                        _with13.Parameters.Add("BASIS_RATE_IN", dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["BASIS_RATE"]).Direction = ParameterDirection.Input;
                        _with13.Parameters["BASIS_RATE_IN"].SourceVersion = DataRowVersion.Current;

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

        /// <summary>
        /// Fetches the eb FRT.
        /// </summary>
        /// <param name="BkgPk">The BKG pk.</param>
        /// <returns></returns>
        public short FetchEBFrt(long BkgPk)
        {
            try
            {
                string sql = "";
                string res = "";
                short check = 0;
                WorkFlow objWK = new WorkFlow();

                sql = "select BOOKING_TRN_AIR_FK from BOOKING_TRN_AIR_FRT_DTLS where BOOKING_TRN_AIR_FK='" + BkgPk + "'";
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

        /// <summary>
        /// Updates up stream.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="SelectCommand">The select command.</param>
        /// <param name="IsUpdate">if set to <c>true</c> [is update].</param>
        /// <param name="strTranType">Type of the string tran.</param>
        /// <param name="strContractRefNo">The string contract reference no.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        public object UpdateUpStream(DataSet dsMain, OracleCommand SelectCommand, bool IsUpdate, string strTranType, string strContractRefNo, long PkValue)
        {
            WorkFlow objWK = new WorkFlow();
            string strValueArgument = null;
            arrMessage.Clear();
            try
            {
                var _with16 = SelectCommand;
                _with16.CommandType = CommandType.StoredProcedure;
                _with16.CommandText = objWK.MyUserName + ".BOOKING_AIR_PKG.BOOKING_AIR_UPDATE_UPSTREAM";
                SelectCommand.Parameters.Clear();

                _with16.Parameters.Add("BOOKING_AIR_PK_IN", Convert.ToInt64(PkValue)).Direction = ParameterDirection.Input;
                _with16.Parameters.Add("TRANS_REFERED_FROM_IN", Convert.ToInt64(strTranType)).Direction = ParameterDirection.Input;
                _with16.Parameters.Add("TRANS_REF_NO_IN", Convert.ToString(strContractRefNo)).Direction = ParameterDirection.Input;

                _with16.Parameters.Add("ISUPDATE", IsUpdate.ToString()).Direction = ParameterDirection.Input;

                _with16.Parameters.Add("BOOKING_STATUS_IN", Convert.ToInt64(dsMain.Tables["tblMaster"].Rows[0]["STATUS"])).Direction = ParameterDirection.Input;

                _with16.Parameters.Add("CHARGEABLE_WEIGHT_IN", Convert.ToDouble(dsMain.Tables["tblMaster"].Rows[0]["CHARGEABLE_WEIGHT"])).Direction = ParameterDirection.Input;

                //Return value of the proc.
                _with16.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 150, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //Execute the command
                _with16.ExecuteNonQuery();
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

        /// <summary>
        /// Gets the data view.
        /// </summary>
        /// <param name="dtFreight">The dt freight.</param>
        /// <param name="strContractRefNo">The string contract reference no.</param>
        /// <param name="strValueArgument">The string value argument.</param>
        /// <returns></returns>
        private DataView getDataView(DataTable dtFreight, string strContractRefNo, string strValueArgument)
        {
            try
            {
                DataTable dstemp = new DataTable();
                DataRow dr = null;
                int nRowCnt = default(int);
                int nColCnt = default(int);
                dstemp = dtFreight.Clone();
                for (nRowCnt = 0; nRowCnt <= dtFreight.Rows.Count - 1; nRowCnt++)
                {
                    if (strContractRefNo == (string.IsNullOrEmpty(dtFreight.Rows[nRowCnt]["TRANS_REF_NO"].ToString()) ? 0 : dtFreight.Rows[nRowCnt]["TRANS_REF_NO"]))
                    {
                        //And strValueArgument = dtFreight.Rows(nRowCnt).Item("BASISPK")
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

        //This function generates the Booking eferrence no. as per the protocol saved by the user.
        /// <summary>
        /// Generates the booking no.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="nCreatedBy">The n created by.</param>
        /// <param name="objWK">The object wk.</param>
        /// <returns></returns>
        public string GenerateBookingNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            string functionReturnValue = null;
            functionReturnValue = GenerateProtocolKey("BOOKING (AIR)", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, objWK);
            return functionReturnValue;
        }

        /// <summary>
        /// Generates the nomination no.
        /// </summary>
        /// <param name="nLocationId">The n location identifier.</param>
        /// <param name="nEmployeeId">The n employee identifier.</param>
        /// <param name="nCreatedBy">The n created by.</param>
        /// <param name="objWK">The object wk.</param>
        /// <returns></returns>
        public string GenerateNominationNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow objWK)
        {
            return GenerateProtocolKey("NOMINATION AIR", nLocationId, nEmployeeId, DateTime.Now, "", "", "", nCreatedBy, objWK);
        }

        /// <summary>
        /// Checks the active job card.
        /// </summary>
        /// <param name="strABEPk">The string abe pk.</param>
        /// <returns></returns>
        public bool CheckActiveJobCard(string strABEPk)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            short intCnt = 0;
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;
            //strBuilder.Append("SELECT JCAET.JOB_CARD_AIR_EXP_PK FROM JOB_CARD_AIR_EXP_TBL JCAET ")
            //strBuilder.Append("WHERE JCAET.JOB_CARD_STATUS=1 ")
            //strBuilder.Append("AND JCAET.BOOKING_AIR_FK=" & strABEPk)
            strBuilder.Append(" UPDATE JOB_CARD_AIR_EXP_TBL J ");
            strBuilder.Append(" SET J.JOB_CARD_STATUS = 2, J.JOB_CARD_CLOSED_ON = SYSDATE ");
            strBuilder.Append(" WHERE J.BOOKING_AIR_FK = " + strABEPk);

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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Save"

        #region "Check Customer Credit Status" 'Returns True if credit balance exist

        /// <summary>
        /// Funs the check customer credit.
        /// </summary>
        /// <param name="dsMain">The ds main.</param>
        /// <param name="lngCustomerPk">The LNG customer pk.</param>
        /// <param name="CreditLimit">The credit limit.</param>
        /// <returns></returns>
        public bool funCheckCustCredit(DataSet dsMain, long lngCustomerPk, string CreditLimit)
        {
            int nRowCnt = default(int);
            double dblBookingAmt = 0;
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            DataTable dt = new DataTable();
            WorkFlow objWF = new WorkFlow();
            string strReturn = null;

            try
            {
                strBuilder.Append("SELECT CMT.CREDIT_LIMIT, ");
                strBuilder.Append("(CMT.CREDIT_LIMIT - NVL(CMT.CREDIT_LIMIT_USED,0)) AS CREDIT_BALANCE ");
                strBuilder.Append("FROM CUSTOMER_MST_TBL CMT ");
                strBuilder.Append("WHERE ");
                strBuilder.Append("CMT.CUSTOMER_MST_PK= " + lngCustomerPk);

                dt = objWF.GetDataTable(strBuilder.ToString());
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

                for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblTransaction"].Rows.Count - 1; nRowCnt++)
                {
                    dblBookingAmt = dblBookingAmt + Convert.ToInt32(dsMain.Tables["tblTransaction"].Rows[nRowCnt]["ALL_IN_TARIFF"]);
                }

                for (nRowCnt = 0; nRowCnt <= dsMain.Tables["tblOtherFreight"].Rows.Count - 1; nRowCnt++)
                {
                    if (!string.IsNullOrEmpty(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"].ToString()) & (dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"] != null))
                    {
                        dblBookingAmt = dblBookingAmt + Convert.ToInt32(dsMain.Tables["tblOtherFreight"].Rows[nRowCnt]["AMOUNT"]);
                    }
                }
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
                else if ((dt.Rows[0][1].ToString()) == "0")
                {
                    return true;
                }
                else
                {
                    if (((Convert.ToDouble(dt.Rows[0][1]) - dblBookingAmt) >= 0))
                    {
                        CreditLimit = Convert.ToString(dblBookingAmt);
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

        #endregion "Check Customer Credit Status" 'Returns True if credit balance exist

        #region "Retrive Address for Quotation"

        /// <summary>
        /// Funs the r address air.
        /// </summary>
        /// <param name="strQRefNo">The string q reference no.</param>
        /// <returns></returns>
        public object funRAddressAir(string strQRefNo)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            //SELECT ADDRESS FROM THE QUOTATION TABLE
            strSql = "SELECT " + "QST.COL_PLACE_MST_FK, PMTC.PLACE_CODE, QST.COL_ADDRESS, " + "QST.DEL_PLACE_MST_FK, PMTD.PLACE_CODE, QST.DEL_ADDRESS " + "FROM QUOTATION_AIR_TBL QST, PLACE_MST_TBL PMTC,PLACE_MST_TBL PMTD " + "WHERE " + "QST.COL_PLACE_MST_FK = PMTC.PLACE_PK " + "AND QST.DEL_PLACE_MST_FK=PMTD.PLACE_PK " + "AND QST.QUOTATION_REF_NO='" + strQRefNo + "'";
            try
            {
                dt = objwf.GetDataTable(strSql);
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

        #endregion "Retrive Address for Quotation"

        #region "Enhance Search Functions"

        /// <summary>
        /// Fetches the booking address air.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchBookingAddressAir(string strCond)
        {
            string strSql = "";
            WorkFlow objwf = new WorkFlow();
            DataTable dt = null;
            string strAddress = "";
            int j = default(int);
            //SELECT ADDRESS FROM THE QUOTATION TABLE
            strSql = "SELECT " + "QST.COL_PLACE_MST_FK, PMTC.PLACE_CODE, QST.COL_ADDRESS, " + "QST.DEL_PLACE_MST_FK, PMTD.PLACE_CODE, QST.DEL_ADDRESS " + "FROM QUOTATION_AIR_TBL QST, PLACE_MST_TBL PMTC,PLACE_MST_TBL PMTD " + "WHERE " + "QST.COL_PLACE_MST_FK = PMTC.PLACE_PK " + "AND QST.DEL_PLACE_MST_FK=PMTD.PLACE_PK " + "AND QST.QUOTATION_REF_NO='" + strCond + "'";
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches for customs s code.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForCustomsSCode(string strCond)
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
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_CUSTOMS_STATUS_CODE";
                var _with17 = SCM.Parameters;
                _with17.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with17.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                //.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input
                _with17.Add("BUSINESS_TYPE_IN", strBusinessType).Direction = ParameterDirection.Input;
                _with17.Add("RETURN_VALUE", OracleDbType.Varchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        /// <summary>
        /// Fetches the customer category address.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustomerCategoryAddress(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string strAddressType = "";
            int Vendor = 0;
            string strLOC_MST_IN = "";
            string strReq = null;
            string FrmFlg = "";
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                businessType = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strAddressType = Convert.ToString(arr.GetValue(5));
            if (arr.Length > 6)
                Vendor = Convert.ToInt32(arr.GetValue(6));
            if (arr.Length > 7)
                FrmFlg = Convert.ToString(arr.GetValue(7));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GETCUSTOMER_CATEGORY_ADDRESS";
                var _with18 = SCM.Parameters;
                _with18.Add("CATEGORY_IN", ifDBNull(strCATEGORY_IN)).Direction = ParameterDirection.Input;
                _with18.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with18.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with18.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with18.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with18.Add("ADDRESS_TYPE_IN", strAddressType).Direction = ParameterDirection.Input;
                _with18.Add("FROM_FLG_IN", ifDBNull(FrmFlg)).Direction = ParameterDirection.Input;
                _with18.Add("VENDOR_IN", Vendor).Direction = ParameterDirection.Input;
                _with18.Add("RETURN_VALUE", OracleDbType.Varchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        /// <summary>
        /// Fetches the customer category days.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchCustomerCategoryDays(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strCATEGORY_IN = "";
            string businessType = "";
            string strAddressType = "";

            string strLOC_MST_IN = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                strCATEGORY_IN = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                businessType = Convert.ToString(arr.GetValue(4));
            if (arr.Length > 5)
                strAddressType = Convert.ToString(arr.GetValue(5));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GETCUSTOMER_CATEGORY_DAYS";
                var _with19 = SCM.Parameters;
                _with19.Add("CATEGORY_IN", ifDBNull(strCATEGORY_IN)).Direction = ParameterDirection.Input;
                _with19.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with19.Add("LOCATION_MST_FK_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with19.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with19.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with19.Add("ADDRESS_TYPE_IN", strAddressType).Direction = ParameterDirection.Input;
                _with19.Add("RETURN_VALUE", OracleDbType.Varchar2, 2000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        /// <summary>
        /// Fetches the type of for inco terms pay.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchForIncoTermsPayType(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSEARCH_IN = "";
            arr = strCond.Split('~');
            strSEARCH_IN = Convert.ToString(arr.GetValue(1));
            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_INCO_TERMS_PAY_TYPE";
                var _with20 = SCM.Parameters;
                _with20.Add("SEARCH_IN", ifDBNull(strSEARCH_IN)).Direction = ParameterDirection.Input;
                _with20.Add("RETURN_VALUE", OracleDbType.Varchar2, 1400, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        /// <summary>
        /// Fetches the spot rates.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <returns></returns>
        public string FetchSpotRates(string strCond)
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
            string strCWUld = "";
            string strSlabType = "";
            string strBusinessType = "";
            arr = strCond.Split('~');
            strCustomerPk = Convert.ToString(arr.GetValue(1));
            strPol = Convert.ToString(arr.GetValue(2));
            strPod = Convert.ToString(arr.GetValue(3));
            strCommodityPk = Convert.ToString(arr.GetValue(4));
            strSDate = Convert.ToString(arr.GetValue(5));
            strCWUld = Convert.ToString(arr.GetValue(6));
            strSlabType = Convert.ToString(arr.GetValue(7));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_SPOT_RATES";
                var _with21 = SCM.Parameters;
                _with21.Add("CUSTOMER_PK_IN", ifDBNull(strCustomerPk)).Direction = ParameterDirection.Input;
                _with21.Add("POL_IN", ifDBNull(strPol)).Direction = ParameterDirection.Input;
                _with21.Add("POD_IN", ifDBNull(strPod)).Direction = ParameterDirection.Input;
                _with21.Add("COMMODITY_PK_IN", ifDBNull(strCommodityPk)).Direction = ParameterDirection.Input;
                _with21.Add("S_DATE_IN", ifDBNull(strSDate)).Direction = ParameterDirection.Input;
                _with21.Add("CW_ULD_IN", ifDBNull(strCWUld)).Direction = ParameterDirection.Input;
                _with21.Add("SLAB_TYPE_IN", ifDBNull(strSlabType)).Direction = ParameterDirection.Input;
                _with21.Add("RETURN_VALUE", OracleDbType.Varchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
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

        #endregion "Enhance Search Functions"

        #region "Supporting Function "

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
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

        /// <summary>
        /// Removes the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object removeDBNull(object col)
        {
            if (object.ReferenceEquals(col, ""))
            {
                return "";
            }
            return col;
        }

        #endregion "Supporting Function "

        #region "To assign data to Cargo Move Code"

        /// <summary>
        /// Filldds the cm code.
        /// </summary>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        public object FillddCMCode(string PkValue)
        {
            System.Text.StringBuilder strBuilder = new System.Text.StringBuilder();
            DataTable dtCM = null;
            WorkFlow objwf = new WorkFlow();
            strBuilder.Append(" select CARGO_MOVE_FK from Quotation_Air_Tbl where Quotation_Air_pk=" + PkValue);
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

        #endregion "To assign data to Cargo Move Code"

        #region "Check for JobCard existence"

        /// <summary>
        /// Funs the job exist.
        /// </summary>
        /// <param name="strSBEPk">The string sbe pk.</param>
        /// <returns></returns>
        public string FunJobExist(string strSBEPk)
        {
            try
            {
                bool boolFound = false;
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                string strReturn = null;
                strSQL = "SELECT JCAET.JOB_CARD_AIR_EXP_PK FROM JOB_CARD_AIR_EXP_TBL JCAET " + "WHERE JCAET.BOOKING_AIR_FK=" + strSBEPk;
                strReturn = objWF.ExecuteScaler(strSQL);
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

        #endregion "Check for JobCard existence"

        #region "FETCH CREDTDAYS AND CREDIT LIMIT"

        /// <summary>
        /// Fetches the credit.
        /// </summary>
        /// <param name="CreditDays">The credit days.</param>
        /// <param name="CreditLimit">The credit limit.</param>
        /// <param name="Pk">The pk.</param>
        /// <param name="type">The type.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <returns></returns>
        public int fetchCredit(object CreditDays, object CreditLimit, string Pk = "0", int type = 0, int CustPk = 0)
        {
            //type
            //1--SRR
            //2--Quotation
            //3--CustomerContract
            try
            {
                System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                System.Text.StringBuilder strCustQuery = new System.Text.StringBuilder();
                strCustQuery.Append("SELECT C.AIR_CREDIT_DAYS,");
                strCustQuery.Append(" C.AIR_CREDIT_LIMIT");
                strCustQuery.Append("FROM customer_mst_tbl c");
                strCustQuery.Append("WHERE c.customer_mst_pk=" + CustPk);
                OracleDataReader dr = null;
                if (type == 1)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD");
                    strQuery.Append("  FROM SRR_AIR_TBL     c");
                    strQuery.Append("  WHERE c.srr_AIR_pk=" + Pk);
                }
                else if (type == 2)
                {
                    strQuery.Append("SELECT Q.CREDIT_DAYS,");
                    strQuery.Append("     Q.CREDIT_LIMIT ");
                    strQuery.Append("     FROM QUOTATION_AIR_TBL Q");
                    strQuery.Append("     WHERE Q.QUOTATION_AIR_PK=" + Pk);
                    strQuery.Append("  --   AND Q.CUSTOMER_MST_FK=" + CustPk);
                }
                else if (type == 3)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD FROM cont_cust_AIR_tbl C  ");
                    strQuery.Append("WHERE C.CONT_CUST_AIR_PK=" + Pk);
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
                if (!string.IsNullOrEmpty(CreditDays.ToString()))
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

        #endregion "FETCH CREDTDAYS AND CREDIT LIMIT"

        #region "Spetial requirment"

        #region "Spacial Request"

        /// <summary>
        /// Saves the transaction hz SPCL.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with22 = SCM;
                    _with22.CommandType = CommandType.StoredProcedure;
                    _with22.CommandText = UserName + ".BKG_TRN_AIR_HAZ_SPL_REQ_PKG.BKG_TRN_AIR_HAZ_SPL_REQ_INS";
                    var _with23 = _with22.Parameters;
                    _with23.Clear();
                    //BKG_TRN_AIR_FK_IN()
                    _with23.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //OUTER_PACK_TYPE_MST_FK_IN()
                    _with23.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //INNER_PACK_TYPE_MST_FK_IN()
                    _with23.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with23.Add("MIN_TEMP_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with23.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with23.Add("MAX_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with23.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_IN()
                    _with23.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //FLASH_PNT_TEMP_UOM_IN()
                    _with23.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //IMDG_CLASS_CODE_IN()
                    _with23.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //UN_NO_IN()
                    _with23.Add("UN_NO_IN", getDefault(strParam[9], "")).Direction = ParameterDirection.Input;
                    //IMO_SURCHARGE_IN()
                    _with23.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //SURCHARGE_AMT_IN()
                    _with23.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    //IS_MARINE_POLLUTANT_IN()
                    _with23.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //EMS_NUMBER_IN()
                    _with23.Add("EMS_NUMBER_IN", getDefault(strParam[13], "")).Direction = ParameterDirection.Input;

                    //Added by Raghavendra
                    _with23.Add("PROPER_SHIPPING_NAME_IN", getDefault(strParam[14], "")).Direction = ParameterDirection.Input;
                    _with23.Add("PACK_CLASS_TYPE_IN", getDefault(strParam[15], "")).Direction = ParameterDirection.Input;
                    //End by

                    //RETURN_VALUE()
                    _with23.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with22.ExecuteNonQuery();
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

        /// <summary>
        /// Fetches the SPCL req.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReq(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT BKG_TRN_AIR_HAZ_SPL_PK,");
                    strQuery.Append("BOOKING_AIR_FK,");
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
                    strQuery.Append("PACK_CLASS_TYPE FROM BKG_TRN_AIR_HAZ_SPL_REQ Q");
                    //,BKG_TRN_AIR_FCL_LCL QT,BKG_AIR_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_AIR_FK=" + strPK);
                    //strQuery.Append("QT.BKG_AIR_FK=QM.BKG_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_AIR_PK=" & strPK)
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

        /// <summary>
        /// Saves the transaction reefer.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionReefer(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with24 = SCM;
                    _with24.CommandType = CommandType.StoredProcedure;
                    _with24.CommandText = UserName + ".BKG_TRN_AIR_REF_SPL_REQ_PKG.BKG_TRN_AIR_REF_SPL_REQ_INS";
                    var _with25 = _with24.Parameters;
                    _with25.Clear();
                    //BOOKING_TRN_AIR_FK_IN()
                    _with25.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with25.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with25.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with25.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //IS_PERSHIABLE_GOODS_IN()
                    _with25.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //Sivachandran 26Jun2008 CR DateTime 04/06/2008 For Reefer special Requirment
                    //MIN_TEMP_IN()
                    _with25.Add("MIN_TEMP_IN", getDefault(strParam[4], "")).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with25.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with25.Add("MAX_TEMP_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with25.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? 0 : Convert.ToInt32(strParam[7])), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with25.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with25.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //REF_VENTILATION_IN()
                    _with25.Add("REF_VENTILATION_IN", getDefault(strParam[10], "")).Direction = ParameterDirection.Input;

                    //sivachandran 26Jun2008 CR DateTime 04/06/2008 For Reefer special Requirment
                    _with25.Add("HAULAGE_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("GENSET_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("CO2_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("O2_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("REQ_SET_TEMP_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("REQ_SET_TEMP_UOM_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("AIR_VENTILATION_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("AIR_VENTILATION_UOM_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("DEHUMIDIFIER_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("FLOORDRAINS_IN", "").Direction = ParameterDirection.Input;
                    _with25.Add("DEFROSTING_INTERVAL_IN", "").Direction = ParameterDirection.Input;
                    //End
                    //.Add("REF_VENTILATION_IN", "").Direction = ParameterDirection.Input
                    //RETURN_VALUE()
                    _with25.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with24.ExecuteNonQuery();
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

        /// <summary>
        /// Fetches the SPCL req reefer.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReqReefer(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT BKG_TRN_AIR_REF_SPL_PK,");
                    strQuery.Append("BOOKING_AIR_FK,");
                    strQuery.Append("VENTILATION,");
                    strQuery.Append("AIR_COOL_METHOD,");
                    strQuery.Append("HUMIDITY_FACTOR,");
                    strQuery.Append("IS_PERISHABLE_GOODS,");
                    //'sivachandran 26Jun2008 CR DateTime 04/06/2008 For Reefer special Requirment
                    strQuery.Append("MIN_TEMP,");
                    strQuery.Append("MIN_TEMP_UOM,");
                    strQuery.Append("MAX_TEMP,");
                    strQuery.Append("MAX_TEMP_UOM,");
                    strQuery.Append("PACK_TYPE_MST_FK,");
                    strQuery.Append("Q.PACK_COUNT,");
                    strQuery.Append("Q.REF_VENTILATION ");
                    strQuery.Append("FROM BKG_TRN_AIR_REF_SPL_REQ Q");
                    //,BKG_TRN_AIR_FCL_LCL QT,BKG_AIR_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_AIR_FK=" + strPK);
                    //strQuery.Append("BKG_AIR_TBL QM," & vbCrLf)
                    //strQuery.Append("BKG_TRN_AIR_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.BKG_TRN_AIR_FK=QT.BKG_TRN_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.BKG_AIR_FK=QM.BKG_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_AIR_PK=" & strPK)
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

        /// <summary>
        /// Fetches the SPCL req odc.
        /// </summary>
        /// <param name="strPK">The string pk.</param>
        /// <returns></returns>
        public DataTable fetchSpclReqODC(string strPK)
        {
            try
            {
                if (!string.IsNullOrEmpty(strPK))
                {
                    System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
                    strQuery.Append("SELECT ");
                    strQuery.Append("BKG_TRN_AIR_ODC_SPL_PK,");
                    strQuery.Append("BOOKING_AIR_FK,");
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
                    strQuery.Append("APPR_REQ ");
                    //Snigdharani - 03/03/2009 - EBooking Integration
                    //strQuery.Append("NO_OF_SLOTS " & vbCrLf)
                    strQuery.Append("FROM BKG_TRN_AIR_ODC_SPL_REQ Q");
                    //,BKG_TRN_AIR_FCL_LCL QT,BKG_AIR_TBL QM" & vbCrLf)
                    strQuery.Append("WHERE ");
                    strQuery.Append("Q.BOOKING_AIR_FK=" + strPK);
                    //strQuery.Append("BKG_AIR_TBL QM," & vbCrLf)
                    //strQuery.Append("BKG_TRN_AIR_FCL_LCL QT" & vbCrLf)
                    //strQuery.Append("WHERE " & vbCrLf)
                    //strQuery.Append("Q.BKG_TRN_AIR_FK=QT.BKG_TRN_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.BKG_AIR_FK=QM.BKG_AIR_PK" & vbCrLf)
                    //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                    //strQuery.Append("AND QM.BKG_AIR_PK=" & strPK)
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

        /// <summary>
        /// Saves the transaction odc.
        /// </summary>
        /// <param name="SCM">The SCM.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="strSpclRequest">The string SPCL request.</param>
        /// <param name="PkValue">The pk value.</param>
        /// <returns></returns>
        private ArrayList SaveTransactionODC(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with26 = SCM;
                    _with26.CommandType = CommandType.StoredProcedure;
                    _with26.CommandText = UserName + ".BKG_TRN_AIR_ODC_SPL_REQ_PKG.BKG_TRN_AIR_ODC_SPL_REQ_INS";
                    var _with27 = _with26.Parameters;
                    _with27.Clear();
                    //BKG_TRN_AIR_FK_IN()
                    _with27.Add("BOOKING_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with27.Add("LENGTH_IN", getDefault(strParam[0], "")).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with27.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0]) ? 0 : Convert.ToInt32(strParam[1])), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with27.Add("HEIGHT_IN", getDefault(strParam[2], "")).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with27.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? 0 : Convert.ToInt32(strParam[3])), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with27.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with27.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? 0 : Convert.ToInt32(strParam[5])), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with27.Add("WEIGHT_IN", getDefault(strParam[6], "")).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with27.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with27.Add("VOLUME_IN", getDefault(strParam[8], "")).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with27.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with27.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with27.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], "")).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with27.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    //.Add("NO_OF_SLOTS", getDefault(strParam(13), 0)).Direction = ParameterDirection.Input
                    //RETURN_VALUE()
                    _with27.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with26.ExecuteNonQuery();
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

        #region "Fetch Grid In AirwayBill"

        /// <summary>
        /// Fetch_airbill_grids the specified airway pk.
        /// </summary>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet fetch_airbill_grid(string AirwayPk, int CurrentPage = 0, int TotalPage = 0, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            int last = default(int);
            int start = default(int);
            string strSQL = null;
            int TotalRecords = default(int);

            try
            {
                strQuery.Append("select rownum as Sl_no, ");
                strQuery.Append(" abt.airway_bill_mst_fk,");
                strQuery.Append(" abt.airway_bill_no,");
                strQuery.Append("'FALSE' SEL ");
                strQuery.Append("  from airway_bill_trn abt, airway_bill_mst_tbl am");
                strQuery.Append(" where abt.airway_bill_mst_fk = " + AirwayPk);
                //add by latha
                strQuery.Append(" and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("  and abt.reference_no is Null");
                strQuery.Append("");

                strSQL = strQuery.ToString();
                TotalRecords = Convert.ToInt32(ObjWk.ExecuteScaler(strSQL));

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

        #endregion "Fetch Grid In AirwayBill"

        #region "Fetch Airway Id and Name"

        /// <summary>
        /// Fetch_s the airway.
        /// </summary>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <returns></returns>
        public DataSet Fetch_Airway(string AirwayPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select amt.airline_id, amt.airline_name");
                strQuery.Append("  from AIRLINE_MST_TBL amt");
                strQuery.Append(" where AMT.AIRLINE_MST_PK =" + AirwayPk);
                strQuery.Append("");

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

        #endregion "Fetch Airway Id and Name"

        #region "Update Airway Bill Trn"

        /// <summary>
        /// Update_s the airway_ bill_ TRN.
        /// </summary>
        /// <param name="BkgNo">The BKG no.</param>
        /// <param name="AirwayBillNo">The airway bill no.</param>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <param name="cmd">The command.</param>
        /// <param name="AirRefNr">The air reference nr.</param>
        /// <returns></returns>
        public ArrayList Update_Airway_Bill_Trn(string BkgNo, string AirwayBillNo, string AirwayPk, OracleCommand cmd, string AirRefNr = "")
        {
            WorkFlow objWK = new WorkFlow();
            short exe = default(short);
            System.Text.StringBuilder strQuery = null;
            arrMessage.Clear();

            try
            {
                cmd.CommandType = CommandType.Text;
                //add by latha for updating  the mst table for cancelled records

                cmd.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_mst_tbl AMT ");
                strQuery.Append("   set AMT.total_nos_used = (AMT.total_nos_used - 1),  ");
                strQuery.Append("   AMT.total_nos_cancelled = (AMT.total_nos_cancelled + 1)  ");
                strQuery.Append(" Where AMT.Airway_Bill_Mst_Pk in (select abt.airway_bill_mst_fk  from airway_bill_trn ABT Where ABT.REFERENCE_NO = '" + BkgNo + "') ");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_mst_tbl AMT ");
                strQuery.Append("   set AMT.total_nos_used = (AMT.total_nos_used - 1),  ");
                strQuery.Append("   AMT.total_nos_cancelled = (AMT.total_nos_cancelled + 1)  ");
                strQuery.Append(" Where AMT.Airway_Bill_Mst_Pk in (select abt.airway_bill_mst_fk  from airway_bill_trn ABT Where ABT.REFERENCE_NO = '" + AirRefNr + "') ");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + BkgNo + "'");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                cmd.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + AirRefNr + "'");
                strQuery.Append("");
                cmd.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                if (AirwayPk != null)
                {
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_trn ABT ");
                    strQuery.Append("   set ABT.Status       = 3, ");
                    strQuery.Append("       ABT.Used_At      = 3, ");
                    strQuery.Append("       ABT.Reference_No = '" + BkgNo + "'");
                    strQuery.Append(" Where ABT.Airway_Bill_Mst_Fk = " + AirwayPk);
                    strQuery.Append("   And ABT.AIRWAY_BILL_NO = " + AirwayBillNo);
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();

                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());
                    //add by latha for updating the totalno used in the mst table
                    cmd.Parameters.Clear();

                    strQuery = new System.Text.StringBuilder();
                    strQuery.Append(" update airway_bill_mst_tbl AMT ");
                    strQuery.Append(" set AMT.total_nos_used = ( select count(*) + 1 from airway_bill_trn trn ");
                    strQuery.Append(" where(trn.reference_no Is Not null) and trn.airway_bill_mst_fk= " + AirwayPk);
                    strQuery.Append(" ) Where AMT.Airway_Bill_Mst_Pk = " + AirwayPk);
                    strQuery.Append("");
                    cmd.CommandText = strQuery.ToString();

                    exe = Convert.ToInt16(cmd.ExecuteNonQuery());
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

        #endregion "Update Airway Bill Trn"

        #region "fetch MaWB Nr"

        /// <summary>
        /// Fetch_s the m awb nr.
        /// </summary>
        /// <param name="BkgNr">The BKG nr.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet Fetch_MAwbNr(string BkgNr, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("   select abn.airway_bill_no");
                strQuery.Append("   from airway_bill_trn abn, airway_bill_mst_tbl am");
                strQuery.Append("   where abn.reference_no = '" + BkgNr + "'");
                //add by latha
                strQuery.Append(" and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("");

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

        #endregion "fetch MaWB Nr"

        #region "getDivFacMW"

        /// <summary>
        /// Gets the div fac mw.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <returns></returns>
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
                        strQuery.Append("from Booking_Air_Cargo_Calc where ");
                        strQuery.Append("booking_air_fk = " + pk);
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

        #endregion "getDivFacMW"

        #region "Fetch MAwb Nr If Spot Rate Select"

        /// <summary>
        /// Fetch_s the mawb NR_ spot rate.
        /// </summary>
        /// <param name="AirbillRefNr">The airbill reference nr.</param>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet Fetch_MawbNr_SpotRate(string AirbillRefNr, string AirwayPk, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select a.airway_bill_no");
                strQuery.Append("from airway_bill_trn a, airway_bill_mst_tbl am");
                strQuery.Append("where am.airway_bill_mst_pk = a.airway_bill_mst_fk");
                //add by latha
                strQuery.Append("and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("and am.airline_mst_fk=" + AirwayPk);
                strQuery.Append("and a.reference_no='" + AirbillRefNr + "'");
                strQuery.Append("");

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

        #endregion "Fetch MAwb Nr If Spot Rate Select"

        #region "Fetch MAWB Nr in Booking"

        /// <summary>
        /// Fetch_s the mawb nr.
        /// </summary>
        /// <param name="AirbillRefNr">The airbill reference nr.</param>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet Fetch_MawbNr(string AirbillRefNr, string AirwayPk, long usrLocFK = 0)
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
                strQuery.Append("and (a.reference_no='" + AirbillRefNr + "'");
                strQuery.Append(" or a.reference_no='" + AirbillRefNr.Replace("BKG", "JCG") + "')");

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

        #endregion "Fetch MAWB Nr in Booking"

        #region "Fetch Aiwaybill MST Fk "

        /// <summary>
        /// Fecth_s the airway_mst_ fk.
        /// </summary>
        /// <param name="ref_nr">The ref_nr.</param>
        /// <param name="Air_Pk">The air_ pk.</param>
        /// <param name="usrLocFK">The usr loc fk.</param>
        /// <returns></returns>
        public DataSet fecth_Airway_mst_Fk(string ref_nr, string Air_Pk, long usrLocFK = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select a.airway_bill_mst_fk");
                strQuery.Append("from airway_bill_trn a, airway_bill_mst_tbl am");
                strQuery.Append("where am.airway_bill_mst_pk = a.airway_bill_mst_fk");
                strQuery.Append("and am.airline_mst_fk=" + Air_Pk);
                //add by latha to fetch the airway bill according to the location
                strQuery.Append("and am.location_mst_fk=" + usrLocFK);
                strQuery.Append("and a.airway_bill_no=" + ref_nr);
                strQuery.Append("");

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

        #endregion "Fetch Aiwaybill MST Fk "

        #region "Fetch Location of Port "

        /// <summary>
        /// Fetch_s the port_ location_ fk.
        /// </summary>
        /// <param name="strPODfk">The string po DFK.</param>
        /// <param name="JCDate">The jc date.</param>
        /// <returns></returns>
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
                //strQuery.Append("" & vbCrLf)
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
                    strQuery.Append("                            TO_DATE('" + JCDate + "', 'DD/MM/YYYY')) ROE_BUY");
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

        #endregion "Fetch Location of Port "

        #region "fetch If confirm is selected "

        /// <summary>
        /// Fetch_jobrefnrs the specified BKGNR.
        /// </summary>
        /// <param name="bkgnr">The BKGNR.</param>
        /// <returns></returns>
        public DataSet fetch_jobrefnr(long bkgnr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("select JCAET.JOBCARD_REF_NO from JOB_CARD_AIR_EXP_TBL  JCAET");
                strQuery.Append("where JCAET.BOOKING_AIR_FK=" + bkgnr);
                strQuery.Append("");
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

        #endregion "fetch If confirm is selected "

        #region "fetch Jobcard Against AIrway bill"

        /// <summary>
        /// Update_s the jobcard_ arwaybill.
        /// </summary>
        /// <param name="BkgNo">The BKG no.</param>
        /// <param name="AirwayBillNo">The airway bill no.</param>
        /// <param name="AirwayPk">The airway pk.</param>
        /// <returns></returns>
        public ArrayList Update_Jobcard_Arwaybill(string BkgNo, string AirwayBillNo, string AirwayPk)
        {
            WorkFlow objWK = new WorkFlow();
            short exe = default(short);
            System.Text.StringBuilder strQuery = null;
            OracleCommand cmd = null;
            OracleTransaction Tran = null;

            arrMessage.Clear();
            try
            {
                objWK.OpenConnection();
                Tran = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = Tran;

                var _with28 = objWK.MyCommand;
                _with28.CommandType = CommandType.Text;

                objWK.MyCommand.Parameters.Clear();
                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append(" set ABT.Status = 0, ABT.Used_At = 0, ABT.Reference_No = Null ");
                strQuery.Append(" Where ABT.REFERENCE_NO = '" + BkgNo + "'");
                strQuery.Append("");
                objWK.MyCommand.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                objWK.MyCommand.Parameters.Clear();

                strQuery = new System.Text.StringBuilder();
                strQuery.Append(" update airway_bill_trn ABT ");
                strQuery.Append("   set ABT.Status       = 3, ");
                strQuery.Append("       ABT.Used_At      = 3, ");
                strQuery.Append("       ABT.Reference_No = '" + BkgNo + "'");
                strQuery.Append(" Where ABT.Airway_Bill_Mst_Fk = " + AirwayPk);
                strQuery.Append("   And ABT.AIRWAY_BILL_NO = " + AirwayBillNo);
                strQuery.Append("");
                objWK.MyCommand.CommandText = strQuery.ToString();

                exe = Convert.ToInt16(cmd.ExecuteNonQuery());

                if (exe > 0)
                {
                    Tran.Commit();
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                else
                {
                    Tran.Rollback();
                }
            }
            catch (OracleException oraexp)
            {
                Tran.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return arrMessage;
        }

        #endregion "fetch Jobcard Against AIrway bill"

        #region "Manual fetch"

        /// <summary>
        /// Funs the manual retrive data.
        /// </summary>
        /// <param name="intIsKGS">The int is KGS.</param>
        /// <param name="strPOL">The string pol.</param>
        /// <param name="strPOD">The string pod.</param>
        /// <param name="strChargeWeightNo">The string charge weight no.</param>
        /// <param name="intContainerPk">The int container pk.</param>
        /// <param name="intNoOfContainers">The int no of containers.</param>
        /// <param name="BookingPK">The booking pk.</param>
        /// <param name="SplitCargo">The split cargo.</param>
        /// <returns></returns>
        public DataSet FunManualRetriveData(short intIsKGS = 0, string strPOL = "", string strPOD = "", string strChargeWeightNo = "", int intContainerPk = 0, int intNoOfContainers = 0, int BookingPK = 0, int SplitCargo = 0)
        {
            try
            {
                OracleDataAdapter Da = new OracleDataAdapter();
                DataSet dsGrid = new DataSet();
                WorkFlow objWFT = new WorkFlow();
                strChargeWeightNo = strChargeWeightNo.Replace(",", "");
                objWFT.MyCommand.CommandType = CommandType.StoredProcedure;
                objWFT.MyCommand.Parameters.Clear();
                var _with29 = objWFT.MyCommand.Parameters;
                _with29.Add("IS_KGS_IN", intIsKGS).Direction = ParameterDirection.Input;
                _with29.Add("POL_IN", strPOL).Direction = ParameterDirection.Input;
                _with29.Add("POD_IN", strPOD).Direction = ParameterDirection.Input;
                _with29.Add("CHARGE_WT_NO_IN", (string.IsNullOrEmpty(strChargeWeightNo) ? "0" : strChargeWeightNo)).Direction = ParameterDirection.Input;
                _with29.Add("CONTAINER_PK_IN", intContainerPk).Direction = ParameterDirection.Input;
                //adding by thiyagarajan on 5/12/08
                _with29.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                _with29.Add("BOOKINGPK", BookingPK).Direction = ParameterDirection.Input;
                _with29.Add("SPLITCARGO", SplitCargo).Direction = ParameterDirection.Input;
                _with29.Add("RES_RefCursor_ALL", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with29.Add("RES_RefCursor_FREIGHT", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsGrid = objWFT.GetDataSet("BOOKING_AIR_FETCH_PKG", "FETCH_MANUAL_DATA");
                if (dsGrid.Tables.Count > 1)
                {
                    FetchManualOFreights(dsGrid.Tables[0]);
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
                //'Exception Handling Added by Gangadhar on 16/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the manual o freights.
        /// </summary>
        /// <param name="dtMain">The dt main.</param>
        private void FetchManualOFreights(DataTable dtMain)
        {
            //TRNTYPEPK
            try
            {
                int rowCnt = default(int);
                DataTable dtOFreights = new DataTable();
                for (rowCnt = 0; rowCnt <= dtMain.Rows.Count - 1; rowCnt++)
                {
                    FetchManualothFreights(dtOFreights);
                    if (dtOFreights.Rows.Count > 0)
                    {
                        //dtMain.Rows[rowCnt]["OCHARGES"] = Cls_FlatRateFreights.GetOTHstring(dtOFreights, 0, 1, 2, 3, 4, "-1");
                    }
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

        /// <summary>
        /// Fetches the manualoth freights.
        /// </summary>
        /// <param name="dtOFreights">The dt o freights.</param>
        private void FetchManualothFreights(DataTable dtOFreights)
        {
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
                //adding by thiyagarajan on 5/12/08
                strBuilder1.Append(" AND nvl(FREIGHT_TYPE,0) = 0 ");
                strBuilder1.Append(" and  1 = 2");
                dtOFreights = (new WorkFlow()).GetDataTable(strBuilder1.ToString());
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

        //Fetch Quotation Other Freights

        #endregion "Manual fetch"

        #region "Fetch_PackType_Pk"

        /// <summary>
        /// Fetches the pack identifier.
        /// </summary>
        /// <param name="PackPk">The pack pk.</param>
        /// <returns></returns>
        public OracleDataReader FetchPackId(int PackPk)
        {
            OracleDataReader dread = null;
            string SQL = null;
            WorkFlow objWK = new WorkFlow();
            try
            {
                SQL = "SELECT PACK_TYPE_ID,PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL P WHERE P.PACK_TYPE_MST_PK=" + PackPk;
                dread = objWK.GetDataReader(SQL);
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dread;
        }

        #endregion "Fetch_PackType_Pk"

        #region "Air Booking Reefer Special Requirment Save"

        /// <summary>
        /// Save_s the air booking_ reefer_ SPL_ req.
        /// </summary>
        /// <param name="Haulage">The haulage.</param>
        /// <param name="GenSet">The gen set.</param>
        /// <param name="ReqSetTemp">The req set temporary.</param>
        /// <param name="Ventilation">The ventilation.</param>
        /// <param name="dehumidifier">The dehumidifier.</param>
        /// <param name="Perishable">The perishable.</param>
        /// <param name="FloorDrains">The floor drains.</param>
        /// <param name="CO2">The c o2.</param>
        /// <param name="O2">The o2.</param>
        /// <param name="PackTypePk">The pack type pk.</param>
        /// <param name="PackCount">The pack count.</param>
        /// <param name="ReqSetTempUOM">The req set temporary uom.</param>
        /// <param name="VentilationUOM">The ventilation uom.</param>
        /// <param name="HumidityFact">The humidity fact.</param>
        /// <param name="defrostingUOM">The defrosting uom.</param>
        /// <param name="UserName">Name of the user.</param>
        /// <param name="hdnBkg_Trn_AirPk">The HDN BKG_ TRN_ air pk.</param>
        /// <param name="Form">The form.</param>
        /// <returns></returns>
        public ArrayList Save_AirBooking_Reefer_Spl_Req(int Haulage, int GenSet, int ReqSetTemp, int Ventilation, int dehumidifier, int Perishable, int FloorDrains, string CO2, string O2, string PackTypePk,
        string PackCount, string ReqSetTempUOM, string VentilationUOM, string HumidityFact, string defrostingUOM, string UserName, int hdnBkg_Trn_AirPk, string Form = "")
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction Tran = null;

            arrMessage.Clear();
            try
            {
                objWK.OpenConnection();
                Tran = objWK.MyConnection.BeginTransaction();
                objWK.MyCommand.Transaction = Tran;

                var _with30 = objWK.MyCommand;
                _with30.CommandType = CommandType.StoredProcedure;
                if (Form == "Sea")
                {
                    _with30.CommandText = UserName + ".BKG_TRN_SEA_REF_SPL_REQ_PKG.BKG_TRN_SEA_REF_SPL_REQ_INS";
                }
                else
                {
                    _with30.CommandText = UserName + ".BKG_TRN_AIR_REF_SPL_REQ_PKG.BKG_TRN_AIR_REF_SPL_REQ_INS";
                }

                var _with31 = _with30.Parameters;
                _with31.Clear();
                if (Form == "Sea")
                {
                    _with31.Add("BOOKING_TRN_SEA_FK_IN", hdnBkg_Trn_AirPk).Direction = ParameterDirection.Input;
                }
                else
                {
                    _with31.Add("BOOKING_AIR_FK_IN", hdnBkg_Trn_AirPk).Direction = ParameterDirection.Input;
                }

                _with31.Add("VENTILATION_IN", "").Direction = ParameterDirection.Input;
                _with31.Add("AIR_COOL_METHOD_IN", "").Direction = ParameterDirection.Input;

                _with31.Add("HUMIDITY_FACTOR_IN", (string.IsNullOrEmpty(HumidityFact) ? "" : HumidityFact)).Direction = ParameterDirection.Input;
                _with31.Add("IS_PERISHABLE_GOODS_IN", Perishable).Direction = ParameterDirection.Input;

                _with31.Add("MIN_TEMP_IN", "").Direction = ParameterDirection.Input;
                _with31.Add("MIN_TEMP_UOM_IN", "").Direction = ParameterDirection.Input;
                _with31.Add("MAX_TEMP_IN", "").Direction = ParameterDirection.Input;
                _with31.Add("MAX_TEMP_UOM_IN", "").Direction = ParameterDirection.Input;

                _with31.Add("HAULAGE_IN", Haulage).Direction = ParameterDirection.Input;
                _with31.Add("GENSET_IN", GenSet).Direction = ParameterDirection.Input;
                _with31.Add("CO2_IN", (string.IsNullOrEmpty(CO2) ? "" : CO2)).Direction = ParameterDirection.Input;
                _with31.Add("O2_IN", (string.IsNullOrEmpty(O2) ? "" : O2)).Direction = ParameterDirection.Input;
                _with31.Add("REQ_SET_TEMP_IN", ReqSetTemp).Direction = ParameterDirection.Input;
                _with31.Add("REQ_SET_TEMP_UOM_IN", (string.IsNullOrEmpty(ReqSetTempUOM) ? "" : ReqSetTempUOM)).Direction = ParameterDirection.Input;
                _with31.Add("AIR_VENTILATION_IN", Ventilation).Direction = ParameterDirection.Input;
                _with31.Add("AIR_VENTILATION_UOM_IN", (string.IsNullOrEmpty(VentilationUOM) ? "" : VentilationUOM)).Direction = ParameterDirection.Input;
                _with31.Add("DEHUMIDIFIER_IN", dehumidifier).Direction = ParameterDirection.Input;
                _with31.Add("FLOORDRAINS_IN", FloorDrains).Direction = ParameterDirection.Input;
                _with31.Add("PACK_TYPE_MST_FK_IN", PackTypePk).Direction = ParameterDirection.Input;
                _with31.Add("PACK_COUNT_IN", (string.IsNullOrEmpty(PackCount) ? "" : PackCount)).Direction = ParameterDirection.Input;
                _with31.Add("DEFROSTING_INTERVAL_IN", (string.IsNullOrEmpty(defrostingUOM) ? "" : defrostingUOM)).Direction = ParameterDirection.Input;
                _with31.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                objWK.MyCommand.ExecuteNonQuery();
                Tran.Commit();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                Tran.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        /// <summary>
        /// Fetches the BKG pk.
        /// </summary>
        /// <param name="BookingId">The booking identifier.</param>
        /// <returns></returns>
        public object FetchBkgPk(string BookingId = "0")
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            int BookingPk = default(int);
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

        /// <summary>
        /// Fetches the BKG_ TRN_ air pk.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <param name="Form">The form.</param>
        /// <returns></returns>
        public int FetchBkg_Trn_AirPk(int BookingPk = 0, string Form = "")
        {
            WorkFlow objWF = new WorkFlow();
            string sqlStr = null;
            int Bkg_Trn_Airpk = 0;
            try
            {
                if (Form == "Sea")
                {
                    sqlStr = "SELECT B.BOOKING_TRN_SEA_PK FROM BOOKING_TRN_SEA_FCL_LCL B WHERE B.BOOKING_SEA_FK=" + BookingPk;
                }
                else
                {
                    sqlStr = "select booking_trn_air_pk from booking_trn_air where booking_air_fk=" + BookingPk;
                }
                Bkg_Trn_Airpk = Convert.ToInt32(objWF.ExecuteScaler(sqlStr));
                return Bkg_Trn_Airpk;
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

        /// <summary>
        /// Fetches the data.
        /// </summary>
        /// <param name="BkgTrn_AirPk">The BKG TRN_ air pk.</param>
        /// <param name="Form">The form.</param>
        /// <returns></returns>
        public OracleDataReader FetchData(int BkgTrn_AirPk, string Form = "")
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            OracleDataReader dread = null;
            string TableName = string.Empty;
            if (Form == "Sea")
            {
                TableName = "BKG_TRN_SEA_REF_SPL_REQ";
            }
            else
            {
                TableName = "bkg_trn_air_ref_spl_req";
            }
            try
            {
                strQuery.Append("select PACK_COUNT, HAULAGE,");
                strQuery.Append("GENSET, CO2, O2, REQ_SET_TEMP,");
                strQuery.Append("REQ_SET_TEMP_UOM, FLOORDRAINS,AIR_VENTILATION,");
                strQuery.Append("AIR_VENTILATION_UOM,DEHUMIDIFIER,HUMIDITY_FACTOR,");
                strQuery.Append("IS_PERISHABLE_GOODS,DEFROSTING_INTERVAL ");
                strQuery.Append("FROM " + TableName);
                strQuery.Append(" BTAR WHERE");
                if (Form == "Sea")
                {
                    strQuery.Append(" BTAR.BOOKING_TRN_SEA_FK=" + BkgTrn_AirPk);
                }
                else
                {
                    strQuery.Append(" BTAR.BOOKING_AIR_FK = " + BkgTrn_AirPk);
                }

                dread = objWK.GetDataReader(strQuery.ToString());
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return dread;
        }

        #endregion "Air Booking Reefer Special Requirment Save"

        #region "FETCH QUOTATION PK"

        /// <summary>
        /// Fetch_s the quote_ pk.
        /// </summary>
        /// <param name="ref_nr">The ref_nr.</param>
        /// <returns></returns>
        public string Fetch_Quote_Pk(string ref_nr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append(" select QMAIN.QUOTATION_AIR_PK from QUOTATION_AIR_TBL  QMAIN where QMAIN.QUOTATION_REF_NO ='" + ref_nr + "'");
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

        /// <summary>
        /// Fetch_s the quote_ status.
        /// </summary>
        /// <param name="QuotePK">The quote pk.</param>
        /// <returns></returns>
        public DataSet Fetch_Quote_Status(int QuotePK)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append(" select QMAIN.STATUS,QMAIN.QUOTATION_TYPE from QUOTATION_AIR_TBL  QMAIN where QMAIN.QUOTATION_AIR_PK =" + QuotePK);
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

        /// <summary>
        /// Fetch_s the cust_ cont_ pk.
        /// </summary>
        /// <param name="ref_nr">The ref_nr.</param>
        /// <returns></returns>
        public string Fetch_Cust_Cont_Pk(string ref_nr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            try
            {
                strQuery.Append(" select QMAIN.CONT_CUST_AIR_PK from CONT_CUST_AIR_TBL  QMAIN where QMAIN.CONT_REF_NO ='" + ref_nr + "'");
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

        /// <summary>
        /// Fetch_s the quote_ entry_ from_ ebk.
        /// </summary>
        /// <param name="ref_nr">The ref_nr.</param>
        /// <returns></returns>
        public DataTable Fetch_Quote_Entry_From_EBK(string ref_nr)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();

            try
            {
                strQuery.Append("    SELECT QTRN.QUOT_GEN_AIR_TRN_PK AS TRNTYPEFK, ");
                strQuery.Append("       QMAIN.QUOTATION_REF_NO AS REFNO, ");
                strQuery.Append("       SRAFST.BREAKPOINT_ID AS BASIS, ");
                strQuery.Append("       SRCMT.COMMODITY_ID AS COMMODITY, ");
                strQuery.Append("       SRPL.PORT_MST_PK, ");
                strQuery.Append("       SRPL.PORT_ID AS POL, ");
                strQuery.Append("       SRPD.PORT_MST_PK, ");
                strQuery.Append("       SRPD.PORT_ID AS POD, ");
                strQuery.Append("       QTFT.FREIGHT_ELEMENT_MST_FK, ");
                strQuery.Append("       SRFEMT.FREIGHT_ELEMENT_ID, ");
                strQuery.Append("        'true' AS SEL, ");
                strQuery.Append("       QTFT.CURRENCY_MST_FK, ");
                strQuery.Append("       SRCUMT.CURRENCY_ID, ");
                strQuery.Append("       DECODE(SRFEMT.CHARGE_BASIS, 1, '%', 2, 'Flat', 3, 'Kgs') AS BASISTYPE, ");
                strQuery.Append("       (CASE SRFEMT.CHARGE_BASIS ");
                strQuery.Append("         WHEN 3 THEN ");
                strQuery.Append("          (CASE ");
                strQuery.Append("         WHEN ");
                strQuery.Append("          QTFT.MIN_AMOUNT > 45 * QBP.APPROVED_RATE THEN ");
                strQuery.Append("          QTFT.MIN_AMOUNT ");
                strQuery.Append("         ELSE ");
                strQuery.Append("          QBP.APPROVED_RATE ");
                strQuery.Append("       END) ELSE QBP.APPROVED_RATE END) BASISRATE, ");
                strQuery.Append("       (CASE SRFEMT.CHARGE_BASIS ");
                strQuery.Append("         WHEN 3 THEN ");
                strQuery.Append("          (CASE ");
                strQuery.Append("         WHEN ");
                strQuery.Append("          QTFT.MIN_AMOUNT > 45 * QBP.APPROVED_RATE THEN ");
                strQuery.Append("          QTFT.MIN_AMOUNT ");
                strQuery.Append("         ELSE ");
                strQuery.Append("          45 * QBP.APPROVED_RATE ");
                strQuery.Append("       END) ELSE QBP.APPROVED_RATE END) RATE, ");
                strQuery.Append("       (CASE SRFEMT.CHARGE_BASIS ");
                strQuery.Append("         WHEN 3 THEN ");
                strQuery.Append("          (CASE ");
                strQuery.Append("         WHEN ");
                strQuery.Append("          QTFT.MIN_AMOUNT > 45 * QBP.APPROVED_RATE THEN ");
                strQuery.Append("          QTFT.MIN_AMOUNT ");
                strQuery.Append("         ELSE ");
                strQuery.Append("          45 * QBP.APPROVED_RATE ");
                strQuery.Append("       END) ELSE QBP.APPROVED_RATE END) BKGRATE, ");
                strQuery.Append("       QBP.AIRFREIGHT_SLABS_TBL_FK AS BASISPK, ");
                strQuery.Append("        '1' AS PYMT_TYPE ");
                strQuery.Append("  FROM QUOTATION_AIR_TBL        QMAIN, ");
                strQuery.Append("       QUOT_GEN_TRN_AIR_TBL     QTRN, ");
                strQuery.Append("       QUOT_AIR_TRN_FREIGHT_TBL QTFT, ");
                strQuery.Append("       QUOTE_AIR_BREAKPOINTS    QBP, ");
                strQuery.Append("       AIRLINE_MST_TBL          SRAMT, ");
                strQuery.Append("       COMMODITY_MST_TBL        SRCMT, ");
                strQuery.Append("       PORT_MST_TBL             SRPL, ");
                strQuery.Append("       PORT_MST_TBL             SRPD, ");
                strQuery.Append("       FREIGHT_ELEMENT_MST_TBL  SRFEMT, ");
                strQuery.Append("       CURRENCY_TYPE_MST_TBL    SRCUMT, ");
                strQuery.Append("       AIRFREIGHT_SLABS_TBL     SRAFST ");
                strQuery.Append("   WHERE (1 = 1) ");
                strQuery.Append("   AND QTRN.QUOT_GEN_AIR_FK = QMAIN.QUOTATION_AIR_PK ");
                strQuery.Append("   AND QTFT.QUOT_GEN_AIR_TRN_FK = QTRN.QUOT_GEN_AIR_TRN_PK ");
                strQuery.Append("   AND QBP.QUOT_GEN_AIR_FRT_FK = QTFT.QUOT_GEN_TRN_FREIGHT_PK ");
                strQuery.Append("   AND QTRN.AIRLINE_MST_FK = SRAMT.AIRLINE_MST_PK(+) ");
                strQuery.Append("   AND QTRN.COMMODITY_MST_FK = SRCMT.COMMODITY_MST_PK(+) ");
                strQuery.Append("   AND QTRN.PORT_MST_POL_FK = SRPL.PORT_MST_PK(+) ");
                strQuery.Append("   AND QTRN.PORT_MST_POD_FK = SRPD.PORT_MST_PK(+) ");
                strQuery.Append("   AND QTFT.FREIGHT_ELEMENT_MST_FK =  SRFEMT.FREIGHT_ELEMENT_MST_PK ");
                strQuery.Append("   AND QBP.AIRFREIGHT_SLABS_TBL_FK =  SRAFST.AIRFREIGHT_SLABS_TBL_PK(+) ");
                strQuery.Append("   AND QTFT.CURRENCY_MST_FK = SRCUMT.CURRENCY_MST_PK(+) ");
                strQuery.Append("   AND QMAIN.STATUS IN (2, 4) ");
                strQuery.Append("   AND QTRN.PORT_MST_POL_FK = 1305 ");
                strQuery.Append("   AND QTRN.PORT_MST_POD_FK = 1326 ");
                strQuery.Append("   AND QMAIN.QUOTATION_AIR_PK = 2023 ");

                strQuery.Append("   AND QBP.AIRFREIGHT_SLABS_TBL_FK =");
                strQuery.Append("       (SELECT AST.AIRFREIGHT_SLABS_TBL_PK");
                strQuery.Append("          FROM AIRFREIGHT_SLABS_TBL AST");
                strQuery.Append("         WHERE BREAKPOINT_RANGE =");
                strQuery.Append("               (SELECT MAX(BREAKPOINT_RANGE)");
                strQuery.Append("                  FROM AIRFREIGHT_SLABS_TBL");
                strQuery.Append("                 WHERE BREAKPOINT_RANGE <= 45))");
                strQuery.Append("   ORDER BY SRFEMT.PREFERENCE");

                return ObjWk.GetDataTable(strQuery.ToString());
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

        #endregion "FETCH QUOTATION PK"

        #region "Credit Control"

        /// <summary>
        /// Fetches the credit details.
        /// </summary>
        /// <param name="BookingPk">The booking pk.</param>
        /// <returns></returns>
        public object FetchCreditDetails(string BookingPk = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder(5000);
            WorkFlow ObjWk = new WorkFlow();
            strSQL.Append(" SELECT BKG.BOOKING_AIR_PK,");
            strSQL.Append(" BKG.CUST_CUSTOMER_MST_FK,");
            // strSQL.Append(" BKG.CREDIT_LIMIT,")
            strSQL.Append(" CMT.SEA_CREDIT_LIMIT,");
            strSQL.Append(" BKG.BOOKING_REF_NO,");
            strSQL.Append("  SUM(BTA.ALL_IN_TARIFF) AS TOTAL");
            strSQL.Append(" FROM BOOKING_TRN_AIR_FRT_DTLS BFD,");
            strSQL.Append(" CUSTOMER_MST_TBL         CMT,");
            strSQL.Append(" BOOKING_AIR_TBL          BKG,");
            strSQL.Append(" BOOKING_TRN_AIR          BTA");

            strSQL.Append(" WHERE BFD.BOOKING_TRN_AIR_FK = BTA.BOOKING_TRN_AIR_PK(+)");
            strSQL.Append(" AND BTA.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK");
            strSQL.Append(" AND BKG.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
            strSQL.Append(" AND BKG.BOOKING_AIR_PK=" + BookingPk);
            strSQL.Append(" GROUP BY BKG.BOOKING_AIR_PK,");
            strSQL.Append("  BKG.CUST_CUSTOMER_MST_FK,");
            strSQL.Append(" CMT.SEA_CREDIT_LIMIT,BKG.BOOKING_REF_NO");

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

        /// <summary>
        /// Fetches the credit policy.
        /// </summary>
        /// <param name="ShipperPK">The shipper pk.</param>
        /// <returns></returns>
        public object FetchCreditPolicy(string ShipperPK = "0")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strSQL.Append(" SELECT CMT.CREDIT_LIMIT,");
            strSQL.Append(" cmt.customer_name,");
            strSQL.Append(" CMT.CREDIT_DAYS,");
            strSQL.Append(" CMT.AIR_APP_BOOKING,");
            strSQL.Append(" CMT.AIR_APP_BL_RELEASE,");
            strSQL.Append(" CMT.AIR_APP_RELEASE_ODR");
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

        #region "Assigning Values to the Radiobuttons"

        /// <summary>
        /// Fetches the reference_from.
        /// </summary>
        /// <param name="Int_Bookingpk">The int_ bookingpk.</param>
        /// <returns></returns>
        public DataSet FetchReference_from(short Int_Bookingpk = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objwf = new WorkFlow();

            sb.Append("SELECT TRN.TRANS_REFERED_FROM");
            sb.Append("  FROM BOOKING_AIR_TBL MAIN, BOOKING_TRN_AIR TRN");
            sb.Append(" WHERE MAIN.BOOKING_AIR_PK = TRN.BOOKING_AIR_FK");
            if (Int_Bookingpk > 0)
            {
                sb.Append(" AND MAIN.BOOKING_AIR_PK=" + Int_Bookingpk);
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

        #endregion "Assigning Values to the Radiobuttons"
    }
}