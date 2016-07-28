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
using Oracle.ManagedDataAccess.Types;
using System;
using System.Collections;
using System.Data;
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    public class Cls_QuotationForBookingAir : CommonFeatures
    {
        private long _PkValue;
        private bool NewRecord;
        private string baseCur;
        private int _Static_Col;

        private int _Col_Incr;

        #region " Supporting Methods "

        public bool quotNewRecord
        {
            get { return NewRecord; }
            set { NewRecord = value; }
        }

        #region " Enhance Search Function for Enquiry "

        public string FetchEnquiryRefNo(string strCond)
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string QUOTEDATE = "";
            string strLOC_MST_IN = "";
            string CustPK = "";
            string strReq = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            if (arr.Length > 2)
                QUOTEDATE = Convert.ToString(arr.GetValue(2));
            if (arr.Length > 3)
                strLOC_MST_IN = Convert.ToString(arr.GetValue(3));
            if (arr.Length > 4)
                CustPK = Convert.ToString(arr.GetValue(4));

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_BOOKING_PKG.GET_BOOKING_ENQUIRY_AIR";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("QUOTEDATE_IN", QUOTEDATE).Direction = ParameterDirection.Input;
                _with1.Add("LogLoc_IN", ifDBNull(strLOC_MST_IN)).Direction = ParameterDirection.Input;
                _with1.Add("CUSTOMER_PK_IN", (string.IsNullOrEmpty(CustPK) ? "" : CustPK)).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.Clob).Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                OracleClob clob = default(OracleClob);
                clob = (OracleClob)SCM.Parameters["RETURN_VALUE"].Value;
                System.IO.StreamReader strReader = new System.IO.StreamReader(clob, Encoding.Unicode);
                strReturn = strReader.ReadToEnd();
                return strReturn.Trim();
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

        #endregion " Enhance Search Function for Enquiry "

        public DataSet getDivFacMW(string Pk)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();
            if ((Pk != null))
            {
                if (!string.IsNullOrEmpty(Pk))
                {
                    strQuery.Append("select distinct cargo_measurement, cargo_weight_in, cargo_division_fact ");
                    strQuery.Append("from QUOTATION_AIR_CARGO_CALC where ");
                    strQuery.Append("quotation_trn_air_fk in ");
                    strQuery.Append("(select mm.quote_trn_air_pk from quotation_trn_air mm where mm.quotation_air_fk = " + Pk);
                    strQuery.Append(")");
                    return objWF.GetDataSet(strQuery.ToString());
                }
            }
            return null;
        }

        #region " Shadows Methods "

        public DataTable fetchCustomerCategory()
        {
            string strSQL = null;
            strSQL = " Select CUSTOMER_CATEGORY_MST_PK, CUSTOMER_CATEGORY_ID " + " from CUSTOMER_CATEGORY_MST_TBL where ACTIVE_FLAG = 1 order by CUSTOMER_CATEGORY_ID ";
            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        private new object removeDBNull(string col)
        {
            if (col == null)
            {
                return "";
            }
            else if (object.ReferenceEquals(col, DBNull.Value))
            {
                return "";
            }
            return col;
        }

        private new object ifDBNull(string col)
        {
            if (col == null)
            {
                return DBNull.Value;
            }
            else if (object.ReferenceEquals(col, DBNull.Value))
            {
                return DBNull.Value;
            }
            else if (Convert.ToString(col).Length == 0)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        private new object ifDBZero(string col, Int16 Zero = 0)
        {
            if (Convert.ToInt32(col) == Zero)
            {
                return DBNull.Value;
            }
            else
            {
                return col;
            }
        }

        #endregion " Shadows Methods "

        #endregion " Supporting Methods "

        #region " Enum and SRC & BPNT function "

        #region " Break Point "

        private new enum BreakPoint
        {
            Default = 0,
            Kgs = 1,
            ULD = 2
        }

        private new string BPNT(BreakPoint Btype)
        {
            switch (Btype)
            {
                case BreakPoint.Kgs:
                    return "'BP'";

                case BreakPoint.ULD:
                    return "'ULD'";

                default:
                    return "";
            }
        }

        private new BreakPoint BreakPointEnum(string BrkPnt)
        {
            switch (BrkPnt)
            {
                case "1":
                    return BreakPoint.Kgs;

                case "2":
                    return BreakPoint.ULD;

                default:
                    return BreakPoint.Default;
            }
        }

        public string BaseCurrency
        {
            get { return baseCur; }
            set { baseCur = value; }
        }

        #endregion " Break Point "

        #region " Source Type "

        public int TransRefer
        {
            get
            {
                //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                //{
                //    return SourceType.SpotRate;
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                //{
                //    return SourceType.CustomerContract;
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //{
                //    return SourceType.Quotation;
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                //{
                //    return SourceType.AirlineTariff;
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                //{
                //    return SourceType.GeneralTariff;
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[5].Selected)
                //{
                //    return SourceType.Manual;
                //}
                //else
                //{
                //    return SourceType.DefaultValue;
                //}
                return 0;
            }
        }

        private new enum SourceType
        {
            DefaultValue = 0,
            SpotRate = 1,
            CustomerContract = 2,
            Quotation = 3,
            Enquiry = 4,
            AirlineTariff = 5,
            GeneralTariff = 6,
            Manual = 7
        }

        private new string SRC(SourceType SType)
        {
            switch (SType)
            {
                case SourceType.DefaultValue:
                    return "'GenTar'";

                case SourceType.SpotRate:
                    return "'SpRate'";

                case SourceType.CustomerContract:
                    return "'Cont'";

                case SourceType.Quotation:
                    return "'Quote'";

                case SourceType.Enquiry:
                    return "'ENQ'";

                case SourceType.AirlineTariff:
                    return "'AirTar'";

                case SourceType.GeneralTariff:
                    return "'GenTar'";

                case SourceType.Manual:
                    return "'Manual'";

                default:
                    return "";
            }
        }

        private new SourceType SourceEnum(string SType)
        {
            switch (SType)
            {
                case "2":
                    return SourceType.CustomerContract;

                case "1":
                    return SourceType.SpotRate;

                case "3":
                    return SourceType.Quotation;

                case "4":
                    return SourceType.Enquiry;

                case "5":
                    return SourceType.AirlineTariff;

                case "6":
                    return SourceType.GeneralTariff;

                case "0":
                    return SourceType.AirlineTariff;

                case "7":
                    return SourceType.Manual;

                default:
                    return SourceType.DefaultValue;
            }
        }

        #endregion " Source Type "

        #region " Charge Basis "

        private new enum ChargeBasis
        {
            DefaultValue = 0,
            Percentage = 1,
            Flat = 2,
            KGs = 3
        }

        private new string CBCS(ChargeBasis ChType)
        {
            switch (ChType)
            {
                case ChargeBasis.Percentage:
                    return "'%'";

                case ChargeBasis.Flat:
                    return "'Flat'";

                case ChargeBasis.KGs:
                    return "'KGs'";

                default:
                    return "";
            }
        }

        private new ChargeBasis CBCSEnum(string CBasis)
        {
            switch (CBasis)
            {
                case "1":
                    return ChargeBasis.Percentage;

                case "2":
                    return ChargeBasis.Flat;

                case "3":
                    return ChargeBasis.KGs;

                default:
                    return ChargeBasis.DefaultValue;
            }
        }

        #endregion " Charge Basis "

        #region " Freight Type "

        private new enum FreightType
        {
            AFC = 1,
            Surcharge = 2,
            OtherCharge = 0,
            Dafault = -1
        }

        private new string FTYP(string FType)
        {
            switch (FType)
            {
                case "1":
                    return "'AFC'";

                case "0":
                    return "'OthChrg'";

                case "2":
                    return "'SurChrg'";

                default:
                    return "";
            }
        }

        private new FreightType FTypeEnum(string FType)
        {
            switch (FType)
            {
                case "1":
                    return FreightType.AFC;

                case "0":
                    return FreightType.OtherCharge;

                case "2":
                    return FreightType.Surcharge;

                default:
                    return FreightType.Dafault;
            }
        }

        #endregion " Freight Type "

        #endregion " Enum and SRC & BPNT function "

        #region " Fetch UWG2 Entry grid. "

        #region " Enquiry Detail "

        // This function fetched the entry grid that can be from enquiry or existing Quotation
        // depending upon whether Quotation No has been provided or not.
        private void GetEnqDetail(string EnqNo = "", string CustomerPK = "", string CustomerID = "", string CustomerCategory = "", string AgentPK = "", string AgentID = "", string CommodityGroup = "", string Sectors = "", DataSet EnqDS = null, string QuoteNo = "",
        int Version = 0, string QuotationStatus = null, DataTable OthDt = null, DataSet CalcDS = null, string ValidFor = null, string QuoteDate = null, short CustomerType = 0, string ShipDate = null, int CreditLimit = 0, int CreditDays = 0,
        int Remarks = 0, int CargoMcode = 0, int BaseCurrencyId = 0, int INCOTerms = 0, int PYMTType = 0, bool AmendFlg = false)
        {
            StringBuilder MasterQuery = new StringBuilder();
            StringBuilder FreightQuery = new StringBuilder();
            StringBuilder OthQuery = new StringBuilder();
            StringBuilder CargoQuery = new StringBuilder();
            StringBuilder TransactionPKs = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            short RowCnt = default(short);
            try
            {
                if (!string.IsNullOrEmpty(QuoteNo))
                {
                    MasterQuery = GetQuoteQuery(QuoteNo, Version, QuotationStatus, ValidFor, QuoteDate, ShipDate, CreditDays, CreditLimit, Remarks, CargoMcode,
                    Convert.ToInt32(CommodityGroup), BaseCurrencyId, INCOTerms, PYMTType);
                }
                else
                {
                    MasterQuery = GetEnquiryQuery(EnqNo);
                }

                EnqDS = objWF.GetDataSet(MasterQuery.ToString().Replace("  ", " "));
                TransactionPKs = GetAllKeys(EnqDS.Tables[0], 0);
                StringBuilder sbSectors = new StringBuilder();
                if (EnqDS.Tables[0].Rows.Count > 0)
                {
                    for (RowCnt = 0; RowCnt <= EnqDS.Tables[0].Rows.Count - 1; RowCnt++)
                    {
                        var _with2 = EnqDS.Tables[0].Rows[RowCnt];
                        sbSectors.Append("(" + _with2["POLFK"] + "," + _with2["PODFK"] + "),");
                    }
                    Sectors = sbSectors.ToString().TrimEnd(',');
                }
                if (!string.IsNullOrEmpty(QuoteNo))
                {
                    FreightQuery = GetQuoteQueryFreights(TransactionPKs.ToString(), Sectors);
                    OthQuery = GetQuoteOthQuery(TransactionPKs.ToString());
                    CargoQuery = GetQuoteCargoQuery(TransactionPKs.ToString(), 2);
                }
                else
                {
                    FreightQuery = GetEnquiryQueryFreights(TransactionPKs.ToString());
                    OthQuery = GetEnquiryOthQuery(TransactionPKs.ToString());
                }

                EnqDS.Tables.Add(objWF.GetDataTable(FreightQuery.ToString().Replace("  ", " ")));
                OthDt = objWF.GetDataTable(OthQuery.ToString().Replace("  ", " "));
                if (!string.IsNullOrEmpty(QuoteNo))
                {
                    CalcDS = objWF.GetDataSet(CargoQuery.ToString().Replace("  ", " "));
                }

                DataRelation REL = null;
                REL = new DataRelation("EnqRelation", EnqDS.Tables[0].Columns["PK"], EnqDS.Tables[1].Columns["FK"]);
                EnqDS.Relations.Add(REL);

                if (EnqDS.Tables[0].Rows.Count > 0)
                {
                    CustomerPK = Convert.ToString(EnqDS.Tables[0].Rows[0]["CUSTOMER_PK"]);
                    CustomerCategory = Convert.ToString(EnqDS.Tables[0].Rows[0]["CUSTOMER_CATPK"]);
                    CommodityGroup = Convert.ToString(EnqDS.Tables[0].Rows[0]["COMM_GRPPK"]);
                    ShipDate = Convert.ToString(EnqDS.Tables[0].Rows[0]["SHIP_DATE"]);
                    CustomerType = Convert.ToInt16(getDefault(EnqDS.Tables[0].Rows[0]["CUST_TYPE"], 0));
                    if (CustomerType == 0)
                    {
                        CustomerID = objWF.ExecuteScaler(" Select CUSTOMER_ID from CUSTOMER_MST_TBL where CUSTOMER_MST_PK = " + CustomerPK);
                    }
                    else
                    {
                        CustomerID = objWF.ExecuteScaler(" Select CUSTOMER_ID from TEMP_CUSTOMER_TBL where CUSTOMER_MST_PK = " + CustomerPK);
                    }
                }
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        #endregion " Enquiry Detail "

        #region " Enquiry Query For (EnqNo.) "

        // This is to fetch enquiry detail Header
        private StringBuilder GetEnquiryQuery(string EnquiryNo)
        {
            if (string.IsNullOrEmpty(EnquiryNo.Trim()))
                EnquiryNo = "-1";
            string slbPK = null;
            string slbID = null;
            string slbType = null;
            slbPK = "( select distinct AIRFREIGHT_SLABS_FK   " + "    from          ENQUIRY_TRN_AIR_FRT_DTLS eq1" + "   where          eq1.ENQUIRY_TRN_AIR_FK  = tran.ENQUIRY_TRN_AIR_PK " + "         and      eq1.AIRFREIGHT_SLABS_FK IS NOT NULL )";

            slbID = "( select distinct BREAKPOINT_ID   " + "    from          ENQUIRY_TRN_AIR_FRT_DTLS eq2, AIRFREIGHT_SLABS_TBL afs2" + "   where          eq2.ENQUIRY_TRN_AIR_FK  = tran.ENQUIRY_TRN_AIR_PK " + "         and      eq2.AIRFREIGHT_SLABS_FK = afs2.AIRFREIGHT_SLABS_TBL_PK " + "         and      eq2.AIRFREIGHT_SLABS_FK IS NOT NULL )";

            slbType = "( select distinct BREAKPOINT_TYPE   " + "    from          ENQUIRY_TRN_AIR_FRT_DTLS eq3, AIRFREIGHT_SLABS_TBL afs3" + "   where          eq3.ENQUIRY_TRN_AIR_FK  = tran.ENQUIRY_TRN_AIR_PK " + "         and      eq3.AIRFREIGHT_SLABS_FK = afs3.AIRFREIGHT_SLABS_TBL_PK " + "         and      eq3.AIRFREIGHT_SLABS_FK IS NOT NULL )";

            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                         ");
            sbSQL.Append("       tran.ENQUIRY_TRN_AIR_PK                      PK,            ");
            //0
            sbSQL.Append("       tran.ENQUIRY_MAIN_AIR_FK                     FK,            ");
            //1
            sbSQL.Append("    " + SourceType.Enquiry + "                      REF_TYPE,      ");
            //2
            sbSQL.Append("    " + SRC(SourceType.Enquiry) + "                 TYPE_ID,       ");
            //3
            sbSQL.Append("       main.ENQUIRY_REF_NO                          REF_NO,        ");
            //4
            sbSQL.Append("    TO_CHAR(tran.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE, ");
            //5
            sbSQL.Append("       tran.PORT_MST_POL_FK                         POLFK,         ");
            //6
            sbSQL.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            //7
            sbSQL.Append("       tran.PORT_MST_POD_FK                         PODFK,         ");
            //8
            sbSQL.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            //9
            sbSQL.Append("       tran.AIRLINE_MST_FK                          AIR_PK,        ");
            //10
            sbSQL.Append("       air.AIRLINE_ID                               AIR_ID,        ");
            //11
            sbSQL.Append("       TO_CHAR(tran.COMMODITY_MST_FK)                       COMM_PK,       ");
            //12
            sbSQL.Append("       cmdt.COMMODITY_ID                            COMM_ID,       ");
            //13
            sbSQL.Append("    " + slbType + "                                 SLAB_TYPE_PK,  ");
            //14
            sbSQL.Append("       decode( " + slbType + ",   ");
            sbSQL.Append("   " + BreakPoint.Kgs + "," + BPNT(BreakPoint.Kgs));
            sbSQL.Append("       ," + BPNT(BreakPoint.ULD) + " )              SLAB_TYPE,     ");
            //15

            sbSQL.Append("       case when decode( " + slbType + ",   ");
            sbSQL.Append("   " + BreakPoint.Kgs + "," + BPNT(BreakPoint.Kgs));
            sbSQL.Append("       ," + BPNT(BreakPoint.ULD) + " )  = 'BP' then ");
            sbSQL.Append("   tran.EXP_CHARGEABLE_WT else tran.QUANTITY  end   BOXES,         ");
            //16
            sbSQL.Append("   " + slbPK + "                                    SLAB,          ");
            //17
            sbSQL.Append("   " + slbID + "                                    SLAB_ID,       ");
            //18
            sbSQL.Append("       tran.EXP_CHARGEABLE_WT                       CH_WT,         ");
            //19
            sbSQL.Append("       tran.ALL_IN_TARIFF                           AI_TRF,        ");
            //20
            sbSQL.Append("       tran.ALL_IN_TARIFF                           AI_QT,         ");
            //21
            sbSQL.Append("       0                                            AIR_RT,        ");
            //22
            sbSQL.Append("       0                                            NET,           ");
            //23
            sbSQL.Append("       tran.TRANS_REF_NO                            REF_NO2,       ");
            //24
            sbSQL.Append("       tran.TRANS_REFERED_FROM                      TYPE2,         ");
            //25
            sbSQL.Append("       main.CUSTOMER_MST_FK                         CUSTOMER_PK,   ");
            //26
            sbSQL.Append("       main.CUSTOMER_CATEGORY_FK                    CUSTOMER_CATPK,");
            //27
            sbSQL.Append("       tran.COMMODITY_GROUP_FK                      COMM_GRPPK,    ");
            //28
            sbSQL.Append("       ''                                           OTH_DTL,       ");
            //29
            sbSQL.Append("       ''                                           OTH_BTN,       ");
            //30
            sbSQL.Append("       ''                                           CRG_BTN,       ");
            //31
            sbSQL.Append("       nvl(CUST_TYPE,1)                             CUST_TYPE      ");
            //32
            sbSQL.Append("   From ENQUIRY_BKG_AIR_TBL            main,                    ");
            sbSQL.Append("        ENQUIRY_TRN_AIR                tran,                    ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOD,                 ");
            sbSQL.Append("        AIRLINE_MST_TBL                air,                     ");
            sbSQL.Append("        COMMODITY_MST_TBL              cmdt                     ");
            sbSQL.Append("   Where                                                        ");
            sbSQL.Append("       main.ENQUIRY_BKG_AIR_PK         =   tran.ENQUIRY_MAIN_AIR_FK ");
            sbSQL.Append("   AND tran.PORT_MST_POL_FK            =   PORTPOL.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.PORT_MST_POD_FK            =   PORTPOD.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.COMMODITY_MST_FK           =   cmdt.COMMODITY_MST_PK(+) ");
            sbSQL.Append("   AND tran.AIRLINE_MST_FK             =   air.AIRLINE_MST_PK(+)    ");
            sbSQL.Append("   AND main.ENQUIRY_REF_NO    = '" + EnquiryNo + "'");

            return sbSQL;
        }

        #endregion " Enquiry Query For (EnqNo.) "

        #region " Enquiry Query For Freights (EnqNo.) "

        // Getting Enquiry Freight rows for selected enquiry raansactions
        private StringBuilder GetEnquiryQueryFreights(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     trf.ENQUIRY_TRN_AIR_FRT_PK                PK,                  ");
            //0
            sbSQL.Append("     trf.ENQUIRY_TRN_AIR_FK                    FK,                  ");
            //1
            sbSQL.Append("     trf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt.FREIGHT_ELEMENT_ID                    FRT_ID,              ");
            //3
            sbSQL.Append("     decode(trf.CHECK_FOR_ALL_IN_RT,1,'true','false')  SELECTED,    ");
            //4
            sbSQL.Append("     decode(trf.CHECK_ADVATOS,1,'true','false')  ADVATOS,           ");
            //5 ''Added Koteshwari for Advatos Column PTSID-JUN18
            sbSQL.Append("     trf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6 '5
            sbSQL.Append("     cur.CURRENCY_ID                           CURR_ID,             ");
            //7 '6
            sbSQL.Append("     0                                         MIN_AMOUNT,          ");
            //8 '7
            sbSQL.Append("     trf.BASIS_RATE                            BASIS_RATE,          ");
            //9 '8
            sbSQL.Append("     trf.TARIFF_RATE                           TARIFF_RATE,         ");
            //10 '9
            sbSQL.Append("     trf.TARIFF_RATE                           QUOTED_RATE,         ");
            //11 '10
            sbSQL.Append("     trf.CHARGE_BASIS                          CH_BASIS,            ");
            //12 '11
            sbSQL.Append("     decode(trf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,     ");
            //13 '12
            sbSQL.Append("     nvl(frt.CHARGE_TYPE,2)                   FRT_TYPE,            ");
            //14 '13
            sbSQL.Append("     1                                         P_TYPE,              ");
            //15 '14
            sbSQL.Append("     'PrePaid'                                 P_TYPE_ID            ");
            //16 '15
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     ENQUIRY_TRN_AIR_FRT_DTLS       trf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt,                            ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           trf.ENQUIRY_TRN_AIR_FK      in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   trf.FREIGHT_ELEMENT_MST_FK  =  frt.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   trf.CURRENCY_MST_FK         =  cur.CURRENCY_MST_PK(+)        ");
            sbSQL.Append("   ORDER BY FRT.PREFERENCE ");
            return sbSQL;
        }

        private StringBuilder GetEnquiryOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     otf.ENQUIRY_AIR_OTH_CHRG_PK               PK,                  ");
            sbSQL.Append("     otf.ENQUIRY_TRN_AIR_FK                    FK,                  ");
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            sbSQL.Append("     otf.BASIS_RATE                            BASIS_RATE,          ");
            sbSQL.Append("     otf.AMOUNT                                TARIFF_RATE,         ");
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,      ");
            sbSQL.Append("    1 PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     ENQUIRY_AIR_OTH_CHRG          otf                              ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.ENQUIRY_TRN_AIR_FK     in (" + ParentKeys + ")       ");

            return sbSQL;
        }

        #endregion " Enquiry Query For Freights (EnqNo.) "

        #region " Quotation Query For (QuotePk.) "

        private StringBuilder GetQuoteQuery(string QuotationPK, long Version = 0, string QuotationStatus = null, string ValidFor = null, string QuoteDate = null, string ShipDate = null, int CREDIT_DAYS = 0, int CREDIT_Limit = 0, int Remarks = 0, int CargoMcode = 0,
        int CommodityGroup = 0, int BaseCurrencyId = 0, int INCOTerms = 0, int PYMTType = 0)
        {
            RenderData(QuotationPK, Version, QuotationStatus, ValidFor, QuoteDate, ShipDate, CREDIT_DAYS, CREDIT_Limit, Remarks, CargoMcode,
            CommodityGroup, BaseCurrencyId, INCOTerms, PYMTType);
            string strType = null;
            strType = "decode(tran.TRANS_REFERED_FROM," + SourceType.SpotRate + "," + SRC(SourceType.SpotRate) + "," + SourceType.AirlineTariff + "," + SRC(SourceType.AirlineTariff) + "," + SourceType.CustomerContract + "," + SRC(SourceType.CustomerContract) + "," + SourceType.GeneralTariff + "," + SRC(SourceType.GeneralTariff) + "," + SourceType.Quotation + "," + SRC(SourceType.Quotation) + "," + SourceType.Manual + "," + SRC(SourceType.Manual) + "," + SourceType.Enquiry + "," + SRC(SourceType.Enquiry) + "," + SRC(SourceType.DefaultValue) + ")";

            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                         ");
            sbSQL.Append("       tran.QUOTE_TRN_AIR_PK                        PK,            ");
            //0
            sbSQL.Append("       tran.QUOTATION_AIR_FK                        FK,            ");
            //1
            sbSQL.Append("       tran.TRANS_REFERED_FROM                      REF_TYPE,      ");
            //2
            sbSQL.Append("    " + strType + "                                 TYPE_ID,       ");
            //3>
            sbSQL.Append("       tran.TRANS_REF_NO                            REF_NO,        ");
            //4>
            sbSQL.Append("    TO_CHAR(main.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE, ");
            //5>
            sbSQL.Append("       tran.PORT_MST_POL_FK                         POLFK,         ");
            //6
            sbSQL.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            //7>
            sbSQL.Append("       tran.PORT_MST_POD_FK                         PODFK,         ");
            //8
            sbSQL.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            //9>
            sbSQL.Append("       tran.AIRLINE_MST_FK                          AIR_PK,        ");
            //10
            sbSQL.Append("       air.AIRLINE_ID                               AIR_ID,        ");
            //11>
            sbSQL.Append("       tran.COMMODITY_MST_FKS                       COMM_PK,       ");
            //12
            sbSQL.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
            sbSQL.Append("                NVL(TRAN.COMMODITY_MST_FKS, 0) || ')') COMM_ID,");
            sbSQL.Append("       slb.BREAKPOINT_TYPE                          SLAB_TYPE_PK,  ");
            //14
            sbSQL.Append("       decode(slb.BREAKPOINT_TYPE,");
            sbSQL.Append("   " + BreakPoint.Kgs + "," + BPNT(BreakPoint.Kgs));
            sbSQL.Append("       ," + BPNT(BreakPoint.ULD) + " )              SLAB_TYPE,     ");
            //15>
            sbSQL.Append("       decode(slb.BREAKPOINT_TYPE," + BreakPoint.ULD);
            sbSQL.Append("      ,tran.QUANTITY,NULL)                          BOXES,         ");
            //16>
            sbSQL.Append("       tran.SLAB_FK                                 SLAB,          ");
            //17
            sbSQL.Append("       slb.BREAKPOINT_ID                            SLAB_ID,       ");
            //18>
            sbSQL.Append("       tran.CHARGEABLE_WEIGHT                       CH_WT,         ");
            //19>
            sbSQL.Append("       tran.ALL_IN_TARIFF                           AI_TRF,        ");
            //20>
            sbSQL.Append("       tran.ALL_IN_QUOTED_TARIFF                    AI_QT,         ");
            //21>
            sbSQL.Append("       nvl(tran.BUYING_RATE,0)                      AIR_RT,        ");
            //22
            sbSQL.Append("       0                                            NET,           ");
            //23
            sbSQL.Append("       tran.TRAN_REF_NO2                            REF_NO2,       ");
            //24
            sbSQL.Append("       tran.REF_TYPE2                               TYPE2,         ");
            //25
            sbSQL.Append("       main.CUSTOMER_MST_FK                         CUSTOMER_PK,   ");
            //26
            sbSQL.Append("       main.CUSTOMER_CATEGORY_MST_FK                CUSTOMER_CATPK,");
            //27
            sbSQL.Append("       tran.COMMODITY_GROUP_FK                      COMM_GRPPK,    ");
            //28
            sbSQL.Append("       ''                                           OTH_DTL,       ");
            //29
            sbSQL.Append("       ''                                           OTH_BTN,       ");
            //30>
            sbSQL.Append("       ''                                           CRG_BTN,       ");
            //31>
            sbSQL.Append("       nvl( CUST_TYPE,1)                            CUST_TYPE      ");
            //32
            sbSQL.Append("   From QUOTATION_AIR_TBL              main,                    ");
            sbSQL.Append("        QUOTATION_TRN_AIR              tran,                    ");
            sbSQL.Append("        AIRFREIGHT_SLABS_TBL           slb,                     ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOD,                 ");
            sbSQL.Append("        AIRLINE_MST_TBL                air,                     ");
            sbSQL.Append("        COMMODITY_MST_TBL              cmdt                     ");

            sbSQL.Append("   Where                                                        ");
            sbSQL.Append("       main.QUOTATION_AIR_PK           =   tran.QUOTATION_AIR_FK    ");
            sbSQL.Append("   AND tran.SLAB_FK              =   slb.AIRFREIGHT_SLABS_TBL_PK(+) ");
            sbSQL.Append("   AND tran.PORT_MST_POL_FK            =   PORTPOL.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.PORT_MST_POD_FK            =   PORTPOD.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.COMMODITY_MST_FK           =   cmdt.COMMODITY_MST_PK(+) ");
            sbSQL.Append("   AND tran.AIRLINE_MST_FK             =   air.AIRLINE_MST_PK(+)    ");
            sbSQL.Append("   AND main.QUOTATION_AIR_PK   = " + QuotationPK);

            return sbSQL;
        }

        private void RenderData(string QuotationPK, long Version = 0, string QuotationStatus = null, string ValidFor = null, string QuoteDate = null, string ShipDate = null, int CREDIT_DAYS = 0, int CREDIT_Limit = 0, int Remarks = 0, int CargoMcode = 0,

        int CommodityGroup = 0, int BaseCurrencyId = 0, int INCOTerms = 0, int PYMTType = 0)
        {
            string strSQL = null;
            DataTable scalerDT = null;
            if (!string.IsNullOrEmpty(Convert.ToString(QuotationPK).Trim()))
            {
                strSQL = " Select nvl(QUOTATION_AIR_TBL.VERSION_NO, 0) VERSION_NO, nvl(STATUS,1), nvl(VALID_FOR,1), " + "        to_char(QUOTATION_DATE,'" + dateFormat + "'), " + "        to_char(EXPECTED_SHIPMENT,'" + dateFormat + "') ," + "        CREDIT_DAYS, " + "        CREDIT_LIMIT,REMARKS,CARGO_MOVE_FK, COMMODITY_GROUP_MST_FK,curr.currency_mst_pk                               base_currency_fk,curr.currency_id, shipping_terms_mst_pk, pymt_type " + "  from  QUOTATION_AIR_TBL,Currency_Type_Mst_Tbl curr " + " where curr.currency_mst_pk(+) = QUOTATION_AIR_TBL.Base_Currency_Fk" + " and  QUOTATION_AIR_PK = " + QuotationPK;

                scalerDT = (new WorkFlow()).GetDataTable(strSQL);

                if (scalerDT.Rows.Count > 0)
                {
                    Version = Convert.ToInt64(removeDBNull(scalerDT.Rows[0][0].ToString()));
                    QuotationStatus = Convert.ToString(removeDBNull(scalerDT.Rows[0][1].ToString()));
                    ValidFor = Convert.ToString(removeDBNull(scalerDT.Rows[0][2].ToString()));
                    QuoteDate = Convert.ToString(removeDBNull(scalerDT.Rows[0][3].ToString()));
                    ShipDate = Convert.ToString(removeDBNull(scalerDT.Rows[0][4].ToString()));
                    CREDIT_DAYS = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][5].ToString()));
                    CREDIT_Limit = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][6].ToString()));
                    Remarks = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][7].ToString()));
                    CargoMcode = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][8].ToString()));
                    CommodityGroup = Convert.ToInt32(removeDBNull(scalerDT.Rows[0][9].ToString()));
                    BaseCurrencyId = Convert.ToInt32(removeDBNull(scalerDT.Rows[0]["currency_id"].ToString()));
                    INCOTerms = Convert.ToInt32(removeDBNull(scalerDT.Rows[0]["shipping_terms_mst_pk"].ToString()));
                    PYMTType = Convert.ToInt32(removeDBNull(scalerDT.Rows[0]["pymt_type"].ToString()));
                }
            }
            else
            {
                QuotationPK = "0";
            }
        }

        #endregion " Quotation Query For (QuotePk.) "

        #region " Quotation Query For Freights (QuotePk.) "

        // Quotation Freight detail
        private StringBuilder GetQuoteQueryFreights(string ParentKeys = "-1", string Sectors = "")
        {
            StringBuilder sbSQL = new StringBuilder();
            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     trf.QUOTE_TRN_AIR_FRT_PK                  PK,                  ");
            sbSQL.Append("     trf.QUOTE_TRN_AIR_FK                      FK,                  ");
            sbSQL.Append("     trf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            sbSQL.Append("     frt.FREIGHT_ELEMENT_ID                    FRT_ID,              ");
            sbSQL.Append("     decode(trf.CHECK_FOR_ALL_IN_RT,1,'true','false')  SELECTED,    ");
            sbSQL.Append("     decode(trf.CHECK_ADVATOS,1,'true','false')  ADVATOS,           ");
            sbSQL.Append("     trf.CURRENCY_MST_FK                       CURR_FK,             ");
            sbSQL.Append("     cur.CURRENCY_ID                           CURR_ID,             ");
            sbSQL.Append("     0                                         MIN_AMOUNT,          ");
            sbSQL.Append("     trf.BASIS_RATE                            BASIS_RATE,          ");
            sbSQL.Append("     trf.TARIFF_RATE                           TARIFF_RATE,         ");
            sbSQL.Append("     trf.QUOTED_RATE                           QUOTED_RATE,         ");
            sbSQL.Append("     trf.CHARGE_BASIS                          CH_BASIS,            ");
            sbSQL.Append("     decode(trf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,     ");
            sbSQL.Append("     trf.FREIGHT_TYPE                          FRT_TYPE,            ");
            sbSQL.Append("     trf.PYMT_TYPE                             P_TYPE,              ");
            sbSQL.Append("     decode(trf.PYMT_TYPE,1,'PrePaid','Collect') P_TYPE_ID          ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     QUOTATION_TRN_AIR_FRT_DTLS     trf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt,                            ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           trf.QUOTE_TRN_AIR_FK        in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   trf.FREIGHT_ELEMENT_MST_FK  =  frt.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   trf.CURRENCY_MST_FK         =  cur.CURRENCY_MST_PK(+)        ");
            return sbSQL;
        }

        private StringBuilder GetQuoteOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     otf.QUOTE_TRN_AIR_OTH_PK                  PK,                  ");
            sbSQL.Append("     otf.QUOTATION_TRN_AIR_FK                  FK,                  ");
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            sbSQL.Append("     otf.BASIS_RATE                            BASIS_RATE,          ");
            sbSQL.Append("     otf.AMOUNT                                TARIFF_RATE,         ");
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,      ");
            sbSQL.Append("     FREIGHT_TYPE PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     QUOTATION_TRN_AIR_OTH_CHRG     otf                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.QUOTATION_TRN_AIR_FK   in (" + ParentKeys + ")       ");

            return sbSQL;
        }

        private StringBuilder GetQuoteCargoQuery(string ParentKeys = "-1", Int16 GridNo = 2)
        {
            StringBuilder sbSQL = new StringBuilder();
            sbSQL.Append("    Select                                                    " + "        ROWNUM                             SNo,                " + "        CARGO_NOP                          NOP,                " + "        CARGO_LENGTH                       Length,             " + "        CARGO_WIDTH                        Width,              " + "        CARGO_HEIGHT                       Height,             " + "        CARGO_CUBE                         Cube,               " + "        CARGO_VOLUME_WT                    VolWeight,          " + "        CARGO_ACTUAL_WT                    ActWeight,          " + "        CARGO_DENSITY                      Density,            " + "    " + GridNo + "                         PK,                 " + "        QUOTATION_TRN_AIR_FK               FK                  " + "     FROM                                                      " + "        QUOTATION_AIR_CARGO_CALC                               " + "     WHERE                                                     " + "        QUOTATION_TRN_AIR_FK    in    (" + ParentKeys + " )    ");

            return sbSQL;
        }

        #endregion " Quotation Query For Freights (QuotePk.) "

        #endregion " Fetch UWG2 Entry grid. "

        #region " Fetch UWG1 Option grid. "

        #region " Header Query "

        // These are similar queries to get Header grid from different sources

        #region " Spot Rate Query."

        private StringBuilder SpotRateQuery(string CustomerPk = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            StringBuilder sbSQL = new StringBuilder();
            sbSQL.Append("    Select                                                         ");
            sbSQL.Append("       tran.RFQ_SPOT_AIR_TRN_PK                     PK,            ");
            sbSQL.Append("       tran.RFQ_SPOT_AIR_FK                         FK,            ");
            sbSQL.Append("    " + SourceType.SpotRate + "                     REF_TYPE,      ");
            sbSQL.Append("    " + SRC(SourceType.SpotRate) + "                TYPE_ID,       ");
            sbSQL.Append("       main.RFQ_REF_NO                              REF_NO,        ");
            sbSQL.Append("       TO_CHAR(tran.VALID_TO,'" + dateFormat + "')  SHIP_DATE,     ");
            sbSQL.Append("       tran.PORT_MST_POL_FK                         POLFK,         ");
            sbSQL.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            sbSQL.Append("       tran.PORT_MST_POD_FK                         PODFK,         ");
            sbSQL.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            sbSQL.Append("       main.AIRLINE_MST_FK                          AIR_PK,        ");
            sbSQL.Append("       air.AIRLINE_ID                               AIR_ID,        ");
            sbSQL.Append("       main.COMMODITY_MST_FK                        COMM_PK,       ");
            sbSQL.Append("       cmdt.COMMODITY_ID                            COMM_ID,       ");
            sbSQL.Append("    " + BreakPoint.Kgs + "                          SLAB_TYPE_PK,  ");
            sbSQL.Append("    " + BPNT(BreakPoint.Kgs) + "                    SLAB_TYPE,     ");
            sbSQL.Append("       0                                            BOXES,         ");
            sbSQL.Append("       NULL                                         SLAB,          ");
            sbSQL.Append("       ''                                           SLAB_ID,       ");
            sbSQL.Append("       0                                            CH_WT,         ");
            sbSQL.Append("       0                                            AI_QT,         ");
            sbSQL.Append("       0                                            NET,           ");
            sbSQL.Append("       ''                                           OTH_DTL,       ");
            sbSQL.Append("       ''                                           OTH_BTN,       ");
            sbSQL.Append("       ''                                           CRG_BTN,       ");
            sbSQL.Append("       'false'                                      SELECTED       ");
            sbSQL.Append("   From RFQ_SPOT_RATE_AIR_TBL          main,                    ");
            sbSQL.Append("        RFQ_SPOT_TRN_AIR_TBL           tran,                    ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOD,                 ");
            sbSQL.Append("        AIRLINE_MST_TBL                air,                     ");
            sbSQL.Append("        COMMODITY_GROUP_MST_TBL        CMGR,                    ");
            sbSQL.Append("        COMMODITY_MST_TBL              cmdt                     ");
            sbSQL.Append("   Where                                                        ");
            sbSQL.Append("       main.RFQ_SPOT_AIR_PK            =   tran.RFQ_SPOT_AIR_FK     ");
            sbSQL.Append("   AND tran.PORT_MST_POL_FK            =   PORTPOL.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.PORT_MST_POD_FK            =   PORTPOD.PORT_MST_PK      ");
            sbSQL.Append("   AND main.COMMODITY_MST_FK           =   cmdt.COMMODITY_MST_PK(+) ");
            sbSQL.Append("   AND main.AIRLINE_MST_FK             =   air.AIRLINE_MST_PK(+)    ");
            sbSQL.Append("   AND main.ACTIVE     =      1    AND     main.APPROVED   =     1  ");
            sbSQL.Append("   AND (tran.PORT_MST_POL_FK,tran.PORT_MST_POD_FK) in (" + Sectors + ")");
            sbSQL.Append("   AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)     ");
            sbSQL.Append("       between tran.VALID_FROM and nvl(tran.VALID_TO,NULL_DATE_FORMAT)    ");

            if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
            {
                sbSQL.Append("  AND CMGR.COMMODITY_GROUP_PK = " + Convert.ToString(CommodityGroup));
                sbSQL.Append("  AND CMGR.COMMODITY_GROUP_PK = MAIN.COMMODITY_GROUP_FK ");
            }

            if (!string.IsNullOrEmpty(Convert.ToString(CustomerPk)))
            {
                sbSQL.Append("  AND ( main.CUSTOMER_MST_FK    = " + Convert.ToString(CustomerPk));
                sbSQL.Append("         or main.CUSTOMER_MST_FK  is NULL ) ");
            }

            return sbSQL;
        }

        #endregion " Spot Rate Query."

        #region " Airline Tariff Query."

        private StringBuilder AirTariffQuery(string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            StringBuilder sbSQL = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type

            sbSQL.Append("    Select                                                         ");
            sbSQL.Append("       tran.TARIFF_TRN_AIR_PK                       PK,            ");
            //0
            sbSQL.Append("       tran.TARIFF_MAIN_AIR_FK                      FK,            ");
            //1
            sbSQL.Append("    " + SourceType.AirlineTariff + "                REF_TYPE,      ");
            //2
            sbSQL.Append("    " + SRC(SourceType.AirlineTariff) + "           TYPE_ID,       ");
            //3
            sbSQL.Append("       main.TARIFF_REF_NO                           REF_NO,        ");
            //4
            sbSQL.Append("       TO_CHAR(tran.VALID_TO,'" + dateFormat + "')  SHIP_DATE,     ");
            //5
            sbSQL.Append("       tran.PORT_MST_POL_FK                         POLFK,         ");
            //6
            sbSQL.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            //7
            sbSQL.Append("       tran.PORT_MST_POD_FK                         PODFK,         ");
            //8
            sbSQL.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            //9
            sbSQL.Append("       main.AIRLINE_MST_FK                          AIR_PK,        ");
            //10
            sbSQL.Append("       air.AIRLINE_ID                               AIR_ID,        ");
            //11
            sbSQL.Append("       NULL                                         COMM_PK,       ");
            //12
            sbSQL.Append("       ''                                           COMM_ID,       ");
            //13
            sbSQL.Append("    " + BreakPoint.Kgs + "                          SLAB_TYPE_PK,  ");
            //14
            sbSQL.Append("    " + BPNT(BreakPoint.Kgs) + "                    SLAB_TYPE,     ");
            //15
            sbSQL.Append("       0                                            BOXES,         ");
            //16
            sbSQL.Append("       NULL                                         SLAB,          ");
            //17
            sbSQL.Append("       ''                                           SLAB_ID,       ");
            //18
            sbSQL.Append("       0                                            CH_WT,         ");
            //19
            sbSQL.Append("       0                                            AI_QT,         ");
            //20
            sbSQL.Append("       0                                            NET,           ");
            //21
            sbSQL.Append("       ''                                           OTH_DTL,       ");
            //22
            sbSQL.Append("       ''                                           OTH_BTN,       ");
            //23
            sbSQL.Append("       ''                                           CRG_BTN,       ");
            //24>
            sbSQL.Append("       'false'                                      SELECTED       ");
            //25>
            sbSQL.Append("   From TARIFF_MAIN_AIR_TBL            main,                    ");
            sbSQL.Append("        TARIFF_TRN_AIR_TBL             tran,                    ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOD,                 ");
            sbSQL.Append("        AIRLINE_MST_TBL                air                      ");
            sbSQL.Append("   Where                                                        ");
            sbSQL.Append("       main.TARIFF_MAIN_AIR_PK         =   tran.TARIFF_MAIN_AIR_FK  ");
            sbSQL.Append("   AND tran.PORT_MST_POL_FK            =   PORTPOL.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.PORT_MST_POD_FK            =   PORTPOD.PORT_MST_PK      ");
            sbSQL.Append("   AND main.AIRLINE_MST_FK             =   air.AIRLINE_MST_PK(+)    ");
            sbSQL.Append("   AND main.ACTIVE                     =      1                     ");
            sbSQL.Append("   AND (tran.PORT_MST_POL_FK,tran.PORT_MST_POD_FK) in (" + Sectors + ")");
            sbSQL.Append("   AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)     ");
            sbSQL.Append("       between tran.VALID_FROM and nvl(tran.VALID_TO,NULL_DATE_FORMAT)    ");
            sbSQL.Append("   AND main.TARIFF_TYPE                = 1 ");

            if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
            {
                sbSQL.Append("  AND main.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup));
            }

            return sbSQL;
        }

        #endregion " Airline Tariff Query."

        #region " General Tariff Query."

        private StringBuilder GenTariffQuery(string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            StringBuilder sbSQL = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type

            sbSQL.Append("    Select                                               ");
            sbSQL.Append("       tran.TARIFF_TRN_AIR_PK                       PK,            ");
            //0
            sbSQL.Append("       tran.TARIFF_MAIN_AIR_FK                      FK,            ");
            //1
            sbSQL.Append("    " + SourceType.GeneralTariff + "                REF_TYPE,      ");
            //2
            sbSQL.Append("    " + SRC(SourceType.GeneralTariff) + "           TYPE_ID,       ");
            //3
            sbSQL.Append("       main.TARIFF_REF_NO                           REF_NO,        ");
            //4
            sbSQL.Append("       TO_CHAR(tran.VALID_TO,'" + dateFormat + "')  SHIP_DATE,     ");
            //5
            sbSQL.Append("       tran.PORT_MST_POL_FK                         POLFK,         ");
            //6
            sbSQL.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            //7
            sbSQL.Append("       tran.PORT_MST_POD_FK                         PODFK,         ");
            //8
            sbSQL.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            //9
            sbSQL.Append("       NULL                                         AIR_PK,        ");
            //10
            sbSQL.Append("       ''                                           AIR_ID,        ");
            //11
            sbSQL.Append("       NULL                                         COMM_PK,       ");
            //12
            sbSQL.Append("       ''                                           COMM_ID,       ");
            //13
            sbSQL.Append("    " + BreakPoint.Kgs + "                          SLAB_TYPE_PK,  ");
            //14
            sbSQL.Append("    " + BPNT(BreakPoint.Kgs) + "                    SLAB_TYPE,     ");
            //15
            sbSQL.Append("       0                                            BOXES,         ");
            //16
            sbSQL.Append("       NULL                                         SLAB,          ");
            //17
            sbSQL.Append("       ''                                           SLAB_ID,       ");
            //18
            sbSQL.Append("       0                                            CH_WT,         ");
            //19
            sbSQL.Append("       0                                            AI_QT,         ");
            //20
            sbSQL.Append("       0                                            NET,           ");
            //21
            sbSQL.Append("       ''                                           OTH_DTL,       ");
            //22
            sbSQL.Append("       ''                                           OTH_BTN,       ");
            //23
            sbSQL.Append("       ''                                           CRG_BTN,       ");
            //24>
            sbSQL.Append("       'false'                                      SELECTED       ");
            //25>
            sbSQL.Append("   From TARIFF_MAIN_AIR_TBL            main,                    ");
            sbSQL.Append("        TARIFF_TRN_AIR_TBL             tran,                    ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOD                  ");
            sbSQL.Append("   Where                                                        ");
            sbSQL.Append("       main.TARIFF_MAIN_AIR_PK         =   tran.TARIFF_MAIN_AIR_FK  ");
            sbSQL.Append("   AND tran.PORT_MST_POL_FK            =   PORTPOL.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.PORT_MST_POD_FK            =   PORTPOD.PORT_MST_PK      ");
            sbSQL.Append("   AND main.ACTIVE                     =      1                     ");
            sbSQL.Append("   AND (tran.PORT_MST_POL_FK,tran.PORT_MST_POD_FK) in (" + Sectors + ")");
            sbSQL.Append("   AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)     ");
            sbSQL.Append("       between tran.VALID_FROM and nvl(tran.VALID_TO,NULL_DATE_FORMAT)    ");
            sbSQL.Append("   AND main.TARIFF_TYPE                = 2 ");

            if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
            {
                sbSQL.Append("  AND main.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup));
            }

            return sbSQL;
        }

        #endregion " General Tariff Query."

        #region " Customer Contract Query."

        private StringBuilder CustContQuery(string CustomerPk = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            StringBuilder sbSQL = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type
            sbSQL.Append("    Select                                                         ");
            sbSQL.Append("       tran.CONT_CUST_TRN_AIR_PK                    PK,            ");
            //0
            sbSQL.Append("       tran.CONT_CUST_AIR_FK                        FK,            ");
            //1
            sbSQL.Append("    " + SourceType.CustomerContract + "             REF_TYPE,      ");
            //2
            sbSQL.Append("    " + SRC(SourceType.CustomerContract) + "        TYPE_ID,       ");
            //3
            sbSQL.Append("       main.CONT_REF_NO                             REF_NO,        ");
            //4
            sbSQL.Append("       TO_CHAR(tran.VALID_TO,'" + dateFormat + "')  SHIP_DATE,     ");
            //5
            sbSQL.Append("       tran.PORT_MST_POL_FK                         POLFK,         ");
            //6
            sbSQL.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            //7
            sbSQL.Append("       tran.PORT_MST_POD_FK                         PODFK,         ");
            //8
            sbSQL.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            //9
            sbSQL.Append("       main.AIRLINE_MST_FK                          AIR_PK,        ");
            //10
            sbSQL.Append("       air.AIRLINE_ID                               AIR_ID,        ");
            //11
            sbSQL.Append("       main.COMMODITY_MST_FK                        COMM_PK,       ");
            //12
            sbSQL.Append("       cmdt.COMMODITY_ID                            COMM_ID,       ");
            //13
            sbSQL.Append("    " + BreakPoint.Kgs + "                          SLAB_TYPE_PK,  ");
            //14
            sbSQL.Append("    " + BPNT(BreakPoint.Kgs) + "                    SLAB_TYPE,     ");
            //15
            sbSQL.Append("       0                                            BOXES,         ");
            //16
            sbSQL.Append("       NULL                                         SLAB,          ");
            //17
            sbSQL.Append("       ''                                           SLAB_ID,       ");
            //18
            sbSQL.Append("       0                                            CH_WT,         ");
            //19
            sbSQL.Append("       0                                            AI_QT,         ");
            //20
            sbSQL.Append("       0                                            NET,           ");
            //21
            sbSQL.Append("       ''                                           OTH_DTL,       ");
            //22
            sbSQL.Append("       ''                                           OTH_BTN,       ");
            //23
            sbSQL.Append("       ''                                           CRG_BTN,       ");
            //24>
            sbSQL.Append("       'false'                                      SELECTED       ");
            //25>
            sbSQL.Append("   From CONT_CUST_AIR_TBL              main,                    ");
            sbSQL.Append("        CONT_CUST_TRN_AIR_TBL          tran,                    ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOD,                 ");
            sbSQL.Append("        AIRLINE_MST_TBL                air,                     ");
            sbSQL.Append("        COMMODITY_MST_TBL              cmdt                     ");
            sbSQL.Append("   Where                                                        ");
            sbSQL.Append("       main.CONT_CUST_AIR_PK           =   tran.CONT_CUST_AIR_FK    ");
            sbSQL.Append("   AND tran.PORT_MST_POL_FK            =   PORTPOL.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.PORT_MST_POD_FK            =   PORTPOD.PORT_MST_PK      ");
            sbSQL.Append("   AND main.COMMODITY_MST_FK           =   cmdt.COMMODITY_MST_PK(+) ");
            sbSQL.Append("   AND main.AIRLINE_MST_FK             =   air.AIRLINE_MST_PK(+)    ");
            sbSQL.Append("   AND main.CONT_APPROVED              =      2                     ");
            sbSQL.Append("   AND (tran.PORT_MST_POL_FK,tran.PORT_MST_POD_FK) in (" + Sectors + ")");
            sbSQL.Append("   AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)     ");
            sbSQL.Append("       between tran.VALID_FROM and nvl(tran.VALID_TO,NULL_DATE_FORMAT)    ");

            if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
            {
                //sbSQL.Append("  AND cmdt.COMMODITY_GROUP_FK    = " & CStr(CommodityGroup) & vbCrLf)
                sbSQL.Append("   AND MAIN.COMMODITY_GROUP_MST_FK    = " + Convert.ToString(CommodityGroup));
            }

            if (!string.IsNullOrEmpty(Convert.ToString(CustomerPk)))
            {
                sbSQL.Append("  AND ( main.CUSTOMER_MST_FK      = " + Convert.ToString(CustomerPk));
                sbSQL.Append("        or main.CUSTOMER_MST_FK  is null )");
            }

            return sbSQL;
        }

        #endregion " Customer Contract Query."

        #region " Quote Query."

        private StringBuilder QuoteQuery(string CustomerPk = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            StringBuilder sbSQL = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type

            sbSQL.Append("    Select                                                         ");
            sbSQL.Append("       tran.QUOTE_TRN_AIR_PK                        PK,            ");
            //0
            sbSQL.Append("       tran.QUOTATION_AIR_FK                        FK,            ");
            //1
            sbSQL.Append("    " + SourceType.Quotation + "                    REF_TYPE,      ");
            //2
            sbSQL.Append("    " + SRC(SourceType.Quotation) + "               TYPE_ID,       ");
            //3
            sbSQL.Append("       main.QUOTATION_REF_NO                        REF_NO,        ");
            //4
            sbSQL.Append("    TO_CHAR(main.EXPECTED_SHIPMENT,'" + dateFormat + "') SHIP_DATE, ");
            //5
            sbSQL.Append("       tran.PORT_MST_POL_FK                         POLFK,         ");
            //6
            sbSQL.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            //7
            sbSQL.Append("       tran.PORT_MST_POD_FK                         PODFK,         ");
            //8
            sbSQL.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            //9
            sbSQL.Append("       tran.AIRLINE_MST_FK                          AIR_PK,        ");
            //10
            sbSQL.Append("       air.AIRLINE_ID                               AIR_ID,        ");
            //11
            sbSQL.Append("       tran.COMMODITY_MST_FK                        COMM_PK,       ");
            //12
            sbSQL.Append("       cmdt.COMMODITY_ID                            COMM_ID,       ");
            //13
            sbSQL.Append("       slb.BREAKPOINT_TYPE                          SLAB_TYPE_PK,  ");
            //14
            sbSQL.Append("       decode(slb.BREAKPOINT_TYPE,");
            sbSQL.Append("   " + BreakPoint.Kgs + "," + BPNT(BreakPoint.Kgs));
            sbSQL.Append("       ," + BPNT(BreakPoint.ULD) + " )              SLAB_TYPE,     ");
            //15
            sbSQL.Append("       decode(slb.BREAKPOINT_TYPE," + BreakPoint.ULD);
            sbSQL.Append("      ,tran.QUANTITY,NULL)                          BOXES,         ");
            //16
            sbSQL.Append("       tran.SLAB_FK                                 SLAB,          ");
            //17
            sbSQL.Append("       slb.BREAKPOINT_ID                            SLAB_ID,       ");
            //18
            sbSQL.Append("       tran.CHARGEABLE_WEIGHT                       CH_WT,         ");
            //19
            sbSQL.Append("       0                                            AI_QT,         ");
            //20
            sbSQL.Append("       0                                            NET,           ");
            //21
            sbSQL.Append("       ''                                           OTH_DTL,       ");
            //22
            sbSQL.Append("       ''                                           OTH_BTN,       ");
            //23
            sbSQL.Append("       ''                                           CRG_BTN,       ");
            //24>
            sbSQL.Append("       'false'                                      SELECTED       ");
            //25>
            sbSQL.Append("   From QUOTATION_AIR_TBL              main,                    ");
            sbSQL.Append("        QUOTATION_TRN_AIR              tran,                    ");
            sbSQL.Append("        AIRFREIGHT_SLABS_TBL           slb,                     ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            sbSQL.Append("        PORT_MST_TBL                   PORTPOD,                 ");
            sbSQL.Append("        AIRLINE_MST_TBL                air,                     ");
            sbSQL.Append("        COMMODITY_MST_TBL              cmdt                     ");
            sbSQL.Append("   Where                                                        ");
            sbSQL.Append("       main.QUOTATION_AIR_PK           =   tran.QUOTATION_AIR_FK    ");
            sbSQL.Append("   AND tran.SLAB_FK              =   slb.AIRFREIGHT_SLABS_TBL_PK(+) ");
            sbSQL.Append("   AND tran.PORT_MST_POL_FK            =   PORTPOL.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.PORT_MST_POD_FK            =   PORTPOD.PORT_MST_PK      ");
            sbSQL.Append("   AND tran.COMMODITY_MST_FK           =   cmdt.COMMODITY_MST_PK(+) ");
            sbSQL.Append("   AND tran.AIRLINE_MST_FK             =   air.AIRLINE_MST_PK(+)    ");
            sbSQL.Append("   AND main.STATUS                    in   (1, 2, 3, 4)             ");
            sbSQL.Append("   AND (tran.PORT_MST_POL_FK,tran.PORT_MST_POD_FK) in (" + Sectors + ")");
            sbSQL.Append("   AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)     ");
            sbSQL.Append(" between main.QUOTATION_DATE AND (main.QUOTATION_DATE + main.VALID_FOR) ");

            if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
            {
                sbSQL.Append("  AND tran.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup));
            }

            if (!string.IsNullOrEmpty(Convert.ToString(CustomerPk)))
            {
                sbSQL.Append("  AND ( main.CUSTOMER_MST_FK    = " + Convert.ToString(CustomerPk));
                sbSQL.Append("        or main.CUSTOMER_MST_FK  is null ) ");
            }

            return sbSQL;
        }

        #endregion " Quote Query."

        #region " Manual Query."

        private StringBuilder ManualQuery(string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            StringBuilder strQuery = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type

            strQuery.Append(" Select                                               ");
            strQuery.Append("  ROWNUM                       PK,            ");
            strQuery.Append("   0                      FK,            ");
            strQuery.Append("                      7                REF_TYPE,      ");
            strQuery.Append("                     'Manual'           TYPE_ID,       ");
            strQuery.Append("       ''                           REF_NO,        ");
            strQuery.Append(" '' SHIP_DATE,     ");
            strQuery.Append("       PORTPOL.PORT_MST_PK                       POLFK,         ");
            strQuery.Append("       PORTPOL.PORT_ID                              POL_ID,        ");
            strQuery.Append("      PORTPOD.PORT_MST_PK                PODFK,         ");
            strQuery.Append("       PORTPOD.PORT_ID                              POD_ID,        ");
            strQuery.Append("       NULL                                         AIR_PK,        ");
            strQuery.Append("       ''                                           AIR_ID,        ");
            strQuery.Append("       ''                                           COMM_PK,       ");
            strQuery.Append("       ''                                           COMM_ID,       ");
            strQuery.Append("       1                          SLAB_TYPE_PK,  ");
            strQuery.Append("       'BP'                    SLAB_TYPE,     ");
            strQuery.Append("       0                                            BOXES,         ");
            strQuery.Append("       NULL                                         SLAB,          ");
            strQuery.Append("       ''                                           SLAB_ID,       ");
            strQuery.Append("       0                                            CH_WT,         ");
            strQuery.Append("       0                                            AI_QT,         ");
            strQuery.Append("       0                                            NET,           ");
            strQuery.Append("       ''                                           OTH_DTL,       ");
            strQuery.Append("       ''                                           OTH_BTN,       ");
            strQuery.Append("       ''                                           CRG_BTN,       ");
            strQuery.Append("       'false'                                      SELECTED       ");
            strQuery.Append("   From          ");
            strQuery.Append("        PORT_MST_TBL                   PORTPOL,                 ");
            strQuery.Append("        PORT_MST_TBL                   PORTPOD                  ");
            strQuery.Append("   Where                                                        ");
            strQuery.Append("   (PORTPOL.PORT_MST_PK,PORTPOD.PORT_MST_PK) in (" + Sectors + ")");

            return strQuery;
        }

        #endregion " Manual Query."

        #endregion " Header Query "

        #region " Freight Level Query "

        // These are to get freight details from different sources

        #region " Spot Rate Query "

        private StringBuilder SpotRateFreightQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();
            sbSQL.Append("    Select Q.* FROM (                                              ");
            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     trf.RFQ_SPOT_TRN_FREIGHT_PK               PK,                  ");
            sbSQL.Append("     trf.RFQ_SPOT_TRN_AIR_FK                   FK,                  ");
            sbSQL.Append("     trf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            sbSQL.Append("     frt.FREIGHT_ELEMENT_ID                    FRT_ID,              ");
            sbSQL.Append("     'false'                                   SELECTED,            ");
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            sbSQL.Append("     trf.CURRENCY_MST_FK                       CURR_FK,             ");
            sbSQL.Append("     cur.CURRENCY_ID                           CURR_ID,             ");
            sbSQL.Append("     trf.MIN_AMOUNT                            MIN_AMOUNT,          ");
            sbSQL.Append("     0                                         BASIS_RATE,          ");
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            sbSQL.Append("     3                                         CH_BASIS,            ");
            sbSQL.Append("  " + CBCS(ChargeBasis.KGs) + "                CH_BASIS_ID,         ");
            sbSQL.Append("  " + FreightType.AFC + "                      FRT_TYPE             ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     RFQ_SPOT_AIR_TRN_FREIGHT_TBL   trf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt,                            ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           trf.RFQ_SPOT_TRN_AIR_FK     in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   trf.FREIGHT_ELEMENT_MST_FK  =  frt.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   trf.CURRENCY_MST_FK         =  cur.CURRENCY_MST_PK(+)        ");

            sbSQL.Append( " UNION ALL ");
            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     srf.RFQ_SPOT_AIR_SURCHG_PK                PK,                  ");
            sbSQL.Append("     srf.RFQ_SPOT_AIR_TRN_FK                   FK,                  ");
            sbSQL.Append("     srf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            sbSQL.Append("     frt1.FREIGHT_ELEMENT_ID                   FRT_ID,              ");
            sbSQL.Append("     'false'                                   SELECTED,            ");
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            sbSQL.Append("     srf.CURRENCY_MST_FK                       CURR_FK,             ");
            sbSQL.Append("     cur1.CURRENCY_ID                          CURR_ID,             ");
            sbSQL.Append("     NULL                                      MIN_AMOUNT,          ");
            sbSQL.Append("     srf.APPROVED_RATE                         BASIS_RATE,          ");
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            sbSQL.Append("     srf.CHARGE_BASIS                          CH_BASIS,            ");
            sbSQL.Append("     decode(srf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,     ");
            sbSQL.Append("  " + FreightType.Surcharge + "                FRT_TYPE             ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     RFQ_SPOT_AIR_SURCHARGE         srf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt1,                           ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur1                            ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           srf.RFQ_SPOT_AIR_TRN_FK     in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   srf.FREIGHT_ELEMENT_MST_FK  =  frt1.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   srf.CURRENCY_MST_FK         =  cur1.CURRENCY_MST_PK(+)     ");
            sbSQL.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT        ");
            sbSQL.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_FK      ");
            sbSQL.Append("     ORDER BY FRT.PREFERENCE       ");

            return sbSQL;
        }

        private StringBuilder SpotRateOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     otf.RFQ_TRN_AIR_OTH_CHRG_PK               PK,                  ");
            //0
            sbSQL.Append("     otf.RFQ_SPOT_AIR_TRN_FK                   FK,                  ");
            //1
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            //3
            sbSQL.Append("     otf.APPROVED_RATE                         BASIS_RATE,          ");
            //4
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //5
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            //6
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,      ");
            //7
            sbSQL.Append("    1 PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     RFQ_SPOT_AIR_OTH_CHRG          otf                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.RFQ_SPOT_AIR_TRN_FK     in (" + ParentKeys + ")      ");

            return sbSQL;
        }

        private StringBuilder GetSpotRateCargoQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                    " + "        ROWNUM                             SNo,                " + "        CARGO_NOP                          NOP,                " + "        CARGO_LENGTH                       Length,             " + "        CARGO_WIDTH                        Width,              " + "        CARGO_HEIGHT                       Height,             " + "        CARGO_CUBE                         Cube,               " + "        CARGO_VOLUME_WT                    VolWeight,          " + "        CARGO_ACTUAL_WT                    ActWeight,          " + "        CARGO_DENSITY                      Density,            " + "        1                                  PK,                 " + "        RFQ_SPOT_AIR_TRN_FK                FK                  " + "     FROM                                                      " + "        RFQ_SPOT_AIR_CARGO_CALC                                " + "     WHERE                                                     " + "        RFQ_SPOT_AIR_TRN_FK     in   (" + ParentKeys + ")      ");

            return sbSQL;
        }

        #endregion " Spot Rate Query "

        #region " Airline Tariff Query "

        private StringBuilder AirTariffFreightQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();
            sbSQL.Append("    Select Q.* FROM (                                              ");
            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     trf.TARIFF_TRN_FREIGHT_PK                 PK,                  ");
            //0
            sbSQL.Append("     trf.TARIFF_TRN_AIR_FK                     FK,                  ");
            //1
            sbSQL.Append("     trf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt.FREIGHT_ELEMENT_ID                    FRT_ID,              ");
            //3
            sbSQL.Append("     'false'                                   SELECTED,            ");
            //4
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            //5'
            sbSQL.Append("     trf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6'5
            sbSQL.Append("     cur.CURRENCY_ID                           CURR_ID,             ");
            //7'6
            sbSQL.Append("     trf.MIN_AMOUNT                            MIN_AMOUNT,          ");
            //8'7
            sbSQL.Append("     0                                         BASIS_RATE,          ");
            //9'8
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //10'9
            sbSQL.Append("     3                                         CH_BASIS,            ");
            //11'10
            sbSQL.Append("  " + CBCS(ChargeBasis.KGs) + "                CH_BASIS_ID,         ");
            //12'11
            sbSQL.Append("  " + FreightType.AFC + "                      FRT_TYPE             ");
            //13'12
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     TARIFF_TRN_AIR_FREIGHT_TBL     trf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt,                            ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           trf.TARIFF_TRN_AIR_FK       in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   trf.FREIGHT_ELEMENT_MST_FK  =  frt.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   trf.CURRENCY_MST_FK         =  cur.CURRENCY_MST_PK(+)        ");

            sbSQL.Append( " UNION ALL ");

            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     srf.TARIFF_AIR_SURCHARGE_PK               PK,                  ");
            //0
            sbSQL.Append("     srf.TARIFF_TRN_AIR_FK                     FK,                  ");
            //1
            sbSQL.Append("     srf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt1.FREIGHT_ELEMENT_ID                   FRT_ID,              ");
            //3
            sbSQL.Append("     'false'                                    SELECTED,            ");
            //4
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            //5'
            sbSQL.Append("     srf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6'5
            sbSQL.Append("     cur1.CURRENCY_ID                          CURR_ID,             ");
            //7'6
            sbSQL.Append("     NULL                                      MIN_AMOUNT,          ");
            //8'7
            sbSQL.Append("     srf.TARIFF_RATE                           BASIS_RATE,          ");
            //9'8
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //10'9
            sbSQL.Append("     srf.CHARGE_BASIS                          CH_BASIS,            ");
            //11'10
            sbSQL.Append("     decode(srf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,     ");
            //12'11
            sbSQL.Append("  " + FreightType.Surcharge + "                FRT_TYPE             ");
            //13'12
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     TARIFF_TRN_AIR_SURCHARGE       srf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt1,                           ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur1                            ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           srf.TARIFF_TRN_AIR_FK       in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   srf.FREIGHT_ELEMENT_MST_FK  =  frt1.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   srf.CURRENCY_MST_FK         =  cur1.CURRENCY_MST_PK(+)        ");
            sbSQL.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT        ");
            sbSQL.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_FK      ");
            sbSQL.Append("     ORDER BY FRT.PREFERENCE       ");
            return sbSQL;
        }

        private StringBuilder AirTariffOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     NULL                                      PK,                  ");
            //0
            sbSQL.Append("     otf.TARIFF_TRN_AIR_FK                     FK,                  ");
            //1
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            //3
            sbSQL.Append("     otf.TARIFF_RATE                           BASIS_RATE,          ");
            //4
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //5
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            //6
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,      ");
            //7
            sbSQL.Append("    1 PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     TARIFF_TRN_AIR_OTH_CHRG         otf                            ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.TARIFF_TRN_AIR_FK     in (" + ParentKeys + ")        ");

            return sbSQL;
        }

        #endregion " Airline Tariff Query "

        #region " General Tariff Query "

        private StringBuilder GenTariffFreightQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select Q.* FROM (                                              ");
            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     trf.TARIFF_TRN_FREIGHT_PK                 PK,                  ");
            //0
            sbSQL.Append("     trf.TARIFF_TRN_AIR_FK                     FK,                  ");
            //1
            sbSQL.Append("     trf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt.FREIGHT_ELEMENT_ID                    FRT_ID,              ");
            //3
            sbSQL.Append("     'false'                                   SELECTED,            ");
            //4
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            //5 '
            sbSQL.Append("     trf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6'5
            sbSQL.Append("     cur.CURRENCY_ID                           CURR_ID,             ");
            //7'6
            sbSQL.Append("     trf.MIN_AMOUNT                            MIN_AMOUNT,          ");
            //8'7
            sbSQL.Append("     0                                         BASIS_RATE,          ");
            //9'8
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //10'9
            sbSQL.Append("     3                                         CH_BASIS,            ");
            //11'10
            sbSQL.Append("  " + CBCS(ChargeBasis.KGs) + "                CH_BASIS_ID,         ");
            //12'11
            sbSQL.Append("  " + FreightType.AFC + "                      FRT_TYPE             ");
            //13'12
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     TARIFF_TRN_AIR_FREIGHT_TBL     trf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt,                            ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           trf.TARIFF_TRN_AIR_FK       in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   trf.FREIGHT_ELEMENT_MST_FK  =  frt.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   trf.CURRENCY_MST_FK         =  cur.CURRENCY_MST_PK(+)        ");

            sbSQL.Append( " UNION ALL ");

            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     srf.TARIFF_AIR_SURCHARGE_PK               PK,                  ");
            //0
            sbSQL.Append("     srf.TARIFF_TRN_AIR_FK                     FK,                  ");
            //1
            sbSQL.Append("     srf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt1.FREIGHT_ELEMENT_ID                   FRT_ID,              ");
            //3
            sbSQL.Append("     'false'                                   SELECTED,            ");
            //4
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            //5 '
            sbSQL.Append("     srf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6'5
            sbSQL.Append("     cur1.CURRENCY_ID                          CURR_ID,             ");
            //7'6
            sbSQL.Append("     NULL                                      MIN_AMOUNT,          ");
            //8'7
            sbSQL.Append("     srf.TARIFF_RATE                           BASIS_RATE,          ");
            //9'8
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //10'9
            sbSQL.Append("     srf.CHARGE_BASIS                          CH_BASIS,            ");
            //11'10
            sbSQL.Append("     decode(srf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,   ");
            //12'11
            sbSQL.Append("  " + FreightType.Surcharge + "                FRT_TYPE             ");
            //13'12
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     TARIFF_TRN_AIR_SURCHARGE       srf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt1,                           ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur1                            ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           srf.TARIFF_TRN_AIR_FK       in (" + ParentKeys + ")           ");
            sbSQL.Append("     and   srf.FREIGHT_ELEMENT_MST_FK  =  frt1.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   srf.CURRENCY_MST_FK         =  cur1.CURRENCY_MST_PK(+)        ");
            sbSQL.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT        ");
            sbSQL.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_FK      ");
            sbSQL.Append("     ORDER BY FRT.PREFERENCE       ");
            return sbSQL;
        }

        private StringBuilder GenTariffOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     NULL                                      PK,                  ");
            //0
            sbSQL.Append("     otf.TARIFF_TRN_AIR_FK                     FK,                  ");
            //1
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            //3
            sbSQL.Append("     otf.TARIFF_RATE                           BASIS_RATE,          ");
            //4
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //5
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            //6
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,      ");
            //7
            sbSQL.Append("    1 PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     TARIFF_TRN_AIR_OTH_CHRG         otf                            ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.TARIFF_TRN_AIR_FK     in (" + ParentKeys + ")        ");

            return sbSQL;
        }

        #endregion " General Tariff Query "

        #region " Customer Contract Query "

        private StringBuilder CustContFreightQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Min Amt, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type

            //CONT_CUST_AIR_FREIGHT_PK, CONT_CUST_TRN_AIR_FK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, MIN_AMOUNT
            //CONT_AIR_BREAKPTS_PK, CURRENT_RATE, APPROVED_RATE, AIRFREIGHT_SLABS_FK, CONT_CUST_AIR_FRT_FK

            sbSQL.Append("    Select Q.* FROM (                                              ");
            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     trf.CONT_CUST_AIR_FREIGHT_PK              PK,                  ");
            //0
            sbSQL.Append("     trf.CONT_CUST_TRN_AIR_FK                  FK,                  ");
            //1
            sbSQL.Append("     trf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt.FREIGHT_ELEMENT_ID                    FRT_ID,              ");
            //3
            sbSQL.Append("     'false'                                   SELECTED,            ");
            //4
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            //5 ''Added Koteshwari for Advatos Column PTSID-JUN18
            sbSQL.Append("     trf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6'5
            sbSQL.Append("     cur.CURRENCY_ID                           CURR_ID,             ");
            //7'6
            sbSQL.Append("     trf.MIN_AMOUNT                            MIN_AMOUNT,          ");
            //8'7
            sbSQL.Append("     0                                         BASIS_RATE,          ");
            //9'8
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //10'9
            sbSQL.Append("     3                                         CH_BASIS,            ");
            //11'10
            sbSQL.Append("  " + CBCS(ChargeBasis.KGs) + "                CH_BASIS_ID,         ");
            //12'11
            sbSQL.Append("  " + FreightType.AFC + "                      FRT_TYPE             ");
            //13'12
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     CONT_CUST_AIR_FREIGHT_TBL      trf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt,                            ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           trf.CONT_CUST_TRN_AIR_FK    in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   trf.FREIGHT_ELEMENT_MST_FK  =  frt.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   trf.CURRENCY_MST_FK         =  cur.CURRENCY_MST_PK(+)        ");

            sbSQL.Append( " UNION ALL ");

            //CONT_AIR_SURCHARGE_PK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, CONT_CUST_TRN_AIR_FK, CURRENT_RATE, APPROVED_RATE, CHARGE_BASIS

            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     srf.CONT_AIR_SURCHARGE_PK                 PK,                  ");
            //0
            sbSQL.Append("     srf.CONT_CUST_TRN_AIR_FK                  FK,                  ");
            //1
            sbSQL.Append("     srf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt1.FREIGHT_ELEMENT_ID                   FRT_ID,              ");
            //3
            sbSQL.Append("     'false'                                   SELECTED,            ");
            //4
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            //5 ''Added Koteshwari for Advatos Column PTSID-JUN18
            sbSQL.Append("     srf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6'5
            sbSQL.Append("     cur1.CURRENCY_ID                          CURR_ID,             ");
            //7'6
            sbSQL.Append("     NULL                                      MIN_AMOUNT,          ");
            //8'7
            sbSQL.Append("     srf.APPROVED_RATE                         BASIS_RATE,          ");
            //9'8
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //10'9
            sbSQL.Append("     srf.CHARGE_BASIS                          CH_BASIS,            ");
            //11'10
            sbSQL.Append("     decode(srf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,     ");
            //12'11
            sbSQL.Append("  " + FreightType.Surcharge + "                FRT_TYPE             ");
            //13'12
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     CONT_CUST_AIR_SURCHARGE        srf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt1,                           ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur1                            ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           srf.CONT_CUST_TRN_AIR_FK    in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   srf.FREIGHT_ELEMENT_MST_FK  =  frt1.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   srf.CURRENCY_MST_FK         =  cur1.CURRENCY_MST_PK(+)        ");
            sbSQL.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT        ");
            sbSQL.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_FK      ");
            sbSQL.Append("     ORDER BY FRT.PREFERENCE       ");
            return sbSQL;
        }

        private StringBuilder CustContOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();
            //CONT_CUST_TRN_AIR_FK, FREIGHT_ELEMENT_MST_FK, CURRENCY_MST_FK, CURRENT_RATE, APPROVED_RATE, CHARGE_BASIS
            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     NULL                                      PK,                  ");
            //0
            sbSQL.Append("     otf.CONT_CUST_TRN_AIR_FK                  FK,                  ");
            //1
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            //3
            sbSQL.Append("     otf.APPROVED_RATE                         BASIS_RATE,          ");
            //4
            sbSQL.Append("     0                                         TARIFF_RATE,         ");
            //5
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            //6
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,      ");
            //7
            sbSQL.Append("    1 PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     CONT_CUST_AIR_OTH_CHRG         otf                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.CONT_CUST_TRN_AIR_FK    in (" + ParentKeys + ")      ");

            return sbSQL;
        }

        #endregion " Customer Contract Query "

        #region " Quote Query "

        private StringBuilder QuoteFreightQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Min Amt, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type

            sbSQL.Append("    Select Q.* FROM (                                              ");
            sbSQL.Append("    Select                                                       ");
            sbSQL.Append("     trf.QUOTE_TRN_AIR_FRT_PK                  PK,                  ");
            //0
            sbSQL.Append("     trf.QUOTE_TRN_AIR_FK                      FK,                  ");
            //1
            sbSQL.Append("     trf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     frt.FREIGHT_ELEMENT_ID                    FRT_ID,              ");
            //3
            sbSQL.Append("     'false'                                   SELECTED,            ");
            //4
            sbSQL.Append("     'false'                                   ADVATOS,             ");
            //5 ''Added Koteshwari for Advatos Column PTSID-JUN18
            sbSQL.Append("     trf.CURRENCY_MST_FK                       CURR_FK,             ");
            //6'5
            sbSQL.Append("     cur.CURRENCY_ID                           CURR_ID,             ");
            //7'6
            sbSQL.Append("     0                                         MIN_AMOUNT,          ");
            //8'7
            sbSQL.Append("     trf.BASIS_RATE                            BASIS_RATE,          ");
            //9'8
            sbSQL.Append("     trf.QUOTED_RATE                           TARIFF_RATE,         ");
            //10'9
            sbSQL.Append("     trf.CHARGE_BASIS                          CH_BASIS,            ");
            //11'10
            sbSQL.Append("     decode(trf.CHARGE_BASIS,1,'%',2,'Flat','KGs') CH_BASIS_ID,     ");
            //12'11
            sbSQL.Append("     trf.FREIGHT_TYPE                          FRT_TYPE             ");
            //13'12
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     QUOTATION_TRN_AIR_FRT_DTLS     trf,                            ");
            sbSQL.Append("     FREIGHT_ELEMENT_MST_TBL        frt,                            ");
            sbSQL.Append("     CURRENCY_TYPE_MST_TBL          cur                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           trf.QUOTE_TRN_AIR_FK        in (" + ParentKeys + ")          ");
            sbSQL.Append("     and   trf.FREIGHT_ELEMENT_MST_FK  =  frt.FREIGHT_ELEMENT_MST_PK(+) ");
            sbSQL.Append("     and   trf.CURRENCY_MST_FK         =  cur.CURRENCY_MST_PK(+)        ");
            sbSQL.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT        ");
            sbSQL.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_FK      ");
            sbSQL.Append("     ORDER BY FRT.PREFERENCE       ");
            return sbSQL;
        }

        private StringBuilder QuoteOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     otf.QUOTE_TRN_AIR_OTH_PK                  PK,                  ");
            //0
            sbSQL.Append("     otf.QUOTATION_TRN_AIR_FK                  FK,                  ");
            //1
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            //3
            sbSQL.Append("     otf.BASIS_RATE                            BASIS_RATE,          ");
            //4
            sbSQL.Append("     otf.AMOUNT                                TARIFF_RATE,         ");
            //5
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            //6
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs')   CH_BASIS_ID,          ");
            //7
            sbSQL.Append("    1 PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     QUOTATION_TRN_AIR_OTH_CHRG     otf                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.QUOTATION_TRN_AIR_FK   in (" + ParentKeys + ")       ");

            return sbSQL;
        }

        #endregion " Quote Query "

        #region " Manual Query "

        private StringBuilder ManualFreightQuery(string ParentKeys = "-1", bool AmendFlg = false)
        {
            StringBuilder strQuery = new StringBuilder();
            // Type, Ref No, Shipment Date, POL, POD, Airline, Commodity, KG/ULD, Boxes, Slab, ChrgWt, All in Quote, Net, OthBtn, CargoBtn
            // + Freight, Sel, Curr, Min Amt, Trf Rate, Trf Amt, Quote Rate, Quote Amt, Charge Basis, Freight Type
            if (AmendFlg == false)
            {
                strQuery.Append("    Select Q.* FROM (                                              ");
                strQuery.Append(" SELECT DISTINCT ROWNUM PK,");
                strQuery.Append("                 1 FK,");
                strQuery.Append("                 FREIGHT_ELEMENT_MST_PK FRT_FK,");
                strQuery.Append("                 FRT.FREIGHT_ELEMENT_ID FRT_ID,");
                strQuery.Append("                 'false' SELECTED,");
                strQuery.Append("                 'false' ADVATOS,");
                //'Added Koteshwari for Advatos Column PTSID-JUN18
                strQuery.Append("                 CUR.CURRENCY_MST_PK CURR_FK,");
                strQuery.Append("                 CUR.CURRENCY_ID CURR_ID,");
                strQuery.Append("                 0 MIN_AMOUNT,");
                strQuery.Append("                 0 BASIS_RATE,");
                strQuery.Append("                 0 TARIFF_RATE,");
                strQuery.Append("                 3 CH_BASIS,");
                strQuery.Append("                 'KGs' CH_BASIS_ID,");
                strQuery.Append("                 1 FRT_TYPE");
                strQuery.Append("   FROM                           ");
                strQuery.Append("         FREIGHT_ELEMENT_MST_TBL FRT,");
                strQuery.Append("        CURRENCY_TYPE_MST_TBL   CUR");
                strQuery.Append("  WHERE CUR.CURRENCY_MST_PK = " + BaseCurrency);
                strQuery.Append("    AND FRT.BUSINESS_TYPE IN (1, 3)");
                strQuery.Append("     AND FRT.CHARGE_BASIS <> 2");
                strQuery.Append("     AND FRT.ACTIVE_FLAG=1        ");
                strQuery.Append("     ) Q, FREIGHT_ELEMENT_MST_TBL FRT        ");
                strQuery.Append("     WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_FK      ");
                strQuery.Append("     ORDER BY FRT.PREFERENCE       ");
            }
            else
            {
                strQuery.Append("  SELECT ROWNUM PK, R.* FROM(");
                strQuery.Append("  SELECT Q.* FROM (SELECT DISTINCT ");
                strQuery.Append("                        1 FK,");
                strQuery.Append("                        FREIGHT_ELEMENT_MST_PK FRT_FK,");
                strQuery.Append("                        FRT.FREIGHT_ELEMENT_ID FRT_ID,");
                strQuery.Append("                        'false' SELECTED,");
                strQuery.Append("                        'false' ADVATOS,");
                strQuery.Append("                        CUR.CURRENCY_MST_PK CURR_FK,");
                strQuery.Append("                        CUR.CURRENCY_ID CURR_ID,");
                strQuery.Append("                        0 MIN_AMOUNT,");
                strQuery.Append("                        0 BASIS_RATE,");
                strQuery.Append("                        0 TARIFF_RATE,");
                strQuery.Append("                        3 CH_BASIS,");
                strQuery.Append("                        'KGs' CH_BASIS_ID,");
                strQuery.Append("                        1 FRT_TYPE");
                strQuery.Append("          FROM FREIGHT_ELEMENT_MST_TBL FRT, CURRENCY_TYPE_MST_TBL CUR");
                strQuery.Append("         WHERE CUR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"] + "");
                strQuery.Append("           AND FRT.BUSINESS_TYPE IN (1, 3)");
                strQuery.Append("           AND FRT.CHARGE_BASIS <> 2");
                strQuery.Append("           AND FRT.ACTIVE_FLAG = 1");
                strQuery.Append("           AND FRT.FREIGHT_ELEMENT_MST_PK NOT IN");
                strQuery.Append("               (SELECT TRF.FREIGHT_ELEMENT_MST_FK FRT_FK");
                strQuery.Append("                  FROM QUOTATION_TRN_AIR_FRT_DTLS TRF,");
                strQuery.Append("                       FREIGHT_ELEMENT_MST_TBL    FRT,");
                strQuery.Append("                       CURRENCY_TYPE_MST_TBL      CUR");
                strQuery.Append("                 WHERE TRF.QUOTE_TRN_AIR_FK IN (" + ParentKeys + ")");
                strQuery.Append("                   AND TRF.FREIGHT_ELEMENT_MST_FK =");
                strQuery.Append("                       FRT.FREIGHT_ELEMENT_MST_PK(+)");
                strQuery.Append("                   AND TRF.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+))) Q,");
                strQuery.Append("       FREIGHT_ELEMENT_MST_TBL FRT");
                strQuery.Append("  WHERE FRT.FREIGHT_ELEMENT_MST_PK = Q.FRT_FK");
                strQuery.Append("  UNION ");
                strQuery.Append("  SELECT DISTINCT ");
                strQuery.Append("                1 FK,");
                strQuery.Append("                TRF.FREIGHT_ELEMENT_MST_FK FRT_FK,");
                strQuery.Append("                FRT.FREIGHT_ELEMENT_ID FRT_ID,");
                strQuery.Append("                DECODE(TRF.CHECK_FOR_ALL_IN_RT, 1, 'true', 'false') SELECTED,");
                strQuery.Append("                DECODE(TRF.CHECK_ADVATOS, 1, 'true', 'false') ADVATOS,");
                strQuery.Append("                TRF.CURRENCY_MST_FK CURR_FK,");
                strQuery.Append("                CUR.CURRENCY_ID CURR_ID,");
                strQuery.Append("                TRF.BASIS_RATE MIN_AMOUNT,");
                strQuery.Append("                TRF.BASIS_RATE BASIS_RATE,");
                strQuery.Append("                TRF.TARIFF_RATE TARIFF_RATE,");
                strQuery.Append("                TRF.CHARGE_BASIS CH_BASIS,");
                strQuery.Append("                DECODE(TRF.CHARGE_BASIS, 1, '%', 2, 'Flat', 'KGs') CH_BASIS_ID,");
                strQuery.Append("                1 FRT_TYPE");
                strQuery.Append("  FROM QUOTATION_TRN_AIR_FRT_DTLS TRF,");
                strQuery.Append("       QUOTATION_TRN_AIR          TRAN,");
                strQuery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT,");
                strQuery.Append("       CURRENCY_TYPE_MST_TBL      CUR");
                strQuery.Append(" WHERE TRF.QUOTE_TRN_AIR_FK IN (" + ParentKeys + ")");
                strQuery.Append("   AND TRAN.QUOTE_TRN_AIR_PK = TRF.QUOTE_TRN_AIR_FK");
                strQuery.Append("   AND TRF.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK(+)");
                strQuery.Append("   AND TRF.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)");
                strQuery.Append("   ) R");
            }
            return strQuery;
        }

        private StringBuilder ManualOthQuery(string ParentKeys = "-1")
        {
            StringBuilder sbSQL = new StringBuilder();

            sbSQL.Append(" SELECT -1 PK,");
            sbSQL.Append("        -1 FK,");
            sbSQL.Append("        FRT.FREIGHT_ELEMENT_MST_PK FRT_FK,");
            sbSQL.Append("        CUR.CURRENCY_MST_PK CURR_FK,");
            sbSQL.Append("        '' BASIS_RATE,");
            sbSQL.Append("        '' TARIFF_RATE,");
            sbSQL.Append("        2 CH_BASIS,");
            sbSQL.Append("        DECODE(FRT.CHARGE_BASIS, 1, '%', 2, 'Flat', 'KGs') CH_BASIS_ID,");
            sbSQL.Append("    1 PYMT_TYPE      ");
            sbSQL.Append("   FROM FREIGHT_ELEMENT_MST_TBL FRT, CURRENCY_TYPE_MST_TBL CUR");
            sbSQL.Append("  WHERE CUR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"]);
            sbSQL.Append("    AND FRT.BUSINESS_TYPE IN (1, 3)");
            sbSQL.Append("    AND FRT.CHARGE_BASIS = 2");
            sbSQL.Append("    AND FRT.ACTIVE_FLAG = 1");
            sbSQL.Append("  ORDER BY FREIGHT_ELEMENT_ID");
            return sbSQL;
        }

        private DataTable ManualFreightTable(DataTable dt, string strPks)
        {
            DataRow dr = null;
            DataTable frtDt = new DataTable();
            int pkCount = 0;
            int drCount = 0;
            frtDt.Columns.Add("PK");
            frtDt.Columns["PK"].DataType = dt.Columns["PK"].DataType;
            frtDt.Columns.Add("FK");
            frtDt.Columns["FK"].DataType = dt.Columns["FK"].DataType;
            frtDt.Columns.Add("FRT_FK");
            frtDt.Columns.Add("FRT_ID");
            frtDt.Columns.Add("SELECTED");
            frtDt.Columns.Add("ADVATOS");
            frtDt.Columns.Add("CURR_FK");
            frtDt.Columns.Add("CURR_ID");
            frtDt.Columns.Add("MIN_AMOUNT");
            frtDt.Columns.Add("BASIS_RATE");
            frtDt.Columns.Add("TARIFF_RATE");
            frtDt.Columns.Add("CH_BASIS");
            frtDt.Columns.Add("CH_BASIS_ID");
            frtDt.Columns.Add("FRT_TYPE");
            string[] strPksArr = strPks.ToString().Split(',');
            if (strPksArr.Length == 2)
                return dt;
            if (strPksArr.Length > 2)
            {
                for (pkCount = 1; pkCount <= strPksArr.Length - 1; pkCount++)
                {
                    for (drCount = 0; drCount <= dt.Rows.Count - 1; drCount++)
                    {
                        dr = frtDt.NewRow();
                        dr[0] = drCount + (dt.Rows.Count * (pkCount - 1));
                        dr[1] = strPksArr[pkCount];
                        dr[2] = dt.Rows[drCount][2];
                        dr[3] = dt.Rows[drCount][3];
                        dr[4] = dt.Rows[drCount][4];
                        dr[5] = dt.Rows[drCount][5];
                        dr[6] = dt.Rows[drCount][6];
                        dr[7] = dt.Rows[drCount][7];
                        dr[8] = dt.Rows[drCount][8];
                        dr[9] = dt.Rows[drCount][9];
                        dr[10] = dt.Rows[drCount][10];
                        dr[11] = dt.Rows[drCount][11];
                        dr[12] = dt.Rows[drCount][12];
                        dr[13] = dt.Rows[drCount][13];
                        frtDt.Rows.Add(dr);
                    }
                }
            }
            return frtDt;
        }

        #endregion " Manual Query "

        #endregion " Freight Level Query "

        #endregion " Fetch UWG1 Option grid. "

        #region " Main Fetch Query "

        public void FetchOne(DataSet GridDS = null, DataSet EnqDS = null, string EnqNo = "", string QuoteNo = "", string CustNo = "", string CustID = "", string CustCategory = "", string AgentNo = "", string AgentID = "", string Sectors = "",
        string CommodityGroup = "", string ShipDate = "", string QuoteDate = "", string Options = null, int Version = 0, string QuotationStatus = null, DataTable OthDt = null, DataTable EnqOthDt = null, DataSet CalcDS = null, string ValidFor = null,
        Int16 CustomerType = 0, int CreditDays = 0, int CreditLimit = 0, int Remarks = 0, int CargoMcode = 0, int BaseCurrencyId = 0, int INCOTerms = 0, int PYMTType = 0, bool AmendFlg = false)
        {
            DataRow DR = null;
            DataTable ExChTable = null;
            decimal Amount = default(decimal);

            try
            {
                if (string.IsNullOrEmpty(EnqNo) & string.IsNullOrEmpty(QuoteNo))
                {
                    Array Arr = null;
                    Arr = Convert.ToString(Sectors).Split('~');
                    Sectors = Cls_SRRAirContract.MakePortPairString(Convert.ToString(Arr.GetValue(0)),Convert.ToString(Arr.GetValue(1)));
                }
                CalcDS = null;
                GetEnqDetail(EnqNo, CustNo, CustID, CustCategory, AgentNo, AgentID, CommodityGroup, Sectors, EnqDS, QuoteNo,
                Version, QuotationStatus, EnqOthDt, CalcDS, ValidFor, QuoteDate, CustomerType, ShipDate, CreditDays, CreditLimit,
                Remarks, CargoMcode, BaseCurrencyId, INCOTerms, PYMTType, AmendFlg);

                foreach (DataRow DR_loopVariable in EnqDS.Tables[0].Rows)
                {
                    DR = DR_loopVariable;
                    DR["OTH_DTL"] = Cls_FlatRateFreights.GetOTHstring(EnqOthDt, 2, 3, 6, 4, 1, Convert.ToString(DR["PK"]), Convert.ToDecimal(getDefault(DR["CH_WT"], 0)), Amount, ExChTable,
                    8);
                    DR["OTH_BTN"] = Amount;
                }

                StringBuilder MasterQuery = new StringBuilder();
                StringBuilder FreightQuery = new StringBuilder();
                StringBuilder OthQuery = new StringBuilder();
                StringBuilder CargoQuery = new StringBuilder();
                StringBuilder TransactionPKs = new StringBuilder();
                WorkFlow objWF = new WorkFlow();

                if (!string.IsNullOrEmpty(Convert.ToString(QuoteNo).Trim()))
                {
                    OthDt = EnqOthDt;
                    if (AmendFlg == true)
                    {
                        //((System.Web.UI.WebControls.RadioButtonList)Options).Items[5].Selected = true;
                        //TransactionPKs = GetAllKeys(EnqDS.Tables[0], 0);
                    }
                    else
                    {
                        return;
                    }
                }

                if (AmendFlg == false)
                {
                    //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                    //{
                    //    MasterQuery = SpotRateQuery(CustNo, Sectors, CommodityGroup, ShipDate);
                    //}
                    //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                    //{
                    //    MasterQuery = CustContQuery(CustNo, Sectors, CommodityGroup, ShipDate);
                    //}
                    //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                    //{
                    //    MasterQuery = QuoteQuery(CustNo, Sectors, CommodityGroup, ShipDate);
                    //}
                    //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                    //{
                    //    MasterQuery = AirTariffQuery(Sectors, CommodityGroup, ShipDate);
                    //}
                    //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                    //{
                    //    MasterQuery = GenTariffQuery(Sectors, CommodityGroup, ShipDate);
                    //}
                    //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[5].Selected)
                    //{
                    //    MasterQuery = ManualQuery(Sectors, CommodityGroup, ShipDate);
                    //}
                }
                else
                {
                    MasterQuery = ManualQuery(Sectors, CommodityGroup, ShipDate);
                }
                GridDS = objWF.GetDataSet(MasterQuery.ToString());
                //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //{
                //    foreach (DataRow DR_loopVariable in GridDS.Tables[0].Rows)
                //    {
                //        DR = DR_loopVariable;
                //        if (DR["SLAB_TYPE_PK"] == BreakPoint.ULD)
                //        {
                //            DR["SLAB_TYPE_PK"] = DR["SLAB"];
                //            DR["SLAB_TYPE"] = DR["SLAB_ID"];
                //        }
                //    }
                //}
                //if (AmendFlg == false)
                //{
                //    TransactionPKs = GetAllKeys(GridDS.Tables[0], 0);
                //    if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                //    {
                //        FreightQuery = SpotRateFreightQuery(TransactionPKs.ToString());
                //        OthQuery = SpotRateOthQuery(TransactionPKs.ToString());
                //        CargoQuery = GetSpotRateCargoQuery(TransactionPKs.ToString());
                //    }
                //    else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                //    {
                //        FreightQuery = CustContFreightQuery(TransactionPKs.ToString());
                //        OthQuery = CustContOthQuery(TransactionPKs.ToString());
                //    }
                //    else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //    {
                //        FreightQuery = QuoteFreightQuery(TransactionPKs.ToString());
                //        OthQuery = QuoteOthQuery(TransactionPKs.ToString());
                //        CargoQuery = GetQuoteCargoQuery(TransactionPKs.ToString(), 1);
                //    }
                //    else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                //    {
                //        FreightQuery = AirTariffFreightQuery(TransactionPKs.ToString());
                //        OthQuery = AirTariffOthQuery(TransactionPKs.ToString());
                //    }
                //    else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                //    {
                //        FreightQuery = GenTariffFreightQuery(TransactionPKs.ToString());
                //        OthQuery = GenTariffOthQuery(TransactionPKs.ToString());
                //    }
                //    else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[5].Selected)
                //    {
                //        FreightQuery = ManualFreightQuery(TransactionPKs.ToString(), AmendFlg);
                //        OthQuery = ManualOthQuery(getDefault(TransactionPKs.ToString(), 0));
                //    }
                //    if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[5].Selected)
                //    {
                //        GridDS.Tables.Add(ManualFreightTable(objWF.GetDataTable(FreightQuery.ToString().Replace("  ", " ")), TransactionPKs.ToString()));
                //    }
                //    else
                //    {
                //        GridDS.Tables.Add(objWF.GetDataTable(FreightQuery.ToString().Replace("  ", " ")));
                //    }
                //}
                //else
                //{
                //    FreightQuery = ManualFreightQuery(TransactionPKs.ToString(), AmendFlg);
                //    OthQuery = ManualOthQuery(getDefault(TransactionPKs.ToString(), 0));
                //    GridDS.Tables.Add(ManualFreightTable(objWF.GetDataTable(FreightQuery.ToString().Replace("  ", " ")), TransactionPKs.ToString()));
                //}
                OthDt = objWF.GetDataTable(OthQuery.ToString().Replace("  ", " "));
                if (CargoQuery.Length > 0)
                {
                    if ((CalcDS != null))
                    {
                        if (CalcDS.Tables[0].Rows.Count > 0)
                        {
                            DataTable tempDT = null;
                            DataRow tempDR = null;
                            Int16 ColCnt = default(Int16);
                            tempDT = objWF.GetDataTable(CargoQuery.ToString().Replace("  ", " "));
                            foreach (DataRow tempDR_loopVariable in tempDT.Rows)
                            {
                                tempDR = tempDR_loopVariable;
                                DR = CalcDS.Tables[0].NewRow();
                                for (ColCnt = 0; ColCnt <= CalcDS.Tables[0].Columns.Count - 1; ColCnt++)
                                {
                                    DR[ColCnt] = tempDR[ColCnt];
                                }
                                CalcDS.Tables[0].Rows.Add(DR);
                            }
                        }
                        else
                        {
                            CalcDS = objWF.GetDataSet(CargoQuery.ToString().Replace("  ", " "));
                        }
                    }
                    else
                    {
                        CalcDS = objWF.GetDataSet(CargoQuery.ToString().Replace("  ", " "));
                    }
                }

                try
                {
                    DataRelation REL = null;
                    REL = new DataRelation("RFQRelation", GridDS.Tables[0].Columns["PK"], GridDS.Tables[1].Columns["FK"]);
                    GridDS.Relations.Add(REL);
                    foreach (DataRow DR_loopVariable in GridDS.Tables[0].Rows)
                    {
                        DR = DR_loopVariable;
                        //DR["OTH_DTL"] = QFORBusinessDev.Master.Cls_FlatRateFreights.GetOTHstring(OthDt, 2, 3, 6, 4, 1, DR["PK"], DR["CH_WT"], Amount, ExChTable);
                        DR["OTH_BTN"] = Amount;
                    }
                }
                catch (Exception eX)
                {
                    throw eX;
                }
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        // Function to get All Pks of parent Table
        private StringBuilder GetAllKeys(DataTable DT, Int16 ColNum)
        {
            StringBuilder functionReturnValue = null;
            functionReturnValue = new StringBuilder();
            functionReturnValue.Append("-1");
            DataRow DR = null;
            try
            {
                foreach (DataRow DR_loopVariable in DT.Rows)
                {
                    DR = DR_loopVariable;
                    functionReturnValue.Append("," + Convert.ToString(DR[ColNum]).Trim());
                }
            }
            catch (System.Exception eX)
            {
                functionReturnValue.Remove(0, functionReturnValue.Length - 1);
            }
            return functionReturnValue;
        }

        //Public Function FetchApprovalDtl(ByRef QuotPK As Integer) As DataSet
        //    Dim strSQL As String
        //    Dim objWF As New  WorkFlow
        //    Dim sb As New StringBuilder(5000)
        //    sb.Append("SELECT U.USER_ID, TO_CHAR(Q.APP_DATE, DATEFORMAT) APP_DATE,Q.STATUS")
        //    sb.Append("  FROM QUOTATION_AIR_TBL Q, USER_MST_TBL U")
        //    sb.Append(" WHERE Q.APP_BY = U.USER_MST_PK")
        //    sb.Append("   AND Q.QUOTATION_AIR_PK = " & QuotPK)
        //    Return objWF.GetDataSet(sb.ToString())
        //End Function
        // Function to populate all Slab Types
        public DataTable SlabTypes()
        {
            string strSQL = null;
            strSQL = " Select 'BP'              SLAB_TYPE,  1                           PK from dual " + " union " + " Select BREAKPOINT_ID     SLAB_TYPE,  AIRFREIGHT_SLABS_TBL_PK     PK " + " from    AIRFREIGHT_SLABS_TBL " + " where   BREAKPOINT_TYPE = 2 " + "   and   ACTIVE_FLAG = 1 ";
            return (new WorkFlow()).GetDataTable(strSQL);
        }

        #endregion " Main Fetch Query "

        #region " Show Rates Queries. "

        public bool GetRates(DataSet DS)
        {
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oDR = default(OracleDataReader);
            DataRow DR = null;
            DataRow pDR = null;
            StringBuilder RateQuery = new StringBuilder();
            decimal ApprovedRate = default(decimal);
            decimal MinAmount = default(decimal);
            BreakPoint SlabType = default(BreakPoint);
            string ChargeableWt = null;
            string SlabPk = null;
            string FrtTrnFk = null;
            string SlabID = null;
            try
            {
                foreach (DataRow DR_loopVariable in DS.Tables[1].Rows)
                {
                    DR = DR_loopVariable;
                    if ((FreightType)DR["FRT_TYPE"] == FreightType.AFC)
                    {
                        foreach (DataRow pDR_loopVariable in DS.Tables[0].Rows)
                        {
                            pDR = pDR_loopVariable;
                            if (pDR["PK"] == DR["FK"] & getDefault(pDR["SELECTED"], "false") == "true")
                            {
                                if ((SourceType)pDR["REF_TYPE"] != SourceType.Quotation)
                                {
                                    if (Convert.ToInt32(DR["SLAB_TYPE_PK"]) == 1)
                                    {
                                        SlabType = BreakPoint.Kgs;
                                    }
                                    else
                                    {
                                        SlabType = BreakPoint.ULD;
                                    }
                                    ChargeableWt = Convert.ToString(pDR["CH_WT"]);
                                    SlabPk = Convert.ToString(getDefault(pDR["SLAB"], 0));
                                    FrtTrnFk = Convert.ToString(DR["PK"]);
                                    switch ((SourceType)pDR["REF_TYPE"])
                                    {
                                        case SourceType.SpotRate:
                                            RateQuery.Append(" Select bp.APPROVED_RATE, trf.MIN_AMOUNT, ");
                                            RateQuery.Append("        afc.AIRFREIGHT_SLABS_TBL_PK, afc.BREAKPOINT_ID ");
                                            RateQuery.Append(" from ");
                                            RateQuery.Append("       RFQ_SPOT_AIR_BREAKPOINTS bp,   AIRFREIGHT_SLABS_TBL afc, ");
                                            RateQuery.Append("       RFQ_SPOT_AIR_TRN_FREIGHT_TBL trf ");
                                            RateQuery.Append(" Where ");
                                            RateQuery.Append("        bp.AIRFREIGHT_SLABS_TBL_FK    =      afc.AIRFREIGHT_SLABS_TBL_PK");
                                            RateQuery.Append("   and  bp.RFQ_SPOT_AIR_FRT_FK        =      trf.RFQ_SPOT_TRN_FREIGHT_PK");
                                            RateQuery.Append("   and  trf.RFQ_SPOT_TRN_FREIGHT_PK   =   " + FrtTrnFk);
                                            if (SlabType == BreakPoint.Kgs)
                                            {
                                                RateQuery.Append("   and  afc.BREAKPOINT_RANGE      = ");
                                                RateQuery.Append("     ( Select Max(BREAKPOINT_RANGE) ");
                                                RateQuery.Append("         from AIRFREIGHT_SLABS_TBL, RFQ_SPOT_AIR_BREAKPOINTS ");
                                                RateQuery.Append("         where BREAKPOINT_RANGE  <= " + ChargeableWt);
                                                RateQuery.Append("           and AIRFREIGHT_SLABS_TBL_FK = AIRFREIGHT_SLABS_TBL_PK ");
                                                RateQuery.Append("           and RFQ_SPOT_AIR_FRT_FK     = " + FrtTrnFk);
                                                RateQuery.Append("     ) ");
                                            }
                                            else
                                            {
                                                RateQuery.Append("   and bp.AIRFREIGHT_SLABS_TBL_FK = " + SlabPk);
                                            }
                                            RateQuery.Append("   and bp.AIRFREIGHT_SLABS_TBL_FK = afc.AIRFREIGHT_SLABS_TBL_PK ");
                                            break;

                                        case SourceType.CustomerContract:
                                            RateQuery.Append(" Select bp.APPROVED_RATE,            trf.MIN_AMOUNT, ");
                                            RateQuery.Append("        afc.AIRFREIGHT_SLABS_TBL_PK, afc.BREAKPOINT_ID ");
                                            RateQuery.Append(" from ");
                                            RateQuery.Append("       CONT_CUST_AIR_BREAKPOINTS bp,  AIRFREIGHT_SLABS_TBL afc,");
                                            RateQuery.Append("       CONT_CUST_AIR_FREIGHT_TBL trf ");
                                            RateQuery.Append(" Where ");
                                            RateQuery.Append("        bp.AIRFREIGHT_SLABS_FK        =   afc.AIRFREIGHT_SLABS_TBL_PK");
                                            RateQuery.Append("   and  bp.CONT_CUST_AIR_FRT_FK       =   trf.CONT_CUST_AIR_FREIGHT_PK");
                                            RateQuery.Append("   and  bp.CONT_CUST_AIR_FRT_FK       =   " + FrtTrnFk);
                                            if (SlabType == BreakPoint.Kgs)
                                            {
                                                RateQuery.Append("   and  afc.BREAKPOINT_RANGE      = ");
                                                RateQuery.Append("     ( Select Max(BREAKPOINT_RANGE) ");
                                                RateQuery.Append("         from AIRFREIGHT_SLABS_TBL, CONT_CUST_AIR_BREAKPOINTS ");
                                                RateQuery.Append("         where BREAKPOINT_RANGE  <= " + ChargeableWt);
                                                RateQuery.Append("           and AIRFREIGHT_SLABS_FK     = AIRFREIGHT_SLABS_TBL_PK ");
                                                RateQuery.Append("           and CONT_CUST_AIR_FRT_FK    = " + FrtTrnFk);
                                                RateQuery.Append("     ) ");
                                            }
                                            else
                                            {
                                                RateQuery.Append("   and bp.AIRFREIGHT_SLABS_FK    = " + SlabPk);
                                            }
                                            RateQuery.Append("   and bp.AIRFREIGHT_SLABS_FK = afc.AIRFREIGHT_SLABS_TBL_PK ");
                                            break;

                                        case SourceType.AirlineTariff:
                                            RateQuery.Append(" Select bp.TARIFF_RATE,              trf.MIN_AMOUNT, ");
                                            RateQuery.Append("        afc.AIRFREIGHT_SLABS_TBL_PK, afc.BREAKPOINT_ID ");
                                            RateQuery.Append(" from ");
                                            RateQuery.Append("       TARIFF_AIR_BREAKPOINTS bp,     AIRFREIGHT_SLABS_TBL afc,");
                                            RateQuery.Append("       TARIFF_TRN_AIR_FREIGHT_TBL trf ");
                                            RateQuery.Append(" Where ");
                                            RateQuery.Append("        bp.AIRFREIGHT_SLABS_TBL_FK    =      afc.AIRFREIGHT_SLABS_TBL_PK ");
                                            RateQuery.Append("   and  bp.TARIFF_TRN_FREIGHT_FK      =      trf.TARIFF_TRN_FREIGHT_PK ");
                                            RateQuery.Append("   and  trf.TARIFF_TRN_FREIGHT_PK     =   " + FrtTrnFk);
                                            if (SlabType == BreakPoint.Kgs)
                                            {
                                                RateQuery.Append("   and  afc.BREAKPOINT_RANGE      = ");
                                                RateQuery.Append("     ( Select Max(BREAKPOINT_RANGE) ");
                                                RateQuery.Append("         from AIRFREIGHT_SLABS_TBL, TARIFF_AIR_BREAKPOINTS ");
                                                RateQuery.Append("         where BREAKPOINT_RANGE  <= " + ChargeableWt);
                                                RateQuery.Append("           and AIRFREIGHT_SLABS_TBL_FK = AIRFREIGHT_SLABS_TBL_PK ");
                                                RateQuery.Append("           and TARIFF_TRN_FREIGHT_FK   = " + FrtTrnFk);
                                                RateQuery.Append("     ) ");
                                            }
                                            else
                                            {
                                                RateQuery.Append("   and bp.AIRFREIGHT_SLABS_TBL_FK = " + SlabPk);
                                            }
                                            RateQuery.Append("   and bp.AIRFREIGHT_SLABS_TBL_FK = afc.AIRFREIGHT_SLABS_TBL_PK ");
                                            break;

                                        case SourceType.GeneralTariff:
                                            RateQuery.Append(" Select bp.TARIFF_RATE,              trf.MIN_AMOUNT, ");
                                            RateQuery.Append("        afc.AIRFREIGHT_SLABS_TBL_PK, afc.BREAKPOINT_ID ");
                                            RateQuery.Append(" from ");
                                            RateQuery.Append("       TARIFF_AIR_BREAKPOINTS bp,     AIRFREIGHT_SLABS_TBL afc,");
                                            RateQuery.Append("       TARIFF_TRN_AIR_FREIGHT_TBL trf ");
                                            RateQuery.Append(" Where ");
                                            RateQuery.Append("        bp.AIRFREIGHT_SLABS_TBL_FK    =      afc.AIRFREIGHT_SLABS_TBL_PK ");
                                            RateQuery.Append("   and  bp.TARIFF_TRN_FREIGHT_FK      =      trf.TARIFF_TRN_FREIGHT_PK ");
                                            RateQuery.Append("   and  trf.TARIFF_TRN_FREIGHT_PK     =   " + FrtTrnFk);
                                            if (SlabType == BreakPoint.Kgs)
                                            {
                                                RateQuery.Append("   and  afc.BREAKPOINT_RANGE      = ");
                                                RateQuery.Append("     ( Select Max(BREAKPOINT_RANGE) ");
                                                RateQuery.Append("         from AIRFREIGHT_SLABS_TBL, TARIFF_AIR_BREAKPOINTS ");
                                                RateQuery.Append("         where BREAKPOINT_RANGE  <= " + ChargeableWt);
                                                RateQuery.Append("           and AIRFREIGHT_SLABS_TBL_FK = AIRFREIGHT_SLABS_TBL_PK ");
                                                RateQuery.Append("           and TARIFF_TRN_FREIGHT_FK   = " + FrtTrnFk);
                                                RateQuery.Append("     ) ");
                                            }
                                            else
                                            {
                                                RateQuery.Append("   and bp.AIRFREIGHT_SLABS_TBL_FK = " + SlabPk);
                                            }
                                            RateQuery.Append("   and bp.AIRFREIGHT_SLABS_TBL_FK = afc.AIRFREIGHT_SLABS_TBL_PK ");
                                            break;

                                        case SourceType.Manual:
                                            RateQuery.Append("SELECT '' TARIFF_RATE,");
                                            RateQuery.Append("       '' MIN_AMOUNT,");
                                            RateQuery.Append("       AFC.AIRFREIGHT_SLABS_TBL_PK,");
                                            RateQuery.Append("       AFC.BREAKPOINT_ID");
                                            RateQuery.Append("  FROM AIRFREIGHT_SLABS_TBL AFC");
                                            if (SlabType == BreakPoint.Kgs)
                                            {
                                                RateQuery.Append(" WHERE AFC.BREAKPOINT_RANGE =");
                                                RateQuery.Append("       (SELECT MAX(AF.BREAKPOINT_RANGE)");
                                                RateQuery.Append("          FROM AIRFREIGHT_SLABS_TBL AF");
                                                RateQuery.Append("         WHERE AF.BREAKPOINT_RANGE <= " + ChargeableWt + ")");
                                            }
                                            else
                                            {
                                                RateQuery.Append(" WHERE AFC.AIRFREIGHT_SLABS_TBL_PK =" + SlabPk);
                                            }
                                            break;
                                    }

                                    oDR = objWF.GetDataReader(RateQuery.ToString().Replace("  ", " "));
                                    RateQuery.Remove(0, RateQuery.Length);
                                    while (oDR.Read())
                                    {
                                        ApprovedRate = Convert.ToDecimal(getDefault(oDR[0], 0));
                                        MinAmount = Convert.ToDecimal(getDefault(oDR[1], 0));
                                        SlabPk = Convert.ToString(getDefault(oDR[2], 0));
                                        SlabID = Convert.ToString(getDefault(oDR[3], 0));
                                    }
                                    if ((SourceType)pDR["REF_TYPE"] != SourceType.Manual)
                                    {
                                        DR["BASIS_RATE"] = getDefault(ApprovedRate, DBNull.Value);
                                        DR["MIN_AMOUNT"] = getDefault(MinAmount, DBNull.Value);
                                    }
                                    pDR["SLAB"] = getDefault(SlabPk, DBNull.Value);
                                    pDR["SLAB_ID"] = getDefault(SlabID, DBNull.Value);
                                }
                            }
                        }
                    }
                }
                return true;
            }
            catch (Exception eX)
            {
                return false;
            }
        }

        #endregion " Show Rates Queries. "

        #region "FETCH CREDTDAYS AND CREDIT LIMIT"

        public int fetchCredit(string CreditDays, string CreditLimit, string Pk = "0", int type = 0, int CustPk = 0)
        {
            try
            {
                OracleDataReader dr = default(OracleDataReader);
                StringBuilder strQuery = new StringBuilder();
                StringBuilder strCustQuery = new StringBuilder();
                strCustQuery.Append("SELECT C.SEA_CREDIT_DAYS,");
                strCustQuery.Append(" C.SEA_CREDIT_LIMIT");
                strCustQuery.Append("FROM customer_mst_tbl c");
                strCustQuery.Append("WHERE c.customer_mst_pk=" + CustPk);
                if (type == 1)
                {
                    strQuery.Append("SELECT C.CREDIT_PERIOD");
                    strQuery.Append("  FROM ENQUIRY_BKG_AIR_TBL     EB,");
                    strQuery.Append("       ENQUIRY_TRN_AIR_FCL_LCL ET,");
                    strQuery.Append("       CONT_CUST_AIR_TBL       C");
                    strQuery.Append("       WHERE");
                    strQuery.Append("       ET.TRANS_REFERED_FROM=2");
                    strQuery.Append("       AND Eb.ENQUIRY_REF_NO='" + Pk + "'");
                    strQuery.Append("       AND EB.ENQUIRY_BKG_AIR_PK=ET.ENQUIRY_MAIN_AIR_FK");
                    strQuery.Append("       AND ET.TRANS_REF_NO=C.CONT_REF_NO");
                    strQuery.Append("     AND C.CUSTOMER_MST_FK=" + CustPk);
                }
                else if (type == 2)
                {
                    strQuery.Append("SELECT Q.CREDIT_DAYS");
                    strQuery.Append("     FROM QUOTATION_AIR_TBL Q");
                    strQuery.Append("     WHERE Q.QUOTATION_AIR_PK=" + Pk);
                    strQuery.Append("     AND Q.CUSTOMER_MST_FK=" + CustPk);
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
                    CreditDays = Convert.ToString(getDefault(dt.Rows[0][0], ""));
                    if (dt.Columns.Count > 1)
                        CreditLimit = Convert.ToString(getDefault(dt.Rows[0][1], ""));
                }
                else
                {
                    dt = (new WorkFlow()).GetDataTable(strCustQuery.ToString());
                    CreditDays = Convert.ToString(getDefault(dt.Rows[0][0], ""));
                    if (dt.Columns.Count > 1)
                        CreditLimit = Convert.ToString(getDefault(dt.Rows[0][1], ""));
                }
                dr = (new WorkFlow()).GetDataReader(strCustQuery.ToString());
                while (dr.Read())
                {
                    if (string.IsNullOrEmpty(Convert.ToString(CreditDays)))
                        CreditDays = Convert.ToString(getDefault(dr[0], ""));
                    if (string.IsNullOrEmpty(Convert.ToString(CreditLimit)))
                        CreditLimit = Convert.ToString(getDefault(dr[1], ""));
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
            catch (Exception ex)
            {
                return 0;
            }
        }

        #endregion "FETCH CREDTDAYS AND CREDIT LIMIT"

        #region " Get Amount and All in Freight "

        #region " Logical Flow "

        // This is basically to compute the Tariff Rate from given Basis Rate and Quantity
        // The Calculation will varry according to ChargeBasis Flat, KGs and Percentage

        #endregion " Logical Flow "

        public void GetAmountValue(DataSet DS, DataTable OthDt, DataTable ExchRateDT = null)
        {
            Int16 RowCnt = default(Int16);
            Int16 cRowCnt = default(Int16);
            DataRow DR = null;
            DataRow DRc = null;
            DataRow crDR = null;
            if (ExchRateDT == null)
            {
                ExchRateDT = FetchCurrAndExchange().Tables[0];
            }
            decimal ExchangeRate = 1;

            BreakPoint SlabType = default(BreakPoint);
            int Boxes = 0;
            long SlabFk = 0;
            decimal ChargeableWt = default(decimal);
            decimal AllInQuote = default(decimal);
            decimal NetValue = default(decimal);
            decimal OthAmount = default(decimal);

            long Currency = 0;
            decimal MinAmount = default(decimal);
            decimal TrfRate = default(decimal);
            decimal BasisRate = default(decimal);
            ChargeBasis ChrgBasis = default(ChargeBasis);
            FreightType FrtType = default(FreightType);

            foreach (DataRow DR_loopVariable in DS.Tables[0].Rows)
            {
                DR = DR_loopVariable;
                if (getDefault(DR["SELECTED"], "false") == "true")
                {
                    if (Convert.ToInt32(DR["SLAB_TYPE_PK"] )== 1)
                    {
                        SlabType = BreakPoint.Kgs;
                    }
                    else
                    {
                        SlabType = BreakPoint.ULD;
                    }
                    Boxes = Convert.ToInt32(getDefault(DR["BOXES"], 0));
                    SlabFk = Convert.ToInt64(getDefault(DR["SLAB"], 0));
                    ChargeableWt = Convert.ToDecimal(getDefault(DR["CH_WT"], 0));
                    AllInQuote = Convert.ToDecimal(getDefault(DR["AI_QT"], 0));
                    NetValue = Convert.ToDecimal(getDefault(DR["NET"], 0));
                    AllInQuote = 0;
                    BasisRate = 0;
                    OthAmount = 0;

                    foreach (DataRow DRc_loopVariable in DS.Tables[1].Rows)
                    {
                        DRc = DRc_loopVariable;
                        if (DRc["FK"] == DR["PK"])
                        {
                            FrtType = (FreightType)DRc["FRT_TYPE"];
                            if (FrtType == FreightType.AFC && SlabType == BreakPoint.ULD)
                            {
                                DRc["CH_BASIS"] = 2;
                                DRc["CH_BASIS_ID"] = "Flat";
                            }
                            else
                            {
                                if (DRc["CH_BASIS_ID"] != "Flat")
                                {
                                    DRc["CH_BASIS"] = 3;
                                    DRc["CH_BASIS_ID"] = "KGs";
                                }
                            }
                            Currency = Convert.ToInt64(DRc["CURR_FK"]);
                            MinAmount = Convert.ToDecimal(getDefault(DRc["MIN_AMOUNT"], 0));
                            BasisRate = Convert.ToDecimal(getDefault(DRc["BASIS_RATE"], 0));
                            ChrgBasis = (ChargeBasis)getDefault(DRc["CH_BASIS"], 2);
                            if (ChrgBasis == ChargeBasis.Flat)
                            {
                                TrfRate = BasisRate;
                            }
                            else if (ChrgBasis == ChargeBasis.Percentage)
                            {
                                if (FrtType == FreightType.AFC)
                                {
                                    if (SlabType == BreakPoint.Kgs)
                                    {
                                        TrfRate = BasisRate * ChargeableWt;
                                    }
                                    else
                                    {
                                        TrfRate = BasisRate * Boxes;
                                    }
                                }
                                else
                                {
                                    DataRow AfcDR = null;
                                    TrfRate = 0;
                                    foreach (DataRow AfcDR_loopVariable in DS.Tables[1].Rows)
                                    {
                                        AfcDR = AfcDR_loopVariable;
                                        if (AfcDR["FK"] == DR["PK"] && (FreightType)AfcDR["FRT_TYPE"] == FreightType.AFC)
                                        {
                                            if ((ChargeBasis)AfcDR["CH_BASIS"] == ChrgBasis && (ChargeBasis)AfcDR["CH_BASIS"] == ChargeBasis.Flat)
                                            {
                                                TrfRate += Convert.ToDecimal(getDefault(AfcDR["BASIS_RATE"], 0)) * Convert.ToDecimal(getDefault(Boxes, 1));
                                            }
                                            else
                                            {
                                                TrfRate += Convert.ToDecimal(getDefault(AfcDR["BASIS_RATE"], 0)) * Convert.ToDecimal(ChargeableWt);
                                            }
                                        }
                                    }
                                    TrfRate = TrfRate * BasisRate / 100;
                                }
                            }
                            else if (ChrgBasis == ChargeBasis.KGs)
                            {
                                TrfRate = BasisRate * ChargeableWt;
                            }

                            if (TrfRate < MinAmount)
                            {
                                TrfRate = MinAmount;
                                if (FrtType == FreightType.AFC)
                                {
                                    DRc["CH_BASIS"] = ChargeBasis.Flat;
                                    DRc["CH_BASIS_ID"] = "Flat";
                                }
                            }
                            DRc["TARIFF_RATE"] = TrfRate;
                            foreach (DataRow crDR_loopVariable in ExchRateDT.Rows)
                            {
                                crDR = crDR_loopVariable;
                                if (Convert.ToInt64(crDR["CURRENCY_MST_PK"]) == Currency)
                                    break; // TODO: might not be correct. Was : Exit For
                            }
                            ExchangeRate = Convert.ToDecimal(getDefault(crDR["EXCHANGE_RATE"], 1));

                            AllInQuote += TrfRate * ExchangeRate;
                        }
                    }
                    OthAmount = Convert.ToDecimal(Cls_FlatRateFreights.UpdateOTHFreights(OthDt, Convert.ToString(getDefault(DR["OTH_DTL"], "")), 2, 3, 6, 4, 1, Convert.ToString(getDefault(DR["PK"], "0")), Convert.ToDecimal(getDefault(ChargeableWt, 1)), ExchRateDT));
                    if (OthAmount != -1)
                        DR["OTH_BTN"] = OthAmount;
                    DR["AI_QT"] = AllInQuote;
                }
            }
        }

        #endregion " Get Amount and All in Freight "

        #region " Get Address of Quotation "

        public DataTable GetAddressOfQuotation(long QuotationPK)
        {
            string strSQL = null;
            strSQL = " Select COL_PLACE_MST_FK, col.PLACE_CODE colplace, COL_ADDRESS, " + " DEL_PLACE_MST_FK, del.PLACE_CODE delplace, DEL_ADDRESS " + " from PLACE_MST_TBL col, PLACE_MST_TBL del, QUOTATION_AIR_TBL q " + " where q.COL_PLACE_MST_FK = col.PLACE_PK(+) and " + "       q.DEL_PLACE_MST_FK = del.PLACE_PK(+) and " + "       QUOTATION_AIR_PK = " + QuotationPK;
            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch
            {
            }
            return new DataTable();
        }

        public DataTable GetAddressOfCustomer(long CustomerPK, Int16 CustomerType)
        {
            string strSQL = null;
            strSQL = " Select '' COL_PLACE_MST_FK, '' colplace, COL_ADDRESS, " + " '' DEL_PLACE_MST_FK, '' delplace, DEL_ADDRESS " + " from V_ALL_CUSTOMER cm " + " where cm.CUSTOMER_MST_PK = " + CustomerPK + "   and cm.CUSTOMER_TYPE = " + CustomerType;
            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch
            {
            }
            return new DataTable();
        }

        public DataTable GetCustPriority(long CustomerPK)
        {
            string strSQL = null;
            strSQL = " Select  DECODE(CMT.PRIORITY, 1, 'Priority 1', 2, 'Priority 2', 3, 'Priority 3',4, 'Priority 4',5, 'Priority 5')PRIORITY, " + "  DECODE(CMT.CRITERIA, 1, 'Global', 2, 'Region')CRITERIA ," + "  DECODE(CMT.CATEGORY, 1, 'VVIP', 2, 'VIP') CATEGORY" + " from CUSTOMER_MST_TBL CMT " + " where CMT.CUSTOMER_MST_PK = " + CustomerPK;

            try
            {
                return (new WorkFlow()).GetDataTable(strSQL);
            }
            catch
            {
            }
            return new DataTable();
        }

        #endregion " Get Address of Quotation "

        #region " Header - Info. "

        public DataTable HeaderInfo(string QuotationPK, bool AmendFlg = false)
        {
            if (AmendFlg == true)
            {
                QuotationPK = "";
            }
            if (string.IsNullOrEmpty(QuotationPK))
                QuotationPK = "-1";
            string strSQL = null;
            strSQL = " Select QUOTATION_AIR_PK,     " + "       QUOTATION_REF_NO,                                                   " + "       to_char(QUOTATION_DATE,'" + dateFormat + "') QUOTATION_DATE,        " + "       VALID_FOR,                                                          " + "       PYMT_TYPE,                                                          " + "       QUOTED_BY,                                                          " + "       COL_PLACE_MST_FK,                                                   " + "       COL_ADDRESS,                                                        " + "       DEL_PLACE_MST_FK,                                                   " + "       DEL_ADDRESS,                                                        " + "       AGENT_MST_FK,                                                       " + "       STATUS,                                                             " + "       CREATED_BY_FK,                                                      " + "       to_char(CREATED_DT,'" + dateFormat + "')        CREATED_DT,         " + "       LAST_MODIFIED_BY_FK,                                                " + "       to_char(LAST_MODIFIED_DT,'" + dateFormat + "')  LAST_MODIFIED_DT,   " + "       VERSION_NO,                                                         " + "       to_char(EXPECTED_SHIPMENT,'" + dateFormat + "') EXPECTED_SHIPMENT_DT," + "       CUSTOMER_MST_FK,                                                    " + "       CUSTOMER_CATEGORY_MST_FK,                                           " + "       SPECIAL_INSTRUCTIONS,                                               " + "       CUST_TYPE ,                                                          " + "       CREDIT_DAYS ,                                                         " + "        REMARKS,                                                          " + "        CARGO_MOVE_FK,                                                    " + "        QUOTATION_TYPE,                                                   " + "        CREDIT_LIMIT,                                                     " + "        COMMODITY_GROUP_MST_FK,BASE_CURRENCY_FK, PORT_GROUP,HEADER_CONTENT,FOOTER_CONTENT    " + "  from QUOTATION_AIR_TBL                                                  " + "  Where  QUOTATION_AIR_PK = " + QuotationPK;

            try
            {
                return (new WorkFlow()).GetDataTable(strSQL.Replace("  ", " "));
            }
            catch
            {
            }
            return new DataTable();
        }

        #endregion " Header - Info. "

        #region " Update Quotation [ Status ] "

        public ArrayList UpdateQuotation(DataTable HDT, DataSet PDT, DataTable OthDT, DataTable CalcDSDT, string QuotationPk, string ValidFor, string Status, string ExpectedShipmentDate, bool gen, string Measure,
        string Wt, string DivFac, string Header = "", string Footer = "", int PYMTType = 0, int INCOTerms = 0, int From_Flag = 0, bool Customer_Approved = false)
        {
            WorkFlow objWK = new WorkFlow();
            Int32 prvstatus = default(Int32);
            OracleTransaction TRAN = default(OracleTransaction);
            prvstatus = getStatus(QuotationPk);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                var _with3 = objWK.MyCommand;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_AIR_TBL_UPD";
                var _with4 = _with3.Parameters;
                _with4.Add("QUOTATION_AIR_PK_IN", QuotationPk).Direction = ParameterDirection.Input;
                _with4.Add("VALID_FOR_IN", ValidFor).Direction = ParameterDirection.Input;
                _with4.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                _with4.Add("EXPECTED_SHIPMENT_DT_IN", ExpectedShipmentDate).Direction = ParameterDirection.Input;
                _with4.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with4.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                _with4.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with4.Add("COL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with4.Add("COL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with4.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with4.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with4.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with4.Add("REMARKS_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with4.Add("Header_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                _with4.Add("Footer_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                _with4.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                _with4.Add("PYMTType_IN", PYMTType).Direction = ParameterDirection.Input;
                _with4.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                _with4.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with3.Parameters["COL_PLACE_MST_FK_IN"].Value = HDT.Rows[0]["COL_PLACE_MST_FK"];
                _with3.Parameters["COL_ADDRESS_IN"].Value = HDT.Rows[0]["COL_ADDRESS"];
                _with3.Parameters["DEL_PLACE_MST_FK_IN"].Value = HDT.Rows[0]["DEL_PLACE_MST_FK"];
                _with3.Parameters["DEL_ADDRESS_IN"].Value = HDT.Rows[0]["DEL_ADDRESS"];
                _with3.Parameters["CARGO_MOVE_FK_IN"].Value = HDT.Rows[0]["CARGO_MOVE_FK"];
                _with3.Parameters["REMARKS_IN"].Value = HDT.Rows[0]["REMARKS"];
                _with3.Parameters["Header_IN"].Value = getDefault(Header, DBNull.Value);
                _with3.Parameters["Footer_IN"].Value = getDefault(Footer, DBNull.Value);
                _with3.ExecuteNonQuery();
                _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                arrMessage = Update_Transaction_Spec(PDT.Tables[0], PDT.Tables[1], _PkValue, objWK.MyCommand, objWK.MyUserName);
                if (!gen)
                {
                    if (prvstatus != 2 & prvstatus != 3)
                    {
                        updateOthCharge(PDT.Tables[0], OthDT, CalcDSDT, objWK.MyCommand, objWK.MyUserName, Measure, Wt, DivFac);
                    }
                }
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All data saved successfully");
                    }
                    else
                    {
                        TRAN.Rollback();
                    }
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All data saved successfully");
                }
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        public int getStatus(string pk)
        {
            StringBuilder strQuery = new StringBuilder();
            OracleDataReader dr = default(OracleDataReader);
            try
            {
                strQuery.Append("SELECT STATUS");
                strQuery.Append("FROM QUOTATION_AIR_TBL AIR");
                strQuery.Append("WHERE AIR.QUOTATION_AIR_PK =");
                strQuery.Append(pk);
                dr = (new WorkFlow()).GetDataReader(strQuery.ToString());
                while (dr.Read())
                {
                    return Convert.ToInt32(dr[0]);
                }
            }
            catch (Exception ex)
            {
                return 0;
            }
            finally
            {
                dr.Close();
            }
            return 0;
        }

        #endregion " Update Quotation [ Status ] "

        #region " Save Quotation "

        #region " Header Part [ Save Quotation ] "

        public ArrayList SaveQuotation(DataTable HDT, DataSet EnqDS, DataTable OthDT, DataTable CalcDT, string QuoteNo, string QuotePk, long nLocationId, long nEmpId, string Measure, string Wt,
        string DivFac, string Header = "", string Footer = "", int PYMTType = 0, int INCOTerms = 0, bool AmendFlg = false, int From_Flag = 0, bool Customer_Approved = false)
        {
            DataTable pDT = null;
            DataTable cDT = null;
            pDT = EnqDS.Tables[0];
            cDT = EnqDS.Tables[1];

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            bool chkFlag = false;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            string strExtendedQTN = null;
            Int16 nIndex = default(Int16);
            string PrevQuotPK = null;
            try
            {
                if (AmendFlg == true)
                {
                    NewRecord = true;
                    PrevQuotPK = QuotePk;
                    QuotePk = "";
                }
                if (string.IsNullOrEmpty(QuoteNo))
                {
                    QuoteNo = GenerateQuoteNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, From_Flag);
                    if (QuoteNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        QuoteNo = "";
                        return arrMessage;
                    }
                    chkFlag = true;
                }

                var _with5 = objWK.MyCommand;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_AIR_TBL_INS";
                _with5.Parameters.Clear();

                var _with6 = _with5.Parameters;
                _with6.Add("QUOTATION_REF_NO_IN", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Input;
                _with6.Add("QUOTATION_DATE_IN", OracleDbType.Varchar2, 30).Direction = ParameterDirection.Input;
                _with6.Add("VALID_FOR_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with6.Add("PYMT_TYPE_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("QUOTED_BY_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("COL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("COL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with6.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with6.Add("AGENT_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("STATUS_IN", OracleDbType.Int32, 1).Direction = ParameterDirection.Input;
                _with6.Add("EXPECTED_SHIPMENT_DT_IN", OracleDbType.Varchar2, 10).Direction = ParameterDirection.Input;
                _with6.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("CUSTOMER_CATEGORY_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("CUST_TYPE_IN", OracleDbType.Int32, 1).Direction = ParameterDirection.Input;
                _with6.Add("SPECIAL_INSTRUCTIONS_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with6.Add("CREDIT_LIMIT_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with6.Add("COMMODITY_GROUP_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with6.Add("CREDIT_DAYS_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with6.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with6.Add("QUOTATION_TYPE_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with6.Add("REMARKS_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with6.Add("Header_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                _with6.Add("Footer_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                _with6.Add("PORT_GROUP_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with6.Add("CONFIG_PK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                _with5.Parameters["SHIPPING_TERMS_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with5.Parameters.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                _with5.Parameters["FROM_FLAG_IN"].SourceVersion = DataRowVersion.Current;

                _with5.Parameters.Add("BASE_CURRENCY_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with5.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;

                //_with5.Parameters["QUOTATION_REF_NO_IN"].SourceVersion = QuoteNo;
                //_with5.Parameters["QUOTATION_DATE_IN"].SourceVersion = HDT.Rows[0]["QUOTATION_DATE"];
                //_with5.Parameters["VALID_FOR_IN"].SourceVersion = HDT.Rows[0]["VALID_FOR"];
                //_with5.Parameters["PYMT_TYPE_IN"].SourceVersion = 1;
                //_with5.Parameters["QUOTED_BY_IN"].SourceVersion = M_CREATED_BY_FK;
                //_with5.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = HDT.Rows[0]["COL_PLACE_MST_FK"];
                //_with5.Parameters["COL_ADDRESS_IN"].SourceVersion = HDT.Rows[0]["COL_ADDRESS"];
                //_with5.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = HDT.Rows[0]["DEL_PLACE_MST_FK"];
                //_with5.Parameters["DEL_ADDRESS_IN"].SourceVersion = HDT.Rows[0]["DEL_ADDRESS"];
                //_with5.Parameters["AGENT_MST_FK_IN"].SourceVersion = HDT.Rows[0]["AGENT_MST_FK"];
                //_with5.Parameters["STATUS_IN"].SourceVersion = HDT.Rows[0]["STATUS"];
                //_with5.Parameters["EXPECTED_SHIPMENT_DT_IN"].SourceVersion = HDT.Rows[0]["EXPECTED_SHIPMENT_DT"];
                //_with5.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = HDT.Rows[0]["CUSTOMER_MST_FK"];
                //_with5.Parameters["CUSTOMER_CATEGORY_MST_FK_IN"].SourceVersion = HDT.Rows[0]["CUSTOMER_CATEGORY_MST_FK"];
                //_with5.Parameters["CUST_TYPE_IN"].SourceVersion = getDefault(HDT.Rows[0]["CUST_TYPE"], 0);
                //_with5.Parameters["SPECIAL_INSTRUCTIONS_IN"].SourceVersion = HDT.Rows[0]["SPECIAL_INSTRUCTIONS"];
                //_with5.Parameters["COMMODITY_GROUP_IN"].SourceVersion = HDT.Rows[0]["COMMODITY_GROUP_MST_FK"];
                //_with5.Parameters["CREDIT_LIMIT_IN"].SourceVersion = HDT.Rows[0]["CREDIT_LIMIT"];
                //_with5.Parameters["CREDIT_DAYS_IN"].SourceVersion = HDT.Rows[0]["CREDIT_DAYS"];
                //_with5.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = HDT.Rows[0]["CARGO_MOVE_FK"];
                //_with5.Parameters["QUOTATION_TYPE_IN"].SourceVersion = HDT.Rows[0]["QUOTATION_TYPE"];
                //_with5.Parameters["REMARKS_IN"].SourceVersion = HDT.Rows[0]["REMARKS"];
                //_with5.Parameters["Header_IN"].SourceVersion = getDefault(Header, DBNull.Value);
                //_with5.Parameters["Footer_IN"].SourceVersion = getDefault(Footer, DBNull.Value);
                //_with5.Parameters["PORT_GROUP_IN"].SourceVersion = getDefault(HDT.Rows[0]["PORT_GROUP"], DBNull.Value);
                //_with5.Parameters["CREATED_BY_FK_IN"].SourceVersion = M_CREATED_BY_FK;
                //_with5.Parameters["CONFIG_PK_IN"].SourceVersion = M_Configuration_PK;
                //_with5.Parameters.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;
                //_with5.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = HDT.Rows[0]["BASE_CURRENCY_FK"];
                _with5.ExecuteNonQuery();
                _PkValue = Convert.ToInt64(_with5.Parameters["RETURN_VALUE"].Value);

                HDT.Rows[0]["QUOTATION_AIR_PK"] = _PkValue;
                arrMessage = SaveTransaction1(pDT, cDT, OthDT, CalcDT, _PkValue, objWK.MyCommand, objWK.MyUserName, Measure, Wt, DivFac,
                From_Flag);
                //'
                if (AmendFlg == true)
                {
                    string str = null;
                    Int32 intIns = default(Int32);
                    Int32 ValidFor = default(Int32);
                    TimeSpan Span = default(TimeSpan);
                    OracleCommand updCmdUser = new OracleCommand();
                    updCmdUser.Transaction = TRAN;
                    System.DateTime QuotDate = default(System.DateTime);
                    QuotDate = GetQuotDt(PrevQuotPK);
                    if (QuotDate > DateTime.Today.Date)
                    {
                        str = " update QUOTATION_AIR_TBL QT SET QT.STATUS = 3 ,";
                        str += " QT.AMEND_FLAG = 1 ";
                        str += " WHERE QT.QUOTATION_AIR_PK=" + PrevQuotPK;
                    }
                    else
                    {
                        Span = DateTime.Today.Subtract(QuotDate);
                        ValidFor = Span.Days;
                        str = " update QUOTATION_AIR_TBL QT SET QT.EXPECTED_SHIPMENT = SYSDATE ,";
                        str += " QT.AMEND_FLAG = 1 ,";
                        str += " QT.VALID_FOR = " + ValidFor;
                        str += " WHERE QT.QUOTATION_AIR_PK=" + PrevQuotPK;
                    }
                    var _with7 = updCmdUser;
                    _with7.Connection = objWK.MyConnection;
                    _with7.Transaction = TRAN;
                    _with7.CommandType = CommandType.Text;
                    _with7.CommandText = str;
                    intIns = _with7.ExecuteNonQuery();
                }
                //'
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") > 0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        QuotePk = Convert.ToString(_PkValue);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        if (chkFlag)
                        {
                            RollbackProtocolKey("QUOTATION (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
                            chkFlag = false;
                        }
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                if (chkFlag)
                {
                    RollbackProtocolKey("QUOTATION (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
                    chkFlag = false;
                }
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                if (chkFlag)
                {
                   RollbackProtocolKey("QUOTATION (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
                }
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        #endregion " Header Part [ Save Quotation ] "

        #region "Update Transaction"

        private ArrayList Update_Transaction_Spec(DataTable pDT, DataTable cDT, long PkValue, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            Int16 AllInRate = default(Int16);
            Int16 BasisType = default(Int16);
            long TransactionPK = 0;
            arrMessage.Clear();
            try
            {
                var _with8 = SCM;
                for (nRowCnt = 0; nRowCnt <= pDT.Rows.Count - 1; nRowCnt++)
                {
                    _with8.CommandType = CommandType.StoredProcedure;
                    _with8.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_TRN_AIR_UPD";
                    DR = pDT.Rows[nRowCnt];
                    var _with9 = _with8.Parameters;
                    _with9.Clear();
                    _with9.Add("QUOTATION_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with9.Add("AIRLINE_MST_FK_IN", DR["AIR_PK"]).Direction = ParameterDirection.Input;
                    _with9.Add("COMMODITY_GROUP_FK_IN", DR["COMM_GRPPK"]).Direction = ParameterDirection.Input;
                    _with9.Add("COMMODITY_MST_FK_IN", getDefault(DR["COMM_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with9.Add("ALL_IN_TARIFF_IN", getDefault(DR["AI_TRF"], 0)).Direction = ParameterDirection.Input;
                    _with9.Add("ALL_IN_QUOTED_TARIFF_IN", getDefault(DR["AI_QT"], 0)).Direction = ParameterDirection.Input;

                    _with9.Add("CHARGEABLE_WEIGHT_IN", getDefault(DR["CH_WT"], 0)).Direction = ParameterDirection.Input;

                    _with9.Add("PACK_COUNT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("VOLUME_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("DENSITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("ACTUAL_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with9.Add("ACTUAL_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    _with9.Add("SLAB_FK_IN", DR["SLAB"]).Direction = ParameterDirection.Input;
                    _with9.Add("QUANTITY_IN", getDefault(DR["BOXES"], 0)).Direction = ParameterDirection.Input;

                    _with9.Add("BUYING_RATE_IN", DR["AIR_RT"]).Direction = ParameterDirection.Input;
                    _with9.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with8.ExecuteNonQuery();

                    TransactionPK = Convert.ToInt64(DR["PK"]);
                    arrMessage = Update_Freights_Spec(DR, cDT, TransactionPK, SCM, UserName);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString().ToUpper()), "SAVED") > 0)
                        {
                            return arrMessage;
                        }
                    }
                }
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
            return new ArrayList();
        }

        private ArrayList Update_Freights_Spec(DataRow pDR, DataTable cDT, long TrnPKValue, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            bool Flag = false;
            Int16 AllInRate = default(Int16);
            Int16 Chkadvatos = default(Int16);
            long FreightPk = 0;
            arrMessage.Clear();
            try
            {
                var _with10 = SCM;
                for (nRowCnt = 0; nRowCnt <= cDT.Rows.Count - 1; nRowCnt++)
                {
                    _with10.CommandType = CommandType.StoredProcedure;
                    _with10.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_TRN_AIR_FRT_DTLS_UPD";
                    DR = cDT.Rows[nRowCnt];
                    Flag = false;

                    if (getDefault(DR["FK"], 0) == getDefault(pDR["PK"], 0))
                    {
                        var _with11 = _with10.Parameters;
                        _with11.Clear();
                        AllInRate = Convert.ToInt16((DR["SELECTED"].ToString() == "true" ? 1 : 0));
                        Chkadvatos = Convert.ToInt16((DR["ADVATOS"].ToString() == "true" ? 1 : 0));
                        _with11.Add("QUOTE_TRN_AIR_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                        _with11.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FRT_FK"]).Direction = ParameterDirection.Input;
                        _with11.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                        _with11.Add("CURRENCY_MST_FK_IN", DR["CURR_FK"]).Direction = ParameterDirection.Input;
                        _with11.Add("TARIFF_RATE_IN", getDefault(DR["TARIFF_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with11.Add("QUOTED_RATE_IN", getDefault(DR["QUOTED_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with11.Add("PYMT_TYPE_IN", getDefault(DR["P_TYPE"], 1)).Direction = ParameterDirection.Input;
                        _with11.Add("CHARGE_BASIS_IN", DR["CH_BASIS"]).Direction = ParameterDirection.Input;
                        _with11.Add("BASIS_RATE_IN", getDefault(DR["BASIS_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with11.Add("FREIGHT_TYPE_IN", DR["FRT_TYPE"]).Direction = ParameterDirection.Input;
                        _with11.Add("CHECK_ADVATOS_IN", Chkadvatos).Direction = ParameterDirection.Input;
                        //'Added Koteshwari for Advatos Column PTSID-JUN18

                        _with11.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with10.ExecuteNonQuery();
                        FreightPk = Convert.ToInt64(_with10.Parameters["RETURN_VALUE"].Value);
                    }
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

        #endregion "Update Transaction"

        #region " Save Transaction "

        private ArrayList SaveTransaction1(DataTable pDT, DataTable cDT, DataTable OthDT, DataTable CalcDT, long PkValue, OracleCommand SCM, string UserName, string Measure, string Wt, string DivFac,
        int From_Flag = 0)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            Int16 AllInRate = default(Int16);
            Int16 BasisType = default(Int16);
            long TransactionPK = 0;
            arrMessage.Clear();
            try
            {
                var _with12 = SCM;
                for (nRowCnt = 0; nRowCnt <= pDT.Rows.Count - 1; nRowCnt++)
                {
                    _with12.CommandType = CommandType.StoredProcedure;
                    _with12.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_TRN_AIR_INS";
                    DR = pDT.Rows[nRowCnt];
                    var _with13 = _with12.Parameters;
                    _with13.Clear();
                    _with13.Add("QUOTATION_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with13.Add("PORT_MST_POL_FK_IN", DR["POLFK"]).Direction = ParameterDirection.Input;
                    _with13.Add("PORT_MST_POD_FK_IN", DR["PODFK"]).Direction = ParameterDirection.Input;
                    _with13.Add("TRANS_REFERED_FROM_IN", DR["REF_TYPE"]).Direction = ParameterDirection.Input;
                    _with13.Add("TRANS_REF_NO_IN", DR["REF_NO"]).Direction = ParameterDirection.Input;
                    _with13.Add("AIRLINE_MST_FK_IN", DR["AIR_PK"]).Direction = ParameterDirection.Input;
                    _with13.Add("COMMODITY_GROUP_FK_IN", DR["COMM_GRPPK"]).Direction = ParameterDirection.Input;
                    _with13.Add("COMMODITY_MST_FK_IN", getDefault(DR["COMM_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with13.Add("ALL_IN_TARIFF_IN", getDefault(DR["AI_TRF"], 0)).Direction = ParameterDirection.Input;
                    _with13.Add("ALL_IN_QUOTED_TARIFF_IN", getDefault(DR["AI_QT"], 0)).Direction = ParameterDirection.Input;

                    _with13.Add("CHARGEABLE_WEIGHT_IN", getDefault(DR["CH_WT"], 0)).Direction = ParameterDirection.Input;

                    _with13.Add("PACK_COUNT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with13.Add("VOLUME_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with13.Add("DENSITY_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with13.Add("ACTUAL_WEIGHT_IN", DBNull.Value).Direction = ParameterDirection.Input;
                    _with13.Add("ACTUAL_VOLUME_IN", DBNull.Value).Direction = ParameterDirection.Input;

                    _with13.Add("SLAB_FK_IN", DR["SLAB"]).Direction = ParameterDirection.Input;
                    _with13.Add("QUANTITY_IN", getDefault(DR["BOXES"], 0)).Direction = ParameterDirection.Input;

                    _with13.Add("TRAN_REF_NO2_IN", getDefault(DR["REF_NO2"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with13.Add("REF_TYPE2_IN", getDefault(DR["TYPE2"], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with13.Add("BUYING_RATE_IN", DR["AIR_RT"]).Direction = ParameterDirection.Input;
                    _with13.Add("FROM_FLAG_IN", From_Flag).Direction = ParameterDirection.Input;
                    _with13.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with12.ExecuteNonQuery();
                    TransactionPK = Convert.ToInt64(_with12.Parameters["RETURN_VALUE"].Value);
                    arrMessage = SaveFreights(DR, cDT, TransactionPK, SCM, UserName);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }

                    arrMessage = SaveOtherFreights(DR, OthDT, TransactionPK, SCM, UserName);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }

                    arrMessage = SaveCargoInfo(DR, CalcDT, TransactionPK, SCM, UserName, Measure, Wt, DivFac);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                    if (Convert.ToInt32(DR["COMM_GRPPK"]) == HAZARDOUS)
                    {
                        arrMessage = SaveTransactionHZSpcl(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                    }
                    else if (Convert.ToInt32(DR["COMM_GRPPK"]) == REEFER)
                    {
                        arrMessage = SaveTransactionReefer(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                    }
                    else if (Convert.ToInt32(DR["COMM_GRPPK"]) == ODC)
                    {
                        arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(DR["strSpclReq"], "")), TransactionPK);
                    }
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
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

        #endregion " Save Transaction "

        #region " Freight Elemenmts "

        private ArrayList SaveFreights(DataRow pDR, DataTable cDT, long TrnPKValue, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            bool Flag = false;
            Int16 AllInRate = default(Int16);
            Int16 Chkadvatos = default(Int16);
            long FreightPk = 0;
            arrMessage.Clear();
            try
            {
                var _with14 = SCM;
                for (nRowCnt = 0; nRowCnt <= cDT.Rows.Count - 1; nRowCnt++)
                {
                    _with14.CommandType = CommandType.StoredProcedure;
                    _with14.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_TRN_AIR_FRT_DTLS_INS";
                    DR = cDT.Rows[nRowCnt];
                    Flag = false;

                    if (getDefault(DR["FK"], 0) == getDefault(pDR["PK"], 0))
                    {
                        var _with15 = _with14.Parameters;
                        _with15.Clear();
                        AllInRate = Convert.ToInt16((DR["SELECTED"] == "true" ? 1 : 0));
                        Chkadvatos = Convert.ToInt16((DR["ADVATOS"] == "true" ? 1 : 0));
                        _with15.Add("QUOTE_TRN_AIR_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                        _with15.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FRT_FK"]).Direction = ParameterDirection.Input;
                        _with15.Add("CHECK_FOR_ALL_IN_RT_IN", AllInRate).Direction = ParameterDirection.Input;
                        _with15.Add("CURRENCY_MST_FK_IN", DR["CURR_FK"]).Direction = ParameterDirection.Input;
                        _with15.Add("TARIFF_RATE_IN", getDefault(DR["TARIFF_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with15.Add("QUOTED_RATE_IN", getDefault(DR["QUOTED_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with15.Add("PYMT_TYPE_IN", getDefault(DR["P_TYPE"], 1)).Direction = ParameterDirection.Input;
                        _with15.Add("CHARGE_BASIS_IN", DR["CH_BASIS"]).Direction = ParameterDirection.Input;
                        _with15.Add("BASIS_RATE_IN", getDefault(DR["BASIS_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with15.Add("FREIGHT_TYPE_IN", DR["FRT_TYPE"]).Direction = ParameterDirection.Input;
                        _with15.Add("CHECK_ADVATOS_IN", Chkadvatos).Direction = ParameterDirection.Input;
                        //'Added Koteshwari for Advatos Column PTSID-JUN18

                        _with15.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with14.ExecuteNonQuery();
                        FreightPk = Convert.ToInt64(_with14.Parameters["RETURN_VALUE"].Value);
                    }
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

        #endregion " Freight Elemenmts "

        #region " Other Freight Elemenmts "

        private ArrayList SaveOtherFreights(DataRow pDR, DataTable OthDT, long TrnPKValue, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            long FreightPk = 0;
            decimal ChargeableWt = default(decimal);
            decimal BasisRate = default(decimal);
            decimal TrfRate = default(decimal);
            ChargeBasis CharBas = default(ChargeBasis);
            arrMessage.Clear();
            try
            {
                var _with16 = SCM;
                for (nRowCnt = 0; nRowCnt <= OthDT.Rows.Count - 1; nRowCnt++)
                {
                    _with16.CommandType = CommandType.StoredProcedure;
                    _with16.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_AIR_OTH_FRT_INS";
                    DR = OthDT.Rows[nRowCnt];
                    CharBas = (ChargeBasis)DR["CH_BASIS"];
                    ChargeableWt = Convert.ToDecimal(getDefault(pDR["CH_WT"], 0));
                    BasisRate = Convert.ToDecimal(getDefault(DR["BASIS_RATE"], 0));
                    if (CharBas == ChargeBasis.Flat)
                    {
                        TrfRate = BasisRate;
                    }
                    else
                    {
                        TrfRate = BasisRate * ChargeableWt;
                    }
                    DR["TARIFF_RATE"] = TrfRate;
                    if (DR["FK"] == pDR["PK"])
                    {
                        var _with17 = _with16.Parameters;
                        _with17.Clear();
                        _with17.Add("QUOTE_TRN_AIR_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                        _with17.Add("FREIGHT_ELEMENT_MST_FK_IN", DR["FRT_FK"]).Direction = ParameterDirection.Input;
                        _with17.Add("CURRENCY_MST_FK_IN", DR["CURR_FK"]).Direction = ParameterDirection.Input;
                        _with17.Add("AMOUNT_IN", getDefault(DR["TARIFF_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with17.Add("FREIGHT_TYPE_IN", getDefault(DR["PYMT_TYPE"], 0)).Direction = ParameterDirection.Input;
                        _with17.Add("CHARGE_BASIS_IN", DR["CH_BASIS"]).Direction = ParameterDirection.Input;
                        _with17.Add("BASIS_RATE_IN", getDefault(DR["BASIS_RATE"], 0)).Direction = ParameterDirection.Input;
                        _with17.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        _with16.ExecuteNonQuery();
                        FreightPk = Convert.ToInt64(_with16.Parameters["RETURN_VALUE"].Value);
                    }
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

        #endregion " Other Freight Elemenmts "

        #region " Cargo Information "

        private ArrayList SaveCargoInfo(DataRow pDR, DataTable CalcDT, long TrnPKValue, OracleCommand SCM, string UserName, string Measure, string Wt, string DivFac)
        {
            Int32 nRowCnt = default(Int32);
            DataRow DR = null;
            long CargoPk = 0;
            arrMessage.Clear();
            try
            {
                if ((CalcDT != null))
                {
                    var _with18 = SCM;
                    for (nRowCnt = 0; nRowCnt <= CalcDT.Rows.Count - 1; nRowCnt++)
                    {
                        _with18.CommandType = CommandType.StoredProcedure;
                        _with18.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_AIR_CARGO_INS";
                        DR = CalcDT.Rows[nRowCnt];
                        if (DR["FK"] == pDR["PK"] & Convert.ToInt32(DR["PK"]) == 2)
                        {
                            var _with19 = _with18.Parameters;
                            _with19.Clear();

                            _with19.Add("QUOTE_TRN_AIR_FK_IN", TrnPKValue).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_NOP_IN", getDefault(DR["NOP"], 0)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_LENGTH_IN", getDefault(DR["Length"], 0)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_WIDTH_IN", getDefault(DR["Width"], 0)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_HEIGHT_IN", getDefault(DR["Height"], 0)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_CUBE_IN", getDefault(DR["Cube"], 0)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_VOLUME_WT_IN", getDefault(DR["VolWeight"], 0)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_ACTUAL_WT_IN", getDefault(DR["ActWeight"], 0)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_DENSITY_IN", getDefault(DR["Density"], 0)).Direction = ParameterDirection.Input;

                            _with19.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                            _with19.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(DivFac) ? "0" : DivFac)).Direction = ParameterDirection.Input;

                            _with19.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with18.ExecuteNonQuery();
                            CargoPk = Convert.ToInt64(_with18.Parameters["RETURN_VALUE"].Value);
                        }
                    }
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

        #endregion " Cargo Information "

        #region " Protocol No Generation "

        public new string GenerateQuoteNo(long nLocationId, long nEmployeeId, long nCreatedBy, WorkFlow ObjWK = null, int From_Flag = 0)
        {
            string functionReturnValue = null;
            if (From_Flag == 0)
            {
                functionReturnValue = GenerateProtocolKey("QUOTATION (AIR)", nLocationId, nEmployeeId, DateTime.Now,"" ,"" ,"" , nCreatedBy, ObjWK);
            }
            else
            {
                functionReturnValue = GenerateProtocolKey("IMPORT QUOTATION AIR", nLocationId, nEmployeeId, DateTime.Now, "","" ,"" , nCreatedBy, ObjWK);
            }
            return functionReturnValue;
        }

        #endregion " Protocol No Generation "

        #endregion " Save Quotation "

        #region "Spetial requirment"

        #region "Spacial Request"

        private ArrayList SaveTransactionHZSpcl(OracleCommand SCM, string UserName, string strSpclRequest, long PkValue)
        {
            if (!string.IsNullOrEmpty(strSpclRequest))
            {
                arrMessage.Clear();
                string[] strParam = null;
                strParam = strSpclRequest.Split('~');
                try
                {
                    var _with20 = SCM;
                    _with20.CommandType = CommandType.StoredProcedure;
                    _with20.CommandText = UserName + ".QUOT_TRN_AIR_HAZ_SPL_REQ_PKG.QUOT_TRN_AIR_HAZ_SPL_REQ_INS";
                    var _with21 = _with20.Parameters;
                    _with21.Clear();
                    _with21.Add("QUOTE_TRN_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    _with21.Add("OUTER_PACK_TYPE_MST_FK_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("INNER_PACK_TYPE_MST_FK_IN", getDefault(strParam[1], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("MIN_TEMP_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[2].ToString()) ? "0" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    _with21.Add("MAX_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "0" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    _with21.Add("FLASH_PNT_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("FLASH_PNT_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "0" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    _with21.Add("IMDG_CLASS_CODE_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("UN_NO_IN", getDefault(strParam[9], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("IMO_SURCHARGE_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    _with21.Add("SURCHARGE_AMT_IN", getDefault(strParam[11], 0)).Direction = ParameterDirection.Input;
                    _with21.Add("IS_MARINE_POLLUTANT_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
                    _with21.Add("EMS_NUMBER_IN", getDefault(strParam[13], DBNull.Value)).Direction = ParameterDirection.Input;
                    _with21.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
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
            arrMessage.Add("All data saved successfully");
            return arrMessage;
        }

        public DataTable fetchSpclReq(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("SELECT QUOTE_TRN_AIR_HAZ_SPL_PK,");
                strQuery.Append("QUOTE_TRN_AIR_FK,");
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
                strQuery.Append("EMS_NUMBER FROM QUOTATION_TRN_AIR_HAZ_SPL_REQ Q");
                //,QUOTATION_TRN_AIR_FCL_LCL QT,QUOTATION_AIR_TBL QM" & vbCrLf)
                strQuery.Append("WHERE ");
                strQuery.Append("Q.QUOTE_TRN_AIR_FK=" + strPK);
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
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
                    var _with22 = SCM;
                    _with22.CommandType = CommandType.StoredProcedure;
                    _with22.CommandText = UserName + ".QUOTE_TRN_AIR_REF_SPL_REQ_PKG.QUOTE_TRN_AIR_REF_SPL_REQ_INS";
                    var _with23 = _with22.Parameters;
                    _with23.Clear();
                    //QUOTE_TRN_AIR_FK_IN()
                    _with23.Add("QUOTE_TRN_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //VENTILATION_IN()
                    _with23.Add("VENTILATION_IN", strParam[0]).Direction = ParameterDirection.Input;
                    //AIR_COOL_METHOD_IN()
                    _with23.Add("AIR_COOL_METHOD_IN", strParam[1]).Direction = ParameterDirection.Input;
                    //HUMIDITY_FACTOR_IN()
                    _with23.Add("HUMIDITY_FACTOR_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //IS_PERISHABLE_GOODS_IN()
                    _with23.Add("IS_PERISHABLE_GOODS_IN", strParam[3]).Direction = ParameterDirection.Input;
                    //MIN_TEMP_IN()
                    _with23.Add("MIN_TEMP_IN", getDefault(strParam[4], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MIN_TEMP_UOM_IN()
                    _with23.Add("MIN_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "0" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_IN()
                    _with23.Add("MAX_TEMP_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //MAX_TEMP_UOM_IN()
                    _with23.Add("MAX_TEMP_UOM_IN", getDefault((string.IsNullOrEmpty(strParam[6]) ? "0" : strParam[7]), 0)).Direction = ParameterDirection.Input;
                    //PACK_TYPE_MST_FK_IN()
                    _with23.Add("PACK_TYPE_MST_FK_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //PACK_COUNT_IN()
                    _with23.Add("PACK_COUNT_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
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

        public DataTable fetchSpclReqReefer(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("SELECT QUOTE_TRN_AIR_REF_SPL_PK,");
                strQuery.Append("QUOTE_TRN_AIR_FK,");
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
                strQuery.Append("FROM QUOTATION_TRN_AIR_REF_SPL_REQ Q");
                //,QUOTATION_TRN_AIR_FCL_LCL QT,QUOTATION_AIR_TBL QM" & vbCrLf)
                strQuery.Append("WHERE ");
                strQuery.Append("Q.QUOTE_TRN_AIR_FK=" + strPK);
                //strQuery.Append("QUOTATION_AIR_TBL QM," & vbCrLf)
                //strQuery.Append("QUOTATION_TRN_AIR_FCL_LCL QT" & vbCrLf)
                //strQuery.Append("WHERE " & vbCrLf)
                //strQuery.Append("Q.QUOTE_TRN_AIR_FK=QT.QUOTE_TRN_AIR_PK" & vbCrLf)
                //strQuery.Append("AND QT.QUOTATION_AIR_FK=QM.QUOTATION_AIR_PK" & vbCrLf)
                //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                //strQuery.Append("AND QM.QUOTATION_AIR_PK=" & strPK)
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
            }
        }

        public DataTable fetchSpclReqODC(string strPK)
        {
            if (!string.IsNullOrEmpty(strPK))
            {
                StringBuilder strQuery = new StringBuilder();
                strQuery.Append("SELECT ");
                strQuery.Append("QUOTE_TRN_AIR_ODC_SPL_PK,");
                strQuery.Append("QUOTE_TRN_AIR_FK,");
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
                strQuery.Append("FROM QUOTATION_TRN_AIR_ODC_SPL_REQ Q");
                //,QUOTATION_TRN_AIR_FCL_LCL QT,QUOTATION_AIR_TBL QM" & vbCrLf)
                strQuery.Append("WHERE ");
                strQuery.Append("Q.QUOTE_TRN_AIR_FK=" + strPK);
                //strQuery.Append("QUOTATION_AIR_TBL QM," & vbCrLf)
                //strQuery.Append("QUOTATION_TRN_AIR_FCL_LCL QT" & vbCrLf)
                //strQuery.Append("WHERE " & vbCrLf)
                //strQuery.Append("Q.QUOTE_TRN_AIR_FK=QT.QUOTE_TRN_AIR_PK" & vbCrLf)
                //strQuery.Append("AND QT.QUOTATION_AIR_FK=QM.QUOTATION_AIR_PK" & vbCrLf)
                //strQuery.Append("AND QT.TRANS_REF_NO='" & strRef & "'" & vbCrLf)
                //strQuery.Append("AND QM.QUOTATION_AIR_PK=" & strPK)
                return (new WorkFlow()).GetDataTable(strQuery.ToString());
            }
            else
            {
                return null;
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
                    var _with24 = SCM;
                    _with24.CommandType = CommandType.StoredProcedure;
                    _with24.CommandText = UserName + ".QUOTE_TRN_AIR_ODC_SPL_REQ_PKG.QUOTE_AIR_ODC_SPL_REQ_INS";
                    var _with25 = _with24.Parameters;
                    _with25.Clear();
                    //QUOTE_TRN_AIR_FK_IN()
                    _with25.Add("QUOTE_TRN_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                    //LENGTH_IN()
                    _with25.Add("LENGTH_IN", getDefault(strParam[0], DBNull.Value)).Direction = ParameterDirection.Input;
                    //LENGTH_UOM_MST_FK_IN()
                    _with25.Add("LENGTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[0]) ? "0" : strParam[1]), 0)).Direction = ParameterDirection.Input;
                    //HEIGHT_IN()
                    _with25.Add("HEIGHT_IN", getDefault(strParam[2], DBNull.Value)).Direction = ParameterDirection.Input;
                    //HEIGHT_UOM_MST_FK_IN()
                    _with25.Add("HEIGHT_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[2]) ? "0" : strParam[3]), 0)).Direction = ParameterDirection.Input;
                    //WIDTH_IN()
                    _with25.Add("WIDTH_IN", getDefault(strParam[4], 0)).Direction = ParameterDirection.Input;
                    //WIDTH_UOM_MST_FK_IN()
                    _with25.Add("WIDTH_UOM_MST_FK_IN", getDefault((string.IsNullOrEmpty(strParam[4]) ? "0" : strParam[5]), 0)).Direction = ParameterDirection.Input;
                    //WEIGHT_IN()
                    _with25.Add("WEIGHT_IN", getDefault(strParam[6], DBNull.Value)).Direction = ParameterDirection.Input;
                    //WEIGHT_UOM_MST_FK_IN()
                    _with25.Add("WEIGHT_UOM_MST_FK_IN", getDefault(strParam[7], 0)).Direction = ParameterDirection.Input;
                    //VOLUME_IN()
                    _with25.Add("VOLUME_IN", getDefault(strParam[8], DBNull.Value)).Direction = ParameterDirection.Input;
                    //VOLUME_UOM_MST_FK_IN()
                    _with25.Add("VOLUME_UOM_MST_FK_IN", getDefault(strParam[9], 0)).Direction = ParameterDirection.Input;
                    //SLOT_LOSS_IN()
                    _with25.Add("SLOT_LOSS_IN", getDefault(strParam[10], 0)).Direction = ParameterDirection.Input;
                    //LOSS_QUANTITY_IN()
                    _with25.Add("LOSS_QUANTITY_IN", getDefault(strParam[11], DBNull.Value)).Direction = ParameterDirection.Input;
                    //APPR_REQ_IN()
                    _with25.Add("APPR_REQ_IN", getDefault(strParam[12], 0)).Direction = ParameterDirection.Input;
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

        private ArrayList updateOthCharge(DataTable PDT, DataTable OthDT, DataTable CalcDSDT, OracleCommand SCM, string UserName, string Measure, string Wt, string DivFac)
        {
            DataRow Pdr = null;
            DataRow Odr = null;
            DataRow dr = null;
            int delFlage = 0;
            arrMessage.Clear();

            try
            {
                foreach (DataRow Pdr_loopVariable in PDT.Rows)
                {
                    Pdr = Pdr_loopVariable;
                    delFlage = 1;
                    //added by vimlesh kumar 26/08/2006 for adding special request

                    //end
                    SCM.CommandType = CommandType.StoredProcedure;
                    foreach (DataRow Odr_loopVariable in OthDT.Rows)
                    {
                        Odr = Odr_loopVariable;
                        SCM.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_TRN_AIR_OTH_CHRG_UPD";
                        if (Odr["FK"] == Pdr["PK"])
                        {
                            var _with26 = SCM.Parameters;
                            _with26.Clear();
                            //DEL_FLAG()
                            _with26.Add("DEL_FLAG", delFlage);
                            //QUOTATION_TRN_AIR_FK_IN()
                            _with26.Add("QUOTATION_TRN_AIR_FK_IN", Odr["FK"]);
                            //FREIGHT_ELEMENT_MST_FK_IN()
                            _with26.Add("FREIGHT_ELEMENT_MST_FK_IN", Odr["FRT_FK"]);
                            //CURRENCY_MST_FK_IN()
                            _with26.Add("CURRENCY_MST_FK_IN", Odr["CURR_FK"]);
                            //CHARGE_BASIS_IN()
                            _with26.Add("CHARGE_BASIS_IN", Odr["CH_BASIS"]);
                            //BASIS_RATE_IN()
                            _with26.Add("BASIS_RATE_IN", Odr["BASIS_RATE"]);
                            //AMOUNT_IN()
                            _with26.Add("AMOUNT_IN", getDefault(Odr["TARIFF_RATE"], 0));
                            _with26.Add("FREIGHT_TYPE_IN", getDefault(Odr["PYMT_TYPE"], 1));
                            //RETURN_VALUE()
                            _with26.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            delFlage += SCM.ExecuteNonQuery();
                        }
                    }
                    delFlage = 1;
                    foreach (DataRow dr_loopVariable in CalcDSDT.Rows)
                    {
                        dr = dr_loopVariable;
                        SCM.CommandText = UserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_AIR_CARGO_UPD";
                        if (dr["FK"] == Pdr["PK"])
                        {
                            var _with27 = SCM.Parameters;
                            _with27.Clear();
                            _with27.Add("DEL_FLAG", delFlage);
                            _with27.Add("QUOTE_TRN_AIR_FK_IN", dr["FK"]).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_NOP_IN", getDefault(dr["NOP"], 0)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_LENGTH_IN", getDefault(dr["Length"], 0)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_WIDTH_IN", getDefault(dr["Width"], 0)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_HEIGHT_IN", getDefault(dr["Height"], 0)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_CUBE_IN", getDefault(dr["Cube"], 0)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_VOLUME_WT_IN", getDefault(dr["VolWeight"], 0)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_ACTUAL_WT_IN", getDefault(dr["ActWeight"], 0)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_DENSITY_IN", getDefault(dr["Density"], 0)).Direction = ParameterDirection.Input;
                            //Manoharan 30Dec06: to save Measurement, weight and Division factor
                            _with27.Add("CARGO_MEASURE_IN", (string.IsNullOrEmpty(Measure) ? "0" : Measure)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_WT_IN", (string.IsNullOrEmpty(Wt) ? "0" : Wt)).Direction = ParameterDirection.Input;
                            _with27.Add("CARGO_DIVFAC_IN", (string.IsNullOrEmpty(DivFac) ? "0" : DivFac)).Direction = ParameterDirection.Input;

                            _with27.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            delFlage += SCM.ExecuteNonQuery();
                        }
                    }
                    if (Convert.ToInt32(Pdr["COMM_GRPPK"]) == HAZARDOUS)
                    {
                        arrMessage = SaveTransactionHZSpcl(SCM, UserName, Convert.ToString(getDefault(Pdr["strSpclReq"], "")), Convert.ToInt32(Pdr["PK"]));
                    }
                    else if (Convert.ToInt32(Pdr["COMM_GRPPK"]) == REEFER)
                    {
                        arrMessage = SaveTransactionReefer(SCM, UserName, Convert.ToString(getDefault(Pdr["strSpclReq"], "")), Convert.ToInt32(Pdr["PK"]));
                    }
                    else if (Convert.ToInt32(Pdr["COMM_GRPPK"]) == ODC)
                    {
                        arrMessage = SaveTransactionODC(SCM, UserName, Convert.ToString(getDefault(Pdr["strSpclReq"], "")), Convert.ToInt32(Pdr["PK"]));
                    }
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") == 0)
                        {
                            return arrMessage;
                        }
                    }
                }

                arrMessage.Add("saved");
                return arrMessage;
                //QUOTATION_AIR_FK_IN()
                //QUOTATION_TRN_REF()
                //FREIGHT_ELEMENT_MST_FK_IN()
                //CURRENCY_MST_FK_IN()
                //AMOUNT_IN()
                //RETURN_VALUE()
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

        #endregion "Spacial Request"

        #endregion "Spetial requirment"

        #region " Quotation Printing - Export AIR LCL "

        public DataSet FetchQuotationAirMain(Int32 Qpk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "  SELECT QAT.QUOTATION_AIR_PK QPK,";
            Strsql += "QAT.QUOTATION_REF_NO QREFNO,";
            Strsql += "CMST.CUSTOMER_NAME CUSTOMERNAME,";
            Strsql += "CDTLS.ADM_CONTACT_PERSON CONTACTPERSON,";
            Strsql += "CDTLS.ADM_FAX_NO FAXNO,";
            Strsql += "CGMST.COMMODITY_GROUP_DESC COMMODITY,";
            Strsql += "QACC.CARGO_NOP NUMBEROFPIECES,";
            Strsql += "QACC.CARGO_VOLUME_WT VOLUME_WEIGHT,";
            Strsql += "QTA.CHARGEABLE_WEIGHT,";
            Strsql += "QAT.header_content,";
            Strsql += "QAT.footer_content,QAT.remarks,";
            Strsql += "QACC.CARGO_ACTUAL_WT WEIGHT,";
            Strsql += "QACC.CARGO_CUBE VOLUME,";
            Strsql += "COLPLC.PLACE_CODE COLLECTIONPOINTCODE,";
            Strsql += "POL.PORT_NAME COLLECTIONPOINTNAME,";
            Strsql += "CMD.CARGO_MOVE_CODE DESTINATIONCODE,";
            Strsql += "POD.PORT_NAME  DESTINATIONNAME,";
            Strsql += "FREMST.FREIGHT_ELEMENT_NAME FREIGHTNAME,";
            Strsql += "CTMST.CURRENCY_ID CURRENCYID,";
            Strsql += "QTAFD.QUOTED_RATE TOTAL,";
            Strsql += "QTAFD.BASIS_RATE BASISRATE,";
            Strsql += "DECODE(QTAFD.CHARGE_BASIS,1,'%',2,'Flat Rate',3,'Kg') CHARGEBASIS,";
            Strsql += "QAT.VALID_FOR VALID_FOR";
            Strsql += "FROM QUOTATION_AIR_TBL     QAT,";
            Strsql += "QUOTATION_TRN_AIR     QTA,";
            Strsql += "QUOTATION_AIR_CARGO_CALC QACC,";
            Strsql += "QUOTATION_TRN_AIR_FRT_DTLS QTAFD,";
            Strsql += "CUSTOMER_MST_TBL      CMST,";
            Strsql += "CUSTOMER_CONTACT_DTLS CDTLS,";
            Strsql += "PLACE_MST_TBL         COLPLC,";
            Strsql += "PLACE_MST_TBL         DELPLC,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL FREMST,";
            Strsql += "CURRENCY_TYPE_MST_TBL CTMST,";
            Strsql += "COMMODITY_GROUP_MST_TBL CGMST,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += "CARGO_MOVE_MST_TBL CMD";
            Strsql += "WHERE CMST.CUSTOMER_MST_PK(+) = QAT.CUSTOMER_MST_FK";
            Strsql += "AND CDTLS.CUSTOMER_MST_FK(+) = CMST.CUSTOMER_MST_PK";
            Strsql += "AND QTA.QUOTATION_AIR_FK(+) = QAT.QUOTATION_AIR_PK";
            Strsql += "AND COLPLC.PLACE_PK(+) = QAT.COL_PLACE_MST_FK";
            Strsql += "AND DELPLC.PLACE_PK(+) = QAT.DEL_PLACE_MST_FK";
            Strsql += "AND QTAFD.QUOTE_TRN_AIR_FK(+)=QTA.QUOTE_TRN_AIR_PK";
            Strsql += "AND QAT.QUOTATION_AIR_PK=QTA.QUOTATION_AIR_FK(+)";
            Strsql += "AND FREMST.FREIGHT_ELEMENT_MST_PK(+)=QTAFD.FREIGHT_ELEMENT_MST_FK";
            Strsql += "AND CTMST.CURRENCY_MST_PK(+)=QTAFD.CURRENCY_MST_FK";
            Strsql += "AND CGMST.COMMODITY_GROUP_PK(+)=QTA.COMMODITY_GROUP_FK ";
            Strsql += "AND QTA.QUOTE_TRN_AIR_PK=QACC.QUOTATION_TRN_AIR_FK(+)";
            Strsql += "AND QTA.PORT_MST_POL_FK=POL.PORT_MST_PK(+) ";
            Strsql += "AND QTA.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ";
            Strsql += "AND QAT.Cargo_Move_Fk=CMD.CARGO_MOVE_PK(+) ";
            Strsql += "AND QAT.QUOTATION_AIR_PK=" + Qpk;
            Strsql += "ORDER BY QTAFD.FREIGHT_ELEMENT_MST_FK";
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        public DataSet FetchOtherFreightElements(Int32 Qpk)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            Strsql = "SELECT QAT.QUOTATION_AIR_PK QPK,";
            Strsql += "QAT.QUOTATION_REF_NO QREFNO,";
            Strsql += "OTHERFRE.FREIGHT_ELEMENT_NAME OTHERFRENAME,";
            Strsql += "OTHERCUR.CURRENCY_ID OTHERCURID,";

            Strsql += "QAOC.AMOUNT OTHERAMOUNT,";
            Strsql += "QAOC.BASIS_RATE OTHERBASISRATE,";
            Strsql += "DECODE(QAOC.CHARGE_BASIS,1,'%',2,'FlatRate',3,'Kgs',4,'Unit') OTHERCHARGE";

            Strsql += "FROM QUOTATION_AIR_TBL     QAT,";
            Strsql += "QUOTATION_TRN_AIR     QTA,";
            Strsql += "QUOTATION_TRN_AIR_OTH_CHRG QAOC,";
            Strsql += "FREIGHT_ELEMENT_MST_TBL OTHERFRE,";
            Strsql += "CURRENCY_TYPE_MST_TBL OTHERCUR";
            Strsql += "WHERE QTA.QUOTATION_AIR_FK(+) = QAT.QUOTATION_AIR_PK";
            Strsql += "AND QAOC.QUOTATION_TRN_AIR_FK(+)=QTA.QUOTE_TRN_AIR_PK";
            Strsql += "AND QAOC.FREIGHT_ELEMENT_MST_FK=OTHERFRE.FREIGHT_ELEMENT_MST_PK(+)";
            Strsql += "AND QAOC.CURRENCY_MST_FK=OTHERCUR.CURRENCY_MST_PK(+)";
            Strsql += "AND QAT.QUOTATION_AIR_PK=" + Qpk;
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        public DataSet FetchQuotationAirFright(Int32 Qpk)
        {
            string Strsql = null;
            WorkFlow ObjWk = new WorkFlow();
            Strsql = " SELECT QA.QUOTATION_AIR_PK,QA.QUOTATION_REF_NO,FE.FREIGHT_ELEMENT_NAME AS DESCRPTION,";
            Strsql += " CC.CURRENCY_ID,QFA.QUOTED_RATE AS AMOUNT,DU.DIMENTION_ID AS COMMENTS,QA.SPECIAL_INSTRUCTIONS";
            Strsql += " FROM QUOTATION_AIR_TBL QA,QUOTATION_TRN_AIR QAA,QUOTATION_TRN_AIR_FRT_DTLS QFA,FREIGHT_ELEMENT_MST_TBL FE,";
            Strsql += " CURRENCY_TYPE_MST_TBL CC,DIMENTION_UNIT_MST_TBL DU";

            Strsql += " WHERE QA.QUOTATION_AIR_PK=" + Qpk;
            Strsql += " AND QA.QUOTATION_AIR_PK=QAA.QUOTATION_AIR_FK";
            Strsql += " AND QAA.QUOTE_TRN_AIR_PK=QFA.QUOTE_TRN_AIR_FK";
            Strsql += " AND CC.CURRENCY_MST_PK(+)=QFA.CURRENCY_MST_FK";
            Strsql += " AND DU.DIMENTION_UNIT_MST_PK(+)=FE.UOM_MST_FK";
            Strsql += " AND DU.DIMENTION_UNIT_MST_PK(+)=FE.UOM_MST_FK";
            Strsql += " AND FE.FREIGHT_ELEMENT_MST_PK=QFA.FREIGHT_ELEMENT_MST_FK";
            try
            {
                return ObjWk.GetDataSet(Strsql);
            }
            catch (Exception ex)
            {
            }
            return new DataSet();
        }

        #endregion " Quotation Printing - Export AIR LCL "

        #region "Fetch UWG3 grid genral type "

        private void GetgenratedFrom(string QuoteNo = "", string Options = null)
        {
            int SrcType = 0;
            WorkFlow objWF = new WorkFlow();
            SrcType = Convert.ToInt32(objWF.ExecuteScaler(" SELECT QT.TRANS_REFERED_FROM FROM QUOT_GEN_TRN_AIR_TBL QT WHERE QT.QUOT_GEN_AIR_FK= " + QuoteNo));
            //if (SrcType == 1)
            //{
            //    ((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected = true;
            //}
            //else if (SrcType == SourceType.CustomerContract)
            //{
            //    ((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected = true;
            //}
            //else if (SrcType == SourceType.Quotation)
            //{
            //    ((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected = true;
            //}
            //else if (SrcType == SourceType.AirlineTariff)
            //{
            //    ((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected = true;
            //}
            //else if (SrcType == SourceType.GeneralTariff)
            //{
            //    ((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected = true;
            //}
            //else if (SrcType == SourceType.DefaultValue)
            //{
            //}
        }

        private StringBuilder Headerquery(int Group = 0)
        {
            StringBuilder strQuery = new StringBuilder();
            if (Group == 1 | Group == 2)
            {
                strQuery.Append("SELECT TRAN.QUOT_GEN_AIR_TRN_PK PK,");
                strQuery.Append("       TRAN.QUOT_GEN_AIR_FK     FK,");
                strQuery.Append("       TRAN.TRANS_REFERED_FROM  REF_TYPE,");
                strQuery.Append("       TRAN.POL_GRP_FK          POLFK,");
                strQuery.Append("       PGL.PORT_GRP_ID          POL_ID,");
                strQuery.Append("       TRAN.POD_GRP_FK          PODFK,");
                //
                strQuery.Append("       PGD.PORT_GRP_ID          POD_ID,");
                strQuery.Append("       TRAN.AIRLINE_MST_FK   AIR_PK,");
                strQuery.Append("       AIR.AIRLINE_ID        AIR_ID,");
                strQuery.Append("       TRAN.COMMODITY_MST_FK COMM_PK,");
                strQuery.Append("       CMDT.COMMODITY_ID     COMM_ID,");
                strQuery.Append("       MAIN.CUSTOMER_MST_FK CUSTOMER_PK,");
                strQuery.Append("       MAIN.CUSTOMER_CATEGORY_MST_FK CUSTOMER_CATPK,");
                strQuery.Append("       TO_CHAR(tran.VALID_TO, dateFormat )  SHIP_DATE,");
                strQuery.Append("       MAIN.COMMODITY_GROUP_MST_FK COMM_GRPPK,");
                strQuery.Append("       NVL(CUST_TYPE, 1) CUST_TYPE");
                strQuery.Append("  FROM QUOTATION_AIR_TBL    MAIN,");
                strQuery.Append("       QUOT_GEN_TRN_AIR_TBL TRAN,");
                strQuery.Append("       PORT_GRP_MST_TBL  PGL,");
                strQuery.Append("       PORT_GRP_MST_TBL  PGD,");
                strQuery.Append("       AIRLINE_MST_TBL   AIR,");
                strQuery.Append("       COMMODITY_MST_TBL CMDT");
                strQuery.Append(" WHERE MAIN.QUOTATION_AIR_PK = TRAN.QUOT_GEN_AIR_FK");
                strQuery.Append("   AND TRAN.POL_GRP_FK = PGL.PORT_GRP_MST_PK");
                strQuery.Append("   AND TRAN.POD_GRP_FK = PGD.PORT_GRP_MST_PK");
                strQuery.Append("   AND TRAN.COMMODITY_MST_FK = CMDT.COMMODITY_MST_PK(+)");
                strQuery.Append("   AND TRAN.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)");
                strQuery.Append("   AND MAIN.QUOTATION_AIR_PK = ");
            }
            else
            {
                strQuery.Append("SELECT TRAN.QUOT_GEN_AIR_TRN_PK PK,");
                strQuery.Append("       TRAN.QUOT_GEN_AIR_FK     FK,");
                strQuery.Append("       TRAN.TRANS_REFERED_FROM  REF_TYPE,");
                strQuery.Append("       TRAN.PORT_MST_POL_FK  POLFK,");
                strQuery.Append("       PORTPOL.PORT_ID       POL_ID,");
                strQuery.Append("       TRAN.PORT_MST_POD_FK  PODFK,");
                //
                strQuery.Append("       PORTPOD.PORT_ID       POD_ID,");
                strQuery.Append("       TRAN.AIRLINE_MST_FK   AIR_PK,");
                strQuery.Append("       AIR.AIRLINE_ID        AIR_ID,");
                strQuery.Append("       TRAN.COMMODITY_MST_FKS COMM_PK,");
                strQuery.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
                strQuery.Append("                NVL(TRAN.COMMODITY_MST_FKS, 0) || ')') COMM_ID,");
                strQuery.Append("       MAIN.CUSTOMER_MST_FK CUSTOMER_PK,");
                strQuery.Append("       MAIN.CUSTOMER_CATEGORY_MST_FK CUSTOMER_CATPK,");
                strQuery.Append("       TO_CHAR(tran.VALID_TO, dateFormat )  SHIP_DATE,");
                strQuery.Append("       MAIN.COMMODITY_GROUP_MST_FK COMM_GRPPK,");
                strQuery.Append("       NVL(CUST_TYPE, 1) CUST_TYPE");
                strQuery.Append("  FROM QUOTATION_AIR_TBL    MAIN,");
                strQuery.Append("       QUOT_GEN_TRN_AIR_TBL TRAN,");
                strQuery.Append("       PORT_MST_TBL      PORTPOL,");
                strQuery.Append("       PORT_MST_TBL      PORTPOD,");
                strQuery.Append("       AIRLINE_MST_TBL   AIR,");
                strQuery.Append("       COMMODITY_MST_TBL CMDT");
                strQuery.Append(" WHERE MAIN.QUOTATION_AIR_PK = TRAN.QUOT_GEN_AIR_FK");
                strQuery.Append("      AND TRAN.PORT_MST_POL_FK = PORTPOL.PORT_MST_PK");
                strQuery.Append("   AND TRAN.PORT_MST_POD_FK = PORTPOD.PORT_MST_PK");
                strQuery.Append("   AND TRAN.COMMODITY_MST_FK = CMDT.COMMODITY_MST_PK(+)");
                strQuery.Append("   AND TRAN.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)");
                strQuery.Append("   AND MAIN.QUOTATION_AIR_PK = ");
            }
            return strQuery;
        }

        public void FetchGeneral(DataSet GridDS = null, DataSet EnqDS = null, string EnqNo = "", string QuoteNo = "", string CustNo = "", string CustID = "", string CustCategory = "", string AgentNo = "", string AgentID = "", string Sectors = "",
        string CommodityGroup = "", string ShipDate = "", string QuoteDate = "", string Options = null, int Version = 0, string QuotationStatus = null, DataTable OthDt = null, DataTable EnqOthDt = null, DataSet CalcDS = null, string ValidFor = null,
        Int16 CustomerType = 0, int CreditDays = 0, int CreditLimit = 0, int Remarks = 0, int CargoMcode = 0, int BaseCurrencyId = 0, int INCOTerms = 0, int PYMTType = 0, int Group = 0, bool AmendFlg = false)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder MasterQueryQuot = new StringBuilder();
            string strSQL = null;
            string strSQL1 = null;
            Array Arr = null;
            string Polpk = null;
            string Podpk = null;
            DataSet DSMain = null;
            DataTable DTMain = null;
            DSMain = GridDS.Clone();
            if (!string.IsNullOrEmpty(QuoteNo))
            {
                quotNewRecord = false;
                EnqDS = new DataSet();
                //((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected = true;
                MasterQueryQuot = Headerquery(Group);
                RenderData(QuoteNo, Version, QuotationStatus, ValidFor, QuoteDate, ShipDate, CreditDays, CreditLimit, Remarks, CargoMcode, Convert.ToInt32(CommodityGroup), BaseCurrencyId, INCOTerms, PYMTType);
                EnqDS = objWF.GetDataSet(MasterQueryQuot.Append(QuoteNo).ToString());
                if (EnqDS.Tables[0].Rows.Count > 0)
                {
                    Sectors = Sectors.ToString().TrimEnd(',');
                    CustNo = Convert.ToString(EnqDS.Tables[0].Rows[0]["CUSTOMER_PK"]);
                    CustCategory = Convert.ToString(EnqDS.Tables[0].Rows[0]["CUSTOMER_CATPK"]);
                    CommodityGroup = Convert.ToString(EnqDS.Tables[0].Rows[0]["COMM_GRPPK"]);
                    Polpk = Convert.ToString(EnqDS.Tables[0].Rows[0]["POLFK"]);
                    Podpk = Convert.ToString(EnqDS.Tables[0].Rows[0]["PODFK"]);

                    CustomerType = Convert.ToInt16(getDefault(EnqDS.Tables[0].Rows[0]["CUST_TYPE"], 0));
                    if (CustomerType == 0)
                    {
                        CustID = objWF.ExecuteScaler(" Select CUSTOMER_NAME from CUSTOMER_MST_TBL where CUSTOMER_MST_PK = " + CustNo);
                    }
                    else
                    {
                        CustID = objWF.ExecuteScaler(" Select CUSTOMER_NAME from TEMP_CUSTOMER_TBL where CUSTOMER_MST_PK = " + CustNo);
                    }
                }
                strSQL = QueryQuot(CustNo, Sectors, CommodityGroup, ShipDate, QuoteNo, Group);
            }
            else
            {
                quotNewRecord = true;
                Arr = Convert.ToString(Sectors).Split('~');
                Sectors = Cls_SRRAirContract.MakePortPairString(Convert.ToString(Arr.GetValue(0)), Convert.ToString(Arr.GetValue(1)));
                strSQL = MasterQuery(Options, CustNo, Sectors, CommodityGroup, ShipDate, Group);
            }

            string ContainerPKs = null;
            bool NewRecord = true;

            DataTable ChildDt = null;
            objWF.MyCommand.Parameters.Clear();
            ChildDt = objWF.GetDataTable(strSQL);
            string TrnPk = GetAllKeys(ChildDt, 18).ToString();
            // ChildDt.Rows(0).Item(1)
            if (AmendFlg == true)
            {
                strSQL = Convert.ToString(GetGridDetails(TrnPk, Polpk, Podpk));
            }
            else
            {
                strSQL = TrnQuery(Options, TrnPk, Group);
            }
            DataTable KGFrtDt = null;
            KGFrtDt = objWF.GetDataTable(strSQL);
            string KGFreights = null;
            KGFreights = getStrFreights(KGFrtDt);
            AddColumns(ChildDt, KGFreights);
            // KGFreights Columns
            TransferKGFreightsData(ChildDt, KGFrtDt, AmendFlg);
            //    KGFreights Data in ChildDt now..
            ChildDt.Columns.Add("OthChrg");
            ChildDt.Columns.Add("OTHDTL");
            strSQL = OthQuery(Options, TrnPk);
            OthDt = objWF.GetDataTable(strSQL);

            if (AmendFlg == true)
            {
                strSQL = Convert.ToString(FetchFrtQuery(TrnPk, Polpk, Podpk));
                strSQL1 = FrtQuery(Options, TrnPk);
            }
            else
            {
                strSQL = FrtQuery(Options, TrnPk);
            }

            DataTable GRFrtDt = null;
            DataTable GRFrtDt1 = null;
            if (AmendFlg == true)
            {
                GRFrtDt1 = objWF.GetDataTable(strSQL1);
            }
            GRFrtDt = objWF.GetDataTable(strSQL);
            string FreightPks = "-1,";
            Int16 rCount = default(Int16);
            if (AmendFlg == true)
            {
                for (rCount = 0; rCount <= GRFrtDt1.Rows.Count - 1; rCount++)
                {
                    FreightPks += Convert.ToString(GRFrtDt1.Rows[rCount][0]) + ",";
                }
            }
            else
            {
                for (rCount = 0; rCount <= GRFrtDt.Rows.Count - 1; rCount++)
                {
                    FreightPks += Convert.ToString(GRFrtDt.Rows[rCount][0]) + ",";
                }
            }
            FreightPks = FreightPks.TrimEnd(',');

            if (AmendFlg == true)
            {
                strSQL = Convert.ToString(GetSlabDetails(FreightPks, Polpk, Podpk));
            }
            else
            {
                strSQL = SlabQuery(Options, FreightPks);
            }

            DataTable SlbDt = null;
            SlbDt = objWF.GetDataTable(strSQL);
            string AirSlabs = null;
            AirSlabs = getStrSlabs(SlbDt);
            AddColumns(GRFrtDt, AirSlabs);
            TransferSlabsData(GRFrtDt, SlbDt, AmendFlg);
            //

            GridDS.Tables.Add(ChildDt);
            GridDS.Tables.Add(GRFrtDt);
            if (!string.IsNullOrEmpty(QuoteNo))
            {
                GetgenratedFrom(QuoteNo, Options);
            }
            if (AmendFlg == true)
            {
                DataRelation rel = new DataRelation("rl_POL_POD", new DataColumn[] {
                    GridDS.Tables[0].Columns["PORT_MST_POL_FK"],
                    GridDS.Tables[0].Columns["PORT_MST_POD_FK"]
                }, new DataColumn[] {
                    GridDS.Tables[1].Columns["POL_PK"],
                    GridDS.Tables[1].Columns["POD_PK"]
                });
                GridDS.Relations.Add(rel);
            }
            else
            {
                DataRelation REL = new DataRelation("RFQRelation", new DataColumn[] { GridDS.Tables[0].Columns["PK"] }, new DataColumn[] { GridDS.Tables[1].Columns["FK"] });

                GridDS.Relations.Add(REL);
            }
            GridDS.Relations[0].Nested = true;
        }

        private string MasterQuery(string Options = null, string CustNo = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "", int Group = 0)
        {
            string strSQL = null;
            if (quotNewRecord != false)
            {
                //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                //{
                //    strSQL = QuerySpotRate(CustNo, Sectors, CommodityGroup, ShipDate);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                //{
                //    strSQL = QueryContCust(CustNo, Sectors, CommodityGroup, ShipDate);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //{
                //    strSQL = QueryQuot(CustNo, Sectors, CommodityGroup, ShipDate);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                //{
                //    strSQL = QueryAirTariff(1, Sectors, CommodityGroup, ShipDate, Group);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                //{
                //    strSQL = QueryAirTariff(2, Sectors, CommodityGroup, ShipDate, Group);
                //}
            }
            else
            {
                strSQL = QueryQuot(CustNo, Sectors, CommodityGroup, ShipDate);
            }

            return strSQL;
        }

        private string TrnQuery(string Options = null, string trnPk = "0", int Group = 0)
        {
            string strSQL = null;
            if (quotNewRecord != false)
            {
                //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                //{
                //    strSQL = QueryTrnSpotRate(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                //{
                //    strSQL = QueryTrnContCust(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //{
                //    strSQL = QueryTrnQuot(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                //{
                //    strSQL = QueryTrnAirTariff(trnPk, Group);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                //{
                //    strSQL = QueryTrnAirTariff(trnPk, Group);
                //}
            }
            else
            {
                strSQL = QueryTrnQuot(trnPk);
            }

            return strSQL;
        }

        private string OthQuery(string Options = null, string trnPk = "0")
        {
            string strSQL = null;
            if (quotNewRecord != false)
            {
                //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                //{
                //    strSQL = SpotRateOthQuery(trnPk).ToString();
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                //{
                //    strSQL = CustContOthQuery(trnPk).ToString();
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //{
                //    strSQL = QueryTrnOthQuot(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                //{
                //    strSQL = AirTariffOthQuery(trnPk).ToString();
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                //{
                //    strSQL = GenTariffOthQuery(trnPk).ToString();
                //}
            }
            else
            {
                strSQL = QueryTrnOthQuot(trnPk);
            }
            return strSQL;
        }

        private string FrtQuery(string Options = null, string trnPk = "0")
        {
            string strSQL = null;
            if (quotNewRecord != false)
            {
                //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                //{
                //    strSQL = QueryTrnFRTSpotRate(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                //{
                //    strSQL = QueryTrnFRTContCust(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //{
                //    strSQL = QueryTrnFRTQuot(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                //{
                //    strSQL = QueryTrnFRTAirTariff(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                //{
                //    strSQL = QueryTrnFRTAirTariff(trnPk);
                //}
            }
            else
            {
                strSQL = QueryTrnFRTQuot(trnPk);
            }
            return strSQL;
        }

        private string SlabQuery(string Options = null, string trnPk = "0")
        {
            string strSQL = null;
            if (quotNewRecord != false)
            {
                //if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[0].Selected)
                //{
                //    strSQL = QueryTrnSlabSpotRate(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[1].Selected)
                //{
                //    strSQL = QueryTrnSlabContCust(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[2].Selected)
                //{
                //    strSQL = QueryTrnSlabQuot(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[3].Selected)
                //{
                //    strSQL = QueryTrnSlabAirTariff(trnPk);
                //}
                //else if (((System.Web.UI.WebControls.RadioButtonList)Options).Items[4].Selected)
                //{
                //    strSQL = QueryTrnSlabAirTariff(trnPk);
                //}
            }
            else
            {
                strSQL = QueryTrnSlabQuot(trnPk);
            }
            return strSQL;
        }

        #region "Manual"

        /// This function return datatable for parent grid depending upon airline and avialable
        /// Air Surcharges also this fuction changes behaviour accoing to constructor called.
        private DataTable FetchManualHDR(string strPolPk, string strPodPk, string Mode, bool IsAirlineTariff, long lngAirlinePk, string ChargeBasis, string AirSuchargeToolTip, string strFDate = "", string strTDate = "", int Group = 0)
        {
            StringBuilder strQuery = new StringBuilder();
            string str = null;
            string strCondition = null;
            string strNewModeCondition = null;
            WorkFlow objWF = new WorkFlow();
            cls_AirlineTariffEntry objAirLineTariff = new cls_AirlineTariffEntry(1, 2, false);
            // Private Const _AirLine_Tariff_Cols As Integer = 7
            DataTable dtMain = null;
            DataTable dtKgs = null;
            int intdtMain_Arr_RowCnt = 0;
            int intdtMainColCnt = 0;
            int intdtKgsRowCnt = 0;
            int Static_Col = 0;
            string[] arrPolPk = null;
            string[] arrPodPk = null;

            ChargeBasis = "";
            AirSuchargeToolTip = "";
            //Spliting the POL and POD Pk's
            arrPolPk = strPolPk.Split(Convert.ToChar(","));
            arrPodPk = strPodPk.Split(Convert.ToChar(","));

            if (Group == 1 | Group == 2)
            {
                if (!(Mode == "EDIT"))
                {
                    strNewModeCondition =  " AND PGL.BIZ_TYPE  = 1";
                    strNewModeCondition +=  "AND PGD.BIZ_TYPE  = 1";
                }
                strCondition = " (PGL.PORT_GRP_MST_PK IN (" + strPolPk + " )AND PGD.PORT_GRP_MST_PK IN (" + strPodPk + " )) ";

                strQuery.Append(" SELECT '' REFNO,");
                strQuery.Append("       '' AIRLINE,");
                strQuery.Append("       PGL.PORT_GRP_MST_PK AS \"PORT_MST_POL_FK\",");
                strQuery.Append("       PGL.PORT_GRP_ID AS \"AOO\",");
                strQuery.Append("       PGL.PORT_GRP_NAME AS \"PolName\",");
                strQuery.Append("       PGD.PORT_GRP_MST_PK AS \"PORT_MST_POD_FK\",");
                strQuery.Append("       PGD.PORT_GRP_ID AS \"AOD\",");
                strQuery.Append("       PGD.PORT_GRP_NAME AS \"PodName\",");
                strQuery.Append("       SYSDATE AS \"VALID_FROM\",");
                strQuery.Append("       '' AS \"VALID_TO\",");
                strQuery.Append("       '' COMMODITY_MST_FK,");
                strQuery.Append("       '' COMMODITY_NAME,");
                strQuery.Append("       0 VOLUME_IN_CBM,");
                strQuery.Append("       0 PACK_COUNT,");
                strQuery.Append("       0 CHARGEABLE_WEIGHT,");
                strQuery.Append("       0 VOLUME_WEIGHT,");
                strQuery.Append("       0 DENSITY,");
                strQuery.Append("       0 FK,");
                strQuery.Append("       0 PK,");
                strQuery.Append("       0 AIRLINE_MST_PK ");
                strQuery.Append("  FROM PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD");
                strQuery.Append(" WHERE (1 = 1)");
                strQuery.Append("   AND (" + strCondition + ")");
                strQuery.Append(strNewModeCondition);
                strQuery.Append(" GROUP BY PGL.PORT_GRP_MST_PK,");
                strQuery.Append("          PGL.PORT_GRP_ID,");
                strQuery.Append("          PGD.PORT_GRP_ID,");
                strQuery.Append("          PGD.PORT_GRP_MST_PK,");
                strQuery.Append("          PGL.PORT_GRP_NAME,");
                strQuery.Append("          PGD.PORT_GRP_NAME");
                strQuery.Append("HAVING PGL.PORT_GRP_ID <> PGD.PORT_GRP_ID");
                strQuery.Append(" ORDER BY PGL.PORT_GRP_ID, PGD.PORT_GRP_ID");
            }
            else
            {
                //Making condition as the record should have only selected POL and POD
                //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
                //is the selected sector.
                for (intdtMain_Arr_RowCnt = 0; intdtMain_Arr_RowCnt <= arrPolPk.Length - 1; intdtMain_Arr_RowCnt++)
                {
                    if (string.IsNullOrEmpty(strCondition))
                    {
                        if (arrPodPk.Length > 0)
                        {
                            strCondition = " (POL.PORT_MST_PK =" + arrPolPk[intdtMain_Arr_RowCnt] + " AND POD.PORT_MST_PK =" + arrPodPk[intdtMain_Arr_RowCnt] + ")";
                        }
                        else
                        {
                            strCondition = " (POL.PORT_MST_PK =" + arrPolPk[intdtMain_Arr_RowCnt] + ")";
                        }
                    }
                    else
                    {
                        if (arrPodPk.Length > 0)
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk[intdtMain_Arr_RowCnt] + " AND POD.PORT_MST_PK =" + arrPodPk[intdtMain_Arr_RowCnt] + ")";
                        }
                        else
                        {
                            strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk[intdtMain_Arr_RowCnt] + ")";
                        }
                    }
                }

                if (!(Mode == "EDIT"))
                {
                    strNewModeCondition =  " AND POL.BUSINESS_TYPE = 1";
                    //BUSINESS_TYPE = 1 :- Is the business type for Air
                    strNewModeCondition +=  " AND POD.BUSINESS_TYPE = 1";
                    //BUSINESS_TYPE = 1 :- Is the business type for Air
                }

                //Making query with the condition added
                //str = str.Empty & " SELECT 0 AS TRANPK,POL.PORT_MST_PK AS ""POLPK"",POL.PORT_ID AS ""AOO"","
                //str &= vbCrLf & " POD.PORT_MST_PK AS ""PODPK"",POD.PORT_ID AS ""AOD"","
                //str &= vbCrLf & " '" & strFDate & "' AS ""Valid From"",'" & strTDate & "' AS ""Valid To"""
                //str &= vbCrLf & " FROM PORT_MST_TBL POL, PORT_MST_TBL POD"
                //str &= vbCrLf & " WHERE (1=1)"
                //str &= vbCrLf & " AND ("
                //str &= vbCrLf & strCondition & ")"
                //str &= vbCrLf & strNewModeCondition
                //str &= vbCrLf & " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_ID,POD.PORT_MST_PK"
                //str &= vbCrLf & " HAVING POL.PORT_ID<>POD.PORT_ID"
                //str &= vbCrLf & " ORDER BY POL.PORT_ID"

                strQuery.Append("SELECT '' REFNO,");
                strQuery.Append("       '' AIRLINE,");
                strQuery.Append("       POL.PORT_MST_PK AS \"PORT_MST_POL_FK\",");
                strQuery.Append("       POL.PORT_ID AS \"AOO\",");
                strQuery.Append("       POL.PORT_NAME AS \"PolName\",");
                strQuery.Append("       POD.PORT_MST_PK AS \"PORT_MST_POD_FK\",");
                strQuery.Append("       POD.PORT_ID AS \"AOD\",");
                strQuery.Append("       POD.PORT_NAME AS \"PodName\",");
                strQuery.Append("       SYSDATE AS \"VALID_FROM\",");
                strQuery.Append("       '' AS \"VALID_TO\",");
                strQuery.Append("       '' COMMODITY_MST_FK,");
                strQuery.Append("       '' COMMODITY_NAME,");
                strQuery.Append("       0 VOLUME_IN_CBM,");
                strQuery.Append("       0 PACK_COUNT,");
                strQuery.Append("       0 CHARGEABLE_WEIGHT,");
                strQuery.Append("       0 VOLUME_WEIGHT,");
                strQuery.Append("       0 DENSITY,");
                strQuery.Append("       0 FK,");
                strQuery.Append("       0 PK,");
                strQuery.Append("       0 AIRLINE_MST_PK ");
                strQuery.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD");
                strQuery.Append(" WHERE (1 = 1)");
                strQuery.Append("   AND (" + strCondition + ")");
                strQuery.Append(strNewModeCondition);
                strQuery.Append(" GROUP BY POL.PORT_MST_PK,");
                strQuery.Append("          POL.PORT_ID,");
                strQuery.Append("          POD.PORT_ID,");
                strQuery.Append("          POD.PORT_MST_PK,");
                strQuery.Append("          POL.PORT_NAME,");
                strQuery.Append("          POD.PORT_NAME");
                strQuery.Append("HAVING POL.PORT_ID <> POD.PORT_ID");
                strQuery.Append(" ORDER BY POL.PORT_ID, POD.PORT_ID");
            }

            try
            {
                dtMain = objWF.GetDataTable(strQuery.ToString());

                //If _Static_Col > 7 Then
                //    dtMain.Columns.Add("Expected_Wt")
                //    dtMain.Columns.Add("Expected_Vol")
                //End If

                dtKgs = objAirLineTariff.FetchKgFreight(Mode, IsAirlineTariff, lngAirlinePk);
                //For intdtKgsRowCnt = 0 To dtKgs.Rows.Count - 1
                //    'Current Rate
                //    dtMain.Columns.Add(dtKgs.Rows(intdtKgsRowCnt).Item("FREIGHT_ELEMENT_MST_PK").ToString, GetType(Decimal))
                //    'Tariff Rate
                //    dtMain.Columns.Add(dtKgs.Rows(intdtKgsRowCnt).Item("FREIGHT_ELEMENT_ID").ToString, GetType(Decimal))
                //    dtMain.Columns.Add(dtKgs.Rows(intdtKgsRowCnt).Item("FREIGHT_ELEMENT_NAME").ToString & '~' & dtKgs.Rows(intdtKgsRowCnt).Item("CHARGE_BASIS").ToString, GetType(Decimal))
                //    'Charge Basis
                //    ChargeBasis &= dtKgs.Rows(intdtKgsRowCnt).Item("CHARGE_BASIS").ToString & ","
                //    'Air Surcharge Tool tip
                //    AirSuchargeToolTip &= dtKgs.Rows(intdtKgsRowCnt).Item("FREIGHT_ELEMENT_NAME").ToString & ","
                //Next

                dtMain.Columns.Add("OthChrg");
                dtMain.Columns.Add("OTHDTL");

                intdtMainColCnt = dtMain.Columns.Count - 3;

                for (intdtMain_Arr_RowCnt = 0; intdtMain_Arr_RowCnt <= dtMain.Rows.Count - 1; intdtMain_Arr_RowCnt++)
                {
                    //Static_Col = _Static_Col
                    //While Static_Col <= intdtMainColCnt

                    for (intdtKgsRowCnt = 0; intdtKgsRowCnt <= dtKgs.Rows.Count - 1; intdtKgsRowCnt++)
                    {
                        if (dtMain.Columns[Static_Col].ColumnName == dtKgs.Rows[intdtKgsRowCnt]["FREIGHT_ELEMENT_MST_PK"].ToString())
                        {
                            dtMain.Rows[intdtMain_Arr_RowCnt][Static_Col] = dtKgs.Rows[intdtKgsRowCnt]["BASIS_VALUE"].ToString();

                            dtMain.Rows[intdtMain_Arr_RowCnt][Static_Col + 1] = dtKgs.Rows[intdtKgsRowCnt]["BASIS_VALUE"].ToString();
                            dtMain.Rows[intdtMain_Arr_RowCnt][Static_Col + 2] = dtKgs.Rows[intdtKgsRowCnt]["BASIS_VALUE"].ToString();
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                    //For loop for Rows in dtKgs

                    //Static_Col = Static_Col + _Col_Incr
                    //End While
                }
                //For loop for Rows in dtMain

                ChargeBasis = ChargeBasis.TrimEnd(Convert.ToChar(","));
                AirSuchargeToolTip = AirSuchargeToolTip.TrimEnd(Convert.ToChar(","));

                return dtMain;
            }
            catch (OracleException SQLEX)
            {
                throw SQLEX;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void CreateGrid(string strPolPk, string strPodPk, Int16 strCondition = 0, DataSet GridDS = null, DataTable OthDt = null, int Group = 0)
        {
            try
            {
                cls_AirlineTariffEntry objAirLineTariff = new cls_AirlineTariffEntry(1, 2, false);

                //Getting the header for Band(0) for New Mode
                GridDS.Tables.Add(FetchManualHDR(strPolPk, strPodPk, "NEW", false, 0, "", "", "", "", Group));

                //Getting the header for Band(1) for New Mode
                GridDS.Tables.Add(FetchManualFreight(strPolPk, strPodPk, "NEW", Group));

                //Creating relations between POL,POD of main header and POL,POD of child header
                DataRelation rel = new DataRelation("rl_POL_POD", new DataColumn[] {
                    GridDS.Tables[0].Columns["PORT_MST_POL_FK"],
                    GridDS.Tables[0].Columns["PORT_MST_POD_FK"]
                }, new DataColumn[] {
                    GridDS.Tables[1].Columns["POLPK"],
                    GridDS.Tables[1].Columns["PODPK"]
                });
                GridDS.Relations.Clear();
                GridDS.Relations.Add(rel);
                WorkFlow objWF = new WorkFlow();
                OthDt = objWF.GetDataTable(ManualOthQuery().ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private string QueryTrnOthManual()
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("SELECT 0 FK,");
            strQuery.Append("           FREIGHT_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_FK,");
            strQuery.Append("           FREIGHT_ELEMENT_ID,");
            strQuery.Append("           FREIGHT_ELEMENT_NAME,");
            strQuery.Append("           CURRENCY_MST_PK CURRENCY_MST_FK,");
            strQuery.Append("           CURRENCY_ID,");
            strQuery.Append("           CURRENCY_NAME,");
            strQuery.Append(" NVL(APPROVED_RATE,0)               CURRENT_RATE,");
            strQuery.Append(" NVL(APPROVED_RATE,0)               REQUEST_RATE,");
            strQuery.Append(" NVL(APPROVED_RATE,0)               APPROVED_RATE");
            strQuery.Append("            CHARGE_BASIS");
            strQuery.Append("      FROM FREIGHT_ELEMENT_MST_TBL, CURRENCY_TYPE_MST_TBL");
            strQuery.Append("     WHERE");
            strQuery.Append("     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1");
            strQuery.Append("     AND CURRENCY_MST_PK =" + HttpContext.Current.Session["currency_mst_pk"]);
            strQuery.Append("     AND NVL(CHARGE_TYPE, 3) = 3");
            strQuery.Append("     AND FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE IN (1, 3)");
            strQuery.Append("     ORDER BY FREIGHT_ELEMENT_ID");

            return strQuery.ToString();
        }

        /// This function return datatable for child grid. It contains Freight form Freight Table
        /// having freight type of "Freight" and Air Freight Slabs defined with default values.
        public DataTable FetchManualFreight(string strPolPk, string strPodPk, string Mode, int Group = 0)
        {
            StringBuilder str = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = null;
            DataTable dtSlabs = null;
            cls_AirlineTariffEntry objAirLineTariff = new cls_AirlineTariffEntry(1, 2, false);

            int i = 0;
            string[] arrPolPk = null;
            string[] arrPodPk = null;
            string strCondition = null;
            string strNewModeCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPolPk.Split(Convert.ToChar(","));
            arrPodPk = strPodPk.Split(Convert.ToChar(","));

            if (Group == 1 | Group == 2)
            {
                strCondition = " (PGL.PORT_GRP_MST_PK IN (" + strPolPk + " )AND PGD.PORT_GRP_MST_PK IN (" + strPodPk + " )) ";

                strNewModeCondition =  " AND PGL.BIZ_TYPE = 1";
                strNewModeCondition +=  " AND PGD.BIZ_TYPE = 1";
                strNewModeCondition +=  " AND FMT.ACTIVE_FLAG = 1";
                strNewModeCondition +=  " AND FMT.CHARGE_TYPE <> 3 ";
                strNewModeCondition +=  " AND FMT.BUSINESS_TYPE IN (1,3)";
                strNewModeCondition +=  " AND FMT.BY_DEFAULT=1";

                str.Append(" SELECT PGL.PORT_GRP_MST_PK \"POLPK\", PGD.PORT_GRP_MST_PK \"PODPK\", ");
                str.Append( " FMT.FREIGHT_ELEMENT_MST_PK  FREIGHT_ELEMENT_MST_FK ,FMT.FREIGHT_ELEMENT_ID AS \"Frt. Ele.\", FREIGHT_ELEMENT_NAME, ");
                str.Append( " CURR.CURRENCY_MST_PK CURRENCY_MST_FK ,CURR.CURRENCY_ID AS \"Curr.\",CURR.CURRENCY_NAME, ");
                str.Append( "  'false' SELECTED, 'false' ADVATOS ,0.00 AS MIN_AMOUNT");
                str.Append( " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_GRP_MST_TBL PGL, PORT_GRP_MST_TBL PGD,");
                str.Append( " CURRENCY_TYPE_MST_TBL CURR ");
                str.Append( " WHERE (1=1)");
                str.Append( " AND (");
                str.Append( strCondition + ")");
                str.Append( strNewModeCondition);
                str.Append( " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"]);
                str.Append( " GROUP BY PGL.PORT_GRP_MST_PK,PGL.PORT_GRP_ID,PGD.PORT_GRP_MST_PK,PGD.PORT_GRP_ID,");
                str.Append( " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,");
                str.Append( " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,CURRENCY_NAME,FREIGHT_ELEMENT_NAME,FMT.PREFERENCE");
                str.Append( " HAVING PGL.PORT_GRP_ID <> PGD.PORT_GRP_ID");
                str.Append( " ORDER BY PGL.PORT_GRP_ID, PGD.PORT_GRP_ID,FMT.PREFERENCE ");
            }
            else
            {
                //Making condition as the record should have only selected POL and POD
                //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
                //is the selected sector.
                for (i = 0; i <= arrPolPk.Length - 1; i++)
                {
                    if (string.IsNullOrEmpty(strCondition))
                    {
                        strCondition = " (POL.PORT_MST_PK =" + Convert.ToString(arrPolPk[i]) + " AND POD.PORT_MST_PK =" + arrPodPk[i].ToString() + ")";
                    }
                    else
                    {
                        strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk[i].ToString() + " AND POD.PORT_MST_PK =" + arrPodPk[i].ToString() + ")";
                    }
                }
                strNewModeCondition =  " AND POL.BUSINESS_TYPE = 1";
                //BUSINESS_TYPE = 1 :- Is the business type for Air
                strNewModeCondition +=  " AND POD.BUSINESS_TYPE = 1";
                //BUSINESS_TYPE = 1 :- Is the business type for Air
                strNewModeCondition +=  " AND FMT.ACTIVE_FLAG = 1";
                strNewModeCondition +=  " AND FMT.CHARGE_TYPE <> 3";
                strNewModeCondition +=  " AND FMT.BUSINESS_TYPE IN (1,3)";
                strNewModeCondition +=  " AND FMT.BY_DEFAULT=1";
                //Making query with the condition added
                //modified by thiyagarajan on 2/12/08 for location based curr. task
                str.Append(" SELECT POL.PORT_MST_PK \"POLPK\",POD.PORT_MST_PK \"PODPK\", ");
                str.Append( " FMT.FREIGHT_ELEMENT_MST_PK  FREIGHT_ELEMENT_MST_FK ,FMT.FREIGHT_ELEMENT_ID AS \"Frt. Ele.\", FREIGHT_ELEMENT_NAME, ");
                str.Append( " CURR.CURRENCY_MST_PK CURRENCY_MST_FK ,CURR.CURRENCY_ID AS \"Curr.\",CURR.CURRENCY_NAME, ");
                str.Append( "  'false' SELECTED, 'false' ADVATOS ,0.00 AS MIN_AMOUNT");
                str.Append( " FROM FREIGHT_ELEMENT_MST_TBL FMT, PORT_MST_TBL POL, PORT_MST_TBL POD,");
                str.Append( " CURRENCY_TYPE_MST_TBL CURR ");
                str.Append( " WHERE (1=1)");
                str.Append( " AND (");
                str.Append( strCondition + ")");
                str.Append( strNewModeCondition);
                str.Append( " AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"]);
                str.Append( " GROUP BY POL.PORT_MST_PK,POL.PORT_ID,POD.PORT_MST_PK,POD.PORT_ID,");
                str.Append( " FMT.FREIGHT_ELEMENT_MST_PK,FMT.FREIGHT_ELEMENT_ID,");
                str.Append( " CURR.CURRENCY_MST_PK,CURR.CURRENCY_ID,CURRENCY_NAME,FREIGHT_ELEMENT_NAME,FMT.PREFERENCE ");
                str.Append( " HAVING POL.PORT_ID<>POD.PORT_ID");
                str.Append( " ORDER BY POL.PORT_ID, POD.PORT_ID,FMT.PREFERENCE");
            }

            try
            {
                dtMain = objWF.GetDataTable(str.ToString());

                dtSlabs = objAirLineTariff.FetchAirFreightSlabs(Mode);
                for (i = 0; i <= dtSlabs.Rows.Count - 1; i++)
                {
                    //Current Rate
                    dtMain.Columns.Add(dtSlabs.Rows[i][0].ToString(), typeof(decimal));
                    //Tariff Rate
                    dtMain.Columns.Add(dtSlabs.Rows[i][1].ToString(), typeof(decimal));
                    dtMain.Columns.Add(dtSlabs.Rows[i][1].ToString() + "BP");
                }
                return dtMain;
            }
            catch (OracleException SQLEX)
            {
                throw SQLEX;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Manual"

        #region "SpotRate"

        private string QuerySpotRate(string CustomerPk = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            string strSQL = null;
            strSQL = "   Select                                                        " + "        MAST.RFQ_REF_NO REFNO,                                      " + "        AIR.AIRLINE_ID AIRLINE,                                  " + "       main.PORT_MST_POL_FK,                                       " + "       PORTPOL.PORT_ID                             PORTPOL_ID,     " + "       PORTPOL.PORT_NAME                           PORTPOL_NAME,   " + "       main.PORT_MST_POD_FK,                                       " + "       PORTPOD.PORT_ID                             PORTPOD_ID,     " + "       PORTPOD.PORT_NAME                           PORTPOD_NAME,   " + "       to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM,  " + "       to_Char(main.VALID_TO, '" + dateFormat + "')   VALID_TO,    " + "       COMMODITY_MST_FK,                                                " + "       COMMODITY_NAME,                                               " + "       VOLUME_IN_CBM,                                              " + "       PACK_COUNT,                                                 " + "       CHARGEABLE_WEIGHT,                                          " + "       VOLUME_WEIGHT,                                              " + "       DENSITY,                                                     " + "       main.RFQ_SPOT_AIR_FK   FK,                                       " + "       main.RFQ_SPOT_AIR_TRN_PK                          PK,AIRLINE_MST_PK  " + "      from  RFQ_SPOT_RATE_AIR_TBL MAST,                         " + "       RFQ_SPOT_TRN_AIR_TBL main,                                  " + "       PORT_MST_TBL PORTPOL,                                       " + "       PORT_MST_TBL PORTPOD,                                        " + "        AIRLINE_MST_TBL AIR,                                              " + "        COMMODITY_MST_TBL COM                                                 " + "      where  MAST.RFQ_SPOT_AIR_PK  =  MAIN.RFQ_SPOT_AIR_FK         " + "       AND AIR.AIRLINE_MST_PK = MAST.AIRLINE_MST_FK         " + "       AND PORT_MST_POL_FK         =   PORTPOL.PORT_MST_PK         " + "       AND PORT_MST_POD_FK         =   PORTPOD.PORT_MST_PK         " + "        AND COM.COMMODITY_MST_PK(+)= MAST.COMMODITY_MST_FK         " + "   AND (MAIN.PORT_MST_POL_FK,MAIN.PORT_MST_POD_FK) in (" + Sectors + ")" + "   AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)     " + "       between MAIN.VALID_FROM and nvl(MAIN.VALID_TO,NULL_DATE_FORMAT)    " + "   AND MAST.ACTIVE     =      1    AND     MAST.APPROVED   =     1  ";
            if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
            {
                strSQL += "  AND MAST.COMMODITY_GROUP_FK    = " + Convert.ToString(CommodityGroup);
            }

            if (!string.IsNullOrEmpty(Convert.ToString(CustomerPk)))
            {
                strSQL += "  AND ( MAST.CUSTOMER_MST_FK    = " + Convert.ToString(CustomerPk);
                strSQL += "         or MAST.CUSTOMER_MST_FK  is NULL ) ";
            }

            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");
            return strSQL;
        }

        private string QueryTrnSpotRate(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            string strSQL = null;
            strSQL = "    Select                                                        " + "       RFQ_SPOT_AIR_TRN_FK   FK,                                        " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       'false' SELECTED,                                           " + "       'false' ADVATOS,                                            " + "      APPROVED_RATE CURRENT_RATE,                                               " + "      APPROVED_RATE REQUEST_RATE,                                               " + "       APPROVED_RATE,                                              " + "       tran.CHARGE_BASIS                                           " + "      from                                                         " + "       RFQ_SPOT_TRN_AIR_TBL        main,                           " + "       RFQ_SPOT_AIR_SURCHARGE      tran,                           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               RFQ_SPOT_AIR_TRN_FK    IN( " + TrnPk + " )              " + "       AND     main.RFQ_SPOT_AIR_TRN_PK = tran.RFQ_SPOT_AIR_TRN_FK " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK        = CURRENCY_MST_PK            " + "       AND     CHARGE_TYPE <> 3                                    " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By PREFERENCE";
            //FREIGHT_ELEMENT_ID                                  "
            //"       AND     CHARGE_BASIS = 3                                    " & vbCrLf & _

            //FREIGHT_TYPE = 2

            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");
            return strSQL;
        }

        private string QueryTrnOthSpotRate(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            string strSQL = null;
            strSQL = "    Select                                                        " + "       RFQ_SPOT_AIR_TRN_FK FK,                                        " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "      APPROVED_RATE  CURRENT_RATE,                                               " + "      APPROVED_RATE REQUEST_RATE,                                               " + "       APPROVED_RATE,                                              " + "       tran.CHARGE_BASIS                                           " + "      from                                                         " + "       RFQ_SPOT_TRN_AIR_TBL        main,                           " + "       RFQ_SPOT_AIR_OTH_CHRG       tran,                           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               RFQ_SPOT_AIR_TRN_FK   IN( " + TrnPk + " )           " + "       AND     main.RFQ_SPOT_AIR_TRN_PK = tran.RFQ_SPOT_AIR_TRN_FK " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK        = CURRENCY_MST_PK            " + "       AND     nvl(CHARGE_TYPE,3) = 3                             " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By PREFERENCE";
            //FREIGHT_ELEMENT_ID                                  "
            //"       AND     CHARGE_BASIS = 2                                    " & vbCrLf & _

            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");
            return strSQL;
        }

        private string QueryTrnFRTSpotRate(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            string strSQL = null;
            strSQL = "    Select                                                        " + "       RFQ_SPOT_TRN_FREIGHT_PK,                                    " + "       RFQ_SPOT_TRN_AIR_FK FK,                                        " + "       FREIGHT_ELEMENT_MST_FK,                                     " + "       FREIGHT_ELEMENT_ID,                                         " + "       FREIGHT_ELEMENT_NAME,                                       " + "       tran.CURRENCY_MST_FK,                                       " + "       CURRENCY_ID,                                                " + "       CURRENCY_NAME,                                              " + "       'false' SELECTED,                                           " + "       'false' ADVATOS,                                            " + "       MIN_AMOUNT                                                  " + "      from                                                         " + "       RFQ_SPOT_AIR_TRN_FREIGHT_TBL                tran,           " + "       FREIGHT_ELEMENT_MST_TBL,                                    " + "       CURRENCY_TYPE_MST_TBL                                       " + "      where                                                        " + "               RFQ_SPOT_TRN_AIR_FK   IN( " + TrnPk + " )        " + "       AND     FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK     " + "       AND     FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1             " + "       AND     CURRENCY_MST_FK             = CURRENCY_MST_PK       " + "       AND     CHARGE_TYPE <> 3                                    " + "       AND     FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)       " + "      Order By PREFERENCE                                  ";
            //"       AND     CHARGE_BASIS                not in(2,3)             " & vbCrLf & _FREIGHT_ELEMENT_ID

            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");
            return strSQL;
        }

        private string QueryTrnSlabSpotRate(string FreightPks)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            string strSQL = null;
            strSQL = "    Select                                                        " + "       RFQ_SPOT_TRN_AIR_FK FK,                                        " + "       tran.FREIGHT_ELEMENT_MST_FK,                                " + "       AIRFREIGHT_SLABS_TBL_PK,                                    " + "       BREAKPOINT_ID,                                              " + "       BREAKPOINT_DESC,                                            " + "      APPROVED_RATE CURRENT_RATE,                                               " + "      APPROVED_RATE REQUEST_RATE,                                               " + "      APPROVED_RATE                                               " + "      from                                                         " + "       RFQ_SPOT_AIR_TRN_FREIGHT_TBL        tran,                   " + "       RFQ_SPOT_AIR_BREAKPOINTS            bpnt,                   " + "       AIRFREIGHT_SLABS_TBL                                        " + "      where                                                        " + "            bpnt.RFQ_SPOT_AIR_FRT_FK in (" + FreightPks + ")       " + "       AND  bpnt.RFQ_SPOT_AIR_FRT_FK  = tran.RFQ_SPOT_TRN_FREIGHT_PK   " + "       AND  AIRFREIGHT_SLABS_TBL.ACTIVE_FLAG  = 1                      " + "       AND  bpnt.AIRFREIGHT_SLABS_TBL_FK  = AIRFREIGHT_SLABS_TBL_PK    " + "       Order By FREIGHT_ELEMENT_MST_FK, BREAKPOINT_RANGE,BREAKPOINT_ID ";

            strSQL = strSQL.Replace("   ", " ");
            strSQL = strSQL.Replace("  ", " ");
            return strSQL;
        }

        #endregion "SpotRate"

        #region "Customer contract"

        private string QueryContCust(string CustomerPk = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "")
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" Select          ");
            strQuery.Append("  MAST.CONT_REF_NO REFNO,       ");
            strQuery.Append("  AIR.AIRLINE_ID AIRLINE,      ");
            strQuery.Append("  main.PORT_MST_POL_FK,       ");
            strQuery.Append("  PORTPOL.PORT_ID      PORTPOL_ID,  ");
            strQuery.Append("  PORTPOL.PORT_NAME     PORTPOL_NAME, ");
            strQuery.Append("  main.PORT_MST_POD_FK,       ");
            strQuery.Append("  PORTPOD.PORT_ID      PORTPOD_ID,  ");
            strQuery.Append("  PORTPOD.PORT_NAME     PORTPOD_NAME, ");
            strQuery.Append("  to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
            strQuery.Append("  to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
            strQuery.Append("  COMMODITY_MST_FK,        ");
            strQuery.Append("  COMMODITY_NAME,         ");
            strQuery.Append("  EXPECTED_VOLUME VOLUME_IN_CBM,        ");
            strQuery.Append("  0 PACK_COUNT,         ");
            strQuery.Append("  EXPECTED_WEIGHT CHARGEABLE_WEIGHT,       ");
            strQuery.Append("  EXPECTED_VOLUME VOLUME_WEIGHT,        ");
            strQuery.Append("  0 DENSITY,          ");
            strQuery.Append("  main.CONT_CUST_AIR_FK FK,       ");
            strQuery.Append("  MAIN.CONT_CUST_TRN_AIR_PK  PK,AIRLINE_MST_PK  ");
            strQuery.Append(" from CONT_CUST_AIR_TBL  MAST,     ");
            strQuery.Append("  CONT_CUST_TRN_AIR_TBL main,      ");
            strQuery.Append("  PORT_MST_TBL PORTPOL,       ");
            strQuery.Append("  PORT_MST_TBL PORTPOD,       ");
            strQuery.Append("  AIRLINE_MST_TBL AIR,        ");
            strQuery.Append("   COMMODITY_MST_TBL COM       ");
            strQuery.Append(" where MAST.CONT_CUST_AIR_PK = MAIN.CONT_CUST_AIR_FK ");
            strQuery.Append("  AND AIR.AIRLINE_MST_PK(+) = MAST.AIRLINE_MST_FK  ");
            strQuery.Append("  AND PORT_MST_POL_FK  = PORTPOL.PORT_MST_PK  ");
            strQuery.Append("  AND PORT_MST_POD_FK  = PORTPOD.PORT_MST_PK  ");
            strQuery.Append("        AND COM.COMMODITY_MST_PK(+)= MAST.COMMODITY_MST_FK         ");
            strQuery.Append(" AND (MAIN.PORT_MST_POL_FK,MAIN.PORT_MST_POD_FK) in (" + Sectors + ")");
            strQuery.Append(" AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  ");
            strQuery.Append("  between MAIN.VALID_FROM and nvl(MAIN.VALID_TO,NULL_DATE_FORMAT) ");
            strQuery.Append("  AND MAST.CONT_APPROVED              =      2   ");
            if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
            {
                strQuery.Append(" AND MAST.Commodity_Group_Mst_Fk =" + Convert.ToString(CommodityGroup));
            }

            if (!string.IsNullOrEmpty(Convert.ToString(CustomerPk)))
            {
                strQuery.Append(" AND ( MAST.CUSTOMER_MST_FK = " + Convert.ToString(CustomerPk));
                strQuery.Append("  or MAST.CUSTOMER_MST_FK is NULL ) ");
            }
            return strQuery.ToString();
        }

        private string QueryTrnContCust(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("Select          ");
            strQuery.Append("  CONT_CUST_TRN_AIR_FK  FK,       ");
            strQuery.Append("  tran.FREIGHT_ELEMENT_MST_FK,      ");
            strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
            strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
            strQuery.Append("  tran.CURRENCY_MST_FK,       ");
            strQuery.Append("  CURRENCY_ID,        ");
            strQuery.Append("  CURRENCY_NAME,        ");
            strQuery.Append("  'false' SELECTED, ");
            strQuery.Append("  'false' ADVATOS,   ");
            //'Added Koteshwari for Advatos Column PTSID-JUN18
            strQuery.Append("  APPROVED_RATE CURRENT_RATE,         ");
            strQuery.Append("  APPROVED_RATE REQUEST_RATE,         ");
            strQuery.Append("  APPROVED_RATE,        ");
            strQuery.Append("  tran.CHARGE_BASIS        ");
            strQuery.Append(" from          ");
            strQuery.Append("  --RFQ_SPOT_TRN_AIR_TBL  main,     ");
            strQuery.Append("  CONT_CUST_AIR_SURCHARGE tran,     ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
            strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
            strQuery.Append(" where          ");
            strQuery.Append("  CONT_CUST_TRN_AIR_FK IN( " + TrnPk + " )   ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
            strQuery.Append("  AND  CURRENCY_MST_FK  = CURRENCY_MST_PK  ");
            strQuery.Append("  AND  CHARGE_TYPE <> 3      ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)  ");
            strQuery.Append(" Order By PREFERENCE ");
            //FREIGHT_ELEMENT_ID
            return strQuery.ToString();
        }

        private string QueryTrnOthContCust(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("Select          ");
            strQuery.Append("  cont_cust_trn_air_fk FK,       ");
            strQuery.Append("  tran.FREIGHT_ELEMENT_MST_FK,      ");
            strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
            strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
            strQuery.Append("  tran.CURRENCY_MST_FK,       ");
            strQuery.Append("  CURRENCY_ID,        ");
            strQuery.Append("  CURRENCY_NAME,        ");
            strQuery.Append("  APPROVED_RATE CURRENT_RATE,         ");
            strQuery.Append("  APPROVED_RATE REQUEST_RATE,         ");
            strQuery.Append("  APPROVED_RATE,        ");
            strQuery.Append("  tran.CHARGE_BASIS        ");
            strQuery.Append(" from          ");
            strQuery.Append("  --RFQ_SPOT_TRN_AIR_TBL  main,     ");
            strQuery.Append("  CONT_CUST_AIR_OTH_CHRG  tran,     ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
            strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
            strQuery.Append(" where          ");
            strQuery.Append("   cont_cust_trn_air_fk IN( " + TrnPk + " )   ");
            strQuery.Append("  --AND  main.RFQ_SPOT_AIR_TRN_PK = tran.RFQ_SPOT_AIR_TRN_FK ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
            strQuery.Append("  AND  CURRENCY_MST_FK  = CURRENCY_MST_PK  ");
            strQuery.Append("  AND  nvl(CHARGE_TYPE,3) = 3      ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)  ");
            strQuery.Append(" Order By FREIGHT_ELEMENT_ID    ");
            return strQuery.ToString();
        }

        private string QueryTrnFRTContCust(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("SELECT          ");
            strQuery.Append("  CONT_CUST_AIR_FREIGHT_PK,      ");
            strQuery.Append("  CONT_CUST_TRN_AIR_FK FK,       ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_FK,       ");
            strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
            strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
            strQuery.Append("  TRAN.CURRENCY_MST_FK,       ");
            strQuery.Append("  CURRENCY_ID,        ");
            strQuery.Append("  CURRENCY_NAME,        ");
            strQuery.Append("  'false' SELECTED, ");
            strQuery.Append("  'false' ADVATOS, ");
            //'Added Koteshwari for Advatos Column PTSID-JUN18
            strQuery.Append("  MIN_AMOUNT         ");
            strQuery.Append(" FROM          ");
            strQuery.Append("  CONT_CUST_AIR_FREIGHT_TBL   TRAN,   ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
            strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
            strQuery.Append(" WHERE          ");
            strQuery.Append("   TRAN.CONT_CUST_TRN_AIR_FK IN(" + TrnPk + " )  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
            strQuery.Append("  AND  CURRENCY_MST_FK   = CURRENCY_MST_PK  ");
            strQuery.Append("  AND  CHARGE_TYPE <> 3      ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE IN(1,3)  ");
            strQuery.Append(" ORDER BY FREIGHT_ELEMENT_ID ");

            return strQuery.ToString();
        }

        private string QueryTrnSlabContCust(string FreightPks)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("SELECT          ");
            strQuery.Append("  CONT_CUST_TRN_AIR_FK FK,       ");
            strQuery.Append("  TRAN.FREIGHT_ELEMENT_MST_FK,      ");
            strQuery.Append("  AIRFREIGHT_SLABS_TBL_PK,      ");
            strQuery.Append("  BREAKPOINT_ID,        ");
            strQuery.Append("  BREAKPOINT_DESC,        ");
            strQuery.Append(" APPROVED_RATE CURRENT_RATE,         ");
            strQuery.Append("  APPROVED_RATE REQUEST_RATE,         ");
            strQuery.Append("  APPROVED_RATE         ");
            strQuery.Append(" FROM          ");
            strQuery.Append("  CONT_CUST_AIR_FREIGHT_TBL  TRAN,    ");
            strQuery.Append("  CONT_CUST_AIR_BREAKPOINTS  BPNT,    ");
            strQuery.Append("  AIRFREIGHT_SLABS_TBL       ");
            strQuery.Append(" WHERE          ");
            strQuery.Append("  BPNT.CONT_CUST_AIR_FRT_FK IN (" + FreightPks + ")  ");
            strQuery.Append("  AND BPNT.CONT_CUST_AIR_FRT_FK = TRAN.CONT_CUST_AIR_FREIGHT_PK ");
            strQuery.Append("  AND AIRFREIGHT_SLABS_TBL.ACTIVE_FLAG = 1    ");
            strQuery.Append("  AND BPNT.AIRFREIGHT_SLABS_FK= AIRFREIGHT_SLABS_TBL_PK ");
            strQuery.Append("  ORDER BY FREIGHT_ELEMENT_MST_FK, BREAKPOINT_RANGE,BREAKPOINT_ID ");

            return strQuery.ToString();
            //
        }

        #endregion "Customer contract"

        #region "Airline Tariff"

        private string QueryAirTariff(string tariffType = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "", int Group = 0)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();

            if (Group == 1 | Group == 2)
            {
                strQuery.Append("Select          ");
                strQuery.Append("  MAST.TARIFF_REF_NO REFNO,       ");
                strQuery.Append("  AIR.AIRLINE_ID AIRLINE,      ");
                strQuery.Append("  MAIN.POL_GRP_FK PORT_MST_POL_FK,       ");
                strQuery.Append("  PGL.PORT_GRP_ID      PORTPOL_ID,  ");
                strQuery.Append("  PGL.PORT_GRP_NAME     PORTPOL_NAME, ");
                strQuery.Append("  MAIN.POD_GRP_FK PORT_MST_POD_FK,       ");
                strQuery.Append("  PGD.PORT_GRP_ID      PORTPOD_ID,  ");
                strQuery.Append("  PGD.PORT_GRP_NAME     PORTPOD_NAME, ");
                strQuery.Append("  to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
                strQuery.Append("  to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
                strQuery.Append("  0 COMMODITY_MST_FK,        ");
                strQuery.Append(" '' COMMODITY_NAME,         ");
                strQuery.Append("   0 VOLUME_IN_CBM,        ");
                strQuery.Append("   0 PACK_COUNT,         ");
                strQuery.Append("   0 CHARGEABLE_WEIGHT,       ");
                strQuery.Append("   0 VOLUME_WEIGHT,        ");
                strQuery.Append("   0 DENSITY,");
                strQuery.Append("  main.TARIFF_MAIN_AIR_FK FK,       ");
                strQuery.Append("  MAIN.TARIFF_TRN_AIR_PK  PK,AIRLINE_MST_PK     ");
                strQuery.Append(" from tariff_main_air_tbl  MAST,     ");
                strQuery.Append("  tariff_trn_air_tbl main,      ");
                strQuery.Append("  PORT_GRP_MST_TBL    PGL,      ");
                strQuery.Append("  PORT_GRP_MST_TBL    PGD,      ");
                strQuery.Append("  AIRLINE_MST_TBL AIR        ");
                strQuery.Append(" where MAST.TARIFF_MAIN_AIR_PK = MAIN.TARIFF_MAIN_AIR_FK");
                strQuery.Append("  AND AIR.AIRLINE_MST_PK(+) = MAST.AIRLINE_MST_FK  ");
                strQuery.Append("  AND MAIN.POL_GRP_FK = PGL.PORT_GRP_MST_PK(+) ");
                strQuery.Append("  AND MAIN.POD_GRP_FK = PGD.PORT_GRP_MST_PK(+)  ");
                strQuery.Append(" AND (MAIN.POL_GRP_FK, MAIN.POD_GRP_FK) IN (" + Sectors + ")");
                strQuery.Append(" AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  ");
                strQuery.Append("  between MAIN.VALID_FROM and nvl(MAIN.VALID_TO,NULL_DATE_FORMAT) ");
                strQuery.Append("  AND MAST.ACTIVE                     =      1   ");
                if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
                {
                    strQuery.Append(" AND MAST.COMMODITY_GROUP_FK =" + Convert.ToString(CommodityGroup));
                }
                strQuery.Append(" AND MAST.TARIFF_TYPE= " + Convert.ToString(tariffType));
            }
            else
            {
                strQuery.Append("Select          ");
                strQuery.Append("  MAST.TARIFF_REF_NO REFNO,       ");
                strQuery.Append("  AIR.AIRLINE_ID AIRLINE,      ");
                strQuery.Append("  main.PORT_MST_POL_FK,       ");
                strQuery.Append("  PORTPOL.PORT_ID      PORTPOL_ID,  ");
                strQuery.Append("  PORTPOL.PORT_NAME     PORTPOL_NAME, ");
                strQuery.Append("  main.PORT_MST_POD_FK,       ");
                strQuery.Append("  PORTPOD.PORT_ID      PORTPOD_ID,  ");
                strQuery.Append("  PORTPOD.PORT_NAME     PORTPOD_NAME, ");
                strQuery.Append("  to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM, ");
                strQuery.Append("  to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO, ");
                strQuery.Append("  0 COMMODITY_MST_FK,        ");
                strQuery.Append(" '' COMMODITY_NAME,         ");
                strQuery.Append("   0 VOLUME_IN_CBM,        ");
                strQuery.Append("   0 PACK_COUNT,         ");
                strQuery.Append("   0 CHARGEABLE_WEIGHT,       ");
                strQuery.Append("   0 VOLUME_WEIGHT,        ");
                strQuery.Append("   0 DENSITY,");
                strQuery.Append("  main.TARIFF_MAIN_AIR_FK FK,       ");
                strQuery.Append("  MAIN.TARIFF_TRN_AIR_PK  PK,AIRLINE_MST_PK     ");
                strQuery.Append(" from tariff_main_air_tbl  MAST,     ");
                strQuery.Append("  tariff_trn_air_tbl main,      ");
                strQuery.Append("  PORT_MST_TBL PORTPOL,       ");
                strQuery.Append("  PORT_MST_TBL PORTPOD,       ");
                strQuery.Append("  AIRLINE_MST_TBL AIR        ");
                strQuery.Append(" where MAST.TARIFF_MAIN_AIR_PK = MAIN.TARIFF_MAIN_AIR_FK");
                strQuery.Append("  AND AIR.AIRLINE_MST_PK(+) = MAST.AIRLINE_MST_FK  ");
                strQuery.Append("  AND PORT_MST_POL_FK  = PORTPOL.PORT_MST_PK  ");
                strQuery.Append("  AND PORT_MST_POD_FK  = PORTPOD.PORT_MST_PK  ");
                strQuery.Append(" AND (MAIN.PORT_MST_POL_FK,MAIN.PORT_MST_POD_FK) in (" + Sectors + ")");
                strQuery.Append(" AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  ");
                strQuery.Append("  between MAIN.VALID_FROM and nvl(MAIN.VALID_TO,NULL_DATE_FORMAT) ");
                strQuery.Append("  AND MAST.ACTIVE                     =      1   ");
                if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
                {
                    strQuery.Append(" AND MAST.COMMODITY_GROUP_FK =" + Convert.ToString(CommodityGroup));
                }
                strQuery.Append(" AND MAST.TARIFF_TYPE= " + Convert.ToString(tariffType));

                //If CStr(CustomerPk) <> "" Then
                //    strQuery.Append(" AND ( MAST.CUSTOMER_MST_FK = " & CStr(CustomerPk) & vbCrLf)
                //    strQuery.Append("  or MAST.CUSTOMER_MST_FK is NULL ) " & vbCrLf)
                //End If
            }

            return strQuery.ToString();
        }

        private string QueryTrnAirTariff(string TrnPk, int Group = 0)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();

            if (Group == 1 & Group == 2)
            {
                strQuery.Append(" Select          ");
                strQuery.Append("  tran.tariff_trn_air_fk FK,       ");
                strQuery.Append("  tran.FREIGHT_ELEMENT_MST_FK,      ");
                strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
                strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
                strQuery.Append("  tran.CURRENCY_MST_FK,       ");
                strQuery.Append("  CURRENCY_ID,        ");
                strQuery.Append("  'false' SELECTED, ");
                strQuery.Append("  'false' ADVATOS,   ");
                //'Added Koteshwari for Advatos Column PTSID-JUN18
                strQuery.Append(" tran.tariff_rate CURRENT_RATE,         ");
                strQuery.Append("  tran.tariff_rate REQUEST_RATE,         ");
                strQuery.Append("  tran.tariff_rate APPROVED_RATE,        ");
                strQuery.Append("  tran.CHARGE_BASIS        ");
                strQuery.Append(" from          ");
                strQuery.Append("  --RFQ_SPOT_TRN_AIR_TBL  main,     ");
                strQuery.Append("  tariff_trn_air_surcharge tran,     ");
                strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
                strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
                strQuery.Append(" where          ");
                strQuery.Append("  tran.tariff_trn_air_fk IN( " + TrnPk + " )   ");
                strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
                strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
                strQuery.Append("  AND  CURRENCY_MST_FK  = CURRENCY_MST_PK  ");
                strQuery.Append("  AND  CHARGE_TYPE <> 3      ");
                strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)  ");
                strQuery.Append(" Order By PREFERENCE ");
                //FREIGHT_ELEMENT_ID
            }
            else
            {
                strQuery.Append(" Select          ");
                strQuery.Append("   tran.tariff_trn_air_fk FK,       ");
                strQuery.Append("  tran.FREIGHT_ELEMENT_MST_FK,      ");
                strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
                strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
                strQuery.Append("  tran.CURRENCY_MST_FK,       ");
                strQuery.Append("  CURRENCY_ID,        ");
                strQuery.Append("  'false' SELECTED, ");
                strQuery.Append("  'false' ADVATOS,   ");
                //'Added Koteshwari for Advatos Column PTSID-JUN18
                strQuery.Append(" tran.tariff_rate CURRENT_RATE,         ");
                strQuery.Append("  tran.tariff_rate REQUEST_RATE,         ");
                strQuery.Append("  tran.tariff_rate APPROVED_RATE,        ");
                strQuery.Append("  tran.CHARGE_BASIS        ");
                strQuery.Append(" from          ");
                strQuery.Append("  --RFQ_SPOT_TRN_AIR_TBL  main,     ");
                strQuery.Append("  tariff_trn_air_surcharge tran,     ");
                strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
                strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
                strQuery.Append(" where          ");
                strQuery.Append("  tran.tariff_trn_air_fk IN( " + TrnPk + " )   ");
                strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
                strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
                strQuery.Append("  AND  CURRENCY_MST_FK  = CURRENCY_MST_PK  ");
                strQuery.Append("  AND  CHARGE_TYPE <> 3      ");
                strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)  ");
                strQuery.Append(" Order By PREFERENCE ");
                //FREIGHT_ELEMENT_ID
            }
            return strQuery.ToString();
        }

        private string QueryTrnOthAirTariff(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" Select          ");
            strQuery.Append("  tran.tariff_trn_air_fk FK,       ");
            strQuery.Append("  tran.FREIGHT_ELEMENT_MST_FK,      ");
            strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
            strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
            strQuery.Append("  tran.CURRENCY_MST_FK,       ");
            strQuery.Append("  CURRENCY_ID,        ");
            strQuery.Append("  CURRENCY_NAME,        ");
            strQuery.Append("  tran.tariff_rate CURRENT_RATE,         ");
            strQuery.Append("  tran.tariff_rate REQUEST_RATE,         ");
            strQuery.Append("  tran.tariff_rate APPROVED_RATE,        ");
            strQuery.Append("  tran.CHARGE_BASIS        ");
            strQuery.Append("  from          ");
            strQuery.Append("  tariff_trn_air_oth_chrg  tran,     ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
            strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
            strQuery.Append(" where          ");
            strQuery.Append("   tran.tariff_trn_air_fk IN(" + TrnPk + " )   ");
            strQuery.Append("   AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
            strQuery.Append("   AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
            strQuery.Append("   AND  CURRENCY_MST_FK  = CURRENCY_MST_PK  ");
            strQuery.Append("   AND  nvl(CHARGE_TYPE,3) = 3      ");
            strQuery.Append("   AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)  ");
            strQuery.Append(" Order By FREIGHT_ELEMENT_ID");

            return strQuery.ToString();
        }

        private string QueryTrnFRTAirTariff(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append(" SELECT          ");
            strQuery.Append("  TARIFF_TRN_FREIGHT_PK,      ");
            strQuery.Append("  tran.tariff_trn_air_fk FK,       ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_FK,       ");
            strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
            strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
            strQuery.Append("  TRAN.CURRENCY_MST_FK,       ");
            strQuery.Append("  CURRENCY_ID,        ");
            strQuery.Append("  CURRENCY_NAME,        ");
            strQuery.Append("  'false' SELECTED, ");
            strQuery.Append("  'false' ADVATOS, ");
            //'Added Koteshwari for Advatos Column PTSID-JUN18
            strQuery.Append("  MIN_AMOUNT         ");
            strQuery.Append(" FROM          ");
            strQuery.Append("  tariff_trn_air_freight_tbl   TRAN,   ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
            strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
            strQuery.Append(" WHERE          ");
            strQuery.Append("   TRAN.TARIFF_TRN_AIR_FK IN( " + TrnPk + " )  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
            strQuery.Append("  AND  CURRENCY_MST_FK   = CURRENCY_MST_PK  ");
            strQuery.Append("  AND  CHARGE_TYPE <> 3      ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE IN(1,3)  ");
            strQuery.Append(" ORDER BY PREFERENCE ");
            //FREIGHT_ELEMENT_ID

            return strQuery.ToString();
        }

        private string QueryTrnSlabAirTariff(string FreightPks)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("SELECT          ");
            strQuery.Append("  tran.tariff_trn_air_fk FK,       ");
            strQuery.Append("  TRAN.FREIGHT_ELEMENT_MST_FK,      ");
            strQuery.Append("  AIRFREIGHT_SLABS_TBL_PK,      ");
            strQuery.Append("  BREAKPOINT_ID,        ");
            strQuery.Append("  BREAKPOINT_DESC,        ");
            strQuery.Append("  bpnt.tariff_rate CURRENT_RATE,         ");
            strQuery.Append("  bpnt.tariff_rate REQUEST_RATE,         ");
            strQuery.Append(" bpnt.tariff_rate APPROVED_RATE         ");
            strQuery.Append(" FROM          ");
            strQuery.Append("  tariff_trn_air_freight_tbl  TRAN,    ");
            strQuery.Append("  tariff_air_breakpoints  BPNT,    ");
            strQuery.Append("  AIRFREIGHT_SLABS_TBL       ");
            strQuery.Append(" WHERE          ");
            strQuery.Append("  BPNT.TARIFF_TRN_FREIGHT_FK IN ( " + FreightPks + ")  ");
            strQuery.Append("  AND BPNT.TARIFF_TRN_FREIGHT_FK = TRAN.TARIFF_TRN_FREIGHT_PK ");
            strQuery.Append("  AND AIRFREIGHT_SLABS_TBL.ACTIVE_FLAG = 1    ");
            strQuery.Append("  AND BPNT.AIRFREIGHT_SLABS_TBL_FK= AIRFREIGHT_SLABS_TBL_PK ");
            strQuery.Append("  ORDER BY FREIGHT_ELEMENT_MST_FK, BREAKPOINT_RANGE,BREAKPOINT_ID ");
            return strQuery.ToString();
            //
        }

        #endregion "Airline Tariff"

        #region "Quotation fetch"

        private string QueryQuot(string CustomerPk = "", string Sectors = "", string CommodityGroup = "", string ShipDate = "", string strPk = "", int Group = 0)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();

            if (Group == 1 | Group == 2)
            {
                strQuery.Append("Select          ");
                if (strPk.Trim().Length > 0)
                {
                    strQuery.Append(" MAIN.REF_NO REFNO, ");
                }
                else
                {
                    strQuery.Append("  MAST.QUOTATION_REF_NO REFNO,       ");
                }
                //strQuery.Append("  AIR.AIRLINE_ID AIRLINE,      " & vbCrLf)
                strQuery.Append("  AIR.AIRLINE_NAME AIRLINE,      ");
                strQuery.Append("  MAIN.POL_GRP_FK PORT_MST_POL_FK,       ");
                strQuery.Append("  PGL.PORT_GRP_ID PORTPOL_ID,  ");
                strQuery.Append("  PGL.PORT_GRP_NAME PORTPOL_NAME, ");
                strQuery.Append("  MAIN.POD_GRP_FK PORT_MST_POD_FK,  ");
                strQuery.Append("  PGD.PORT_GRP_ID PORTPOD_ID, ");
                strQuery.Append("  PGD.PORT_GRP_NAME PORTPOD_NAME,");
                strQuery.Append("  TO_CHAR(MAIN.VALID_FROM, '" + dateFormat + "') VALID_FROM,");
                strQuery.Append("  TO_CHAR(MAIN.VALID_TO, '" + dateFormat + "') VALID_TO,");
                strQuery.Append("   MAIN.COMMODITY_MST_FKS COMMODITY_MST_FK,        ");
                //10
                strQuery.Append("   COM.COMMODITY_NAME ,         ");
                //11
                strQuery.Append("   0 VOLUME_IN_CBM,        ");
                //12
                strQuery.Append("   0 PACK_COUNT,         ");
                //13
                strQuery.Append("   0 CHARGEABLE_WEIGHT,       ");
                //14
                strQuery.Append("   0 VOLUME_WEIGHT,        ");
                //15
                strQuery.Append("   0 DENSITY,");
                //16
                strQuery.Append("  MAIN.QUOT_GEN_AIR_FK FK,       ");
                //17
                strQuery.Append("  MAIN.QUOT_GEN_AIR_TRN_PK  PK,");
                //18
                strQuery.Append("  NVL(AIRLINE_MST_PK,0) AIRLINE_MST_PK     ");
                //19
                strQuery.Append(" FROM QUOTATION_AIR_TBL  MAST,     ");
                strQuery.Append("  QUOT_GEN_TRN_AIR_TBL MAIN,      ");
                strQuery.Append("  PORT_GRP_MST_TBL     PGL,     ");
                strQuery.Append("  PORT_GRP_MST_TBL     PGD,    ");
                strQuery.Append("  AIRLINE_MST_TBL AIR ,");
                strQuery.Append("  COMMODITY_MST_TBL COM       ");
                strQuery.Append(" WHERE MAST.QUOTATION_AIR_PK = MAIN.QUOT_GEN_AIR_FK");
                strQuery.Append("  AND AIR.AIRLINE_MST_PK(+) = MAIN.AIRLINE_MST_FK  ");
                strQuery.Append("  AND MAIN.POL_GRP_FK = PGL.PORT_GRP_MST_PK ");
                strQuery.Append("  AND MAIN.POD_GRP_FK = PGD.PORT_GRP_MST_PK  ");
                //AND COM.COMMODITY_MST_PK(+)=MAIN.COMMODITY_MST_FK
                strQuery.Append("  AND COM.COMMODITY_MST_PK(+)=MAIN.COMMODITY_MST_FK   ");
                //
                if (strPk.Trim().Length > 0)
                {
                    strQuery.Append(" AND MAIN.QUOT_GEN_AIR_FK = " + strPk);
                }
                else
                {
                    strQuery.Append(" AND (MAIN.POL_GRP_FK,MAIN.POD_GRP_FK) in (" + Sectors + ")");
                    strQuery.Append(" AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  ");
                    strQuery.Append("  between MAIN.VALID_FROM and nvl(MAIN.VALID_TO,NULL_DATE_FORMAT)");
                    strQuery.Append("  AND MAST.Quotation_Type          =      0   ");
                    strQuery.Append("  AND MAST.STATUS          =      2   ");

                    if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
                    {
                        strQuery.Append(" AND MAST.Commodity_Group_Mst_Fk =" + Convert.ToString(CommodityGroup));
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(CustomerPk)))
                    {
                        strQuery.Append(" AND ( MAST.CUSTOMER_MST_FK = " + Convert.ToString(CustomerPk));
                        strQuery.Append("  or MAST.CUSTOMER_MST_FK is NULL ) ");
                    }
                }
            }
            else
            {
                strQuery.Append("Select          ");
                if (strPk.Trim().Length > 0)
                {
                    strQuery.Append(" MAIN.REF_NO REFNO, ");
                }
                else
                {
                    strQuery.Append("  MAST.QUOTATION_REF_NO REFNO,       ");
                }
                //strQuery.Append("  AIR.AIRLINE_ID AIRLINE,      " & vbCrLf)
                strQuery.Append("  AIR.AIRLINE_NAME AIRLINE,      ");
                strQuery.Append("  main.PORT_MST_POL_FK,       ");
                strQuery.Append("  PORTPOL.PORT_ID      PORTPOL_ID,  ");
                strQuery.Append("  PORTPOL.PORT_NAME     PORTPOL_NAME,");
                strQuery.Append("  main.PORT_MST_POD_FK,       ");
                strQuery.Append("  PORTPOD.PORT_ID      PORTPOD_ID,  ");
                strQuery.Append("  PORTPOD.PORT_NAME     PORTPOD_NAME,");
                strQuery.Append("  to_Char(main.VALID_FROM, '" + dateFormat + "') VALID_FROM,");
                strQuery.Append("  to_Char(main.VALID_TO, '" + dateFormat + "') VALID_TO,");
                strQuery.Append("   MAIN.COMMODITY_MST_FKS COMMODITY_MST_FK,        ");
                //10
                // strQuery.Append("   COM.COMMODITY_NAME ,         " & vbCrLf) '11
                strQuery.Append("       ROWTOCOL('SELECT CM.COMMODITY_NAME FROM COMMODITY_MST_TBL CM WHERE CM.COMMODITY_MST_PK IN(' ||");
                strQuery.Append("                NVL(MAIN.COMMODITY_MST_FKS, 0) || ')') COMMODITY_NAME,");
                strQuery.Append("   0 VOLUME_IN_CBM,        ");
                //12
                strQuery.Append("   0 PACK_COUNT,         ");
                //13
                strQuery.Append("   0 CHARGEABLE_WEIGHT,       ");
                //14
                strQuery.Append("   0 VOLUME_WEIGHT,        ");
                //15
                strQuery.Append("   0 DENSITY,");
                //16
                strQuery.Append("  MAIN.QUOT_GEN_AIR_FK FK,       ");
                //17
                strQuery.Append("  MAIN.QUOT_GEN_AIR_TRN_PK  PK,");
                //18
                strQuery.Append("  NVL(AIRLINE_MST_PK,0)AIRLINE_MST_PK     ");
                //19
                strQuery.Append(" from quotation_air_tbl  MAST,     ");
                strQuery.Append("  quot_gen_trn_air_tbl main,      ");
                strQuery.Append("  PORT_MST_TBL PORTPOL,       ");
                strQuery.Append("  PORT_MST_TBL PORTPOD,       ");
                strQuery.Append("  AIRLINE_MST_TBL AIR ,");
                strQuery.Append("  COMMODITY_MST_TBL COM       ");
                strQuery.Append(" where MAST.QUOTATION_AIR_PK = MAIN.QUOT_GEN_AIR_FK");
                strQuery.Append("  AND AIR.AIRLINE_MST_PK(+) = MAIN.AIRLINE_MST_FK  ");
                strQuery.Append("  AND PORT_MST_POL_FK  = PORTPOL.PORT_MST_PK  ");
                strQuery.Append("  AND PORT_MST_POD_FK  = PORTPOD.PORT_MST_PK  ");
                //AND COM.COMMODITY_MST_PK(+)=MAIN.COMMODITY_MST_FK
                strQuery.Append("  AND COM.COMMODITY_MST_PK(+)=MAIN.COMMODITY_MST_FK   ");
                //
                if (strPk.Trim().Length > 0)
                {
                    strQuery.Append(" AND MAIN.QUOT_GEN_AIR_FK = " + strPk);
                }
                else
                {
                    strQuery.Append(" AND (MAIN.PORT_MST_POL_FK,MAIN.PORT_MST_POD_FK) in (" + Sectors + ")");
                    strQuery.Append(" AND ROUND(TO_DATE('" + ShipDate + "','" + dateFormat + "')-0.5)  ");
                    strQuery.Append("  between MAIN.VALID_FROM and nvl(MAIN.VALID_TO,NULL_DATE_FORMAT)");
                    strQuery.Append("  AND MAST.Quotation_Type          =      0   ");
                    strQuery.Append("  AND MAST.STATUS          =      2   ");

                    if (!string.IsNullOrEmpty(Convert.ToString(CommodityGroup)))
                    {
                        strQuery.Append(" AND MAST.Commodity_Group_Mst_Fk =" + Convert.ToString(CommodityGroup));
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(CustomerPk)))
                    {
                        strQuery.Append(" AND ( MAST.CUSTOMER_MST_FK = " + Convert.ToString(CustomerPk));
                        strQuery.Append("  or MAST.CUSTOMER_MST_FK is NULL ) ");
                    }
                }
            }

            return strQuery.ToString();
        }

        private string QueryTrnQuot(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("Select          ");
            strQuery.Append("   tran.quot_gen_air_trn_fk FK,       ");
            strQuery.Append("  tran.FREIGHT_ELEMENT_MST_FK,      ");
            strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
            strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
            strQuery.Append("  tran.CURRENCY_MST_FK,       ");
            strQuery.Append("  CURRENCY_ID,  ");
            strQuery.Append("  'false' SELECTED, ");
            strQuery.Append("  'false' ADVATOS,   ");
            //'Added Koteshwari for Advatos Column PTSID-JUN18
            strQuery.Append(" APPROVED_RATE CURRENT_RATE,         ");
            strQuery.Append(" APPROVED_RATE REQUEST_RATE,         ");
            strQuery.Append("   APPROVED_RATE,        ");
            strQuery.Append("  tran.CHARGE_BASIS        ");
            strQuery.Append(" from          ");
            strQuery.Append("  --RFQ_SPOT_TRN_AIR_TBL  main,     ");
            strQuery.Append("  QUOT_AIR_SURCHARGE tran,     ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
            strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
            strQuery.Append(" where          ");
            strQuery.Append("  tran.quot_gen_air_trn_fk IN(" + TrnPk + " )   ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
            strQuery.Append("  AND  CURRENCY_MST_FK  = CURRENCY_MST_PK  ");
            strQuery.Append("  AND  CHARGE_TYPE <> 3      ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)  ");
            strQuery.Append(" Order By PREFERENCE ");
            //FREIGHT_ELEMENT_ID

            return strQuery.ToString();
        }

        private string QueryTrnOthQuot(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            StringBuilder sbSQL = new StringBuilder();
            //strQuery.Append("Select          " & vbCrLf)
            //strQuery.Append("  tran.quot_gen_air_trn_fk FK,       " & vbCrLf)
            //strQuery.Append("  tran.FREIGHT_ELEMENT_MST_FK,      " & vbCrLf)
            //strQuery.Append("  FREIGHT_ELEMENT_ID,        " & vbCrLf)
            //strQuery.Append("  FREIGHT_ELEMENT_NAME,       " & vbCrLf)
            //strQuery.Append("  tran.CURRENCY_MST_FK,       " & vbCrLf)
            //strQuery.Append("  CURRENCY_ID,        " & vbCrLf)
            //strQuery.Append("  CURRENCY_NAME,        " & vbCrLf)
            //strQuery.Append(" APPROVED_RATE CURRENT_RATE,         " & vbCrLf)
            //strQuery.Append("  APPROVED_RATE REQUEST_RATE,         " & vbCrLf)
            //strQuery.Append("   APPROVED_RATE,        " & vbCrLf)
            //strQuery.Append("  tran.CHARGE_BASIS        " & vbCrLf)
            //strQuery.Append(" from          " & vbCrLf)
            //strQuery.Append("  QUOT_AIR_OTH_CHRG  tran,     " & vbCrLf)
            //strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      " & vbCrLf)
            //strQuery.Append("  CURRENCY_TYPE_MST_TBL       " & vbCrLf)
            //strQuery.Append(" where          " & vbCrLf)
            //strQuery.Append("    tran.quot_gen_air_trn_fk IN(" & TrnPk & " )   " & vbCrLf)
            //strQuery.Append("   AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  " & vbCrLf)
            //strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   " & vbCrLf)
            //strQuery.Append("  AND  CURRENCY_MST_FK  = CURRENCY_MST_PK  " & vbCrLf)
            //strQuery.Append("  AND  nvl(FREIGHT_TYPE,0) = 0      " & vbCrLf)
            //strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE in(1,3)  " & vbCrLf)
            //strQuery.Append(" Order By FREIGHT_ELEMENT_ID" & vbCrLf)
            sbSQL.Append("    Select                                                          ");
            sbSQL.Append("     otf.QUOTE_TRN_AIR_OTH_PK                  PK,                  ");
            //0
            sbSQL.Append("     otf.QUOTATION_TRN_AIR_FK                  FK,                  ");
            //1
            sbSQL.Append("     otf.FREIGHT_ELEMENT_MST_FK                FRT_FK,              ");
            //2
            sbSQL.Append("     otf.CURRENCY_MST_FK                       CURR_FK,             ");
            //3
            sbSQL.Append("     otf.BASIS_RATE                            BASIS_RATE,          ");
            //4
            sbSQL.Append("     otf.AMOUNT                                TARIFF_RATE,         ");
            //5
            sbSQL.Append("     otf.CHARGE_BASIS                          CH_BASIS,            ");
            //6
            sbSQL.Append("     decode(otf.CHARGE_BASIS,1,'%',2,'Flat','KGs')   CH_BASIS_ID,          ");
            //7
            sbSQL.Append("      FREIGHT_TYPE PYMT_TYPE      ");
            sbSQL.Append("    from                                                            ");
            sbSQL.Append("     QUOT_AIR_OTH_CHRG     otf                             ");
            sbSQL.Append("    where                                                           ");
            sbSQL.Append("           otf.QUOTATION_TRN_AIR_FK   in (" + TrnPk + ")       ");
            return sbSQL.ToString();

            //Return strQuery.ToString
        }

        private string QueryTrnFRTQuot(string TrnPk)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("SELECT          ");
            strQuery.Append("  TRAN.QUOT_GEN_TRN_FREIGHT_PK,      ");
            strQuery.Append("  tran.quot_gen_air_trn_fk FK,       ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_FK,       ");
            strQuery.Append("  FREIGHT_ELEMENT_ID,        ");
            strQuery.Append("  FREIGHT_ELEMENT_NAME,       ");
            strQuery.Append("  TRAN.CURRENCY_MST_FK,       ");
            strQuery.Append("  CURRENCY_ID,        ");
            strQuery.Append("  CURRENCY_NAME,        ");
            strQuery.Append("  decode(TRAN.CHECK_FOR_ALL_IN_RT,1,'true','false')  SELECTED, ");
            strQuery.Append("  decode(TRAN.CHECK_ADVATOS,1,'true','false')  ADVATOS,  ");
            //'Added Koteshwari for Advatos Column PTSID-JUN18
            strQuery.Append("  MIN_AMOUNT         ");
            strQuery.Append("  FROM          ");
            strQuery.Append("  QUOT_AIR_TRN_FREIGHT_TBL   TRAN,   ");
            strQuery.Append("  FREIGHT_ELEMENT_MST_TBL,      ");
            strQuery.Append("  CURRENCY_TYPE_MST_TBL       ");
            strQuery.Append(" WHERE          ");
            strQuery.Append("   TRAN.quot_gen_air_trn_fk IN( " + TrnPk + "  )  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK  ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1   ");
            strQuery.Append("  AND  CURRENCY_MST_FK   = CURRENCY_MST_PK  ");
            strQuery.Append("  AND  CHARGE_TYPE <> 3     ");
            strQuery.Append("  AND  FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE IN(1,3)  ");
            strQuery.Append(" ORDER BY FREIGHT_ELEMENT_ID   ");

            return strQuery.ToString();
        }

        private string QueryTrnSlabQuot(string FreightPks)
        {
            // Child Record :==========[ RFQ_SPOT_TRN_AIR_FCL_LCL ]====
            StringBuilder strQuery = new StringBuilder();
            strQuery.Append("SELECT          ");
            strQuery.Append("  tran.quot_gen_air_trn_fk FK,       ");
            strQuery.Append("  TRAN.FREIGHT_ELEMENT_MST_FK,      ");
            strQuery.Append("  AIRFREIGHT_SLABS_TBL_PK,      ");
            strQuery.Append("  BREAKPOINT_ID,        ");
            strQuery.Append("  BREAKPOINT_DESC,        ");
            strQuery.Append("   NVL(APPROVED_RATE,0)               CURRENT_RATE,         ");
            strQuery.Append("   NVL(APPROVED_RATE,0)               REQUEST_RATE,        ");
            strQuery.Append("  NVL(APPROVED_RATE,0)               APPROVED_RATE        ");
            strQuery.Append(" FROM          ");
            strQuery.Append("  QUOT_AIR_TRN_FREIGHT_TBL  TRAN,    ");
            strQuery.Append("  QUOTE_AIR_BREAKPOINTS  BPNT,    ");
            strQuery.Append("  AIRFREIGHT_SLABS_TBL       ");
            strQuery.Append(" WHERE          ");
            strQuery.Append("   BPNT.QUOT_GEN_AIR_FRT_FK IN ( " + FreightPks + " )  ");
            strQuery.Append("  AND BPNT.QUOT_GEN_AIR_FRT_FK = TRAN.QUOT_GEN_TRN_FREIGHT_PK");
            strQuery.Append("  AND AIRFREIGHT_SLABS_TBL.ACTIVE_FLAG = 1    ");
            strQuery.Append("  AND BPNT.AIRFREIGHT_SLABS_TBL_FK= AIRFREIGHT_SLABS_TBL_PK");
            strQuery.Append("  ORDER BY FREIGHT_ELEMENT_MST_FK, BREAKPOINT_RANGE,BREAKPOINT_ID");
            strQuery.Append("");

            return strQuery.ToString();
            //
        }

        #endregion "Quotation fetch"

        #region " Transfer Data [ Slabs ] "

        //RFQ_SPOT_AIR_FRT_FK, RFQ_SPOT_TRN_FREIGHT_PK
        private void TransferSlabsData(DataTable GridDt, DataTable SlabDt, bool AmendFlg = false)
        {
            Int16 GRowCnt = default(Int16);
            Int16 SRowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            string frtpk = null;
            string Fk = null;
            string polpk = null;
            string podpk = null;
            DataRow DRChild = null;
            if (AmendFlg == false)
            {
                for (GRowCnt = 0; GRowCnt <= GridDt.Rows.Count - 1; GRowCnt++)
                {
                    frtpk = Convert.ToString(GridDt.Rows[GRowCnt]["FREIGHT_ELEMENT_MST_FK"]);
                    Fk = Convert.ToString(GridDt.Rows[GRowCnt]["FK"]);
                    for (SRowCnt = 0; SRowCnt <= SlabDt.Select("FREIGHT_ELEMENT_MST_FK = " + frtpk + "and FK=" + Fk).Length - 1; SRowCnt++)
                    {
                        DRChild = SlabDt.Select("FREIGHT_ELEMENT_MST_FK = " + frtpk + "and FK=" + Fk)[SRowCnt];
                        for (ColCnt = 0; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(DRChild["AIRFREIGHT_SLABS_TBL_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[GRowCnt][ColCnt] = DRChild["CURRENT_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 1] = DRChild["REQUEST_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 2] = DRChild["APPROVED_RATE"];
                                //Exit For
                            }
                        }
                    }
                }
            }
            else
            {
                for (GRowCnt = 0; GRowCnt <= GridDt.Rows.Count - 1; GRowCnt++)
                {
                    frtpk = Convert.ToString(GridDt.Rows[GRowCnt]["FREIGHT_ELEMENT_MST_FK"]);
                    polpk = Convert.ToString(GridDt.Rows[GRowCnt]["POL_PK"]);
                    podpk = Convert.ToString(GridDt.Rows[GRowCnt]["POD_PK"]);
                    for (SRowCnt = 0; SRowCnt <= SlabDt.Select("FREIGHT_ELEMENT_MST_FK = " + frtpk + "and POLPK=" + polpk + "and PODPK=" + podpk).Length - 1; SRowCnt++)
                    {
                        DRChild = SlabDt.Select("FREIGHT_ELEMENT_MST_FK = " + frtpk + "and POLPK=" + polpk + "and PODPK=" + podpk)[SRowCnt];
                        for (ColCnt = 0; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(DRChild["AIRFREIGHT_SLABS_TBL_PK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[GRowCnt][ColCnt] = DRChild["CURRENT_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 1] = DRChild["REQUEST_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 2] = DRChild["APPROVED_RATE"];
                                //Exit For
                            }
                        }
                    }
                }
            }
            //For SRowCnt = 0 To SlabDt.Rows.Count - 1
            //    frtpk = SlabDt.Rows(SRowCnt).Item("FREIGHT_ELEMENT_MST_FK")
            //    fk =
            //    For GRowCnt = 0 To GridDt.Rows.Count - 1
            //        If GridDt.Rows(GRowCnt).Item("FREIGHT_ELEMENT_MST_FK") = frtpk Then
            //            For ColCnt = 0 To GridDt.Columns.Count - 1
            //                If CStr(SlabDt.Rows(SRowCnt).Item("AIRFREIGHT_SLABS_TBL_PK")) = CStr(GridDt.Columns(ColCnt).Caption) Then
            //                    GridDt.Rows(GRowCnt).Item(ColCnt) = SlabDt.Rows(SRowCnt).Item("CURRENT_RATE")
            //                    GridDt.Rows(GRowCnt).Item(ColCnt + 1) = SlabDt.Rows(SRowCnt).Item("REQUEST_RATE")
            //                    GridDt.Rows(GRowCnt).Item(ColCnt + 2) = SlabDt.Rows(SRowCnt).Item("APPROVED_RATE")
            //                    'Exit For
            //                End If
            //            Next ColCnt
            //            ' Exit For
            //        End If
            //    Next GRowCnt
            //Next SRowCnt
        }

        #endregion " Transfer Data [ Slabs ] "

        #region " Getting Slabs "

        private string getStrSlabs(DataTable DT)
        {
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            StringBuilder strBuilder = new StringBuilder();
            string frpk = "-1";
            string slpk = "-1";
            if (DT.Rows.Count > 0)
            {
                frpk = Convert.ToString(DT.Rows[0]["FREIGHT_ELEMENT_MST_FK"]);
            }
            strBuilder.Append("");
            for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
            {
                if (frpk != Convert.ToString(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"]))
                    break; // TODO: might not be correct. Was : Exit For
                if (slpk != Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]))
                {
                    strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"].ToString())).Trim() + ",");
                    strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_ID"].ToString())).Trim() + ",");
                    strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["BREAKPOINT_DESC"].ToString())).Trim() + ".,");
                    slpk = Convert.ToString(DT.Rows[RowCnt]["AIRFREIGHT_SLABS_TBL_PK"]);
                }
            }
            if (DT.Rows.Count > 0)
                strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion " Getting Slabs "

        #region " Getting Freights "

        private string getStrFreights(DataTable DT)
        {
            bool WithBasis = false;
            Int16 RowCnt = default(Int16);
            Int16 ln = default(Int16);
            StringBuilder strBuilder = new StringBuilder();
            if (DT.Columns.Contains("CHARGE_BASIS"))
            {
                WithBasis = true;
            }
            string strChBasis = "";
            string intChBasis = "";
            string frpk = "-1";
            strBuilder.Append("");
            if (DT.Rows.Count > 0)
            {
                frpk = "-1";
            }
            for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
            {
                if (DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"] != frpk)
                {
                    if (WithBasis)
                    {
                        if (Convert.ToInt32(DT.Rows[RowCnt]["CHARGE_BASIS"]) == 1)
                        {
                            strChBasis = "(%)";
                        }
                        else if (Convert.ToInt32(DT.Rows[RowCnt]["CHARGE_BASIS"]) == 2)
                        {
                            strChBasis = "(Flat)";
                        }
                        else if (Convert.ToInt32(DT.Rows[RowCnt]["CHARGE_BASIS"]) == 3)
                        {
                            strChBasis = "(Kgs)";
                        }
                        else
                        {
                            strChBasis = "(--)";
                        }
                        intChBasis = "~" + Convert.ToString(DT.Rows[RowCnt]["CHARGE_BASIS"]);
                    }
                    strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"].ToString())).Trim() + ",");
                    strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_ID"].ToString())).Trim() + strChBasis + ",");
                    strBuilder.Append(Convert.ToString(removeDBNull(DT.Rows[RowCnt]["FREIGHT_ELEMENT_NAME"].ToString())).Trim() + intChBasis + ",");
                    frpk = DT.Rows[RowCnt]["FREIGHT_ELEMENT_MST_FK"].ToString();
                }
            }
            if (DT.Rows.Count > 0)
                strBuilder.Remove(strBuilder.Length - 1, 1);
            return strBuilder.ToString();
        }

        #endregion " Getting Freights "

        #region " Column Add "

        private void AddColumns(DataTable DT, string FRTs)
        {
            Array CHeads = null;
            string hed = null;
            CHeads = FRTs.Split(',');
            int RwCnt = 0;
            RwCnt = DT.Rows.Count;
            Int16 i = default(Int16);
            if (RwCnt > 0)
            {
                //For i = 0 To (CHeads.Length) / RwCnt - 3 Step 3
                //    DT.Columns.Add(CStr(CHeads(i)), GetType(Decimal))
                //    DT.Columns.Add(CStr(CHeads(i + 1)), GetType(Decimal))
                //    DT.Columns.Add(CStr(CHeads(i + 2)), GetType(Decimal))
                //Next
                for (i = 0; i <= CHeads.Length - 3; i += 3)
                {
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 1)), typeof(decimal));
                    DT.Columns.Add(Convert.ToString(CHeads.GetValue(i + 2)), typeof(decimal));
                }
            }
            //For i = 0 To CHeads.Length - 3 Step 3
            //    DT.Columns.Add(CStr(CHeads(i)), GetType(Decimal))
            //    DT.Columns.Add(CStr(CHeads(i + 1)), GetType(Decimal))
            //    DT.Columns.Add(CStr(CHeads(i + 2)), GetType(Decimal))
            //Next
        }

        #endregion " Column Add "

        #region " Transfer Data [ Kg Freights ] "

        private void TransferKGFreightsData(DataTable GridDt, DataTable FrtDt, bool AmendFlg = false)
        {
            Int16 RowCnt = default(Int16);
            Int16 ColCnt = default(Int16);
            Int16 GRowCnt = default(Int16);
            Int16 SRowCnt = default(Int16);
            string frtpk = null;
            string Fk = null;
            string polpk = null;
            string podpk = null;
            DataRow DRChild = null;
            if (AmendFlg == false)
            {
                for (GRowCnt = 0; GRowCnt <= GridDt.Rows.Count - 1; GRowCnt++)
                {
                    Fk = Convert.ToString(GridDt.Rows[GRowCnt]["PK"]);
                    for (SRowCnt = 0; SRowCnt <= FrtDt.Select("FK=" + Fk).Length - 1; SRowCnt++)
                    {
                        DRChild = FrtDt.Select("FK=" + Fk)[SRowCnt];
                        for (ColCnt = 0; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(DRChild["FREIGHT_ELEMENT_MST_FK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[GRowCnt][ColCnt] = DRChild["CURRENT_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 1] = DRChild["REQUEST_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 2] = DRChild["APPROVED_RATE"];
                            }
                        }
                    }
                }
                //'
            }
            else
            {
                for (GRowCnt = 0; GRowCnt <= GridDt.Rows.Count - 1; GRowCnt++)
                {
                    polpk = Convert.ToString(GridDt.Rows[GRowCnt]["PORT_MST_POL_FK"]);
                    podpk = Convert.ToString(GridDt.Rows[GRowCnt]["PORT_MST_POD_FK"]);
                    for (SRowCnt = 0; SRowCnt <= FrtDt.Select("POL_PK=" + polpk + "and POD_PK=" + podpk).Length - 1; SRowCnt++)
                    {
                        DRChild = FrtDt.Select("POL_PK=" + polpk + "and POD_PK=" + podpk)[SRowCnt];
                        for (ColCnt = 0; ColCnt <= GridDt.Columns.Count - 1; ColCnt++)
                        {
                            if (Convert.ToString(DRChild["FREIGHT_ELEMENT_MST_FK"]) == Convert.ToString(GridDt.Columns[ColCnt].Caption))
                            {
                                GridDt.Rows[GRowCnt][ColCnt] = DRChild["CURRENT_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 1] = DRChild["REQUEST_RATE"];
                                GridDt.Rows[GRowCnt][ColCnt + 2] = DRChild["APPROVED_RATE"];
                            }
                        }
                    }
                }
            }
        }

        #endregion " Transfer Data [ Kg Freights ] "

        #region "Save General"

        #region " Header Save "

        #region " Logical Flow "

        // The Save operation deals most of the business rules in this layer itself
        // There are very less that is dealt on database layer. there we simply have
        // procedures to get the save request and act accordingly on the database.
        // The Save Operation works on seven DataBase Tables Simultaneously.
        // All these has to be done in a single transaction so that in the event of any exceptional
        // case all the operation can be rollbacked.
        // The same database procedure will work for both Insertion and Updation
        // It depends upon the value provided in Primary Key field
        // If it is provided a value it means that the record is already exixting so the request is for updation
        // If it is provided as NULL it will refer that the record is to be inserted.
        // This logic has been implemented in the DataBase procedure also.
        //-------------------------------------------------------------------------------------------------------
        // From Here IU will mean Insert & Update Operation
        // The Following is for IU in Master Table
        // It initiate a transaction
        // Through the same Command Object several calls will be made for IU in other Child Tables
        // If a corresponding AirWay Bill no is provided then it will go and update that Airway bill table
        // also within the same transaction.
        // In case of New Record it will generate a new Reference no for the Spot Rate
        // Before IU if verifies for any clash with other existing RFQ.
        // if so the call to CheckDependency method will return a Message and so the transaction will
        // be cancelled.
        // The Spot Rate is always based on Contract and so the validity period should always be kept within
        // that contract. This Validity is taken care by the DataBase procedure which returns -1 in such event
        // In other cases that returns the Primary Key Value.
        // By keeping track on the return value here it can be decided whether that clashes with contract validity.
        // Lastly this invokes a function that performs the IU for other transactions.
        // There we pass the Command Object By Reference that already attached the Transaction
        // So other DataBase Procedures invoked through that command Object will remain within that transaction.
        // The One thing that is important to understand is that passing the return value to child save functions.
        // In case of Update that will be available in the DataTable also
        // But in case of insert we need the newly inserted PK value to be used as FK in insertion of child tables.
        // This is why the returned PK value is passed to the child save function.
        // The same concept continues in child save methods also.

        #endregion " Logical Flow "

        #region "Update General Quotation"

        public ArrayList Update_Transaction_Gen(DataSet Gridds, DataTable OthDt, long PkValue, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            Int32 cRCnt = default(Int32);
            long TranPk = 0;
            long BaseCurrency = 0;
            long gTranPk = 0;
            Cls_AirlineRFQSpotRate objRfq = new Cls_AirlineRFQSpotRate();
            //BaseCurrency = objRfq.GetBaseCurrency();
            bool Flag = false;
            arrMessage.Clear();
            try
            {
                var _with28 = SCM;
                _with28.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= Gridds.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    for (cRCnt = 0; cRCnt <= Gridds.Tables[1].Rows.Count - 1; cRCnt++)
                    {
                        //'Added
                        if (Gridds.Tables[1].Rows[nRowCnt]["SELECTED"] == "true" | Gridds.Tables[1].Rows[nRowCnt]["SELECTED"] == "1")
                        {
                            // gTranPk is the transaction Pk that is available before save
                            // based on this we have OtherCharge Info for all rows
                            // this key value is relevent in case of filtering relevent rows
                            // fron Other Charge Table for a particular Transaction Row
                            gTranPk = Convert.ToInt64(Gridds.Tables[0].Rows[nRowCnt]["PK"]);
                            _with28.CommandText = UserName + ".QUOTATION_GEN_AIR_TBL_PKG.QUOT_GEN_TRN_AIR_TBL_UPD";
                            var _with29 = _with28.Parameters;
                            _with29.Clear();
                            _with29.Add("QUOT_GEN_AIR_TRN_PK_IN", Gridds.Tables[0].Rows[nRowCnt]["PK"]).Direction = ParameterDirection.Input;
                            _with29.Add("QUOT_GEN_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                            _with29.Add("AIRLINE_MST_FK_IN", getDefault(Gridds.Tables[0].Rows[nRowCnt]["AIRLINE_MST_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                            _with29.Add("COMMODITY_IN", getDefault(Gridds.Tables[0].Rows[nRowCnt]["COMMODITY_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                            _with29.Add("REF_NO_IN", Gridds.Tables[0].Rows[nRowCnt]["REFNO"]).Direction = ParameterDirection.Input;
                            _with29.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[nRowCnt]["VALID_FROM"]).Date)).Direction = ParameterDirection.Input;
                            if ((object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value)))
                            {
                                _with29.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else if (Gridds.Tables[0].Rows[nRowCnt]["VALID_TO"] == "  /  /    ")
                            {
                                _with29.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with29.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[nRowCnt]["VALID_TO"]))).Direction = ParameterDirection.Input;
                            }

                            _with29.Add("POL_GRP_FK_IN", (string.IsNullOrEmpty(Gridds.Tables[0].Rows[nRowCnt]["POL_GRP_FK"].ToString()) ? DBNull.Value : Gridds.Tables[0].Rows[nRowCnt]["POL_GRP_FK"])).Direction = ParameterDirection.Input;
                            _with29.Add("POD_GRP_FK_IN", (string.IsNullOrEmpty(Gridds.Tables[0].Rows[nRowCnt]["POD_GRP_FK"].ToString()) ? DBNull.Value : Gridds.Tables[0].Rows[nRowCnt]["POD_GRP_FK"])).Direction = ParameterDirection.Input;
                            _with29.Add("TARIFF_GRP_FK_IN", (string.IsNullOrEmpty(Gridds.Tables[0].Rows[nRowCnt]["TARIFF_GRP_FK"].ToString()) ? DBNull.Value : Gridds.Tables[0].Rows[nRowCnt]["TARIFF_GRP_FK"])).Direction = ParameterDirection.Input;
                            _with29.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with28.ExecuteNonQuery();
                            TranPk = Convert.ToInt64(_with28.Parameters["RETURN_VALUE"].Value);
                            if (TranPk == -1)
                            {
                                throw new Exception("Spot Rate should be within Contract Period only.");
                            }
                            Flag = true;
                        }
                        if (Flag == true)
                        {
                            Flag = false;
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                    arrMessage = SaveFrtTran(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }
                    arrMessage = SaveSurcharge(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName, BaseCurrency);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }

                    arrMessage = SaveOthCharge(OthDt, TranPk, gTranPk, SCM, UserName, BaseCurrency);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }
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

        #endregion "Update General Quotation"

        #region " Header Part [ Save Quotation ] "

        public ArrayList UpdateQuotationGen(DataTable HDT, DataSet GridDS, DataTable OthDt, string QuotationPk, string ValidFor, string Status, string ExpectedShipmentDate, bool gen, string Measure, string Wt,
        string DivFac, string Header = "", string Footer = "", int PYMTType = 0, int INCOTerms = 0, int From_Flag = 0, bool Customer_Approved = false)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            Int32 prvstatus = default(Int32);
            prvstatus = getStatus(QuotationPk);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            try
            {
                var _with30 = objWK.MyCommand;
                _with30.CommandType = CommandType.StoredProcedure;
                _with30.CommandText = objWK.MyUserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_AIR_TBL_UPD";
                var _with31 = _with30.Parameters;
                _with31.Add("QUOTATION_AIR_PK_IN", QuotationPk).Direction = ParameterDirection.Input;
                _with31.Add("VALID_FOR_IN", ValidFor).Direction = ParameterDirection.Input;
                _with31.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                _with31.Add("EXPECTED_SHIPMENT_DT_IN", ExpectedShipmentDate).Direction = ParameterDirection.Input;
                _with31.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with31.Add("VERSION_NO_IN", Version_No).Direction = ParameterDirection.Input;
                _with31.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;
                _with31.Add("COL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with31.Add("COL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with31.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with31.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with31.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with31.Add("REMARKS_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with31.Add("Header_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                _with31.Add("Footer_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                _with31.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                _with31.Add("PYMTType_IN", PYMTType).Direction = ParameterDirection.Input;
                _with31.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                _with31.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;
                _with31.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                //_with30.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = getDefault(HDT.Rows[0]["COL_PLACE_MST_FK"], "");
                //_with30.Parameters["COL_ADDRESS_IN"].SourceVersion = getDefault(HDT.Rows[0]["COL_ADDRESS"], "");
                //_with30.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = getDefault(HDT.Rows[0]["DEL_PLACE_MST_FK"], "");
                //_with30.Parameters["DEL_ADDRESS_IN"].SourceVersion = getDefault(HDT.Rows[0]["DEL_ADDRESS"], "");
                //_with30.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = getDefault(HDT.Rows[0]["CARGO_MOVE_FK"],"");
                //_with30.Parameters["REMARKS_IN"].SourceVersion = getDefault(HDT.Rows[0]["REMARKS"], "");
                ////added by gopi
                //_with30.Parameters["Header_IN"].SourceVersion = getDefault(Header, DBNull.Value).ToString();
                //_with30.Parameters["Footer_IN"].SourceVersion = getDefault(Footer, DBNull.Value);
                _with30.Parameters.Add("PORT_GROUP_IN", getDefault(HDT.Rows[0]["PORT_GROUP"], DBNull.Value)).Direction = ParameterDirection.Input;
                _with30.ExecuteNonQuery();
                _PkValue = Convert.ToInt64(objWK.MyCommand.Parameters["RETURN_VALUE"].Value);
                arrMessage = Update_Transaction_Gen(GridDS, OthDt, _PkValue, objWK.MyCommand, objWK.MyUserName);
                TRAN.Commit();
                arrMessage.Add("All data saved successfully");
                return arrMessage;
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
        }

        public ArrayList SaveQuotationGen(DataTable HDT, DataSet GridDS, DataTable OthDt, string QuoteNo, string QuotePk, long nLocationId, long nEmpId, bool NRec = false, string Options = null, string Header = "",
        string Footer = "", int PYMTType = 0, int INCOTerms = 0, bool AmendFlg = false, int From_Flag = 0, bool Customer_Approved = false)
        {
            DataTable pDT = null;
            DataTable cDT = null;

            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = default(OracleTransaction);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            arrMessage.Clear();
            objWK.MyCommand.Transaction = TRAN;
            bool chkFlag = false;
            string PrevQuotPK = null;
            try
            {
                if (AmendFlg == true)
                {
                    NewRecord = true;
                    PrevQuotPK = QuotePk;
                    QuotePk = "";
                }
                if (string.IsNullOrEmpty(QuoteNo))
                {
                    QuoteNo = GenerateQuoteNo(nLocationId, nEmpId, M_CREATED_BY_FK, objWK, From_Flag);
                    if (QuoteNo == "Protocol Not Defined.")
                    {
                        arrMessage.Add("Protocol Not Defined.");
                        QuoteNo = "";
                        return arrMessage;
                    }
                }

                var _with32 = objWK.MyCommand;
                _with32.CommandType = CommandType.StoredProcedure;
                _with32.CommandText = objWK.MyUserName + ".QUOTATION_AIR_TBL_PKG.QUOTATION_AIR_TBL_INS";
                _with32.Parameters.Clear();
                chkFlag = true;
                var _with33 = _with32.Parameters;
                _with33.Add("QUOTATION_REF_NO_IN", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Input;
                _with33.Add("QUOTATION_DATE_IN", OracleDbType.Varchar2, 10).Direction = ParameterDirection.Input;
                _with33.Add("VALID_FOR_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with33.Add("PYMT_TYPE_IN", OracleDbType.Int32, PYMTType).Direction = ParameterDirection.Input;
                _with33.Add("QUOTED_BY_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("COL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("COL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with33.Add("DEL_PLACE_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("DEL_ADDRESS_IN", OracleDbType.Varchar2, 200).Direction = ParameterDirection.Input;
                _with33.Add("AGENT_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("STATUS_IN", OracleDbType.Int32, 1).Direction = ParameterDirection.Input;
                _with33.Add("EXPECTED_SHIPMENT_DT_IN", OracleDbType.Varchar2, 10).Direction = ParameterDirection.Input;
                _with33.Add("CUSTOMER_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("CUSTOMER_CATEGORY_MST_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("CUST_TYPE_IN", OracleDbType.Int32, 1).Direction = ParameterDirection.Input;
                _with33.Add("SPECIAL_INSTRUCTIONS_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with33.Add("COMMODITY_GROUP_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with33.Add("CREDIT_LIMIT_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with33.Add("CREDIT_DAYS_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with33.Add("CREATED_BY_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("CARGO_MOVE_FK_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                _with33.Add("QUOTATION_TYPE_IN", OracleDbType.Int32, 3).Direction = ParameterDirection.Input;
                //added by gopi
                _with33.Add("HEADER_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                _with33.Add("FOOTER_IN", OracleDbType.Varchar2, 2000).Direction = ParameterDirection.Input;
                //end
                _with33.Add("REMARKS_IN", OracleDbType.Varchar2, 1000).Direction = ParameterDirection.Input;
                _with33.Add("CONFIG_PK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                //ADDED BY SURYA PRASAD
                _with33.Add("BASE_CURRENCY_FK_IN", OracleDbType.Int32, 10).Direction = ParameterDirection.Input;
                _with33.Add("FROM_FLAG_IN", getDefault(From_Flag, 0)).Direction = ParameterDirection.Input;
                _with33.Add("SHIPPING_TERMS_MST_PK_IN", INCOTerms).Direction = ParameterDirection.Input;
                _with32.Parameters["SHIPPING_TERMS_MST_PK_IN"].SourceVersion = DataRowVersion.Current;

                _with32.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;

                //_with32.Parameters["QUOTATION_REF_NO_IN"].SourceVersion = QuoteNo;
                //_with32.Parameters["QUOTATION_DATE_IN"].SourceVersion = HDT.Rows[0]["QUOTATION_DATE"];
                //_with32.Parameters["VALID_FOR_IN"].SourceVersion = HDT.Rows[0]["VALID_FOR"];
                //_with32.Parameters["PYMT_TYPE_IN"].SourceVersion = PYMTType;
                //_with32.Parameters["QUOTED_BY_IN"].SourceVersion = M_CREATED_BY_FK;
                //_with32.Parameters["COL_PLACE_MST_FK_IN"].SourceVersion = HDT.Rows[0]["COL_PLACE_MST_FK"];
                //_with32.Parameters["COL_ADDRESS_IN"].SourceVersion = HDT.Rows[0]["COL_ADDRESS"];
                //_with32.Parameters["DEL_PLACE_MST_FK_IN"].SourceVersion = HDT.Rows[0]["DEL_PLACE_MST_FK"];
                //_with32.Parameters["DEL_ADDRESS_IN"].SourceVersion = HDT.Rows[0]["DEL_ADDRESS"];
                //_with32.Parameters["AGENT_MST_FK_IN"].SourceVersion = HDT.Rows[0]["AGENT_MST_FK"];
                //_with32.Parameters["STATUS_IN"].SourceVersion = HDT.Rows[0]["STATUS"];
                //_with32.Parameters["EXPECTED_SHIPMENT_DT_IN"].SourceVersion = HDT.Rows[0]["EXPECTED_SHIPMENT_DT"];
                //_with32.Parameters["CUSTOMER_MST_FK_IN"].SourceVersion = HDT.Rows[0]["CUSTOMER_MST_FK"];
                //_with32.Parameters["CUSTOMER_CATEGORY_MST_FK_IN"].SourceVersion = HDT.Rows[0]["CUSTOMER_CATEGORY_MST_FK"];
                //_with32.Parameters["CUST_TYPE_IN"].SourceVersion = getDefault(HDT.Rows[0]["CUST_TYPE"], 0);
                //_with32.Parameters["SPECIAL_INSTRUCTIONS_IN"].SourceVersion = HDT.Rows[0]["SPECIAL_INSTRUCTIONS"];
                //_with32.Parameters["COMMODITY_GROUP_IN"].SourceVersion = HDT.Rows[0]["COMMODITY_GROUP_MST_FK"];
                //_with32.Parameters["CREDIT_LIMIT_IN"].SourceVersion = HDT.Rows[0]["CREDIT_LIMIT"];
                //_with32.Parameters["CREDIT_DAYS_IN"].SourceVersion = HDT.Rows[0]["CREDIT_DAYS"];
                //_with32.Parameters["CARGO_MOVE_FK_IN"].SourceVersion = HDT.Rows[0]["CARGO_MOVE_FK"];
                //_with32.Parameters["QUOTATION_TYPE_IN"].SourceVersion = HDT.Rows[0]["QUOTATION_TYPE"];
                //_with32.Parameters["REMARKS_IN"].SourceVersion = HDT.Rows[0]["REMARKS"];
                //_with32.Parameters["Header_IN"].SourceVersion = getDefault(Header, DBNull.Value);
                //_with32.Parameters["Footer_IN"].SourceVersion = getDefault(Footer, DBNull.Value);
                //_with32.Parameters["CREATED_BY_FK_IN"].SourceVersion = M_CREATED_BY_FK;
                //_with32.Parameters["CONFIG_PK_IN"].SourceVersion = M_Configuration_PK;
                //_with32.Parameters["BASE_CURRENCY_FK_IN"].SourceVersion = HDT.Rows[0]["BASE_CURRENCY_FK"];
                _with32.Parameters.Add("CUSTOMER_APPROVED_IN", (Customer_Approved ? 1 : 0)).Direction = ParameterDirection.Input;
                _with32.Parameters.Add("PORT_GROUP_IN", HDT.Rows[0]["PORT_GROUP"]).Direction = ParameterDirection.Input;
                _with32.ExecuteNonQuery();
                _PkValue = Convert.ToInt64(_with32.Parameters["RETURN_VALUE"].Value);

                HDT.Rows[0]["QUOTATION_AIR_PK"] = _PkValue;

                arrMessage = SaveTransaction(GridDS, OthDt, _PkValue, objWK.MyCommand, objWK.MyUserName, Convert.ToString(_PkValue), Options);
                //'
                if (AmendFlg == true)
                {
                    string str = null;
                    Int32 intIns = default(Int32);
                    Int32 ValidFor = default(Int32);
                    TimeSpan Span = default(TimeSpan);
                    OracleCommand updCmdUser = new OracleCommand();
                    updCmdUser.Transaction = TRAN;
                    System.DateTime QuotDate = default(System.DateTime);
                    QuotDate = GetQuotDt(PrevQuotPK);
                    if (QuotDate > DateTime.Today.Date)
                    {
                        str = " update QUOTATION_AIR_TBL QT SET QT.STATUS = 3 ,";
                        str += " QT.AMEND_FLAG = 1 ,";
                        str += " WHERE QT.QUOTATION_AIR_PK=" + PrevQuotPK;
                    }
                    else
                    {
                        Span = DateTime.Today.Subtract(QuotDate);
                        ValidFor = Span.Days;
                        str = " update QUOTATION_AIR_TBL QT SET QT.EXPECTED_SHIPMENT = SYSDATE ,";
                        str += " QT.AMEND_FLAG = 1 ,";
                        str += " QT.VALID_FOR = " + ValidFor;
                        str += " WHERE QT.QUOTATION_AIR_PK=" + PrevQuotPK;
                    }
                    var _with34 = updCmdUser;
                    _with34.Connection = objWK.MyConnection;
                    _with34.Transaction = TRAN;
                    _with34.CommandType = CommandType.Text;
                    _with34.CommandText = str;
                    intIns = _with34.ExecuteNonQuery();
                }
                //'
                if (arrMessage.Count > 0)
                {
                    if (string.Compare(Convert.ToString(arrMessage[0].ToString()).ToUpper(), "SAVED") > 0)
                    {
                        arrMessage.Add("All data saved successfully");
                        TRAN.Commit();
                        QuotePk = Convert.ToString(_PkValue);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        if (chkFlag)
                        {
                           RollbackProtocolKey("QUOTATION (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
                            chkFlag = false;
                        }
                        return arrMessage;
                    }
                }
            }
            catch (OracleException oraexp)
            {
                TRAN.Rollback();
                if (chkFlag)
                {
                   RollbackProtocolKey("QUOTATION (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
                    chkFlag = false;
                }
                arrMessage.Add(oraexp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                if (chkFlag)
                {
                   RollbackProtocolKey("QUOTATION (AIR)", Convert.ToInt32(HttpContext.Current.Session["LOGED_IN_LOC_FK"]), Convert.ToInt32(HttpContext.Current.Session["EMP_PK"]), QuoteNo, System.DateTime.Now);
                    chkFlag = false;
                }
                arrMessage.Add(ex.Message);
                return arrMessage;
            }
            finally
            {
                objWK.MyCommand.Connection.Close();
            }
            return new ArrayList();
        }

        #endregion " Header Part [ Save Quotation ] "

        #endregion " Header Save "

        #region " Transaction Save "

        #region " Logical Flow "

        // Getting the command string from main save function it performs IU on transaction
        // then it invokes other 4 Save operation using the same command object.
        // the contract period should not be violated on port-pair level also
        // the DataBase Procedure returns -1 in such event that will cause a exception to be
        // raised and finally the transaction will be rollbacked.

        #endregion " Logical Flow "

        #region "SaveTransaction"

        public ArrayList SaveTransaction(DataSet Gridds, DataTable OthDt, long PkValue, OracleCommand SCM, string UserName, string ContractNo, string Options = null)
        {
            Int32 nRowCnt = default(Int32);
            Int32 cRCnt = default(Int32);
            bool Flag = false;
            long TranPk = 0;
            long BaseCurrency = 0;
            long gTranPk = 0;
            Cls_AirlineRFQSpotRate objRfq = new Cls_AirlineRFQSpotRate();
            //BaseCurrency = objRfq.GetBaseCurrency();
            int AirlinePk = 0;
            arrMessage.Clear();
            try
            {
                var _with35 = SCM;
                _with35.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= Gridds.Tables[0].Rows.Count - 1; nRowCnt++)
                {
                    for (cRCnt = 0; cRCnt <= Gridds.Tables[1].Rows.Count - 1; cRCnt++)
                    {
                        //'Added
                        if (Gridds.Tables[1].Rows[cRCnt]["SELECTED"] == "true" | Gridds.Tables[1].Rows[cRCnt]["SELECTED"] == "1")
                        {
                            // gTranPk is the transaction Pk that is available before save
                            // based on this we have OtherCharge Info for all rows
                            // this key value is relevent in case of filtering relevent rows
                            // fron Other Charge Table for a particular Transaction Row

                            gTranPk = Convert.ToInt64(Gridds.Tables[0].Rows[nRowCnt]["PK"]);
                            _with35.CommandText = UserName + ".QUOTATION_GEN_AIR_TBL_PKG.QUOT_GEN_TRN_AIR_TBL_INS";
                            var _with36 = _with35.Parameters;
                            _with36.Clear();
                            if (NewRecord)
                            {
                                //QUOT_GEN_AIR_TRN_PK_IN()
                                _with36.Add("QUOT_GEN_AIR_TRN_PK_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with36.Add("QUOT_GEN_AIR_TRN_PK_IN", Gridds.Tables[0].Rows[nRowCnt]["PK"]).Direction = ParameterDirection.Input;
                            }
                            //QUOT_GEN_AIR_FK_IN()
                            _with36.Add("QUOT_GEN_AIR_FK_IN", PkValue).Direction = ParameterDirection.Input;
                            //AIRLINE_MST_FK_IN()
                            _with36.Add("AIRLINE_MST_FK_IN", getDefault(Gridds.Tables[0].Rows[nRowCnt]["AIRLINE_MST_PK"], DBNull.Value)).Direction = ParameterDirection.Input;
                            _with36.Add("COMMODITY_IN", getDefault(Gridds.Tables[0].Rows[nRowCnt]["COMMODITY_MST_FK"], DBNull.Value)).Direction = ParameterDirection.Input;
                            //PORT_MST_POL_FK_IN()
                            _with36.Add("PORT_MST_POL_FK_IN", Gridds.Tables[0].Rows[nRowCnt]["PORT_MST_POL_FK"]).Direction = ParameterDirection.Input;
                            //PORT_MST_POD_FK_IN()
                            _with36.Add("PORT_MST_POD_FK_IN", Gridds.Tables[0].Rows[nRowCnt]["PORT_MST_POD_FK"]).Direction = ParameterDirection.Input;
                            //TRANS_REFERED_FROM_IN()
                            //_with36.Add("TRANS_REFERED_FROM_IN", Convert.ToInt64(TransRefer(Options))).Direction = ParameterDirection.Input;
                            //REF_NO
                            _with36.Add("REF_NO_IN", Gridds.Tables[0].Rows[nRowCnt]["REFNO"]).Direction = ParameterDirection.Input;

                            //VALID_FROM_IN()
                            _with36.Add("VALID_FROM_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[nRowCnt]["VALID_FROM"]).Date)).Direction = ParameterDirection.Input;
                            //VALID_TO_IN()
                            if ((object.ReferenceEquals(Gridds.Tables[0].Rows[0]["VALID_TO"], DBNull.Value)))
                            {
                                _with36.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else if (Gridds.Tables[0].Rows[nRowCnt]["VALID_TO"] == "  /  /    ")
                            {
                                _with36.Add("VALID_TO_IN", DBNull.Value).Direction = ParameterDirection.Input;
                            }
                            else
                            {
                                _with36.Add("VALID_TO_IN", Convert.ToString(Convert.ToDateTime(Gridds.Tables[0].Rows[nRowCnt]["VALID_TO"]))).Direction = ParameterDirection.Input;
                            }
                            // New for contract No
                            // .Add("CONT_MAIN_AIR_FK_IN", ContractNo).Direction = ParameterDirection.Input
                            //RETURN_VALUE()
                            _with36.Add("POL_GRP_FK_IN", (string.IsNullOrEmpty(Gridds.Tables[0].Rows[nRowCnt]["POL_GRP_FK"].ToString()) ? DBNull.Value : Gridds.Tables[0].Rows[nRowCnt]["POL_GRP_FK"])).Direction = ParameterDirection.Input;
                            _with36.Add("POD_GRP_FK_IN", (string.IsNullOrEmpty(Gridds.Tables[0].Rows[nRowCnt]["POD_GRP_FK"].ToString()) ? DBNull.Value : Gridds.Tables[0].Rows[nRowCnt]["POD_GRP_FK"])).Direction = ParameterDirection.Input;
                            _with36.Add("TARIFF_GRP_FK_IN", (string.IsNullOrEmpty(Gridds.Tables[0].Rows[nRowCnt]["TARIFF_GRP_FK"].ToString()) ? DBNull.Value : Gridds.Tables[0].Rows[nRowCnt]["TARIFF_GRP_FK"])).Direction = ParameterDirection.Input;
                            _with36.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                            _with35.ExecuteNonQuery();
                            TranPk = Convert.ToInt64(_with35.Parameters["RETURN_VALUE"].Value);
                            if (TranPk == -1)
                            {
                                throw new Exception("Spot Rate should be within Contract Period only.");
                            }
                            Flag = true;
                        }
                        //'Added
                        if (Flag == true)
                        {
                            Flag = false;
                            break; // TODO: might not be correct. Was : Exit For
                        }
                    }
                    arrMessage = SaveFrtTran(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }
                    arrMessage = SaveSurcharge(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName, BaseCurrency);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }

                    arrMessage = SaveOthCharge(OthDt, TranPk, gTranPk, SCM, UserName, BaseCurrency);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }
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

        #endregion "SaveTransaction"

        #endregion " Transaction Save "

        #region " Other Charge Save "

        #region " Logical Flow "

        // This is a simple call to OtherCharge procedure for IU operation
        // Here the one difference is that we have two Pk Values
        // PkValue and gTranPk
        // The PkValue is the returned value from Transcation and
        // the gTranPk is the Value stored in grid
        // these two values will be same while doing Update Operation
        // but in case of Insert this will differ
        // gTranPk is being used to filter relevent rows to save and
        // PkValue is used as Foreign Key for transaction table in the Save Operation.

        #endregion " Logical Flow "

        private ArrayList SaveOthCharge(DataTable DT, long PkValue, long gTranPk, OracleCommand SCM, string UserName, long BaseCurrency)
        {
            Int16 nRowCnt = default(Int16);
            long TranPk = 0;
            arrMessage.Clear();
            Int16 delFlage = default(Int16);
            delFlage = 1;
            try
            {
                var _with37 = SCM;
                _with37.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= DT.Rows.Count - 1; nRowCnt++)
                {
                    if (Convert.ToInt32(DT.Rows[nRowCnt][1]) == gTranPk)
                    {
                        _with37.CommandText = UserName + ".QUOTATION_GEN_AIR_TBL_PKG.QUOTATION_TRN_AIR_OTH_CHRG_UPD";
                        var _with38 = _with37.Parameters;
                        _with38.Clear();
                        //QUOT_GEN_AIR_TRN_FK_IN()
                        //FREIGHT_ELEMENT_MST_FK_IN()
                        //CHARGE_BASIS_IN()
                        //APPROVED_RATE_IN()
                        //CURRENCY_MST_FK_IN()
                        //RETURN_VALUE()
                        _with38.Clear();
                        //DEL_FLAG()
                        _with38.Add("DEL_FLAG", delFlage);
                        //QUOTATION_TRN_AIR_FK_IN()
                        _with38.Add("QUOTATION_TRN_AIR_FK_IN", PkValue);
                        //FREIGHT_ELEMENT_MST_FK_IN()
                        _with38.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(DT.Rows[nRowCnt]["FRT_FK"]));
                        //CURRENCY_MST_FK_IN()
                        _with38.Add("CURRENCY_MST_FK_IN", DT.Rows[nRowCnt]["CURR_FK"]);
                        //CHARGE_BASIS_IN()
                        _with38.Add("CHARGE_BASIS_IN", DT.Rows[nRowCnt]["CH_BASIS"]);
                        //BASIS_RATE_IN()
                        _with38.Add("BASIS_RATE_IN", getDefault(DT.Rows[nRowCnt]["BASIS_RATE"], 0));
                        //AMOUNT_IN()
                        _with38.Add("AMOUNT_IN", getDefault(DT.Rows[nRowCnt]["TARIFF_RATE"], 0));
                        _with38.Add("FREIGHT_TYPE_IN", getDefault(DT.Rows[nRowCnt]["PYMT_TYPE"], 1));
                        //RETURN_VALUE()
                        _with38.Add("RETURN_VALUE", OracleDbType.Varchar2, 20, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        //.Add("QUOT_GEN_AIR_TRN_FK_IN", _
                        //        PkValue _
                        //        ).Direction = ParameterDirection.Input

                        //.Add("FREIGHT_ELEMENT_MST_FK_IN", _
                        //        CLng(DT.Rows(nRowCnt).Item("FREIGHT_ELEMENT_MST_FK")) _
                        //        ).Direction = ParameterDirection.Input

                        //.Add("CURRENCY_MST_FK_IN", _
                        //        DT.Rows(nRowCnt).Item("CURRENCY_MST_FK") _
                        //        ).Direction = ParameterDirection.Input

                        //.Add("APPROVED_RATE_IN", _
                        //        DT.Rows(nRowCnt).Item("APPROVED_RATE") _
                        //        ).Direction = ParameterDirection.Input

                        //.Add("CHARGE_BASIS_IN", _
                        //        DT.Rows(nRowCnt).Item("CHARGE_BASIS") _
                        //        ).Direction = ParameterDirection.Input

                        //.Add("RETURN_VALUE", _
                        //            OracleDbType.Int32, 10, _
                        //            "RETURN_VALUE").Direction = ParameterDirection.Output
                        _with37.ExecuteNonQuery();
                        //TranPk = CLng(.Parameters["RETURN_VALUE"].Value)
                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(arrMessage[0].ToString(), "saved")>0)
                            {
                                arrMessage.Add("All data saved successfully");
                            }
                            else
                            {
                                return arrMessage;
                            }
                        }
                    }
                    delFlage += 1;
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

        #endregion " Other Charge Save "

        #region " Surcharge Save Kg "

        #region " Logical Flow "

        // This is for IU in Surcharge Table
        // It is simply performing IU for corresponding transaction
        // the surcharge information is available in the same row where the Transaction is in the DataTable
        // so in this procedure we loop through the columns and saves the transactions accordingly.
        // The surcharge columns are dynamically built columns.

        #endregion " Logical Flow "

        public ArrayList SaveSurcharge(DataSet Gridds, Int16 nRowCnt, long PkValue, OracleCommand SCM, string UserName, long BaseCurrency)
        {
            Int16 nColCnt = default(Int16);
            long TranPk = 0;
            string chargeBasis = null;
            arrMessage.Clear();
            try
            {
                var _with39 = SCM;
                _with39.CommandType = CommandType.StoredProcedure;
                //For nColCnt = 20 To Gridds.Tables(0).Columns.Count - 3 Step 3
                for (nColCnt = 20; nColCnt <= Gridds.Tables[0].Columns.Count - 6; nColCnt += 3)
                {
                    _with39.CommandText = UserName + ".QUOTATION_GEN_AIR_TBL_PKG.QUOT_AIR_SURCHARGE_INS";
                    var _with40 = _with39.Parameters;
                    _with40.Clear();
                    //QUOT_GEN_AIR_TRN_FK_IN()
                    //FREIGHT_ELEMENT_MST_FK_IN()
                    //CHARGE_BASIS_IN()
                    //CURRENCY_MST_FK_IN()
                    //APPROVED_RATE_IN()
                    //RETURN_VALUE()
                    _with40.Add("QUOT_GEN_AIR_TRN_FK_IN", PkValue).Direction = ParameterDirection.Input;

                    _with40.Add("FREIGHT_ELEMENT_MST_FK_IN", Convert.ToInt64(Gridds.Tables[0].Columns[nColCnt].Caption)).Direction = ParameterDirection.Input;

                    _with40.Add("CURRENCY_MST_FK_IN", BaseCurrency).Direction = ParameterDirection.Input;

                    _with40.Add("APPROVED_RATE_IN", Gridds.Tables[0].Rows[nRowCnt][nColCnt + 1]).Direction = ParameterDirection.Input;

                    chargeBasis = Gridds.Tables[0].Columns[nColCnt + 2].Caption;
                    chargeBasis = chargeBasis.Substring(chargeBasis.Length - 1, 1);

                    _with40.Add("CHARGE_BASIS_IN", Convert.ToInt64(chargeBasis)).Direction = ParameterDirection.Input;

                    _with40.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with39.ExecuteNonQuery();
                    TranPk = Convert.ToInt64(_with39.Parameters["RETURN_VALUE"].Value);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }
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

        #endregion " Surcharge Save Kg "

        #region " Freight Transaction Save "

        #region " Logical Flow "

        //
        //
        //
        //

        #endregion " Logical Flow "

        public ArrayList SaveFrtTran(DataSet Gridds, Int16 RowNo, long PkValue, OracleCommand SCM, string UserName)
        {
            Int32 nRowCnt = default(Int32);
            long TranPk = 0;
            arrMessage.Clear();
            try
            {
                var _with41 = SCM;
                _with41.CommandType = CommandType.StoredProcedure;
                for (nRowCnt = 0; nRowCnt <= Gridds.Tables[1].Rows.Count - 1; nRowCnt++)
                {
                    //If ((Gridds.Tables(0).Rows(RowNo).Item("PORT_MST_POL_FK") = Gridds.Tables(1).Rows(nRowCnt).Item("POLPK")) And (Gridds.Tables(0).Rows(RowNo).Item("PORT_MST_POD_FK") = Gridds.Tables(1).Rows(nRowCnt).Item("PODPK"))) Then
                    if (Gridds.Tables[1].Rows[nRowCnt]["SELECTED"] == "true" | Gridds.Tables[1].Rows[nRowCnt]["SELECTED"] == "1")
                    {
                        _with41.CommandText = UserName + ".QUOTATION_GEN_AIR_TBL_PKG.QUOT_AIR_TRN_FREIGHT_TBL_INS";
                        var _with42 = _with41.Parameters;
                        _with42.Clear();
                        //QUOT_GEN_AIR_TRN_FK_IN()
                        _with42.Add("QUOT_GEN_AIR_TRN_FK_IN", PkValue).Direction = ParameterDirection.Input;
                        // PkValue Can also be provided
                        //FREIGHT_ELEMENT_MST_FK_IN()
                        _with42.Add("FREIGHT_ELEMENT_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["FREIGHT_ELEMENT_MST_FK"]).Direction = ParameterDirection.Input;
                        //CURRENCY_MST_FK_IN()
                        _with42.Add("CURRENCY_MST_FK_IN", Gridds.Tables[1].Rows[nRowCnt]["CURRENCY_MST_FK"]).Direction = ParameterDirection.Input;
                        //MIN_AMOUNT_IN()
                        //.Add("MIN_AMOUNT_IN", _
                        //        Gridds.Tables(1).Rows(nRowCnt).Item("MIN_AMOUNT") _
                        //        ).Direction = ParameterDirection.Input
                        _with42.Add("MIN_AMOUNT_IN", (string.IsNullOrEmpty(Gridds.Tables[1].Rows[nRowCnt]["MIN_AMOUNT"].ToString()) ? 0 : Gridds.Tables[1].Rows[nRowCnt]["MIN_AMOUNT"])).Direction = ParameterDirection.Input;
                        //'Added Koteshwari for Advatos  PTSID-JUN18
                        _with42.Add("CHECK_FOR_ALL_IN_RT_IN", (Gridds.Tables[1].Rows[nRowCnt]["SELECTED"] == "true" ? 1 : 0)).Direction = ParameterDirection.Input;
                        _with42.Add("CHECK_ADVATOS_IN", (Gridds.Tables[1].Rows[nRowCnt]["ADVATOS"] == "true" ? 1 : 0)).Direction = ParameterDirection.Input;
                        //'End
                        //RETURN_VALUE()
                        _with42.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        _with41.ExecuteNonQuery();

                        TranPk = Convert.ToInt64(_with41.Parameters["RETURN_VALUE"].Value);
                        arrMessage = SaveSlabs(Gridds, Convert.ToInt16(nRowCnt), TranPk, SCM, UserName);
                        if (arrMessage.Count > 0)
                        {
                            if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                            {
                                arrMessage.Add("All data saved successfully");
                            }
                            else
                            {
                                return arrMessage;
                            }
                        }
                    }
                    //End If
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

        #endregion " Freight Transaction Save "

        #region " Slabs Save "

        #region " Logical Flow "

        //
        //

        #endregion " Logical Flow "

        public ArrayList SaveSlabs(DataSet Gridds, Int16 nRowCnt, long PkValue, OracleCommand SCM, string UserName)
        {
            Int32 nColCnt = default(Int32);
            long TranPk = 0;
            arrMessage.Clear();
            try
            {
                var _with43 = SCM;
                _with43.CommandType = CommandType.StoredProcedure;
                //For nColCnt = 9 To Gridds.Tables(1).Columns.Count - 3 Step 3
                for (nColCnt = 11; nColCnt <= Gridds.Tables[1].Columns.Count - 3; nColCnt += 3)
                {
                    _with43.CommandText = UserName + ".QUOTATION_GEN_AIR_TBL_PKG.QUOTE_AIR_BREAKPOINTS_INS";
                    var _with44 = _with43.Parameters;
                    _with44.Clear();
                    //QUOT_GEN_AIR_FRT_FK_IN()
                    //AIRFREIGHT_SLABS_TBL_FK_IN()
                    //APPROVED_RATE_IN()
                    //RETURN_VALUE()
                    _with44.Add("QUOT_GEN_AIR_FRT_FK_IN", PkValue).Direction = ParameterDirection.Input;

                    _with44.Add("AIRFREIGHT_SLABS_TBL_FK_IN", Convert.ToInt64(Gridds.Tables[1].Columns[nColCnt].Caption)).Direction = ParameterDirection.Input;

                    _with44.Add("APPROVED_RATE_IN", Gridds.Tables[1].Rows[nRowCnt][nColCnt + 1]).Direction = ParameterDirection.Input;

                    _with44.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with43.ExecuteNonQuery();
                    TranPk = Convert.ToInt64(_with43.Parameters["RETURN_VALUE"].Value);
                    if (arrMessage.Count > 0)
                    {
                        if (string.Compare(arrMessage[0].ToString(), "saved") > 0)
                        {
                            arrMessage.Add("All data saved successfully");
                        }
                        else
                        {
                            return arrMessage;
                        }
                    }
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

        #endregion " Slabs Save "

        #endregion "Save General"

        #endregion "Fetch UWG3 grid genral type "

        #region "Fetch Barcode Manager Pk"

        public int FetchBarCodeManagerPk(string Configid)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;

            WorkFlow objWF = new WorkFlow();

            StrSql = "select bdmt.bcd_mst_pk,bdmt.field_name from  barcode_data_mst_tbl bdmt" + "where bdmt.config_id_fk= '" + Configid + "'";
            DsBarManager = objWF.GetDataSet(StrSql);
            if (DsBarManager.Tables[0].Rows.Count > 0)
            {
                var _with45 = DsBarManager.Tables[0].Rows[0];
                strReturn =  Convert.ToInt32(removeDBNull(_with45["bcd_mst_pk"].ToString()));
            }
            return strReturn;
        }

        #endregion "Fetch Barcode Manager Pk"

        #region "Fetch Barcode Type"

        public DataSet FetchBarCodeField(int BarCodeManagerPk)
        {
            string StrSql = null;
            DataSet DsBarManager = null;
            int strReturn = 0;
            WorkFlow objWF = new WorkFlow();
            StringBuilder strQuery = new StringBuilder();

            strQuery.Append("select bdmt.bcd_mst_pk, bdmt.field_name, bdmt.default_value");
            strQuery.Append("  from barcode_data_mst_tbl bdmt, barcode_doc_data_tbl bddt");
            strQuery.Append(" where bdmt.bcd_mst_pk = bddt.bcd_mst_fk");
            strQuery.Append("   and bdmt.BCD_MST_FK= " + BarCodeManagerPk);
            strQuery.Append(" ORDER BY default_value desc");

            DsBarManager = objWF.GetDataSet(strQuery.ToString());
            return DsBarManager;
        }

        #endregion "Fetch Barcode Type"

        #region "Fetch Header "

        public DataSet Fetch_Main_Header(string QuotPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strquery = new StringBuilder();

            strquery.Append("SELECT          ");
            strquery.Append("  AIR.AIRLINE_NAME AIRLINE,");
            strquery.Append("  COMM_GRP.COMMODITY_GROUP_CODE COMMODITY_G,");
            strquery.Append("  QUOTT.VALID_FROM VALID_FROM,");
            strquery.Append("  QUOTT.VALID_TO VALID_TO,");
            strquery.Append("  POL_P.PORT_NAME POL,");
            strquery.Append("  POD_P.PORT_NAME POD,");
            strquery.Append("  QUOTT.QUOT_GEN_AIR_TRN_PK QUOT_TRN_PK,");
            strquery.Append("  QUOT.HEADER_CONTENT HEADER,");
            strquery.Append("  QUOT.FOOTER_CONTENT FOOTER");
            strquery.Append(" FROM          ");
            strquery.Append("  QUOTATION_AIR_TBL QUOT,   ");
            strquery.Append("  QUOT_GEN_TRN_AIR_TBL QUOTT,            ");
            strquery.Append("  AIRLINE_MST_TBL AIR,      ");
            strquery.Append("  COMMODITY_GROUP_MST_TBL COMM_GRP,");
            strquery.Append("  PORT_MST_TBL POL_P,");
            strquery.Append("  PORT_MST_TBL POD_P");
            strquery.Append(" WHERE          ");
            strquery.Append("  QUOT.QUOTATION_AIR_PK = QUOTT.QUOT_GEN_AIR_FK");
            strquery.Append("  AND QUOTT.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)");
            strquery.Append("  AND QUOT.COMMODITY_GROUP_MST_FK = COMM_GRP.COMMODITY_GROUP_PK");
            strquery.Append("  AND QUOTT.PORT_MST_POL_FK = POL_P.PORT_MST_PK");
            strquery.Append("  AND QUOTT.PORT_MST_POD_FK = POD_P.PORT_MST_PK");
            strquery.Append("  and QUOT.QUOTATION_AIR_PK =" + QuotPk);
            return ObjWk.GetDataSet(strquery.ToString());
        }

        #endregion "Fetch Header "

        #region "Fetch Freight Elements "

        public DataSet FreightElement(string QuotPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strquery = new StringBuilder();

            strquery.Append("SELECT          ");
            strquery.Append("  QUOTT.QUOT_GEN_AIR_TRN_PK QUOT_TRN_PK,");
            strquery.Append("  FRT.FREIGHT_ELEMENT_NAME FRT_NAME,");
            strquery.Append("  CURR.CURRENCY_ID CURR_NAME,");
            strquery.Append("   decode(TRAN.CHECK_FOR_ALL_IN_RT,1,'true','false')  SELECTED,    ");
            strquery.Append("   decode(TRAN.CHECK_ADVATOS,1,'true','false')  ADVATOS,");
            strquery.Append("  TRAN.MIN_AMOUNT MIN_AMT,");
            strquery.Append("  ASLAB.BREAKPOINT_ID,");
            strquery.Append("  BREAK.APPROVED_RATE B_RATE,");
            strquery.Append("  ASLAB.SEQUENCE_NO");
            strquery.Append(" FROM          ");
            strquery.Append("  QUOTATION_AIR_TBL QUOT,   ");
            strquery.Append("  QUOT_GEN_TRN_AIR_TBL QUOTT,            ");
            strquery.Append("  QUOT_AIR_TRN_FREIGHT_TBL   TRAN,   ");
            strquery.Append("  QUOTE_AIR_BREAKPOINTS BREAK,");
            strquery.Append("  FREIGHT_ELEMENT_MST_TBL FRT,      ");
            strquery.Append("  CURRENCY_TYPE_MST_TBL CURR,");
            strquery.Append("  AIRFREIGHT_SLABS_TBL ASLAB");
            strquery.Append(" WHERE          ");
            strquery.Append("  QUOT.QUOTATION_AIR_PK = QUOTT.QUOT_GEN_AIR_FK");
            strquery.Append("  AND QUOTT.QUOT_GEN_AIR_TRN_PK = TRAN.QUOT_GEN_AIR_TRN_FK");
            strquery.Append("  AND tran.quot_gen_trn_freight_pk = break.quot_gen_air_frt_fk");
            strquery.Append("  AND  TRAN.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK");
            strquery.Append("  AND  TRAN.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
            strquery.Append("  AND BREAK.AIRFREIGHT_SLABS_TBL_FK = ASLAB.AIRFREIGHT_SLABS_TBL_PK");
            strquery.Append("  and break.approved_rate>0");
            strquery.Append("  and QUOT.QUOTATION_AIR_PK = " + QuotPk);
            strquery.Append("   ORDER BY ASLAB.SEQUENCE_NO ");
            return ObjWk.GetDataSet(strquery.ToString());
        }

        #endregion "Fetch Freight Elements "

        #region "Fetch Vatos Freight Elements "

        public DataSet VatosFreightElement(string QuotPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strquery = new StringBuilder();

            strquery.Append("SELECT          ");
            strquery.Append("  QUOTT.QUOT_GEN_AIR_TRN_PK QUOT_TRN_PK,");
            strquery.Append("  FRT.FREIGHT_ELEMENT_NAME FRT_NAME,");
            strquery.Append("  CURR.CURRENCY_ID CURR_NAME,");
            strquery.Append("   decode(TRAN.CHECK_FOR_ALL_IN_RT,1,'true','false')  SELECTED,    ");
            strquery.Append("   decode(TRAN.CHECK_ADVATOS,1,'true','false')  ADVATOS,");
            strquery.Append("  TRAN.MIN_AMOUNT MIN_AMT,");
            strquery.Append("  ASLAB.BREAKPOINT_ID,");
            strquery.Append("  BREAK.APPROVED_RATE B_RATE");
            strquery.Append(" FROM          ");
            strquery.Append("  QUOTATION_AIR_TBL QUOT,   ");
            strquery.Append("  QUOT_GEN_TRN_AIR_TBL QUOTT,            ");
            strquery.Append("  QUOT_AIR_TRN_FREIGHT_TBL   TRAN,   ");
            strquery.Append("  QUOTE_AIR_BREAKPOINTS BREAK,");
            strquery.Append("  FREIGHT_ELEMENT_MST_TBL FRT,      ");
            strquery.Append("  CURRENCY_TYPE_MST_TBL CURR,");
            strquery.Append("  AIRFREIGHT_SLABS_TBL ASLAB");
            strquery.Append(" WHERE          ");
            strquery.Append("  QUOT.QUOTATION_AIR_PK = QUOTT.QUOT_GEN_AIR_FK");
            strquery.Append("  AND QUOTT.QUOT_GEN_AIR_TRN_PK = TRAN.QUOT_GEN_AIR_TRN_FK");
            strquery.Append("  AND tran.quot_gen_trn_freight_pk = break.quot_gen_air_frt_fk");
            strquery.Append("  AND  TRAN.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK");
            strquery.Append("  AND  TRAN.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
            strquery.Append("  AND BREAK.AIRFREIGHT_SLABS_TBL_FK = ASLAB.AIRFREIGHT_SLABS_TBL_PK");
            strquery.Append("  and break.approved_rate>0");
            strquery.Append("  AND TRAN.CHECK_ADVATOS = 1");
            strquery.Append("  and QUOT.QUOTATION_AIR_PK = " + QuotPk);
            strquery.Append("  order by APPROVED_RATE asc ");
            return ObjWk.GetDataSet(strquery.ToString());
        }

        #endregion "Fetch Vatos Freight Elements "

        #region "Fetch Surcharges"

        public DataSet Surcharges(string QuotPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strquery = new StringBuilder();

            strquery.Append("SELECT          ");
            strquery.Append("  QUOTT.QUOT_GEN_AIR_TRN_PK QUOT_TRN_PK,");
            strquery.Append("  FRT.FREIGHT_ELEMENT_NAME FRT_NAME,");
            strquery.Append("  CURR.CURRENCY_ID CURR_NAME,");
            strquery.Append("  SURCHARG.APPROVED_RATE CHARGE,");
            strquery.Append("  decode(SURCHARG.CHARGE_BASIS, 1, '%', 2, 'Flat', 'KGs') CH_BASIS_ID ");
            strquery.Append(" FROM          ");
            strquery.Append("  QUOTATION_AIR_TBL QUOT,   ");
            strquery.Append("  QUOT_GEN_TRN_AIR_TBL QUOTT,     ");
            strquery.Append("  QUOT_AIR_SURCHARGE SURCHARG,");
            strquery.Append("  FREIGHT_ELEMENT_MST_TBL FRT,");
            strquery.Append("  CURRENCY_TYPE_MST_TBL CURR      ");
            strquery.Append(" WHERE          ");
            strquery.Append("  QUOT.QUOTATION_AIR_PK = QUOTT.QUOT_GEN_AIR_FK");
            strquery.Append("  AND QUOTT.QUOT_GEN_AIR_TRN_PK = SURCHARG.QUOT_GEN_AIR_TRN_FK");
            strquery.Append("  AND SURCHARG.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK  ");
            strquery.Append("  AND SURCHARG.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
            strquery.Append("  AND SURCHARG.APPROVED_RATE>0");
            strquery.Append("  and QUOT.QUOTATION_AIR_PK =" + QuotPk);

            return ObjWk.GetDataSet(strquery.ToString());
        }

        public DataSet GenOtherCharges(string QuotPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strquery = new StringBuilder();

            strquery.Append("Select QUOT.QUOTATION_AIR_PK ,");
            strquery.Append("  QTR.QUOT_GEN_AIR_TRN_PK FK,");
            //by thiyagarajan
            strquery.Append("  FRT.FREIGHT_ELEMENT_NAME FRT_NAME,");
            strquery.Append("  CURR.CURRENCY_ID CURR_NAME,");
            strquery.Append("  otf.BASIS_RATE BASIS_RATE,");
            strquery.Append("  otf.AMOUNT TARIFF_RATE,");
            strquery.Append(" otf.CHARGE_BASIS CH_BASIS,");
            strquery.Append("  decode(otf.CHARGE_BASIS, 1, '%', 2, 'Flat', 'KGs') CH_BASIS_ID ");
            strquery.Append("  from QUOT_AIR_OTH_CHRG otf,");
            strquery.Append("  FREIGHT_ELEMENT_MST_TBL FRT,");
            strquery.Append("  CURRENCY_TYPE_MST_TBL CURR,");
            strquery.Append("  QUOTATION_AIR_TBL QUOT,");
            strquery.Append(" QUOT_GEN_TRN_AIR_TBL QTR  ");
            strquery.Append("  where OTF.QUOTATION_TRN_AIR_FK = QTR.QUOT_GEN_AIR_TRN_PK");
            strquery.Append("  AND OTF.FREIGHT_ELEMENT_MST_FK=FRT.FREIGHT_ELEMENT_MST_PK(+)");
            strquery.Append("  AND OTF.CURRENCY_MST_FK= CURR.CURRENCY_MST_PK(+) ");
            strquery.Append("  AND QTR.QUOT_GEN_AIR_FK = QUOT.QUOTATION_AIR_PK");
            strquery.Append("  and QUOT.QUOTATION_AIR_PK =" + QuotPk);

            return ObjWk.GetDataSet(strquery.ToString());
        }

        #endregion "Fetch Surcharges"

        #region "Fetch Other Charges"

        public DataSet OtherCharges(string QuotPk)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder strquery = new StringBuilder();

            strquery.Append("SELECT          ");
            strquery.Append("  QUOTT.QUOT_GEN_AIR_TRN_PK QUOT_TRN_PK,");
            strquery.Append("  FRT.FREIGHT_ELEMENT_NAME FRT_NAME,");
            strquery.Append("  CURR.CURRENCY_NAME CURR_NAME,");
            strquery.Append("  OTH.AMOUNT CHARGE,");
            strquery.Append("  OTH.CHARGE_BASIS BASIS");
            strquery.Append(" FROM          ");
            strquery.Append("  QUOTATION_AIR_TBL QUOT,   ");
            strquery.Append("  QUOT_GEN_TRN_AIR_TBL QUOTT,     ");
            strquery.Append("  QUOT_AIR_OTH_CHRG OTH,");
            strquery.Append("  FREIGHT_ELEMENT_MST_TBL FRT,");
            strquery.Append("  CURRENCY_TYPE_MST_TBL CURR      ");
            strquery.Append(" WHERE          ");
            strquery.Append("  QUOT.QUOTATION_AIR_PK = QUOTT.QUOT_GEN_AIR_FK");
            strquery.Append("  AND QUOTT.QUOT_GEN_AIR_TRN_PK = OTH.QUOTATION_TRN_AIR_FK");
            strquery.Append("  AND OTH.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK  ");
            strquery.Append("  AND OTH.CURRENCY_MST_FK = CURR.CURRENCY_MST_PK");
            strquery.Append("  AND OTH.AMOUNT>0");
            strquery.Append("  and QUOT.QUOTATION_AIR_PK = " + QuotPk);

            return ObjWk.GetDataSet(strquery.ToString());
        }

        #endregion "Fetch Other Charges"

        #region "Fetch Location"

        public DataSet FetchLocation(long Loc)
        {
            WorkFlow ObjWk = new WorkFlow();
            StringBuilder StrSqlBuilder = new StringBuilder();
            StrSqlBuilder.Append("  SELECT L.Office_Name CORPORATE_NAME,");
            StrSqlBuilder.Append("  COP.GST_NO,COP.COMPANY_REG_NO,COP.HOME_PAGE URL, ");
            StrSqlBuilder.Append("  L.LOCATION_ID , L.LOCATION_NAME, ");
            StrSqlBuilder.Append("  L.ADDRESS_LINE1,L.ADDRESS_LINE2,L.ADDRESS_LINE3,L.TELE_PHONE_NO,L.FAX_NO,L.E_MAIL_ID,");
            StrSqlBuilder.Append("  L.CITY,CMST.COUNTRY_NAME COUNTRY,L.ZIP, L.LOCATION_MST_PK");
            StrSqlBuilder.Append("  FROM CORPORATE_MST_TBL COP,LOCATION_MST_TBL L,COUNTRY_MST_TBL CMST");
            StrSqlBuilder.Append("  WHERE CMST.COUNTRY_MST_PK(+)=L.COUNTRY_MST_FK AND L.LOCATION_MST_PK = " + Loc + "");

            try
            {
                return ObjWk.GetDataSet(StrSqlBuilder.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Location"

        #region "Fetch Custumer "

        public DataSet Fetch_Custumer(int CustPk, string CustId = "")
        {
            StringBuilder strQuery = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("  ");
                strQuery.Append("         SELECT Q.CUSTOMER_NAME,");
                strQuery.Append("         Q.ADM_ADDRESS_1,");
                strQuery.Append("         case when Q.ADM_ADDRESS_2 is null then ' ' else Q.ADM_ADDRESS_2 end ADM_ADDRESS_2,");
                strQuery.Append("         case when Q.ADM_ADDRESS_3 is null then ' ' else Q.ADM_ADDRESS_3 end ADM_ADDRESS_3,");
                strQuery.Append("         case when Q.ADM_ZIP_CODE is null then ' ' else Q.ADM_ZIP_CODE end ADM_ZIP_CODE,");
                strQuery.Append("         case when Q.ADM_CITY is null then ' ' else Q.ADM_CITY end ADM_CITY,");
                strQuery.Append("         case when Q.COUNTRY_NAME is null then ' ' else Q.COUNTRY_NAME end COUNTRY_NAME");
                strQuery.Append("         FROM  (");
                strQuery.Append("  SELECT CUST.CUSTOMER_NAME,");
                strQuery.Append("         CC.ADM_ADDRESS_1,");
                strQuery.Append("         CC.ADM_ADDRESS_2,");
                strQuery.Append("         CC.ADM_ADDRESS_3,");
                strQuery.Append("         CC.ADM_ZIP_CODE,");
                strQuery.Append("         CC.ADM_CITY,");
                strQuery.Append("         CCC.COUNTRY_NAME");
                strQuery.Append("    FROM CUSTOMER_MST_TBL      CUST,");
                strQuery.Append("         CUSTOMER_CONTACT_DTLS CC,");
                strQuery.Append("         COUNTRY_MST_TBL       CCC");
                strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                strQuery.Append("     AND CC.ADM_COUNTRY_MST_FK = CCC.COUNTRY_MST_PK");
                strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
                if (CustId.Trim().Length > 0)
                    strQuery.Append("     AND CUST.CUSTOMER_NAME = '" + CustId + "'");
                strQuery.Append("");
                strQuery.Append(" UNION ");
                strQuery.Append("");
                strQuery.Append("  SELECT CUST.CUSTOMER_NAME,");
                strQuery.Append("         CC.ADM_ADDRESS_1,");
                strQuery.Append("         CC.ADM_ADDRESS_2,");
                strQuery.Append("         CC.ADM_ADDRESS_3,");
                strQuery.Append("         CC.ADM_ZIP_CODE,");
                strQuery.Append("         CC.ADM_CITY,");
                strQuery.Append("         (select ccc.country_name from country_mst_tbl ccc, location_mst_tbl l ");
                strQuery.Append("         where l.country_mst_fk = ccc.country_mst_pk ");
                strQuery.Append("         and l.location_mst_pk = cc.adm_location_mst_fk)  country_name");
                strQuery.Append("    FROM TEMP_CUSTOMER_TBL      CUST,");
                strQuery.Append("         TEMP_CUSTOMER_CONTACT_DTLS CC");
                strQuery.Append("   WHERE CC.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK");
                strQuery.Append("     AND CUST.CUSTOMER_MST_PK =" + CustPk);
                if (CustId.Trim().Length > 0)
                    strQuery.Append("     AND CUST.CUSTOMER_NAME = '" + CustId + "'");
                strQuery.Append("     )Q");

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Custumer "

        #region "Fetch Header"

        public DataSet Fetch_Specific_header(string QuotPk)
        {
            StringBuilder strQuery = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                strQuery.Append("SELECT MAIN.QUOTATION_AIR_PK,");
                strQuery.Append("       TRAN.QUOTE_TRN_AIR_PK,");
                strQuery.Append("       PORTPOL.PORT_NAME POL_ID,");
                strQuery.Append("       PORTPOD.PORT_NAME POD_ID,");
                strQuery.Append("       AIR.AIRLINE_NAME AIR_ID,");
                strQuery.Append("       CMDTGRP.COMMODITY_GROUP_CODE COMM_ID,");
                strQuery.Append("       DECODE(SLB.BREAKPOINT_TYPE, 1, 'BP', 'ULD') SLAB_TYPE,");
                strQuery.Append("   MAIN.HEADER_CONTENT  HEADER,");
                strQuery.Append("   MAIN.FOOTER_CONTENT  FOOTER,");
                strQuery.Append("       TRAN.CHARGEABLE_WEIGHT CH_WT,");
                strQuery.Append("       DECODE(MAIN.PYMT_TYPE, 1, 'PREPAID', 'COLLECT') P_TYPE_ID");
                strQuery.Append("  FROM QUOTATION_AIR_TBL    MAIN,");
                strQuery.Append("       QUOTATION_TRN_AIR    TRAN,");
                strQuery.Append("       AIRFREIGHT_SLABS_TBL SLB,");
                strQuery.Append("       PORT_MST_TBL         PORTPOL,");
                strQuery.Append("       PORT_MST_TBL         PORTPOD,");
                strQuery.Append("       AIRLINE_MST_TBL      AIR,");
                strQuery.Append("       COMMODITY_MST_TBL    CMDT,");
                strQuery.Append("       COMMODITY_GROUP_MST_TBL CMDTGRP");
                strQuery.Append(" WHERE MAIN.QUOTATION_AIR_PK = TRAN.QUOTATION_AIR_FK");
                strQuery.Append("   AND TRAN.SLAB_FK = SLB.AIRFREIGHT_SLABS_TBL_PK(+)");
                strQuery.Append("   AND TRAN.PORT_MST_POL_FK = PORTPOL.PORT_MST_PK");
                strQuery.Append("   AND TRAN.PORT_MST_POD_FK = PORTPOD.PORT_MST_PK");
                strQuery.Append("   AND TRAN.COMMODITY_MST_FK = CMDT.COMMODITY_MST_PK(+)");
                strQuery.Append("   AND TRAN.AIRLINE_MST_FK = AIR.AIRLINE_MST_PK(+)");
                strQuery.Append("   AND  MAIN.COMMODITY_GROUP_MST_FK=CMDTGRP.COMMODITY_GROUP_PK(+)");
                strQuery.Append("   AND MAIN.QUOTATION_AIR_PK = " + QuotPk);

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Header"

        #region "Fetch Frieght Elements "

        public DataSet Fetch_Freight_Specific(string QuotPk, int BaseCurPk, string QuotationDate)
        {
            StringBuilder strQuery = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            try
            {
                strQuery.Append("  SELECT qta.quotation_air_fk PK,");
                strQuery.Append("         Qta.quote_trn_air_pk  fk,");
                strQuery.Append("         FRT.FREIGHT_ELEMENT_ID FRT_ID,");
                strQuery.Append("         CUR.CURRENCY_ID CURR_ID,");
                strQuery.Append("         TRF.BASIS_RATE BASIS_RATE,");
                strQuery.Append("         TRF.TARIFF_RATE TARIFF_RATE,");
                strQuery.Append("         ROUND((SELECT GET_EX_RATE( TRF.CURRENCY_MST_FK, " + BaseCurPk + ",TO_DATE('" + QuotationDate + "',DATEFORMAT )) FROM DUAL),6) ROE,");
                strQuery.Append("         ROUND(TRF.QUOTED_RATE * (SELECT GET_EX_RATE(TRF.CURRENCY_MST_FK, " + BaseCurPk + ",TO_DATE('" + QuotationDate + "', DATEFORMAT))FROM DUAL),0) QUOTED_RATE,");
                strQuery.Append("         DECODE(TRF.CHARGE_BASIS, 1, '%', 2, 'FLAT', 'KGS') CH_BASIS_ID");
                strQuery.Append("    FROM QUOTATION_AIR_TBL          QAT,");
                strQuery.Append("         QUOTATION_TRN_AIR          QTA    ,");
                strQuery.Append("         QUOTATION_TRN_AIR_FRT_DTLS TRF,");
                strQuery.Append("         FREIGHT_ELEMENT_MST_TBL    FRT,");
                strQuery.Append("         CURRENCY_TYPE_MST_TBL      CUR");
                strQuery.Append("   WHERE QTA.QUOTATION_AIR_FK = QAT.QUOTATION_AIR_PK");
                strQuery.Append("     AND TRF.QUOTE_TRN_AIR_FK = QTA.QUOTE_TRN_AIR_PK");
                strQuery.Append("     AND TRF.FREIGHT_ELEMENT_MST_FK = FRT.FREIGHT_ELEMENT_MST_PK(+)");
                strQuery.Append("     AND TRF.CURRENCY_MST_FK = CUR.CURRENCY_MST_PK(+)");
                strQuery.Append("     AND QAT.QUOTATION_AIR_PK = " + QuotPk);

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Frieght Elements "

        #region "Cargo Details "

        public DataSet Fetch_Cargo_Details(string QuotPk)
        {
            StringBuilder strQuery = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //strQuery.Append("" & vbCrLf)
                //strQuery.Append(" SELECT                                                    " & vbCrLf)
                //strQuery.Append("        CARGO_LENGTH                       LENGTH,             " & vbCrLf)
                //strQuery.Append("        CARGO_WIDTH                        WIDTH,              " & vbCrLf)
                //strQuery.Append("        CARGO_HEIGHT                       HEIGHT,             " & vbCrLf)
                //strQuery.Append("        CARGO_CUBE                         CUBE,               " & vbCrLf)
                //strQuery.Append("        CARGO_VOLUME_WT                    VOLWEIGHT,          " & vbCrLf)
                //strQuery.Append("        CARGO_ACTUAL_WT                    ACTWEIGHT,          " & vbCrLf)
                //strQuery.Append("        CARGO_DENSITY                      DENSITY,            " & vbCrLf)
                //strQuery.Append("        QUOT.QUOTATION_AIR_PK               FK                  " & vbCrLf)
                //strQuery.Append("     FROM                                                      " & vbCrLf)
                //strQuery.Append("        QUOTATION_AIR_CARGO_CALC QACC,     " & vbCrLf)
                //strQuery.Append("        QUOTATION_AIR_TBL QUOT, " & vbCrLf)
                //strQuery.Append("        QUOTATION_TRN_AIR QTR                          " & vbCrLf)
                //strQuery.Append("     WHERE  QTR.QUOTATION_AIR_FK=QUOT.QUOTATION_AIR_PK       " & vbCrLf)
                //strQuery.Append("     AND QACC.QUOTATION_TRN_AIR_FK=QTR.QUOTE_TRN_AIR_PK" & vbCrLf)
                //strQuery.Append("     AND QUOT.QUOTATION_AIR_PK=" & QuotPk)
                //'  strQuery.Append("     AND QAT.QUOTATION_AIR_PK = " & QuotPk)
                //strQuery.Append("" & vbCrLf)

                strQuery.Append("");
                strQuery.Append(" SELECT       ");
                strQuery.Append("        Qtr.Quote_Trn_Air_Pk                    PK,                                               ");
                strQuery.Append("        QACC.CARGO_MEASUREMENT                 DIMENSION,");
                strQuery.Append("        QACC.CARGO_LENGTH                       LENGTH,             ");
                strQuery.Append("        QACC.CARGO_WIDTH                        WIDTH,              ");
                strQuery.Append("        QACC.CARGO_HEIGHT                       HEIGHT,             ");
                strQuery.Append("        QACC.CARGO_CUBE                         CUBE,               ");
                strQuery.Append("        QACC.CARGO_VOLUME_WT                    VOLWEIGHT,          ");
                strQuery.Append("        QACC.CARGO_ACTUAL_WT                    ACTWEIGHT,          ");
                strQuery.Append("        QACC.CARGO_NOP                          NOOFPIECES ");
                strQuery.Append("   FROM                                                      ");
                strQuery.Append("        QUOTATION_AIR_CARGO_CALC QACC,     ");
                strQuery.Append("        QUOTATION_AIR_TBL QUOT, ");
                strQuery.Append("        QUOTATION_TRN_AIR QTR                          ");
                strQuery.Append("     WHERE  QTR.QUOTATION_AIR_FK=QUOT.QUOTATION_AIR_PK       ");
                strQuery.Append("     AND QACC.QUOTATION_TRN_AIR_FK=QTR.QUOTE_TRN_AIR_PK");
                strQuery.Append("     AND QUOT.QUOTATION_AIR_PK=" + QuotPk);
                strQuery.Append("        ");

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Cargo Details "

        #region "Fetch Other Charges"

        public DataSet Fetch_Other_Charges_Specific(string QuotPk)
        {
            StringBuilder strQuery = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            try
            {
                //strQuery.Append("" & vbCrLf)
                //strQuery.Append("  SELECT OTF.QUOTE_TRN_AIR_OTH_PK PK," & vbCrLf)
                //strQuery.Append("          QUOT.QUOTATION_AIR_PK  FK," & vbCrLf)
                //strQuery.Append("         OTF.FREIGHT_ELEMENT_MST_FK FRT_FK," & vbCrLf)
                //strQuery.Append("         OTF.CURRENCY_MST_FK CURR_FK," & vbCrLf)
                //strQuery.Append("         OTF.BASIS_RATE BASIS_RATE," & vbCrLf)
                //strQuery.Append("         OTF.AMOUNT TARIFF_RATE," & vbCrLf)
                //strQuery.Append("         OTF.CHARGE_BASIS CH_BASIS," & vbCrLf)
                //strQuery.Append("         DECODE(OTF.CHARGE_BASIS, 1, '%', 2, 'FLAT', 'KGS') CH_BASIS_ID," & vbCrLf)
                //strQuery.Append("         FREIGHT_TYPE PYMT_TYPE" & vbCrLf)
                //strQuery.Append("    FROM QUOTATION_TRN_AIR_OTH_CHRG OTF," & vbCrLf)
                //strQuery.Append("         QUOTATION_AIR_TBL          QUOT," & vbCrLf)
                //strQuery.Append("         QUOTATION_TRN_AIR          QTR" & vbCrLf)
                //strQuery.Append("   WHERE QTR.QUOTATION_AIR_FK = QUOT.QUOTATION_AIR_PK" & vbCrLf)
                //strQuery.Append("     AND OTF.QUOTATION_TRN_AIR_FK = QTR.QUOTE_TRN_AIR_PK" & vbCrLf)
                //strQuery.Append("     AND QUOT.QUOTATION_AIR_PK = " & QuotPk)
                strQuery.Append("SELECT QUOT.QUOTATION_AIR_PK ,");
                //strQuery.Append("         OTF.QUOTATION_TRN_AIR_FK FG," & vbCrLf) ' by thiyagarajan
                strQuery.Append("         QTR.QUOTE_TRN_AIR_PK FG,");
                strQuery.Append("         FMT.FREIGHT_ELEMENT_NAME FRT_FK,");
                strQuery.Append("         CTMT.CURRENCY_ID CURR_FK,");
                strQuery.Append("         OTF.BASIS_RATE BASIS_RATE,");
                strQuery.Append("          QTR.Chargeable_Weight,");
                strQuery.Append("         OTF.AMOUNT TARIFF_RATE,");
                strQuery.Append("         DECODE(OTF.CHARGE_BASIS, 1, '%', 2, 'FLAT', 'KGS') CH_BASIS_ID,");
                strQuery.Append("          (BASIS_RATE  * QTR.Chargeable_Weight)RATE");
                strQuery.Append("   FROM  QUOTATION_TRN_AIR_OTH_CHRG OTF,");
                strQuery.Append("         QUOTATION_AIR_TBL          QUOT,");

                strQuery.Append("         QUOTATION_TRN_AIR          QTR,");
                strQuery.Append("         FREIGHT_ELEMENT_MST_TBL   FMT,");
                strQuery.Append("         CURRENCY_TYPE_MST_TBL   CTMT");
                strQuery.Append("   WHERE QTR.QUOTATION_AIR_FK = QUOT.QUOTATION_AIR_PK");
                strQuery.Append("     AND OTF.QUOTATION_TRN_AIR_FK = QTR.QUOTE_TRN_AIR_PK");
                strQuery.Append("     AND OTF.FREIGHT_ELEMENT_MST_FK=FMT.FREIGHT_ELEMENT_MST_PK(+)");
                strQuery.Append("     AND OTF.CURRENCY_MST_FK= CTMT.CURRENCY_MST_PK(+)");
                strQuery.Append("     AND QUOT.QUOTATION_AIR_PK = " + QuotPk);

                //strQuery.Append("SELECT MAIN1.QUOTATION_SEA_PK," & vbCrLf)
                //strQuery.Append("       fcl_lcl.quote_trn_sea_pk," & vbCrLf)
                //strQuery.Append("       FRT3.FREIGHT_ELEMENT_NAME," & vbCrLf)
                //strQuery.Append("       CURR3.CURRENCY_ID," & vbCrLf)
                //strQuery.Append("       QUOT_OTHER.AMOUNT" & vbCrLf)
                //strQuery.Append("  FROM QUOTATION_SEA_TBL          MAIN1," & vbCrLf)
                //strQuery.Append("       QUOTATION_TRN_SEA_FCL_LCL  FCL_LCL," & vbCrLf)
                //strQuery.Append("       QUOTATION_TRN_SEA_OTH_CHRG QUOT_OTHER," & vbCrLf)
                //strQuery.Append("       FREIGHT_ELEMENT_MST_TBL    FRT3," & vbCrLf)
                //strQuery.Append("       CURRENCY_TYPE_MST_TBL      CURR3" & vbCrLf)
                //strQuery.Append(" WHERE FCL_LCL.QUOTATION_SEA_FK = MAIN1.QUOTATION_SEA_PK" & vbCrLf)
                //strQuery.Append("   AND QUOT_OTHER.QUOTATION_TRN_SEA_FK = FCL_LCL.QUOTE_TRN_SEA_PK" & vbCrLf)
                //strQuery.Append("   AND QUOT_OTHER.FREIGHT_ELEMENT_MST_FK = FRT3.FREIGHT_ELEMENT_MST_PK" & vbCrLf)
                //strQuery.Append("   AND QUOT_OTHER.CURRENCY_MST_FK = CURR3.CURRENCY_MST_PK" & vbCrLf)
                //strQuery.Append("   AND MAIN1.QUOTATION_SEA_PK = " & QuotPk)

                return objWF.GetDataSet(strQuery.ToString());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Other Charges"

        #region "Fetch Quotation Int32"

        public string FetchQuotNr(int QuotPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = " SELECT Q.QUOTATION_REF_NO FROM QUOTATION_AIR_TBL Q WHERE Q.QUOTATION_AIR_PK = " + QuotPK;
            return objWF.ExecuteScaler(strSQL);
        }

        public DataSet FetchApprovalDtl(int QuotPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT U.USER_ID, TO_CHAR(Q.APP_DATE, DATEFORMAT) APP_DATE,Q.STATUS");
            sb.Append("  FROM QUOTATION_AIR_TBL Q, USER_MST_TBL U");
            sb.Append(" WHERE Q.APP_BY = U.USER_MST_PK");
            sb.Append("   AND Q.QUOTATION_AIR_PK = " + QuotPK);
            return objWF.GetDataSet(sb.ToString());
        }

        public bool IsCustomerApproved(int QuotationAirPk)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                int status =  Convert.ToInt32(objWF.ExecuteScaler("SELECT NVL(Q.CUSTOMER_APPROVED,0) FROM QUOTATION_AIR_TBL Q WHERE Q.QUOTATION_AIR_PK=" + QuotationAirPk));
                if (status == 1)
                    return true;
                else
                    return false;
            }
            catch (Exception ex)
            {
            }
            return false;
        }

        #endregion "Fetch Quotation Number"

        #region "Get type and Fetch Pay Type"

        public int get_TYPE(string pk)
        {
            StringBuilder strQuery = new StringBuilder();
            OracleDataReader dr = default(OracleDataReader);
            try
            {
                strQuery.Append("SELECT quotation_type");
                strQuery.Append("FROM QUOTATION_AIR_TBL AIR");
                strQuery.Append("WHERE AIR.QUOTATION_AIR_PK =");
                strQuery.Append(pk);
                dr = (new WorkFlow()).GetDataReader(strQuery.ToString());
                while (dr.Read())
                {
                    return Convert.ToInt32(dr[0]);
                }
            }
            catch (Exception ex)
            {
                return 0;
            }
            finally
            {
                dr.Close();
            }
            return 0;
        }

        public int FetchPayType(int ShipPK)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT SH.FREIGHT_TYPE FROM SHIPPING_TERMS_MST_TBL SH WHERE SH.SHIPPING_TERMS_MST_PK = " + ShipPK;
            return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
        }

        #endregion "Get type and Fetch Pay Type"

        #region "PortGroup"

        public DataSet FetchFromPortGroup(int QuotPK = 0, int PortGrpPK = 0, string POLPK = "0")
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append("SELECT POL.PORT_MST_PK POL_PK,");
                    sb.Append("       POL.PORT_ID POL_ID,");
                    sb.Append("       POD.PORT_MST_PK POD_PK,");
                    sb.Append("       POD.PORT_ID POD_ID,");
                    sb.Append("       T.POL_GRP_FK,");
                    sb.Append("       T.POD_GRP_FK,T.TARIFF_GRP_FK");
                    sb.Append("  FROM QUOT_GEN_TRN_AIR_TBL T, PORT_MST_TBL POL, PORT_MST_TBL POD");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.QUOT_GEN_AIR_TRN_PK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POL_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 1 AND P.ACTIVE_FLAG = 1 ");
                    if (POLPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + POLPK + ") ");
                    }
                }

                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet FetchToPortGroup(int QuotPK = 0, int PortGrpPK = 0, string PODPK = "0")
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append("SELECT POL.PORT_MST_PK POL_PK,");
                    sb.Append("       POL.PORT_ID POL_ID,");
                    sb.Append("       POD.PORT_MST_PK POD_PK,");
                    sb.Append("       POD.PORT_ID POD_ID,");
                    sb.Append("       T.POL_GRP_FK,");
                    sb.Append("       T.POD_GRP_FK,T.TARIFF_GRP_FK");
                    sb.Append("  FROM QUOT_GEN_TRN_AIR_TBL T, PORT_MST_TBL POL, PORT_MST_TBL POD");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.QUOT_GEN_AIR_TRN_PK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, PGT.PORT_GRP_MST_FK POD_GRP_FK FROM PORT_MST_TBL P, PORT_GRP_TRN_TBL PGT WHERE P.PORT_MST_PK = PGT.PORT_MST_FK AND PGT.PORT_GRP_MST_FK =" + PortGrpPK);
                    sb.Append(" AND P.BUSINESS_TYPE = 1 AND P.ACTIVE_FLAG = 1");
                    if (PODPK != "0")
                    {
                        sb.Append(" AND PGT.PORT_MST_FK IN (" + PODPK + ") ");
                    }
                }

                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet FetchTariffGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append("SELECT POL.PORT_MST_PK POL_PK,");
                    sb.Append("       POL.PORT_ID POL_ID,");
                    sb.Append("       POD.PORT_MST_PK POD_PK,");
                    sb.Append("       POD.PORT_ID POD_ID,");
                    sb.Append("       T.POL_GRP_FK,");
                    sb.Append("       T.POD_GRP_FK,T.TARIFF_GRP_FK");
                    sb.Append("  FROM QUOT_GEN_TRN_AIR_TBL T, PORT_MST_TBL POL, PORT_MST_TBL POD");
                    sb.Append(" WHERE T.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("   AND T.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("   AND T.QUOT_GEN_AIR_TRN_PK = " + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT POL.PORT_MST_PK       POL_PK,");
                    sb.Append("       POL.PORT_ID           POL_ID,");
                    sb.Append("       POD.PORT_MST_PK       POD_PK,");
                    sb.Append("       POD.PORT_ID           POD_ID,");
                    sb.Append("       TGM.POL_GRP_MST_FK    POL_GRP_FK,");
                    sb.Append("       TGM.POD_GRP_MST_FK    POD_GRP_FK,");
                    sb.Append("       TGM.TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL       POL,");
                    sb.Append("       PORT_MST_TBL       POD,");
                    sb.Append("       TARIFF_GRP_TRN_TBL TGT,");
                    sb.Append("       TARIFF_GRP_MST_TBL TGM");
                    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                    sb.Append("   AND POL.PORT_MST_PK = TGT.POL_MST_FK");
                    sb.Append("   AND POD.PORT_MST_PK = TGT.POD_MST_FK");
                    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                    sb.Append("   AND POL.BUSINESS_TYPE = 1");
                    sb.Append("   AND POL.ACTIVE_FLAG = 1");
                }

                return (objWF.GetDataSet(sb.ToString()));
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

        public DataSet FetchTariffPODGrp(int QuotPK = 0, int PortGrpPK = 0, string TariffPK = "0")
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            try
            {
                if (QuotPK != 0)
                {
                    sb.Append(" SELECT P.PORT_MST_PK, P.PORT_ID, T.POD_GRP_FK, T.TARIFF_GRP_FK FROM QUOTATION_TRN_SEA_FCL_LCL T, PORT_MST_TBL P WHERE T.PORT_MST_POD_FK = P.PORT_MST_PK AND T.TARIFF_GRP_FK = " + TariffPK);
                    sb.Append(" AND T.QUOTATION_SEA_FK =" + QuotPK);
                }
                else
                {
                    sb.Append(" SELECT P.PORT_MST_PK,");
                    sb.Append("        P.PORT_ID,");
                    sb.Append("        TGM.POD_GRP_MST_FK POD_GRP_FK,");
                    sb.Append("        TGM.TARIFF_GRP_MST_PK");
                    sb.Append("  FROM PORT_MST_TBL P, TARIFF_GRP_TRN_TBL TGT, TARIFF_GRP_MST_TBL TGM");
                    sb.Append(" WHERE TGM.TARIFF_GRP_MST_PK = TGT.TARIFF_GRP_MST_FK");
                    sb.Append("   AND P.PORT_MST_PK = TGT.POD_MST_FK");
                    sb.Append("   AND TGM.TARIFF_GRP_MST_PK =" + TariffPK);
                    sb.Append("   AND P.BUSINESS_TYPE = 1");
                    sb.Append("   AND P.ACTIVE_FLAG = 1");
                }

                return (objWF.GetDataSet(sb.ToString()));
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

        public string FetchPrtGroup(int QuotPK)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                strSQL = " SELECT NVL(Q.PORT_GROUP,0) PORT_GROUP FROM QUOTATION_AIR_TBL Q WHERE Q.QUOTATION_AIR_PK = " + QuotPK;
                return objWF.ExecuteScaler(strSQL);
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

        #endregion "PortGroup"

        #region "Get Quotation Status"

        public int GetQuotStatus(string QuotationPK)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT COUNT(Q.QUOTATION_AIR_PK)");
            sb.Append("   FROM QUOTATION_AIR_TBL Q");
            sb.Append("   WHERE Q.STATUS <> 2");
            sb.Append("   AND Q.STATUS <> 4");
            sb.Append("   AND Q.QUOTATION_AIR_PK = " + QuotationPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Get Quotation Status"

        #region "Get QuotFrom"

        public int FetchQuotFrom(int QuotPK, bool QuoType)
        {
            try
            {
                string strSQL = null;
                WorkFlow objWF = new WorkFlow();
                if (QuoType == true)
                {
                    strSQL = " SELECT DISTINCT T.TRANS_REFERED_FROM FROM QUOT_GEN_TRN_AIR_TBL T WHERE T.QUOT_GEN_AIR_FK = " + QuotPK;
                }
                else
                {
                    strSQL = "  SELECT DISTINCT QA.TRANS_REFERED_FROM  FROM QUOTATION_TRN_AIR QA WHERE QA.QUOTATION_AIR_FK = " + QuotPK;
                }

                return Convert.ToInt32(objWF.ExecuteScaler(strSQL));
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

        #endregion "Get QuotFrom"

        #region "Get Freight Details"

        public object FetchFrtQuery(string trnPk = "0", string POLPK = "0", string PODPK = "0")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT DISTINCT QTRN.PORT_MST_POL_FK POL_PK,");
            sb.Append("       QTRN.PORT_MST_POD_FK POD_PK,");
            sb.Append("       FREIGHT_ELEMENT_MST_FK,");
            sb.Append("       FREIGHT_ELEMENT_ID,");
            sb.Append("       FREIGHT_ELEMENT_NAME,");
            sb.Append("       TRAN.CURRENCY_MST_FK,");
            sb.Append("       CURRENCY_ID,");
            sb.Append("       CURRENCY_NAME,");
            sb.Append("       DECODE(TRAN.CHECK_FOR_ALL_IN_RT, 1, 'true', 'false') SELECTED,");
            sb.Append("       DECODE(TRAN.CHECK_ADVATOS, 1, 'true', 'false') ADVATOS,");
            sb.Append("       MIN_AMOUNT");
            sb.Append("  FROM QUOT_AIR_TRN_FREIGHT_TBL TRAN,");
            sb.Append("       QUOT_GEN_TRN_AIR_TBL     QTRN,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL,");
            sb.Append("       CURRENCY_TYPE_MST_TBL");
            sb.Append(" WHERE TRAN.QUOT_GEN_AIR_TRN_FK IN (" + trnPk + ")");
            sb.Append("   AND QTRN.QUOT_GEN_AIR_TRN_PK = TRAN.QUOT_GEN_AIR_TRN_FK");
            sb.Append("   AND FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1");
            sb.Append("   AND CURRENCY_MST_FK = CURRENCY_MST_PK");
            sb.Append("   AND CHARGE_TYPE <> 3");
            sb.Append("   AND FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE IN (1, 3)");
            sb.Append("  UNION");
            sb.Append("  SELECT DISTINCT POL.PORT_MST_PK POL_PK,");
            sb.Append("       POD.PORT_MST_PK POD_PK,");
            sb.Append("       FMT.FREIGHT_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_FK,");
            sb.Append("       FMT.FREIGHT_ELEMENT_ID AS \"Frt. Ele.\",");
            sb.Append("       FREIGHT_ELEMENT_NAME,");
            sb.Append("       CURR.CURRENCY_MST_PK CURRENCY_MST_FK,");
            sb.Append("       CURR.CURRENCY_ID AS \"Curr.\",");
            sb.Append("       CURR.CURRENCY_NAME,");
            sb.Append("       'false' SELECTED,");
            sb.Append("       'false' ADVATOS,");
            sb.Append("       0.00 AS MIN_AMOUNT");
            sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CURR");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND ((POL.PORT_MST_PK IN( " + POLPK + ") AND POD.PORT_MST_PK IN( " + PODPK + ")))");
            sb.Append("   AND POL.BUSINESS_TYPE = 1");
            sb.Append("   AND POD.BUSINESS_TYPE = 1");
            sb.Append("   AND FMT.ACTIVE_FLAG = 1");
            sb.Append("   AND FMT.CHARGE_TYPE <> 3");
            sb.Append("   AND FMT.BUSINESS_TYPE IN (1, 3)");
            sb.Append("   AND FMT.BY_DEFAULT = 1");
            sb.Append("   AND CURR.CURRENCY_MST_PK =" + HttpContext.Current.Session["currency_mst_pk"] + "");
            sb.Append("AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN");
            sb.Append("       (SELECT DISTINCT FREIGHT_ELEMENT_MST_FK");
            sb.Append("          FROM QUOT_AIR_TRN_FREIGHT_TBL TRAN,");
            sb.Append("               QUOT_GEN_TRN_AIR_TBL     QTRN,");
            sb.Append("               FREIGHT_ELEMENT_MST_TBL,");
            sb.Append("               CURRENCY_TYPE_MST_TBL");
            sb.Append("         WHERE TRAN.QUOT_GEN_AIR_TRN_FK IN (" + trnPk + ")");
            sb.Append("           AND QTRN.QUOT_GEN_AIR_TRN_PK = TRAN.QUOT_GEN_AIR_TRN_FK");
            sb.Append("           AND FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK");
            sb.Append("           AND FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1");
            sb.Append("           AND CURRENCY_MST_FK = CURRENCY_MST_PK");
            sb.Append("           AND CHARGE_TYPE <> 3");
            sb.Append("           AND FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE IN (1, 3))");
            sb.Append("   GROUP BY POL.PORT_MST_PK,");
            sb.Append("          POL.PORT_ID,");
            sb.Append("          POD.PORT_MST_PK,");
            sb.Append("          POD.PORT_ID,");
            sb.Append("          FMT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("          FMT.FREIGHT_ELEMENT_ID,");
            sb.Append("          CURR.CURRENCY_MST_PK,");
            sb.Append("          CURR.CURRENCY_ID,");
            sb.Append("          CURRENCY_NAME,");
            sb.Append("          FREIGHT_ELEMENT_NAME");
            sb.Append("   HAVING POL.PORT_ID <> POD.PORT_ID");
            sb.Append("  ORDER BY FREIGHT_ELEMENT_ID");
            return sb.ToString();
        }

        public object GetGridDetails(string trnPk = "0", string POLPK = "0", string PODPK = "0")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT DISTINCT QTRAN.PORT_MST_POL_FK POL_PK,");
            sb.Append("       QTRAN.PORT_MST_POD_FK POD_PK,");
            sb.Append("       TRAN.FREIGHT_ELEMENT_MST_FK,");
            sb.Append("       FREIGHT_ELEMENT_ID,");
            sb.Append("       FREIGHT_ELEMENT_NAME,");
            sb.Append("       TRAN.CURRENCY_MST_FK,");
            sb.Append("       CURRENCY_ID,");
            sb.Append("       'false' SELECTED,");
            sb.Append("       'false' ADVATOS,");
            sb.Append("       APPROVED_RATE CURRENT_RATE,");
            sb.Append("       APPROVED_RATE REQUEST_RATE,");
            sb.Append("       APPROVED_RATE,");
            sb.Append("       TRAN.CHARGE_BASIS");
            sb.Append("  FROM QUOT_AIR_SURCHARGE      TRAN,");
            sb.Append("       QUOT_GEN_TRN_AIR_TBL    QTRAN,");
            sb.Append("       FREIGHT_ELEMENT_MST_TBL,");
            sb.Append("       CURRENCY_TYPE_MST_TBL");
            sb.Append(" WHERE TRAN.QUOT_GEN_AIR_TRN_FK IN (" + trnPk + ")");
            sb.Append("   AND FREIGHT_ELEMENT_MST_FK = FREIGHT_ELEMENT_MST_PK");
            sb.Append("   AND QTRAN.QUOT_GEN_AIR_TRN_PK = TRAN.QUOT_GEN_AIR_TRN_FK");
            sb.Append("   AND FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1");
            sb.Append("   AND CURRENCY_MST_FK = CURRENCY_MST_PK");
            sb.Append("   AND CHARGE_TYPE <> 3");
            sb.Append("   AND FREIGHT_ELEMENT_MST_TBL.BUSINESS_TYPE IN (1, 3)");
            sb.Append("  UNION ");
            sb.Append(" SELECT DISTINCT POL.PORT_MST_PK POL_PK,");
            sb.Append("       POD.PORT_MST_PK POD_PK,");
            sb.Append("       FMT.FREIGHT_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_FK,");
            sb.Append("       FMT.FREIGHT_ELEMENT_ID AS \"Frt. Ele.\",");
            sb.Append("       FREIGHT_ELEMENT_NAME,");
            sb.Append("       CURR.CURRENCY_MST_PK CURRENCY_MST_FK,");
            sb.Append("       CURR.CURRENCY_ID AS \"Curr.\",");
            sb.Append("       'false' SELECTED,");
            sb.Append("       'false' ADVATOS,");
            sb.Append("       0.00 AS MIN_AMOUNT,");
            sb.Append("       0.00 AS CURRENT_RATE,");
            sb.Append("       0.00 AS APPROVED_RATE,");
            sb.Append("       0 CHARGE_BASIS");
            sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD,");
            sb.Append("       CURRENCY_TYPE_MST_TBL   CURR");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND ((POL.PORT_MST_PK IN( " + POLPK + " ) AND POD.PORT_MST_PK IN( " + PODPK + ")))");
            sb.Append("   AND CURR.CURRENCY_MST_PK = " + HttpContext.Current.Session["currency_mst_pk"] + "");
            sb.Append("   AND FMT.ACTIVE_FLAG = 1 ");
            sb.Append(" ORDER BY FREIGHT_ELEMENT_MST_FK");
            return sb.ToString();
        }

        public object GetSlabDetails(string FreightPks = "0", string POLPK = "0", string PODPK = "0")
        {
            StringBuilder sb = new StringBuilder(5000);
            sb.Append("SELECT DISTINCT QTRAN.PORT_MST_POL_FK POLPK,");
            sb.Append("                QTRAN.PORT_MST_POD_FK PODPK,");
            sb.Append("                TRAN.FREIGHT_ELEMENT_MST_FK FREIGHT_ELEMENT_MST_FK,");
            sb.Append("                AIRFREIGHT_SLABS_TBL_PK,");
            sb.Append("                BREAKPOINT_ID,");
            sb.Append("                BREAKPOINT_DESC,");
            sb.Append("                NVL(APPROVED_RATE, 0) CURRENT_RATE,");
            sb.Append("                NVL(APPROVED_RATE, 0) REQUEST_RATE,");
            sb.Append("                NVL(APPROVED_RATE, 0) APPROVED_RATE");
            sb.Append("  FROM QUOT_AIR_TRN_FREIGHT_TBL TRAN,");
            sb.Append("       QUOT_GEN_TRN_AIR_TBL     QTRAN,");
            sb.Append("       QUOTE_AIR_BREAKPOINTS    BPNT,");
            sb.Append("       AIRFREIGHT_SLABS_TBL");
            sb.Append("  WHERE BPNT.QUOT_GEN_AIR_FRT_FK IN (" + FreightPks + ")");
            sb.Append("   AND QTRAN.QUOT_GEN_AIR_TRN_PK = TRAN.QUOT_GEN_AIR_TRN_FK");
            sb.Append("   AND BPNT.QUOT_GEN_AIR_FRT_FK = TRAN.QUOT_GEN_TRN_FREIGHT_PK");
            sb.Append("   AND AIRFREIGHT_SLABS_TBL.ACTIVE_FLAG = 1");
            sb.Append("   AND BPNT.AIRFREIGHT_SLABS_TBL_FK = AIRFREIGHT_SLABS_TBL_PK");
            sb.Append("  UNION  ");
            sb.Append(" SELECT DISTINCT POL.PORT_MST_PK POLPK,");
            sb.Append("                POD.PORT_MST_PK PODPK,");
            sb.Append("                FMT.FREIGHT_ELEMENT_MST_PK FREIGHT_ELEMENT_MST_FK,");
            sb.Append("                AIR.AIRFREIGHT_SLABS_TBL_PK,");
            sb.Append("                AIR.BREAKPOINT_ID,");
            sb.Append("                AIR.BREAKPOINT_DESC,");
            sb.Append("                0  CURRENT_RATE,");
            sb.Append("                0  REQUEST_RATE,");
            sb.Append("                0  APPROVED_RATE");
            sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
            sb.Append("       AIRFREIGHT_SLABS_TBL    AIR,");
            sb.Append("       PORT_MST_TBL            POL,");
            sb.Append("       PORT_MST_TBL            POD");
            sb.Append(" WHERE (1 = 1)");
            sb.Append("   AND ((POL.PORT_MST_PK IN( " + POLPK + " ) AND POD.PORT_MST_PK IN( " + PODPK + ")))");
            sb.Append("   AND AIR.ACTIVE_FLAG = 1");
            sb.Append("   AND FMT.ACTIVE_FLAG = 1");
            sb.Append("    ORDER BY FREIGHT_ELEMENT_MST_FK, AIRFREIGHT_SLABS_TBL_PK");
            return sb.ToString();
        }

        public System.DateTime GetQuotDt(string QuotPK)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT Q.QUOTATION_DATE ");
            sb.Append("   FROM QUOTATION_AIR_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_AIR_PK = " + QuotPK);
            try
            {
                return Convert.ToDateTime(objWF.ExecuteScaler(sb.ToString()));
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

        public int GetQuotAmendSt(string QuotPK)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT Q.AMEND_FLAG ");
            sb.Append("   FROM QUOTATION_AIR_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_AIR_PK = " + QuotPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        public int GetAutoQuote(string QuotPK)
        {
            StringBuilder sb = new StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            sb.Append("   SELECT Q.AUTO_CREATE ");
            sb.Append("   FROM QUOTATION_SEA_TBL Q");
            sb.Append("   WHERE Q.QUOTATION_SEA_PK = " + QuotPK);
            try
            {
                return Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

        #endregion "Get Freight Details"
    }
}

#region " Apply Rate Mapping. "

//Option Grid
//PK,            0       'FK,            1       'REF_TYPE,      2       'TYPE_ID,       3>
//REF_NO,        4>      'SHIP_DATE,     5>      'POLFK,         6       'POL_ID,        7>
//PODFK,         8       'POD_ID,        9>      'AIR_PK,        10      'AIR_ID,        11>
//COMM_PK,       12      'COMM_ID,       13>     'SLAB_TYPE_PK,  14      'SLAB_TYPE,     15>
//BOXES,         16>     'SLAB,          17      'SLAB_ID,       18>     'CH_WT,         19>
//AI_QT,         20>     'NET,           21      'OTH_DTL,       22      'OTH_BTN,       23>
//CRG_BTN,       24>     'SELECTED       25>
//Entry Grid
//PK,            0       'FK,            1       'REF_TYPE,      2       'TYPE_ID,       3
//REF_NO,        4       'SHIP_DATE,     5       'POLFK,         6       'POL_ID,        7
//PODFK,         8       'POD_ID,        9       'AIR_PK,        10      'AIR_ID,        11
//COMM_PK,       12      'COMM_ID,       13      'SLAB_TYPE_PK,  14      'SLAB_TYPE,     15
//BOXES,         16      'SLAB,          17      'SLAB_ID,       18      'CH_WT,         19
//AI_TRF,        20      'AI_QT,         21      'AIR_RT,        22      'NET,           23
//REF_NO2,       24      'TYPE2,         25      'CUSTOMER_PK,   26      'CUSTOMER_CATPK,27
//COMM_GRPPK,    28      'OTH_DTL,       29      'OTH_BTN,       30      'CRG_BTN        31
//OptionGrid~EntryGrid [ for general apply rates ]
//"0~0,1~1,2~2,3~3,4~4,5~5,6~6,7~7,8~8,9~9,10~10,11~11,12~12,13~13,14~14,15~15,16~16,17~17,18~18,19~19," & _
//"20~20,20~21,21~23,22~29,23~30" Remaining columns [22,24,25,26,27,28,31] Add Mode
//OptionGrid~EntryGrid [ for apply rates in enquiry grid.] Modify Mode
//"14~14,15~15,16~16,17~17,18~18,19~19,20~20,20~21,21~23,4~24,2~25,22~29,23~30[22,26,27,28,31]
//=============Freight Level Option Grid
//PK,            0       'FK,            1       'FRT_FK,        2       'FRT_ID,        3>
//SELECTED,      4>      'CURR_FK,       5       'CURR_ID,       6>      'MIN_AMOUNT,    7>
//BASIS_RATE,    8>      'TARIFF_RATE,   9>      'CH_BASIS,      10      'CH_BASIS_ID,   11>
//FRT_TYPE       12
//=============Entry Grid
//PK,            0       'FK,            1       'FRT_FK,        2       'FRT_ID,        3
//SELECTED,      4       'CURR_FK,       5       'CURR_ID,       6       'MIN_AMOUNT,    7
//BASIS_RATE,    8       'TARIFF_RATE,   9       'QUOTED_RATE,   10      'CH_BASIS,      11
//CH_BASIS_ID,   12      'FRT_TYPE       13      'P_TYPE,        14      'P_TYPE_ID      15
//OptionGrid~EntryGrid [ for general apply rates ]
//"0~0,1~1,2~2,3~3,4~4,5~5,6~6,7~7,8~8,9~9,9~10,10~11,11~12,12~13[14,15]"
//OptionGrid~EntryGrid [ for apply rates in enquiry grid.] Modify Mode
//"5~5,6~6,7~7,8~8,9~9,9~10,10~11,11~12,12~13"

#endregion " Apply Rate Mapping. "