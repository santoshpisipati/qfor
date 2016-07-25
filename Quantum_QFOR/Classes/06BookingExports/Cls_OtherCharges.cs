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
using System.Data;
using System.Web;

// This is the class file for form getOtherCharges.asx
// Apart from this it also have two shared method for conversion of table information in
// compitable string and the another for modifying table according to provided string
namespace Quantum_QFOR
{
    public class Cls_OtherCharges : CommonFeatures
    {
        private string RequerstStrings;

        public DataTable MyTable;

        #region " Property [ Freight & Currency ] "

        // From the string it will extract another string which will refer Freight & Currency
        // this strion will be in comma seperated format so it can be used in sql IN condition.
        public string FreightAndCurrency
        {
            get
            {
                Array arr = null;
                arr = RequerstStrings.Split('^');
                Int16 i = default(Int16);
                string outStr = null;
                for (i = 0; i <= arr.Length - 1; i++)
                {
                    Array innerArr = null;
                    innerArr = Convert.ToString(arr.GetValue(i)).Split('~');
                    outStr += "(" + innerArr.GetValue(0) + "," + innerArr.GetValue(1) + "),";
                }
                outStr = outStr.TrimEnd(',');
                return outStr;
            }
        }

        public string PortPks
        {
            get
            {
                string functionReturnValue = null;
                return functionReturnValue;
                return functionReturnValue;
            }
        }

        #endregion " Property [ Freight & Currency ] "

        #region " Constructor "

        // this constructor first get the request query string as parameter.
        // it suppress the last character so that now it can be broken for rows elements
        // According to provided business type it will return a data table in MyTable

        public Cls_OtherCharges(string str, Int16 BizType = 1, bool AllInclusive = true, string PortPks = "", string POLPK = "", string PODPK = "")
        {
            RequerstStrings = str.TrimEnd('^');
            MyTable = OtherCharges(BizType, AllInclusive, PortPks, POLPK, PODPK);
        }

        #endregion " Constructor "

        #region " Other Charges "

        // this is the base function of this class
        // According to given string and business type it prepares a datatable
        // If there is no string then it will prepare a default datatable containing all relevent
        // freight element with Base currency.
        // it collects freight elements according to provided business type
        // if a string is provided then all information will come in the datatable
        // and other freight which has not been included; will be also included in the table
        //
        //modified by thiyagarajan on 2/12/08 for location based curr. task
        private DataTable OtherCharges(Int16 BizType, bool AllInclusive, string PortPks = "", string POLPK = "", string PODPK = "")
        {
            DataTable frtDt = null;
            DataTable exfrDt = null;
            DataRow DR = null;
            DataRow nDR = null;
            string strSQL = null;
            string frtStr = "-1";
            string BizTypeCond = "1,3";
            if (BizType == 2)
                BizTypeCond = "2,3";
            Int16 ColCnt = default(Int16);
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            try
            {
                if (RequerstStrings.Trim().Length > 0 & RequerstStrings != "0.00" & RequerstStrings != "0")
                {
                    // If String is provided then prepare a DataTable and incorporate given information.
                    sb.Append("SELECT DISTINCT 'true' SEL,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("                FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                CTM.CURRENCY_MST_PK,");
                    sb.Append("                CTM.CURRENCY_ID,");
                    sb.Append("                'Flat' CHARGE_BASIS,");
                    sb.Append("                0 CURRENT_RATE,");
                    sb.Append("                0 REQUEST_RATE,");
                    sb.Append("                0 APPROVED_RATE");
                    sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
                    sb.Append("       CURRENCY_TYPE_MST_TBL   CTM,");
                    sb.Append("       FREIGHT_CONFIG_TRN_TBL  FCT");
                    sb.Append(" WHERE FMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                    sb.Append("   AND (FMT.FREIGHT_ELEMENT_MST_PK, CTM.CURRENCY_MST_PK) IN (" + FreightAndCurrency + ") ");
                    //If PortPks <> "" Then
                    //    sb.Append("   AND FCT.PORT_MST_FK IN (" & PortPks & ") ")
                    //End If
                    sb.Append("   AND FMT.BUSINESS_TYPE IN (" + BizTypeCond + ") ");
                    sb.Append("   AND FMT.BY_DEFAULT = 1");
                    sb.Append(" ORDER BY FREIGHT_ELEMENT_ID");

                    frtDt = (new WorkFlow()).GetDataTable(sb.ToString());

                    //Sel, FreightPk, FreightId, CurrencyPk, CurrencyId, Basis, Current, Request, [ Approved ]

                    Int16 RowCnt = default(Int16);
                    Int16 i = default(Int16);
                    Array arr = null;
                    Array arr2 = null;
                    arr = RequerstStrings.Split('^');
                    for (RowCnt = 0; RowCnt <= frtDt.Rows.Count - 1; RowCnt++)
                    {
                        for (i = 0; i <= arr.Length - 1; i++)
                        {
                            arr2 = Convert.ToString(arr.GetValue(i)).Split('~');
                            if (frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"] == Convert.ToString(arr2.GetValue(0)) & frtDt.Rows[RowCnt]["CURRENCY_MST_PK"] == Convert.ToString(arr2.GetValue(1)))
                            {
                                frtDt.Rows[RowCnt]["CHARGE_BASIS"] = ChargeBasis(Convert.ToInt16(arr2.GetValue(2)));
                                frtDt.Rows[RowCnt]["CURRENT_RATE"] = Convert.ToInt32(arr2.GetValue(3));
                                frtDt.Rows[RowCnt]["REQUEST_RATE"] = Convert.ToInt32(arr2.GetValue(4));
                                if (arr2.Length > 5)
                                    frtDt.Rows[RowCnt]["APPROVED_RATE"] = Convert.ToString(arr2.GetValue(5));
                                frtStr += "," + Convert.ToString(frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"]);
                            }
                        }
                    }
                    // Add other freights which are not already available in DataTable
                    if (AllInclusive == true)
                    {
                        sb.Remove(0, sb.Length);
                        //sb.Append("SELECT DISTINCT 'false' SEL,")
                        //sb.Append("                FMT.FREIGHT_ELEMENT_MST_PK,")
                        //sb.Append("                FMT.FREIGHT_ELEMENT_ID,")
                        //sb.Append("                CURRENCY_MST_PK,")
                        //sb.Append("                CURRENCY_ID,")
                        //sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,")
                        //sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,")
                        //sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,")
                        //sb.Append("                0 APPROVED_RATE")
                        //sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FMT,")
                        //sb.Append("       CURRENCY_TYPE_MST_TBL   CTM,")
                        //sb.Append("       FREIGHT_CONFIG_TRN_TBL  FCT")
                        //sb.Append(" WHERE FMT.ACTIVE_FLAG = 1")
                        //sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK")
                        //sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK NOT IN (" & frtStr & ") ")
                        //If PortPks <> "" Then
                        //    sb.Append("   AND FCT.PORT_MST_FK IN (" & PortPks & ") ")
                        //End If
                        //sb.Append("   AND FMT.BUSINESS_TYPE IN (" & BizTypeCond & ") ")
                        //sb.Append("   AND CTM.CURRENCY_MST_PK = " & HttpContext.Current.Session("CURRENCY_MST_PK"))
                        //sb.Append("   AND FCT.CHARGE_TYPE = 3")
                        //sb.Append("   AND BY_DEFAULT = 1")
                        //sb.Append(" ORDER BY FREIGHT_ELEMENT_ID")

                        sb.Append("SELECT *");
                        sb.Append("  FROM (SELECT *");
                        sb.Append("          FROM (SELECT DISTINCT 'false' SEL, FMT.FREIGHT_ELEMENT_MST_PK,");
                        sb.Append("                       FMT.FREIGHT_ELEMENT_ID,");
                        sb.Append("                       CTM.CURRENCY_MST_PK,");
                        sb.Append("                       CTM.CURRENCY_ID, ");
                        sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,");
                        sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,");
                        sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,");
                        sb.Append("                0 APPROVED_RATE");
                        sb.Append("                  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
                        sb.Append("                       CURRENCY_TYPE_MST_TBL   CTM,");
                        sb.Append("                       FREIGHT_CONFIG_TRN_TBL  FCT");
                        sb.Append("                 WHERE FMT.ACTIVE_FLAG = 1");
                        sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                        sb.Append("                   AND CTM.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                        sb.Append("                   AND FMT.BUSINESS_TYPE IN (" + BizTypeCond + ")");
                        sb.Append("                   AND FCT.PORT_MST_FK IN (" + POLPK + ")");
                        sb.Append("                   AND FCT.CHARGE_TYPE IN (1)");
                        sb.Append("                   AND FMT.BY_DEFAULT = 1");
                        sb.Append("                UNION");
                        sb.Append("                SELECT DISTINCT 'false' SEL, FMT.FREIGHT_ELEMENT_MST_PK,");
                        sb.Append("                       FMT.FREIGHT_ELEMENT_ID,");
                        sb.Append("                       CTM.CURRENCY_MST_PK,");
                        sb.Append("                       CTM.CURRENCY_ID,");
                        sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,");
                        sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,");
                        sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,");
                        sb.Append("                0 APPROVED_RATE");
                        sb.Append("                  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
                        sb.Append("                       CURRENCY_TYPE_MST_TBL   CTM,");
                        sb.Append("                       FREIGHT_CONFIG_TRN_TBL  FCT");
                        sb.Append("                 WHERE FMT.ACTIVE_FLAG = 1");
                        sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                        sb.Append("                   AND CTM.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                        sb.Append("                   AND FMT.BUSINESS_TYPE IN (" + BizTypeCond + ")");
                        sb.Append("                   AND FCT.PORT_MST_FK IN (" + PODPK + ")");
                        sb.Append("                   AND FCT.CHARGE_TYPE IN (2)");
                        sb.Append("                   AND FMT.BY_DEFAULT = 1) Q");
                        sb.Append("        UNION ");
                        sb.Append("        SELECT DISTINCT 'false' SEL, FMT.FREIGHT_ELEMENT_MST_PK,");
                        sb.Append("               FMT.FREIGHT_ELEMENT_ID,");
                        sb.Append("               CTM.CURRENCY_MST_PK,");
                        sb.Append("               CTM.CURRENCY_ID,");
                        sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,");
                        sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,");
                        sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,");
                        sb.Append("                0 APPROVED_RATE");
                        sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL FMT,");
                        sb.Append("               CURRENCY_TYPE_MST_TBL   CTM,");
                        sb.Append("               FREIGHT_CONFIG_TRN_TBL  FCT");
                        sb.Append("         WHERE FMT.ACTIVE_FLAG = 1");
                        sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                        sb.Append("   AND CTM.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                        sb.Append("   AND FMT.BUSINESS_TYPE IN (" + BizTypeCond + ")");
                        sb.Append("   AND FCT.PORT_MST_FK IN (" + POLPK + ", " + PODPK + ")");
                        sb.Append("           AND FCT.CHARGE_TYPE IN (3)");
                        sb.Append("           AND FMT.BY_DEFAULT = 1)");
                        sb.Append("   WHERE FREIGHT_ELEMENT_MST_PK NOT IN (" + frtStr + ")");
                        sb.Append(" ORDER BY FREIGHT_ELEMENT_ID");

                        exfrDt = (new WorkFlow()).GetDataTable(sb.ToString());

                        foreach (DataRow DR_loopVariable in exfrDt.Rows)
                        {
                            DR = DR_loopVariable;
                            nDR = frtDt.NewRow();
                            for (ColCnt = 0; ColCnt <= exfrDt.Columns.Count - 1; ColCnt++)
                            {
                                nDR[ColCnt] = DR[ColCnt];
                            }
                            frtDt.Rows.Add(nDR);
                        }
                    }
                }
                else
                {
                    sb.Remove(0, sb.Length);
                    // If No String is provided then prepare a default DataTable.
                    //sb.Remove(0, sb.Length)
                    //sb.Append("SELECT DISTINCT 'false' SEL,")
                    //sb.Append("                FMT.FREIGHT_ELEMENT_MST_PK,")
                    //sb.Append("                FMT.FREIGHT_ELEMENT_ID,")
                    //sb.Append("                CURRENCY_MST_PK,")
                    //sb.Append("                CURRENCY_ID,")
                    //sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,")
                    //sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,")
                    //sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,")
                    //sb.Append("                0 APPROVED_RATE")
                    //sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FMT,")
                    //sb.Append("       CURRENCY_TYPE_MST_TBL   CTM,")
                    //sb.Append("       FREIGHT_CONFIG_TRN_TBL  FCT")
                    //sb.Append(" WHERE FMT.ACTIVE_FLAG = 1")
                    //sb.Append("   AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK")
                    //If PortPks <> "" Then
                    //    sb.Append("   AND FCT.PORT_MST_FK IN (" & PortPks & ") ")
                    //End If
                    //sb.Append("   AND FMT.BUSINESS_TYPE IN (" & BizTypeCond & ") ")
                    //sb.Append("   AND CTM.CURRENCY_MST_PK = " & HttpContext.Current.Session("CURRENCY_MST_PK"))
                    //sb.Append("   AND FCT.CHARGE_TYPE = 3")
                    //sb.Append("   AND BY_DEFAULT = 1")
                    //sb.Append(" ORDER BY FREIGHT_ELEMENT_ID")
                    sb.Append("SELECT *");
                    sb.Append("  FROM (SELECT *");
                    sb.Append("          FROM (SELECT DISTINCT 'false' SEL, FMT.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("                       FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                       CTM.CURRENCY_MST_PK,");
                    sb.Append("                       CTM.CURRENCY_ID, ");
                    sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,");
                    sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,");
                    sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,");
                    sb.Append("                0 APPROVED_RATE");
                    sb.Append("                  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
                    sb.Append("                       CURRENCY_TYPE_MST_TBL   CTM,");
                    sb.Append("                       FREIGHT_CONFIG_TRN_TBL  FCT");
                    sb.Append("                 WHERE FMT.ACTIVE_FLAG = 1");
                    sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                    sb.Append("                   AND CTM.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    sb.Append("                   AND FMT.BUSINESS_TYPE IN (" + BizTypeCond + ")");
                    sb.Append("                   AND FCT.PORT_MST_FK IN (" + POLPK + ")");
                    sb.Append("                   AND FCT.CHARGE_TYPE IN (1)");
                    sb.Append("                   AND FMT.BY_DEFAULT = 1");
                    sb.Append("                UNION");
                    sb.Append("                SELECT DISTINCT 'false' SEL, FMT.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("                       FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("                       CTM.CURRENCY_MST_PK,");
                    sb.Append("                       CTM.CURRENCY_ID,");
                    sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,");
                    sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,");
                    sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,");
                    sb.Append("                0 APPROVED_RATE");
                    sb.Append("                  FROM FREIGHT_ELEMENT_MST_TBL FMT,");
                    sb.Append("                       CURRENCY_TYPE_MST_TBL   CTM,");
                    sb.Append("                       FREIGHT_CONFIG_TRN_TBL  FCT");
                    sb.Append("                 WHERE FMT.ACTIVE_FLAG = 1");
                    sb.Append("                   AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                    sb.Append("                   AND CTM.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    sb.Append("                   AND FMT.BUSINESS_TYPE IN (" + BizTypeCond + ")");
                    sb.Append("                   AND FCT.PORT_MST_FK IN (" + PODPK + ")");
                    sb.Append("                   AND FCT.CHARGE_TYPE IN (2)");
                    sb.Append("                   AND FMT.BY_DEFAULT = 1) Q");
                    sb.Append("        UNION ");
                    sb.Append("        SELECT DISTINCT 'false' SEL, FMT.FREIGHT_ELEMENT_MST_PK,");
                    sb.Append("               FMT.FREIGHT_ELEMENT_ID,");
                    sb.Append("               CTM.CURRENCY_MST_PK,");
                    sb.Append("               CTM.CURRENCY_ID,");
                    sb.Append("                DECODE(CHARGE_BASIS, 1, '%', 2, 'Flat', 'Kgs.') CHARGE_BASIS,");
                    sb.Append("                NVL(FMT.BASIS_VALUE, 0) CURRENT_RATE,");
                    sb.Append("                NVL(FMT.BASIS_VALUE, 0) REQUEST_RATE,");
                    sb.Append("                0 APPROVED_RATE");
                    sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL FMT,");
                    sb.Append("               CURRENCY_TYPE_MST_TBL   CTM,");
                    sb.Append("               FREIGHT_CONFIG_TRN_TBL  FCT");
                    sb.Append("         WHERE FMT.ACTIVE_FLAG = 1");
                    sb.Append("           AND FMT.FREIGHT_ELEMENT_MST_PK = FCT.FREIGHT_ELEMENT_FK");
                    sb.Append("   AND CTM.CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"]);
                    sb.Append("   AND FMT.BUSINESS_TYPE IN (" + BizTypeCond + ")");
                    sb.Append("   AND FCT.PORT_MST_FK IN (" + POLPK + ", " + PODPK + ")");
                    sb.Append("           AND FCT.CHARGE_TYPE IN (3)");
                    sb.Append("           AND FMT.BY_DEFAULT = 1)");
                    sb.Append(" ORDER BY FREIGHT_ELEMENT_ID");
                    frtDt = (new WorkFlow()).GetDataTable(sb.ToString());
                }
                return frtDt;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception eX)
            {
                this.ErrorMessage = "Error generating other freights.";
                throw eX;
            }
        }

        private new string ChargeBasis(Int16 Value)
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

        #endregion " Other Charges "

        #region " SHARED: Update Other Freight Table with supplied string "

        public static double UpdateOTHFreights(DataTable DT = null, bool WithApproval = true, string strRows = "", Int16 FreightCol = 0, Int16 CurrencyCol = 1, Int16 BasisCol = 2, Int16 CurrentCol = 3, Int16 RequestCol = 4, Int16 ApprovedCol = 5, Int16 PkCol = 6,
        string PKValue = "", DataTable ExchTable = null)
        {
            try
            {
                strRows = strRows.TrimEnd('^');
                if (string.IsNullOrEmpty(strRows))
                    return 0.0;
                // Nothing to update
                bool ToCreate = false;
                if (DT == null)
                {
                    DT = (new WorkFlow()).GetDataTable(" Select FREIGHT_ELEMENT_MST_FK, " + " CURRENCY_MST_FK, CHARGE_BASIS, " + " 0 CURRENT_RATE, 0 REQUEST_RATE, " + " 0 APPROVED_RATE, " + PKValue + " RFQ_SPOT_AIR_TRN_FK " + " from RFQ_SPOT_AIR_OTH_CHRG where 1 = 2 ");
                    ToCreate = true;
                }

                if (ExchTable == null)
                {
                    ExchTable = (new WorkFlow()).GetDataTable(" Select CURRENCY_MST_BASE_FK, " + " CURRENCY_MST_FK, EXCHANGE_RATE " + " from V_EXCHANGE_RATE where " + " sysdate between FROM_DATE and TO_DATE and exch_rate_type_fk = 1 ");
                }
                double sum = 0;
                Array arr = null;
                arr = strRows.Split('^');
                Int16 i = default(Int16);
                Int16 exRowCnt = default(Int16);
                Int16 RowCnt = default(Int16);
                Int16 ColCnt = default(Int16);
                Int16 sumCol = default(Int16);
                if (WithApproval)
                {
                    sumCol = 5;
                }
                else
                {
                    sumCol = 4;
                }
                bool Flag = true;
                DataRow DR = null;
                for (i = 0; i <= arr.Length - 1; i++)
                {
                    Array innerArr = null;
                    innerArr = Convert.ToString(arr.GetValue(i)).Split('~');
                    if (ToCreate)
                    {
                        DR = DT.NewRow();
                        DR[FreightCol] = Convert.ToString(innerArr.GetValue(0));
                        DR[CurrencyCol] = Convert.ToString(innerArr.GetValue(1));
                        DR[BasisCol] = Convert.ToString(innerArr.GetValue(2));
                        DR[CurrentCol] = Convert.ToString(innerArr.GetValue(3));
                        DR[RequestCol] = Convert.ToString(innerArr.GetValue(4));
                        if (WithApproval)
                        {
                            DR[ApprovedCol] = Convert.ToString(innerArr.GetValue(5));
                            DR[PkCol] = PKValue;
                        }
                        else
                        {
                            DR[PkCol] = PKValue;
                        }
                        DT.Rows.Add(DR);
                        for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                        {
                            if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_FK"] == Convert.ToString(innerArr.GetValue(1)))
                            {
                                sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(sumCol));
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                    else
                    {
                        Flag = false;
                        for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                        {
                            if (getDefault(DT.Rows[RowCnt][PkCol], "-1") == PKValue)
                            {
                                if (DT.Rows[RowCnt][FreightCol] == Convert.ToString(innerArr.GetValue(0)))
                                {
                                    DT.Rows[RowCnt][CurrencyCol] = Convert.ToString(innerArr.GetValue(1));
                                    DT.Rows[RowCnt][BasisCol] = Convert.ToString(innerArr.GetValue(2));
                                    DT.Rows[RowCnt][CurrentCol] = Convert.ToString(innerArr.GetValue(3));
                                    DT.Rows[RowCnt][RequestCol] = Convert.ToString(innerArr.GetValue(4));
                                    if (WithApproval)
                                        DT.Rows[RowCnt][ApprovedCol] = Convert.ToString(innerArr.GetValue(5));
                                    Flag = true;
                                    for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                                    {
                                        if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_FK"] == Convert.ToString(innerArr.GetValue(1)))
                                        {
                                            sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(sumCol));
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
                            DR[FreightCol] = Convert.ToInt16(innerArr.GetValue(0));
                            DR[CurrencyCol] = Convert.ToInt16(innerArr.GetValue(1));
                            DR[BasisCol] = Convert.ToInt16(innerArr.GetValue(2));
                            DR[CurrentCol] = Convert.ToInt16(innerArr.GetValue(3));
                            DR[RequestCol] = Convert.ToInt16(innerArr.GetValue(4));
                            if (WithApproval)
                            {
                                DR[ApprovedCol] = Convert.ToInt16(innerArr.GetValue(5));
                                DR[PkCol] = PKValue;
                            }
                            else
                            {
                                DR[PkCol] = PKValue;
                            }
                            DT.Rows.Add(DR);
                            for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                            {
                                if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_FK"] == Convert.ToString(innerArr.GetValue(1)))
                                {
                                    sum += Convert.ToInt16(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt16(innerArr.GetValue(sumCol));
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
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        public static string GetOTHstring(DataTable DT, bool WithApproval, Int16 FreightCol = 0, Int16 CurrencyCol = 1, Int16 BasisCol = 2, Int16 CurrentCol = 3, Int16 RequestCol = 4, Int16 ApprovedCol = 5, Int16 PkCol = 6, string PkVal = "-1")
        {
            string functionReturnValue = null;
            try
            {
                Int16 RowCnt = default(Int16);
                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (DT.Rows[RowCnt][PkCol] == PkVal)
                    {
                        functionReturnValue += DT.Rows[RowCnt][FreightCol] + "~" + DT.Rows[RowCnt][CurrencyCol] + "~" + DT.Rows[RowCnt][BasisCol] + "~" + DT.Rows[RowCnt][CurrentCol] + "~" + DT.Rows[RowCnt][RequestCol] + (WithApproval == true ? "~" + DT.Rows[RowCnt][ApprovedCol] + "^" : "^");
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        #endregion " SHARED: Update Other Freight Table with supplied string "
    }
}