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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_bl_MultiSelect : CommonFeatures
    {
        WorkFlow objDAL = new WorkFlow();
        #region "variables"
        string RequerstStrings;

        public DataTable MyTable;
        #endregion

        #region "Fetch Called by Select Container/Sector"
        //This function returns all the active sectors from the database.
        //If the given POL and POD are present then the value will come as checked.
        public DataTable fn_ActiveSector(long LocationPk, string strPOLPk = "", string strPODPk = "")
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            Int16 i = default(Int16);
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = null;
            //Spliting the POL and POD Pk's
            arrPolPk = strPOLPk.Split(',');
            arrPodPk = strPODPk.Split(',');
            //Making condition as the record should have only selected POL and POD
            //POL and POD are on the same position in the array so nth element of arrPolPk and of arrPodPk
            //is the selected sector.
            for (i = 0; i <= arrPolPk.Length - 1; i++)
            {
                if (string.IsNullOrEmpty(strCondition))
                {
                    strCondition = " (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
                else
                {
                    strCondition += " OR (POL.PORT_MST_PK =" + arrPolPk.GetValue(i) + " AND POD.PORT_MST_PK =" + arrPodPk.GetValue(i) + ")";
                }
            }
            //Creating the sql if the user has already selected one port pair in calling form 
            //incase of veiwing also then that port pair will come and active port pair in the grid.
            //BUSINESS_TYPE = 2 :- Is the business type for SEA            
            strSQL.Append(" SELECT POL.PORT_MST_PK ,POL.PORT_ID AS \"POL\", ");
            strSQL.Append(" POD.PORT_MST_PK,POD.PORT_ID,'1' CHK ");
            strSQL.Append(" FROM PORT_MST_TBL POL, PORT_MST_TBL POD  ");
            strSQL.Append(" WHERE POL.PORT_MST_PK <> POD.PORT_MST_PK  ");
            strSQL.Append(" AND POL.BUSINESS_TYPE = 2 ");
            strSQL.Append(" AND POD.BUSINESS_TYPE = 2 ");
            strSQL.Append(" AND ( ");
            strSQL.Append(strCondition);
            strSQL.Append(" ) ");
            strSQL.Append(" UNION ");
            strSQL.Append("SELECT POL.PORT_MST_PK AS POLPK, POL.PORT_ID AS POL, ");
            strSQL.Append("POD.PORT_MST_PK AS PODPK, POD.PORT_ID POD, ");
            strSQL.Append("(CASE WHEN (" + strCondition + ") ");
            strSQL.Append("THEN '1' ELSE '0' END ) CHK ");
            strSQL.Append("FROM SECTOR_MST_TBL SMT, ");
            strSQL.Append("PORT_MST_TBL POL, ");
            strSQL.Append("PORT_MST_TBL POD, LOC_PORT_MAPPING_TRN LPM ");
            strSQL.Append("WHERE SMT.FROM_PORT_FK = POL.PORT_MST_PK ");
            strSQL.Append("AND   SMT.TO_PORT_FK = POD.PORT_MST_PK ");
            strSQL.Append("AND   LPM.PORT_MST_FK = POL.PORT_MST_PK ");
            strSQL.Append("AND   POL.BUSINESS_TYPE = 2 ");
            strSQL.Append("AND   POD.BUSINESS_TYPE = 2 ");
            strSQL.Append("AND   LPM.LOCATION_MST_FK =" + LocationPk);
            strSQL.Append("AND   SMT.ACTIVE = 1 ");
            strSQL.Append("ORDER BY CHK DESC,POL");
            try
            {
                return objDAL.GetDataTable(strSQL.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "fn_ActiveContainers"
        //This function returns all the active containers.
        //with '20GP','20RF','40GP','40RF','45GP' as default checked active containers.
        public DataTable fn_ActiveContainers()
        {
            System.Text.StringBuilder strSQL = new System.Text.StringBuilder();
            strSQL.Append(" SELECT ");
            strSQL.Append(" CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, ");
            strSQL.Append(" 0 CHK ");
            strSQL.Append(" FROM CONTAINER_TYPE_MST_TBL CMT ");
            strSQL.Append(" WHERE CMT.ACTIVE_FLAG=1  ");
            strSQL.Append(" ORDER BY CMT.PREFERENCES ");
            try
            {
                return objDAL.GetDataTable(strSQL.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion


        #region " Property "

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

        public string FlatRateString
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
                    outStr += "(" + innerArr.GetValue(0) + "," + innerArr.GetValue(1) + "," + innerArr.GetValue(2) + "),";
                }
                outStr = outStr.TrimEnd(',');
                return outStr;
            }
        }

        #endregion

        #region " Constructor "


        public void fn_getfrights(string str, Int16 BizType = 2, decimal ChargeableWeight = 1, bool AllInclusive = true)
        {
            RequerstStrings = str.TrimEnd('^');
            if (BizType == 2 | BizType == 4)
            {
                MyTable = FlatFreights(BizType);
            }
            else
            {
                MyTable = FlatFreights(BizType, ChargeableWeight, AllInclusive);
            }

        }

        #endregion

        #region " Add Freights "
        //modified by thiyagarajan on 29/11/08 for location based currency task
        //Session("CURRENCY_MST_PK") replaced Corporate currency task

        private DataTable FlatFreights(Int16 BizType, decimal ChargeableWeight, bool AllInclusive = true)
        {

            DataTable frtDt = null;
            DataTable exfrDt = null;
            DataRow DR = null;
            DataRow nDR = null;
            string strSQL = null;
            string frtStr = "-1";
            string BizTypeCond = "2,3";
            if (BizType == 1)
                BizTypeCond = "1,3";
            Int16 ColCnt = default(Int16);
            try
            {
                if (RequerstStrings.Trim().Length > 0)
                {
                    strSQL = " Select " + " 'true'      SEL,    FREIGHT_ELEMENT_MST_PK,     FREIGHT_ELEMENT_ID,     " + " CURRENCY_MST_PK,    CURRENCY_ID,                'Flat'  CHARGE_BASIS,   " + " 0 RATE,             0 AMOUNT,                                            " + " 'PrePaid' \"Pymt. Type\"  " + " from        FREIGHT_ELEMENT_MST_TBL,        CURRENCY_TYPE_MST_TBL       " + " where       FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG     =    1              " + " AND (FREIGHT_ELEMENT_MST_PK, CURRENCY_MST_PK ) in (" + FreightAndCurrency + ")" + " AND BUSINESS_TYPE in (" + BizTypeCond + ")                              " + " AND nvl(FREIGHT_TYPE,0) = 0                                             " + " AND BY_DEFAULT = 1                                                      " + " Order By FREIGHT_ELEMENT_ID                                             ";

                    frtDt = (new WorkFlow()).GetDataTable(strSQL);

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
                                frtDt.Rows[RowCnt]["RATE"] = Convert.ToInt32(arr2.GetValue(3));
                                if (Convert.ToInt32(arr2.GetValue(2)) == 2)
                                {
                                    frtDt.Rows[RowCnt]["AMOUNT"] = Convert.ToDouble(arr2.GetValue(3)).ToString(AmountFormat);
                                    //Val(arr2(3))
                                }
                                else
                                {
                                    frtDt.Rows[RowCnt]["AMOUNT"] = Convert.ToString(ChargeableWeight * Convert.ToDecimal(Convert.ToDouble(arr2.GetValue(3)).ToString(AmountFormat)));
                                    //Val(arr2(3))
                                }
                                if (arr2.Length > 4)
                                {
                                    frtDt.Rows[RowCnt]["Pymt. Type"] = (arr2.GetValue(4) == "2" ? "Collect" : "PrePaid");
                                }
                                else
                                {
                                    frtDt.Rows[RowCnt]["Pymt. Type"] = "PrePaid";
                                }
                                frtStr += "," + Convert.ToString(frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"]);
                            }
                        }
                    }

                    // Add other freights which are not already available in DataTable
                    if (AllInclusive == true)
                    {
                        strSQL = " Select " + " 'false'     SEL,            FREIGHT_ELEMENT_MST_PK,         FREIGHT_ELEMENT_ID,     " + " CURRENCY_MST_PK,            CURRENCY_ID,                                            " + " DECODE(CHARGE_BASIS,1,'%',2,'Flat','Kgs.') CHARGE_BASIS,    BASIS_VALUE RATE,       " + " DECODE(CHARGE_BASIS,2, BASIS_VALUE, BASIS_VALUE * " + ChargeableWeight + ") AMOUNT,  " + " 'PrePaid' \"Pymt. Type\"" + " from        FREIGHT_ELEMENT_MST_TBL,        CURRENCY_TYPE_MST_TBL                   " + " where       FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG     =   1                           " + " AND     FREIGHT_ELEMENT_MST_PK  not in (" + frtStr + ")                             " + " AND     BUSINESS_TYPE in (" + BizTypeCond + ")                                      " + " AND     CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " AND     nvl(FREIGHT_TYPE,0) = 0                                                     " + " AND     BY_DEFAULT  = 1                                                             " + " Order By FREIGHT_ELEMENT_ID                                                         ";

                        exfrDt = (new WorkFlow()).GetDataTable(strSQL);

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
                    // If No String is provided then prepare a default DataTable.
                    strSQL = " Select " + " 'false'     SEL,            FREIGHT_ELEMENT_MST_PK,         FREIGHT_ELEMENT_ID,     " + " CURRENCY_MST_PK,            CURRENCY_ID,                                            " + " DECODE(CHARGE_BASIS,1,'%',2,'Flat','Kgs.') CHARGE_BASIS,    BASIS_VALUE RATE,       " + " DECODE(CHARGE_BASIS,2, BASIS_VALUE, BASIS_VALUE * " + ChargeableWeight + ") AMOUNT,  " + " 'PrePaid' \"Pymt. Type\"  " + " from        FREIGHT_ELEMENT_MST_TBL,        CURRENCY_TYPE_MST_TBL                   " + " where       FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG     =   1                           " + " AND     BUSINESS_TYPE in (" + BizTypeCond + ")                                      " + " AND     CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " AND     nvl(FREIGHT_TYPE,0) = 0                                                     " + " AND     BY_DEFAULT  = 1                                                             " + " Order By FREIGHT_ELEMENT_ID                                                         ";

                    frtDt = (new WorkFlow()).GetDataTable(strSQL);

                }
                return frtDt;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception eX)
            {
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
        private DataTable FlatFreights(Int16 BizType)
        {
            //modified by thiyagarajan on 29/11/08 for location based currency task
            //Session("CURRENCY_MST_PK") replaced Corporate currency task
            DataTable frtDt = null;
            DataTable exfrDt = null;
            DataRow DR = null;
            DataRow nDR = null;
            string strSQL = null;
            string frtStr = "-1";
            string BizTypeCond = null;
            //modyfying by thiyagarajan on 14/1/09:VEK Req.
            if (BizType == 4)
                BizTypeCond = "1,2,3,4";
            if (BizType == 2)
                BizTypeCond = "2,3";
            if (BizType == 1)
                BizTypeCond = "1,3";
            //end
            Int16 ColCnt = default(Int16);
            try
            {
                if (RequerstStrings.Trim().Length > 0)
                {
                    strSQL = " Select " + " FREIGHT_ELEMENT_MST_PK, FREIGHT_ELEMENT_ID, " + " CURRENCY_MST_PK, CURRENCY_ID, '' AMOUNT, " + " 'PrePaid' \"Pymt. Type\"" + " from FREIGHT_ELEMENT_MST_TBL, CURRENCY_TYPE_MST_TBL " + " where FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1 " + " AND (FREIGHT_ELEMENT_MST_PK, CURRENCY_MST_PK ) in (" + FreightAndCurrency + ")" + " AND BUSINESS_TYPE in (" + BizTypeCond + ") " + " AND nvl(FREIGHT_TYPE,0) = 0 " + " AND BY_DEFAULT = 1 " + " Order By FREIGHT_ELEMENT_ID ";
                    //vimlesh kumar(20/04/2007) for other charges FREIGHT_TYPE should be nothing.
                    frtDt = (new WorkFlow()).GetDataTable(strSQL);

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
                            if (frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"] == arr2.GetValue(0) & frtDt.Rows[RowCnt]["CURRENCY_MST_PK"] == arr2.GetValue(1))
                            {
                                frtDt.Rows[RowCnt]["AMOUNT"] = Convert.ToDouble(arr2.GetValue(2)).ToString(AmountFormat);
                                frtDt.Rows[RowCnt]["Pymt. Type"] = (arr2.GetValue(3) == "2" ? "Collect" : "PrePaid");
                                frtStr += "," + Convert.ToString(frtDt.Rows[RowCnt]["FREIGHT_ELEMENT_MST_PK"]);
                            }
                        }
                    }
                    strSQL = " Select " + " FREIGHT_ELEMENT_MST_PK, FREIGHT_ELEMENT_ID, " + " CURRENCY_MST_PK, CURRENCY_ID, '' AMOUNT, " + " 'PrePaid' \"Pymt. Type\"" + " from FREIGHT_ELEMENT_MST_TBL, CURRENCY_TYPE_MST_TBL " + " where FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1 " + " AND FREIGHT_ELEMENT_MST_PK not in (" + frtStr + ")" + " AND BUSINESS_TYPE in (" + BizTypeCond + ") " + " AND CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " AND nvl(FREIGHT_TYPE,0) = 0 " + " AND BY_DEFAULT = 1 " + " Order By FREIGHT_ELEMENT_ID ";
                    exfrDt = (new WorkFlow()).GetDataTable(strSQL);
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
                else
                {
                    strSQL = " Select " + " FREIGHT_ELEMENT_MST_PK, FREIGHT_ELEMENT_ID, " + " CURRENCY_MST_PK, CURRENCY_ID, '' AMOUNT, " + " 'PrePaid' \"Pymt. Type\"" + " from FREIGHT_ELEMENT_MST_TBL, CURRENCY_TYPE_MST_TBL " + " where FREIGHT_ELEMENT_MST_TBL.ACTIVE_FLAG = 1 " + " AND CURRENCY_MST_PK = " + HttpContext.Current.Session["CURRENCY_MST_PK"] + " AND BUSINESS_TYPE in (" + BizTypeCond + ") " + " AND nvl(FREIGHT_TYPE,0) = 0 " + " AND BY_DEFAULT = 1 " + " Order By FREIGHT_ELEMENT_ID ";
                    frtDt = (new WorkFlow()).GetDataTable(strSQL);
                }
                return frtDt;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception eX)
            {
                throw eX;
            }
        }

        #endregion

        #region "Active container & Basis"

        public DataTable ActiveContainers()
        {
            string Str = null;
            WorkFlow objWF = new WorkFlow();
            string DefaultContainers = null;
            Str = "SELECT " + "CMT.CONTAINER_TYPE_MST_PK, CMT.CONTAINER_TYPE_MST_ID, 'false' CHK  " + "FROM CONTAINER_TYPE_MST_TBL CMT " + "WHERE CMT.ACTIVE_FLAG=1  " + "ORDER BY CMT.PREFERENCES ";
            try
            {
                DataTable dt = objWF.GetDataTable(Str);
                return dt;
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public DataTable AcitveDimentions()
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            strSQL = "";
            strSQL = "SELECT " + "  UOM.DIMENTION_UNIT_MST_PK, " + "  UOM.DIMENTION_ID, " + "  '0' CHK " + "FROM " + "  DIMENTION_UNIT_MST_TBL UOM " + "WHERE " + "  UOM.ACTIVE = 1 " + "ORDER BY " + "  UOM.DIMENTION_ID ";
            try
            {
                return objWF.GetDataTable(strSQL);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion


        #region "fn_FetchSectors"
        public DataSet fn_FetchSectors(Int32 CurrentPage = 0, Int32 TotalPage = 0, string strWhereCondition = "", string hdnPks = "0", string Biztype = "3", string Process = "", long LocFk = 0)
        {

            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select ROWNUM SLNR, qry.*");
                sb.Append("  from ");
                sb.Append("  ( ");
                sb.Append(" SELECT S.SECTOR_MST_PK,");
                sb.Append("       S.SECTOR_ID,");
                sb.Append("       S.SECTOR_DESC,");
                sb.Append("       S.FROM_PORT_FK,");
                sb.Append("       FP.PORT_ID POL,");
                sb.Append("       S.TO_PORT_FK,");
                sb.Append("       TP.PORT_ID POD,");
                sb.Append("                       '' EXTRA1,");
                sb.Append("                       '' EXTRA2,");
                sb.Append("                       '0' SEL");
                sb.Append("  FROM SECTOR_MST_TBL S, PORT_MST_TBL FP, PORT_MST_TBL TP");
                sb.Append(" WHERE S.FROM_PORT_FK = FP.PORT_MST_PK");
                sb.Append("   AND S.TO_PORT_FK = TP.PORT_MST_PK");
                sb.Append("   AND S.BUSINESS_TYPE  IN (3," + Biztype + ")  AND S.SECTOR_MST_PK NOT IN (" + hdnPks + ")  ");
                sb.Append("  UNION ");
                sb.Append(" SELECT S.SECTOR_MST_PK,");
                sb.Append("       S.SECTOR_ID,");
                sb.Append("       S.SECTOR_DESC,");
                sb.Append("       S.FROM_PORT_FK,");
                sb.Append("       FP.PORT_ID POL,");
                sb.Append("       S.TO_PORT_FK,");
                sb.Append("       TP.PORT_ID POD,");
                sb.Append("                       '' EXTRA1,");
                sb.Append("                       '' EXTRA2,");
                sb.Append("                       '1' SEL");
                sb.Append("  FROM SECTOR_MST_TBL S, PORT_MST_TBL FP, PORT_MST_TBL TP");
                sb.Append(" WHERE S.FROM_PORT_FK = FP.PORT_MST_PK");
                sb.Append("   AND S.TO_PORT_FK = TP.PORT_MST_PK");
                sb.Append("   AND S.BUSINESS_TYPE  IN (3," + Biztype + ")  AND S.SECTOR_MST_PK IN (" + hdnPks + ")  ");

                sb.Append("  ) qry");


                ///'''''''''''''''common''''''''''''''''''''
                //Get the Total Pages
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string StrSqlCount = null;

                StrSqlCount = "SELECT COUNT(*) FROM ( ";
                StrSqlCount = StrSqlCount + sb.ToString();
                StrSqlCount = StrSqlCount + " ) ";

                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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

                ///'''''''''''''''''''''''''''''''''''''''''''''''''''
                ///'''''''''''''''''''''''''''common''''''''''''''''''''''''''''''''''
                string StrSqlRecords = null;
                StrSqlRecords = "SELECT * FROM ( ";
                StrSqlRecords = StrSqlRecords + sb.ToString();
                StrSqlRecords = StrSqlRecords + " ) WHERE SLNR BETWEEN " + start + " AND " + last;
                return objWF.GetDataSet(StrSqlRecords);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region "fn_FetchCustomers"
        public DataSet fn_FetchCustomers(Int32 CurrentPage = 0, Int32 TotalPage = 0, string strWhereCondition = "", string hdnPks = "", string Biztype = "3", string Process = "", long LocFk = 0)
        {
            try
            {
                hdnPks = hdnPks.TrimEnd(',');
                if (string.IsNullOrEmpty(hdnPks))
                {
                    hdnPks = "0";
                }
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select ROWNUM SLNR, qry.*");
                sb.Append("  from ");
                sb.Append("  ( ");
                if (LocFk == 0)
                {
                    sb.Append(" SELECT * FROM ");
                    sb.Append("( SELECT ");
                    sb.Append(" CUST.CUSTOMER_MST_PK,");
                    sb.Append(" CUST.CUSTOMER_ID,");
                    sb.Append(" CUST.CUSTOMER_NAME,");
                    sb.Append("                       '' EXTRA1,");
                    sb.Append("                       '' EXTRA2,");
                    sb.Append("                       '' EXTRA3,");
                    sb.Append("                       '' EXTRA4,");
                    sb.Append("                       '' EXTRA5,");
                    sb.Append("                       '' EXTRA6,");
                    sb.Append("                       '0' SEL");
                    sb.Append(" FROM ");
                    sb.Append(" CUSTOMER_MST_TBL CUST ,");
                    sb.Append(" CUSTOMER_CATEGORY_TRN CR ");
                    sb.Append(" WHERE ");
                    sb.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    sb.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                    sb.Append(" AND CUST.ACTIVE_FLAG =1 ");
                    sb.Append(" AND CUST.Temp_Party <> 1");
                    sb.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                    sb.Append(" UNION ");
                    sb.Append(" SELECT");
                    sb.Append(" CUST.CUSTOMER_MST_PK,");
                    sb.Append(" CUST.CUSTOMER_ID,");
                    sb.Append(" CUST.CUSTOMER_NAME,");
                    sb.Append("                       '' EXTRA1,");
                    sb.Append("                       '' EXTRA2,");
                    sb.Append("                       '' EXTRA3,");
                    sb.Append("                       '' EXTRA4,");
                    sb.Append("                       '' EXTRA5,");
                    sb.Append("                       '' EXTRA6,");
                    sb.Append("                       '1' SEL");
                    sb.Append(" FROM ");
                    sb.Append(" CUSTOMER_MST_TBL CUST,");
                    sb.Append(" CUSTOMER_CATEGORY_TRN CR ");
                    sb.Append(" WHERE");
                    sb.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    sb.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ")");
                    if (hdnPks == null)
                    {
                        sb.Append(" AND CUST.CUSTOMER_MST_PK IN (0)");
                        sb.Append(" AND CUST.ACTIVE_FLAG =1 ");
                        sb.Append(" AND CUST.Temp_Party <> 1");
                        sb.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                        sb.Append(" ) T ORDER BY CUSTOMER_ID,CHK DESC ");
                    }
                    else
                    {
                        sb.Append(" AND CUST.CUSTOMER_MST_PK IN (" + hdnPks + ") ");
                        sb.Append(" AND CUST.ACTIVE_FLAG =1");
                        sb.Append(" AND CUST.Temp_Party <> 1");
                        sb.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                        sb.Append(" ) T  ORDER BY SEL DESC,CUSTOMER_ID ");
                    }
                }
                else
                {
                    sb.Append(" SELECT * FROM ( ");
                    sb.Append("  SELECT ");
                    sb.Append(" CUST.CUSTOMER_MST_PK,");
                    sb.Append(" CUST.CUSTOMER_ID,");
                    sb.Append(" CUST.CUSTOMER_NAME,");
                    sb.Append("                       '' EXTRA1,");
                    sb.Append("                       '' EXTRA2,");
                    sb.Append("                       '' EXTRA3,");
                    sb.Append("                       '' EXTRA4,");
                    sb.Append("                       '' EXTRA5,");
                    sb.Append("                       '' EXTRA6,");
                    sb.Append("                       '0' SEL");
                    sb.Append(" FROM ");
                    sb.Append(" CUSTOMER_MST_TBL      CUST,");
                    sb.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                    sb.Append(" CUSTOMER_CONTACT_DTLS CTDL ");
                    sb.Append(" WHERE ");
                    sb.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    sb.Append(" CUST.BUSINESS_TYPE IN (3," + Biztype + ") AND ");
                    sb.Append(" CUST.CUSTOMER_MST_PK NOT IN (0, " + hdnPks + ") AND");
                    sb.Append(" CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK AND ");
                    sb.Append(" CTDL.ADM_LOCATION_MST_FK in (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" + LocFk + ") UNION SELECT (" + LocFk + ") AS LOCATION_MST_FK from DUAL) ");
                    //(" & LocFk & ")
                    sb.Append(" AND CUST.ACTIVE_FLAG = 1");
                    sb.Append(" AND CUST.TEMP_PARTY <> 1");
                    sb.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                    sb.Append(" UNION");
                    sb.Append(" SELECT ");
                    sb.Append(" CUST.CUSTOMER_MST_PK,");
                    sb.Append(" CUST.CUSTOMER_ID,");
                    sb.Append(" CUST.CUSTOMER_NAME,");
                    sb.Append("                       '' EXTRA1,");
                    sb.Append("                       '' EXTRA2,");
                    sb.Append("                       '' EXTRA3,");
                    sb.Append("                       '' EXTRA4,");
                    sb.Append("                       '' EXTRA5,");
                    sb.Append("                       '' EXTRA6,");
                    sb.Append("                       '1' SEL");
                    sb.Append(" FROM");
                    sb.Append(" CUSTOMER_MST_TBL CUST, ");
                    sb.Append(" CUSTOMER_CATEGORY_TRN CR ,");
                    sb.Append(" CUSTOMER_CONTACT_DTLS CTDL");
                    sb.Append(" WHERE");
                    sb.Append(" CUST.CUSTOMER_MST_PK = CR.CUSTOMER_MST_FK  AND ");
                    sb.Append(" CUST.BUSINESS_TYPE IN ( 3," + Biztype + ")");
                    if (Convert.ToInt32(hdnPks) == 0 | string.IsNullOrEmpty(hdnPks))
                    {
                        sb.Append(" AND CUST.CUSTOMER_MST_PK  IN (0) ");
                    }
                    else
                    {
                        sb.Append(" AND CUST.CUSTOMER_MST_PK IN (" + hdnPks + " ) ");
                    }
                    sb.Append(" AND CTDL.CUSTOMER_MST_FK = CUST.CUSTOMER_MST_PK ");
                    sb.Append(" AND CTDL.ADM_LOCATION_MST_FK in (SELECT PM.LOCATION_MST_FK FROM LOC_PORT_MAPPING_TRN LPM, PORT_MST_TBL PM WHERE LPM.PORT_MST_FK=PM.PORT_MST_PK AND LPM.LOCATION_MST_FK = (" + LocFk + ") UNION SELECT (" + LocFk + ") AS LOCATION_MST_FK from DUAL) ");
                    //(" & LocFk & ")
                    sb.Append(" AND CUST.ACTIVE_FLAG = 1");
                    sb.Append(" AND CUST.TEMP_PARTY <> 1");
                    sb.Append(" AND CR.CUSTOMER_CATEGORY_MST_FK =(SELECT CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL T  WHERE CUSTOMER_CATEGORY_ID LIKE'" + Process + "')");
                    sb.Append(" ) T  ORDER BY SEL DESC,CUSTOMER_ID ");
                }
                sb.Append("  ) qry");
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string StrSqlCount = null;
                StrSqlCount = "SELECT COUNT(*) FROM ( ";
                StrSqlCount = StrSqlCount + sb.ToString();
                StrSqlCount = StrSqlCount + " ) ";
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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
                string StrSqlRecords = null;
                StrSqlRecords = "SELECT * FROM ( ";
                StrSqlRecords = StrSqlRecords + sb.ToString();
                StrSqlRecords = StrSqlRecords + " ) WHERE SLNR BETWEEN " + start + " AND " + last;
                return objWF.GetDataSet(StrSqlRecords);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        #endregion

        #region " Freight Element Function "
        public DataSet fn_FetchFrtEle(Int32 CurrentPage = 0, Int32 TotalPage = 0, string strWhereCondition = "", string hdnPks = "", string Biztype = "3", string Process = "", long LocFk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            hdnPks = hdnPks.TrimEnd(',');
            if (string.IsNullOrEmpty(hdnPks))
            {
                hdnPks = "0";
            }
            sb.Append("SELECT FRT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("               FRT.FREIGHT_ELEMENT_ID,");
            sb.Append("               FRT.FREIGHT_ELEMENT_NAME,");
            sb.Append("               '' EXTRA1,");
            sb.Append("               '' EXTRA2,");
            sb.Append("               '' EXTRA3,");
            sb.Append("               '' EXTRA4,");
            sb.Append("               '' EXTRA5,");
            sb.Append("               '' EXTRA6,");
            sb.Append("               '1' SEL");
            sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL FRT");
            sb.Append("         WHERE FRT.ACTIVE_FLAG = 1");
            sb.Append(strWhereCondition);
            sb.Append("           AND FRT.FREIGHT_ELEMENT_MST_PK IN (" + hdnPks + ")");
            sb.Append("        UNION");
            sb.Append("        SELECT FRT.FREIGHT_ELEMENT_MST_PK,");
            sb.Append("               FRT.FREIGHT_ELEMENT_ID,");
            sb.Append("               FRT.FREIGHT_ELEMENT_NAME,");
            sb.Append("               '' EXTRA1,");
            sb.Append("               '' EXTRA2,");
            sb.Append("               '' EXTRA3,");
            sb.Append("               '' EXTRA4,");
            sb.Append("               '' EXTRA5,");
            sb.Append("               '' EXTRA6,");
            sb.Append("               '0' SEL");
            sb.Append("          FROM FREIGHT_ELEMENT_MST_TBL FRT");
            sb.Append("         WHERE FRT.ACTIVE_FLAG = 1");
            sb.Append(strWhereCondition);
            sb.Append("           AND FRT.FREIGHT_ELEMENT_MST_PK NOT IN (" + hdnPks + ")");
            sb.Append("         ORDER BY SEL DESC, FREIGHT_ELEMENT_ID");
            try
            {
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string StrSqlCount = null;
                StrSqlCount = "SELECT COUNT(*) FROM ( ";
                StrSqlCount = StrSqlCount + sb.ToString();
                StrSqlCount = StrSqlCount + " ) ";
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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
                string StrSqlRecords = null;
                StrSqlRecords = "SELECT * FROM (SELECT ROWNUM SLNR, QRY.* FROM( ";
                StrSqlRecords = StrSqlRecords + sb.ToString();
                StrSqlRecords = StrSqlRecords + " )QRY ) WHERE SLNR BETWEEN " + start + " AND " + last;
                return objWF.GetDataSet(StrSqlRecords);
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

        #region " Container Number Function "
        public DataSet fn_FetchContNr(Int32 CurrentPage = 0, Int32 TotalPage = 0, string strWhereCondition = "", string hdnPks = "", string Biztype = "3", string Process = "", long LocFk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            hdnPks = hdnPks.TrimEnd(',');
            if (string.IsNullOrEmpty(hdnPks))
            {
                hdnPks = "0";
            }
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            //Modified by Faheem
            //sb.Append("SELECT CONT.ON_HIRE_CONT_PK,")
            //sb.Append("               CONT.CONT_NO,")
            //sb.Append("               CONTTYPE.CONTAINER_TYPE_MST_ID,")
            //sb.Append("               '' EXTRA1,")
            //sb.Append("               '' EXTRA2,")
            //sb.Append("               '' EXTRA3,")
            //sb.Append("               '' EXTRA4,")
            //sb.Append("               '' EXTRA5,")
            //sb.Append("               '' EXTRA6,")
            //sb.Append("               '1' SEL")
            //sb.Append("          FROM WO_ONHIRE_CONT_DTL CONT, CONTAINER_TYPE_MST_TBL CONTTYPE")
            //sb.Append("         WHERE CONT.CONT_TYPE_MST_FK = CONTTYPE.CONTAINER_TYPE_MST_PK")
            //sb.Append(strWhereCondition)
            //sb.Append("         ORDER BY CONT.CONT_NO")
            sb.Append("SELECT MAX(CONT.ON_HIRE_CONT_PK)ON_HIRE_CONT_PK,");
            sb.Append("               CONT.CONT_NO,");
            sb.Append("               CONTTYPE.CONTAINER_TYPE_MST_ID,");
            sb.Append("               '' EXTRA1,");
            sb.Append("               '' EXTRA2,");
            sb.Append("               '' EXTRA3,");
            sb.Append("               '' EXTRA4,");
            sb.Append("               '' EXTRA5,");
            sb.Append("               '' EXTRA6,");
            sb.Append("               '1' SEL");
            sb.Append("          FROM WO_ONHIRE_CONT_DTL CONT, CONTAINER_TYPE_MST_TBL CONTTYPE");
            sb.Append("         WHERE CONT.CONT_TYPE_MST_FK = CONTTYPE.CONTAINER_TYPE_MST_PK");
            sb.Append(strWhereCondition);
            sb.Append("         GROUP BY CONT.CONT_NO,CONTTYPE.CONTAINER_TYPE_MST_ID,CONTTYPE.Preferences  ");
            //sb.Append("         ORDER BY CONT.CONT_NO")
            sb.Append("          ORDER BY CONTTYPE.Preferences");
            try
            {
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                Int32 TotalRecords = default(Int32);
                string StrSqlCount = null;
                StrSqlCount = "SELECT COUNT(*) FROM ( ";
                StrSqlCount = StrSqlCount + sb.ToString();
                StrSqlCount = StrSqlCount + " ) ";
                TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(StrSqlCount.ToString()));
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
                string StrSqlRecords = null;
                StrSqlRecords = "SELECT * FROM (SELECT ROWNUM SLNR, QRY.* FROM( ";
                StrSqlRecords = StrSqlRecords + sb.ToString();
                StrSqlRecords = StrSqlRecords + " )QRY ) WHERE SLNR BETWEEN " + start + " AND " + last;
                return objWF.GetDataSet(StrSqlRecords);
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
    }
}