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
using System;
using System.Data;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsVenderAgeingReport : CommonFeatures
    {
        /// <summary>
        /// Gets the location.
        /// </summary>
        /// <param name="userLocPK">The user loc pk.</param>
        /// <param name="strALL">The string all.</param>
        /// <returns></returns>
        public DataSet GetLocation(string userLocPK, string strALL)
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            string strReturn = null;
            WorkFlow objWF = new WorkFlow();
            OracleDataReader dr = null;
            try
            {
                strQuery.Append("");
                strQuery.Append("   SELECT '<ALL>' LOCATION_ID, ");
                strQuery.Append("       0 LOCATION_MST_PK, ");
                strQuery.Append("       0 REPORTING_TO_FK, ");
                strQuery.Append("       0 LOCATION_TYPE_FK ");
                strQuery.Append("  FROM DUAL ");
                strQuery.Append("UNION ");
                strQuery.Append(" SELECT L.LOCATION_ID, ");
                strQuery.Append("       L.LOCATION_MST_PK, ");
                strQuery.Append("       L.REPORTING_TO_FK, ");
                strQuery.Append("       L.LOCATION_TYPE_FK ");
                strQuery.Append("  FROM LOCATION_MST_TBL L ");
                strQuery.Append(" START WITH L.LOCATION_TYPE_FK = 1 ");
                strQuery.Append("        AND L.ACTIVE_FLAG = 1 ");
                strQuery.Append("        AND L.LOCATION_MST_PK =" + userLocPK);
                strQuery.Append(" CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK ");
                dr = objWF.GetDataReader(strQuery.ToString());
                while (dr.Read())
                {
                    strReturn += dr["LOCATION_MST_PK"] + "~$";
                }
                dr.Close();
                if (strReturn == "0~$")
                {
                    strQuery = new System.Text.StringBuilder();

                    strQuery.Append(" SELECT L.LOCATION_ID, ");
                    strQuery.Append("       L.LOCATION_MST_PK, ");
                    strQuery.Append("       L.REPORTING_TO_FK, ");
                    strQuery.Append("       L.LOCATION_TYPE_FK ");
                    strQuery.Append("  FROM LOCATION_MST_TBL L ");
                    strQuery.Append("  WHERE L.LOCATION_MST_PK = " + userLocPK);
                    strQuery.Append("UNION ");
                    strQuery.Append(" SELECT L.LOCATION_ID, ");
                    strQuery.Append("       L.LOCATION_MST_PK, ");
                    strQuery.Append("       L.REPORTING_TO_FK, ");
                    strQuery.Append("       L.LOCATION_TYPE_FK ");
                    strQuery.Append("  FROM LOCATION_MST_TBL L ");
                    strQuery.Append("  WHERE L.REPORTING_TO_FK = " + userLocPK);
                    dr = objWF.GetDataReader(strQuery.ToString());
                    while (dr.Read())
                    {
                        strReturn += dr["LOCATION_MST_PK"] + "~$";
                    }
                    dr.Close();
                }

                strALL = strReturn;
                return objWF.GetDataSet(strQuery.ToString());
                //Manjunath  PTS ID:Sep-02  26/09/2011
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

        #region "Function to check whether a user is an administrator or not"                  ' Added by Prakash Chandra on 29/05/2008

        // Returns true if the user is an administrator
        // Else returns false
        /// <summary>
        /// Determines whether the specified string user identifier is administrator.
        /// </summary>
        /// <param name="strUserID">The string user identifier.</param>
        /// <returns></returns>
        public bool IsAdministrator(string strUserID)
        {
            string strSQL = null;
            Int16 Admin = default(Int16);
            WorkFlow objWF = new WorkFlow();

            strSQL = "SELECT COUNT(*) FROM User_Mst_Tbl U WHERE U.ROLE_MST_FK = ";
            strSQL = strSQL + "(SELECT R.ROLE_MST_TBL_PK FROM ROLE_MST_TBL R WHERE R.ROLE_ID = 'ADMIN')";
            strSQL = strSQL + "AND U.USER_MST_PK = " + HttpContext.Current.Session["USER_PK"];
            try
            {
                Admin = Convert.ToInt16(objWF.ExecuteScaler(strSQL.ToString()));

                if (Admin == 1)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch (OracleException sqlExp)
            {
                throw sqlExp;
            }
            catch (Exception exp)
            {
                throw exp;
            }
        }

        #endregion "Function to check whether a user is an administrator or not"                  ' Added by Prakash Chandra on 29/05/2008

        #region "GetVendorAgeingRpt"

        /// <summary>
        /// Gets the vendor ageing RPT.
        /// </summary>
        /// <param name="Process">The process.</param>
        /// <param name="asOnDate">As on date.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="intRow1">The int row1.</param>
        /// <param name="intRow2">The int row2.</param>
        /// <param name="intRow3">The int row3.</param>
        /// <param name="intRow4">The int row4.</param>
        /// <param name="intRow5">The int row5.</param>
        /// <param name="intRow6">The int row6.</param>
        /// <param name="intRow7">The int row7.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <param name="lngCountryPk">The LNG country pk.</param>
        /// <param name="supplierPk">The supplier pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="podpk">The podpk.</param>
        /// <param name="lngLocationPk">The LNG location pk.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <returns></returns>
        public object GetVendorAgeingRpt(int Process, System.DateTime asOnDate, int bizType, string intRow1, string intRow2, string intRow3, string intRow4, string intRow5, string intRow6, string intRow7,
        Int32 Cargotype, string lngCountryPk = "", string supplierPk = "", string PolPk = "", string podpk = "", string lngLocationPk = "", string CurrPK = "")
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            string strCondition = null;
            if (!string.IsNullOrEmpty(lngCountryPk) & lngCountryPk != "0")
            {
                strCondition = strCondition + " AND COU.COUNTRY_MST_PK = " + lngCountryPk;
            }
            if (!string.IsNullOrEmpty(supplierPk) & supplierPk != "0")
            {
                strCondition = strCondition + " AND VND.VENDOR_MST_PK = " + supplierPk;
            }
            if (!string.IsNullOrEmpty(PolPk) & PolPk != "0")
            {
                strCondition = strCondition + " And POL.PORT_MST_PK = " + PolPk;
            }
            if (!string.IsNullOrEmpty(podpk) & podpk != "0")
            {
                strCondition = strCondition + " And POD.PORT_MST_PK = " + podpk;
            }

            strSQL = " SELECT distinct QRY.INVOICE_REF_NO  SUPPLIER_REF_NO,QRY. VENDOR_NAME  VENDER_NAME,";
            strSQL += " QRY.LOCATION_NAME  LOCATION_NAME,";
            strSQL += " (CASE WHEN QRY.INVDATE<='" + intRow1 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL11,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow2 + "' AND QRY.INVDATE<='" + intRow3 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL12,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow4 + "' AND QRY.INVDATE<='" + intRow5 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL13,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow6 + "' AND QRY.INVDATE<='" + intRow7 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL14,";
            strSQL += " (CASE WHEN QRY.INVDATE>'" + intRow7 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL15,";
            strSQL += " QRY.CREATED_DT";
            strSQL += " FROM  ";
            strSQL += "(SELECT distinct ";
            strSQL += " LOC.LOCATION_NAME,";
            strSQL += "  ROUND(INV.INVOICE_AMT * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE), 2) INVOICE_AMT,";
            strSQL += " VND.VENDOR_NAME,";
            strSQL += " INV.INVOICE_REF_NO, ";
            strSQL += " (TO_DATE('" + asOnDate + "','dd/MM/yyyy')-TO_DATE(INV.SUPPLIER_DUE_DT,'dd/MM/yyyy')) INVDATE, ";
            strSQL += "  INV.CREATED_DT ";
            strSQL += "  FROM  VENDOR_MST_TBL VND,";
            strSQL += "  INV_SUPPLIER_TBL INV, ";

            strSQL += "  PORT_MST_TBL POL, ";
            strSQL += "  PORT_MST_TBL POD, ";
            strSQL += "  INV_SUPPLIER_TRN_TBL  INVTRNTBL,";
            strSQL += " COUNTRY_MST_TBL COU,";
            strSQL += "location_mst_tbl loc,";

            if (Process == 2 & bizType == 2)
            {
                strSQL += "JOB_CARD_SEA_IMP_TBL  JOB_EXP,    ";
                strSQL += "JOB_TRN_SEA_IMP_PIA   JOB_TRN_PIA,";
            }
            if (Process == 2 & bizType == 1)
            {
                strSQL += "JOB_CARD_AIR_IMP_TBL  JOB_EXP,    ";
                strSQL += "JOB_TRN_AIR_IMP_PIA   JOB_TRN_PIA,";
            }
            if (Process == 1 & bizType == 2)
            {
                strSQL += "JOB_CARD_SEA_EXP_TBL  JOB_EXP,Booking_Sea_Tbl book, ";
                strSQL += "JOB_TRN_SEA_EXP_PIA   JOB_TRN_PIA,";
            }
            if (Process == 1 & bizType == 1)
            {
                strSQL += "JOB_CARD_AIR_EXP_TBL JOB_EXP,Booking_Air_Tbl book,";
                strSQL += "JOB_TRN_AIR_EXP_PIA   JOB_TRN_PIA,";
            }
            strSQL += " USER_MST_TBL   USRTBL";
            strSQL += "   WHERE INV.VENDOR_MST_FK=VND.VENDOR_MST_PK";
            strSQL += "  AND USRTBL.USER_MST_PK = INV.CREATED_BY_FK";

            if (Process == 1)
            {
                strSQL += " AND JOB_EXP.BOOKING_SEA_FK=BOOK.BOOKING_SEA_PK(+)";
                strSQL += " AND BOOK.PORT_MST_POL_FK=POL.PORT_MST_PK(+) AND BOOK.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ";
            }
            else
            {
                strSQL += " AND job_exp.PORT_MST_POL_FK=POL.PORT_MST_PK(+) AND job_exp.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ";
            }

            strSQL += " and inv.inv_supplier_pk= INVTRNTBL.INV_SUPPLIER_TBL_FK";
            if (Process == 2 & bizType == 2)
            {
                strSQL += " and JOB_EXP.CARGO_TYPE=" + Cargotype;
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_SEA_IMP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_SEA_IMP_FK =JOB_EXP.JOB_CARD_SEA_IMP_PK(+) ";
            }
            if (Process == 2 & bizType == 1)
            {
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_AIR_IMP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_AIR_IMP_FK =JOB_EXP.JOB_CARD_AIR_IMP_PK(+) ";
            }
            if (Process == 1 & bizType == 2)
            {
                strSQL += " and book.cargo_type= " + Cargotype;
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_SEA_EXP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_SEA_EXP_FK =JOB_EXP.JOB_CARD_SEA_EXP_PK(+) ";
            }
            if (Process == 1 & bizType == 1)
            {
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_AIR_EXP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_AIR_EXP_FK =JOB_EXP.JOB_CARD_AIR_EXP_PK(+) ";
            }

            if (bizType > 0)
            {
                strSQL += "  AND vnd.business_type=" + bizType;
            }

            if (Process > 0)
            {
                strSQL += "  AND inv.process_type=" + Process;
            }

            strSQL += "and INVTRNTBL.Element_Approved=1";
            strSQL += "and loc.location_mst_pk(+)=USRTBL.DEFAULT_LOCATION_FK";
            strSQL += " and loc.country_mst_fk=cou.country_mst_pk ";

            if ((Convert.ToInt32(lngLocationPk) > 0))
            {
                strSQL += " and USRTBL.DEFAULT_LOCATION_FK =" + lngLocationPk;
            }

            strSQL += strCondition;
            strSQL += " ORDER BY CREATED_DT DESC)QRY  ";

            try
            {
                if (bizType == 1)
                {
                    strSQL = strSQL.ToUpper().Replace("SEA", "AIR");
                }
                if (Process == 2)
                {
                    strSQL = strSQL.ToUpper().Replace("EXP", "IMP");
                }

                System.Text.StringBuilder sqlstr1 = new System.Text.StringBuilder();
                sqlstr1.Append("SELECT ROWNUM \"SLNR\", T.*  FROM ");
                sqlstr1.Append("  (" + strSQL.ToString() + " ");
                sqlstr1.Append("  ) T");

                return objWF.GetDataSet(sqlstr1.ToString());
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

        #endregion "GetVendorAgeingRpt"

        #region "GetVendorAgeingGrid"

        /// <summary>
        /// Gets the vendor ageing grid.
        /// </summary>
        /// <param name="Process">The process.</param>
        /// <param name="asOnDate">As on date.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="intRow1">The int row1.</param>
        /// <param name="intRow2">The int row2.</param>
        /// <param name="intRow3">The int row3.</param>
        /// <param name="intRow4">The int row4.</param>
        /// <param name="intRow5">The int row5.</param>
        /// <param name="intRow6">The int row6.</param>
        /// <param name="intRow7">The int row7.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <param name="lngCountryPk">The LNG country pk.</param>
        /// <param name="supplierPk">The supplier pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="podpk">The podpk.</param>
        /// <param name="lngLocationPk">The LNG location pk.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public object GetVendorAgeingGrid(int Process, System.DateTime asOnDate, int bizType, string intRow1, string intRow2, string intRow3, string intRow4, string intRow5, string intRow6, string intRow7,
        Int32 Cargotype, string lngCountryPk = "", string supplierPk = "", string PolPk = "", string podpk = "", string lngLocationPk = "", string CurrPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (!string.IsNullOrEmpty(lngCountryPk) & lngCountryPk != "0")
            {
                strCondition = strCondition + " AND COU.COUNTRY_MST_PK = " + lngCountryPk;
            }
            if (!string.IsNullOrEmpty(supplierPk) & supplierPk != "0")
            {
                strCondition = strCondition + " AND VND.VENDOR_MST_PK = " + supplierPk;
            }
            if (!string.IsNullOrEmpty(PolPk) & PolPk != "0")
            {
                strCondition = strCondition + " And POL.PORT_MST_PK = " + PolPk;
            }
            if (!string.IsNullOrEmpty(podpk) & podpk != "0")
            {
                strCondition = strCondition + " And POD.PORT_MST_PK = " + podpk;
            }

            strSQL = " SELECT distinct QRY. INVOICE_REF_NO  SUPPLIER_REF_NO,";
            strSQL += " QRY. VENDOR_NAME  VENDER_NAME,";
            strSQL += " QRY.LOCATION_NAME LOCATION_NAME,";
            strSQL += " (CASE WHEN QRY.INVDATE<='" + intRow1 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL11,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow2 + "' AND QRY.INVDATE<='" + intRow3 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL12,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow4 + "' AND QRY.INVDATE<='" + intRow5 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL13,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow6 + "' AND QRY.INVDATE<='" + intRow7 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL14,";
            strSQL += " (CASE WHEN QRY.INVDATE>'" + intRow7 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL15,";
            strSQL += " QRY.CREATED_DT ";
            strSQL += " FROM  ";
            strSQL += "(SELECT distinct ";
            strSQL += " LOC.LOCATION_NAME,";
            strSQL += "  ROUND(INV.INVOICE_AMT * GET_EX_RATE(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE), 2) INVOICE_AMT,";
            strSQL += " VND.VENDOR_NAME,";
            strSQL += " INV.INVOICE_REF_NO, ";
            strSQL += " (TO_DATE('" + asOnDate + "','dd/MM/yyyy')-TO_DATE(INV.SUPPLIER_DUE_DT,'dd/MM/yyyy')) INVDATE, ";
            strSQL += "  INV.CREATED_DT ";
            strSQL += "  FROM  VENDOR_MST_TBL VND,";
            strSQL += "  INV_SUPPLIER_TBL INV, ";
            strSQL += "  PORT_MST_TBL POL, ";
            strSQL += "  PORT_MST_TBL POD, ";
            strSQL += "  INV_SUPPLIER_TRN_TBL  INVTRNTBL,";
            strSQL += " COUNTRY_MST_TBL COU,";
            strSQL += "location_mst_tbl loc,";

            if (Process == 2 & bizType == 2)
            {
                strSQL += "JOB_CARD_SEA_IMP_TBL  JOB_EXP,    ";
                strSQL += "JOB_TRN_SEA_IMP_PIA   JOB_TRN_PIA,";
            }
            if (Process == 2 & bizType == 1)
            {
                strSQL += "JOB_CARD_AIR_IMP_TBL  JOB_EXP,    ";
                strSQL += "JOB_TRN_AIR_IMP_PIA   JOB_TRN_PIA,";
            }
            if (Process == 1 & bizType == 2)
            {
                strSQL += "JOB_CARD_SEA_EXP_TBL  JOB_EXP,Booking_Sea_Tbl book, ";
                strSQL += "JOB_TRN_SEA_EXP_PIA   JOB_TRN_PIA,";
            }
            if (Process == 1 & bizType == 1)
            {
                strSQL += "JOB_CARD_AIR_EXP_TBL JOB_EXP,Booking_Air_Tbl book,";
                strSQL += "JOB_TRN_AIR_EXP_PIA   JOB_TRN_PIA,";
            }
            strSQL += " USER_MST_TBL   USRTBL";

            strSQL += "   WHERE INV.VENDOR_MST_FK=VND.VENDOR_MST_PK";
            strSQL += "  AND USRTBL.USER_MST_PK = INV.CREATED_BY_FK";

            if (Process == 1)
            {
                strSQL += " AND JOB_EXP.BOOKING_SEA_FK=BOOK.BOOKING_SEA_PK(+)";
                strSQL += " AND BOOK.PORT_MST_POL_FK=POL.PORT_MST_PK(+) AND BOOK.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ";
            }
            else
            {
                strSQL += " AND job_exp.PORT_MST_POL_FK=POL.PORT_MST_PK(+) AND job_exp.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ";
            }

            strSQL += " and inv.inv_supplier_pk= INVTRNTBL.INV_SUPPLIER_TBL_FK";
            if (Process == 2 & bizType == 2)
            {
                strSQL += " and JOB_EXP.CARGO_TYPE=" + Cargotype;
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_SEA_IMP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_SEA_IMP_FK =JOB_EXP.JOB_CARD_SEA_IMP_PK(+) ";
            }
            if (Process == 2 & bizType == 1)
            {
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_AIR_IMP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_AIR_IMP_FK =JOB_EXP.JOB_CARD_AIR_IMP_PK(+) ";
            }
            if (Process == 1 & bizType == 2)
            {
                strSQL += " and book.cargo_type= " + Cargotype;
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_SEA_EXP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_SEA_EXP_FK =JOB_EXP.JOB_CARD_SEA_EXP_PK(+) ";
            }
            if (Process == 1 & bizType == 1)
            {
                strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_AIR_EXP_PIA_PK(+) ";
                strSQL += " AND JOB_TRN_PIA.JOB_CARD_AIR_EXP_FK =JOB_EXP.JOB_CARD_AIR_EXP_PK(+) ";
            }

            if (bizType > 0)
            {
                strSQL += "  AND vnd.business_type=" + bizType;
            }

            if (Process > 0)
            {
                strSQL += "  AND inv.process_type=" + Process;
            }

            strSQL += "and INVTRNTBL.Element_Approved=1";
            strSQL += "and loc.location_mst_pk(+)=USRTBL.DEFAULT_LOCATION_FK";
            strSQL += " and loc.country_mst_fk=cou.country_mst_pk ";

            if ((Convert.ToInt32(lngLocationPk) > 0))
            {
                strSQL += " and USRTBL.DEFAULT_LOCATION_FK =" + lngLocationPk;
            }

            if (flag == 0)
            {
                strSQL += " AND 1=2 ";
            }
            strSQL += strCondition;
            strSQL += " ORDER BY CREATED_DT DESC )QRY  ";
            try
            {
                if (bizType == 1)
                {
                    strSQL = strSQL.ToUpper().Replace("SEA", "AIR");
                }
                if (Process == 2)
                {
                    strSQL = strSQL.ToUpper().Replace("EXP", "IMP");
                }

                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append(("(" + strSQL.ToString() + ""));
                strCount.Append(" )");
                TotalRecords = Convert.ToInt32(ObjWk.ExecuteScaler(strCount.ToString()));

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

                sqlstr2.Append(" Select * from (");
                sqlstr2.Append(" SELECT ROWNUM SL_NO, q.*  FROM ( ");
                sqlstr2.Append("  (" + strSQL.ToString() + " ");

                sqlstr2.Append(" ) q )) ");
                sqlstr2.Append("   WHERE \"SL_NO\"  BETWEEN " + start + " AND " + last + "");
                strSQL = sqlstr2.ToString();
                return ObjWk.GetDataSet(strSQL);
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        /// <summary>
        /// Gets the vendor ageing grid1.
        /// </summary>
        /// <param name="Process">The process.</param>
        /// <param name="asOnDate">As on date.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="intRow1">The int row1.</param>
        /// <param name="intRow2">The int row2.</param>
        /// <param name="intRow3">The int row3.</param>
        /// <param name="intRow4">The int row4.</param>
        /// <param name="intRow5">The int row5.</param>
        /// <param name="intRow6">The int row6.</param>
        /// <param name="intRow7">The int row7.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <param name="lngCountryPk">The LNG country pk.</param>
        /// <param name="supplierPk">The supplier pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="podpk">The podpk.</param>
        /// <param name="lngLocationPk">The LNG location pk.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="ReportFlag">The report flag.</param>
        /// <returns></returns>
        public object GetVendorAgeingGrid1(int Process, System.DateTime asOnDate, int bizType, string intRow1, string intRow2, string intRow3, string intRow4, string intRow5, string intRow6, string intRow7,
        Int32 Cargotype, string lngCountryPk = "", string supplierPk = "", string PolPk = "", string podpk = "", string lngLocationPk = "", string CurrPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0,
        short ReportFlag = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSQL = "";
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            // biz type both and import-
            if (Process == 2 & bizType != 1 & bizType != 2)
            {
                //strSQL = GetVendorAgeingQuery(2, asOnDate, 1, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                //Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);

                strSQL += " UNION ";

                strSQL += GetVendorAgeingQuery(2, asOnDate, 2, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);
                // biz type both and export
            }
            else if (Process == 1 & bizType != 1 & bizType != 2)
            {
                //strSQL = GetVendorAgeingQuery(1, asOnDate, 1, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                //Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);

                strSQL += " UNION ";

                strSQL += GetVendorAgeingQuery(1, asOnDate, 2, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);
                // biz type Sea and both process
            }
            else if (Process != 1 & Process != 2 & bizType == 2)
            {
                //strSQL = GetVendorAgeingQuery(1, asOnDate, 2, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                //Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);

                strSQL += " UNION ";

                strSQL += GetVendorAgeingQuery(2, asOnDate, 2, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);
                // biz type Air and both process
            }
            else if (Process != 1 & Process != 2 & bizType == 1)
            {
                //strSQL = GetVendorAgeingQuery(1, asOnDate, 1, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                //Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);

                strSQL += " UNION ";

                strSQL += GetVendorAgeingQuery(2, asOnDate, 1, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);
                // biz type both and both process
            }
            else if (Process != 1 & Process != 2 & bizType != 1 & bizType != 2)
            {
                //strSQL = GetVendorAgeingQuery(2, asOnDate, 2, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                //Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);

                strSQL += " UNION ";

                strSQL += GetVendorAgeingQuery(2, asOnDate, 1, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);

                strSQL += " UNION ";

                strSQL += GetVendorAgeingQuery(1, asOnDate, 2, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);

                strSQL += " UNION ";

                strSQL += GetVendorAgeingQuery(1, asOnDate, 1, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);
            }
            else
            {
                //strSQL = GetVendorAgeingQuery(Process, asOnDate, bizType, intRow1, intRow2, intRow3, intRow4, intRow5, intRow6, intRow7,
                //Cargotype, lngCountryPk, supplierPk, PolPk, podpk, lngLocationPk, CurrPK, flag);
            }
            strSQL += " ORDER BY INVDATE DESC ";

            try
            {
                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                if (ReportFlag == 1)
                {
                    sqlstr2.Append(" Select * from ( SELECT ROWNUM SL_NO, q.*  FROM ( ");
                    sqlstr2.Append("  (" + strSQL.ToString() + " ");
                    sqlstr2.Append(" ) q ) where (q.col11 > 0 or q.COL12 > 0 or q.COL13 > 0 or q.COL14 > 0 or q.col15 > 0) ) ");
                }
                else
                {
                    strCount.Append(" SELECT COUNT(*)  from ( SELECT q.*  FROM (");
                    strCount.Append(("(" + strSQL.ToString() + ""));
                    strCount.Append(" ))q where (q.col11 > 0 or q.COL12 > 0 or q.COL13 > 0 or q.COL14 > 0 or q.col15 > 0))");
                    TotalRecords = Convert.ToInt32(ObjWk.ExecuteScaler(strCount.ToString()));
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

                    sqlstr2.Append(" Select * from ( SELECT ROWNUM SL_NO, q.*  FROM ( ");
                    sqlstr2.Append("  (" + strSQL.ToString() + " ");
                    sqlstr2.Append(" )) q  where (q.col11 > 0 or q.COL12 > 0 or q.COL13 > 0 or q.COL14 > 0 or q.col15 > 0)) ");
                    sqlstr2.Append("   WHERE \"SL_NO\"  BETWEEN " + start + " AND " + last + "");
                }

                strSQL = sqlstr2.ToString();
                return ObjWk.GetDataSet(strSQL);
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        /// <summary>
        /// Gets the vendor ageing query.
        /// </summary>
        /// <param name="Process">The process.</param>
        /// <param name="asOnDate">As on date.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="intRow1">The int row1.</param>
        /// <param name="intRow2">The int row2.</param>
        /// <param name="intRow3">The int row3.</param>
        /// <param name="intRow4">The int row4.</param>
        /// <param name="intRow5">The int row5.</param>
        /// <param name="intRow6">The int row6.</param>
        /// <param name="intRow7">The int row7.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <param name="lngCountryPk">The LNG country pk.</param>
        /// <param name="supplierPk">The supplier pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="podpk">The podpk.</param>
        /// <param name="lngLocationPk">The LNG location pk.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public object GetVendorAgeingQuery(int Process, System.DateTime asOnDate, int bizType, string intRow1, string intRow2, string intRow3, string intRow4, string intRow5, string intRow6, string intRow7,
        Int32 Cargotype, string lngCountryPk = "", string supplierPk = "", string PolPk = "", string podpk = "", string lngLocationPk = "", string CurrPK = "", Int32 flag = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (!string.IsNullOrEmpty(lngCountryPk) & lngCountryPk != "0")
            {
                strCondition = strCondition + " AND COU.COUNTRY_MST_PK = " + lngCountryPk;
            }
            if (!string.IsNullOrEmpty(supplierPk) & supplierPk != "0")
            {
                strCondition = strCondition + " AND VND.VENDOR_MST_PK = " + supplierPk;
            }
            if (!string.IsNullOrEmpty(PolPk) & PolPk != "0")
            {
                strCondition = strCondition + " And POL.PORT_MST_PK = " + PolPk;
            }
            if (!string.IsNullOrEmpty(podpk) & podpk != "0")
            {
                strCondition = strCondition + " And POD.PORT_MST_PK = " + podpk;
            }

            strSQL = " SELECT distinct QRY. INVOICE_REF_NO  SUPPLIER_REF_NO,";
            strSQL += " QRY. INV_SUPPLIER_PK  INV_SUPPLIER_PK,";
            strSQL += " QRY. BUSINESS_TYPE  BUSINESS_TYPE,";
            strSQL += " QRY. PROCESS_TYPE  PROCESS_TYPE,";
            strSQL += " QRY. VENDOR_NAME  VENDER_NAME,";
            strSQL += " QRY. INVOICE_DATE INVDATE,";
            strSQL += " QRY.LOCATION_NAME LOCATION_NAME,";
            strSQL += " (CASE WHEN QRY.INVDATE<='" + intRow1 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL11,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow2 + "' AND QRY.INVDATE<='" + intRow3 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL12,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow4 + "' AND QRY.INVDATE<='" + intRow5 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL13,";
            strSQL += " (CASE WHEN QRY.INVDATE>='" + intRow6 + "' AND QRY.INVDATE<='" + intRow7 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL14,";
            strSQL += " (CASE WHEN QRY.INVDATE>'" + intRow7 + "' THEN QRY.INVOICE_AMT ELSE 0 END ) COL15,";
            strSQL += " QRY.CREATED_DT ";
            strSQL += " FROM  ";
            strSQL += "(SELECT distinct ";
            strSQL += " LOC.LOCATION_NAME,";
            strSQL += "  ROUND(INV.INVOICE_AMT * GET_EX_RATE_BUY(INV.CURRENCY_MST_FK," + CurrPK + ", INV.INVOICE_DATE), 2) INVOICE_AMT,";
            strSQL += " VND.VENDOR_NAME,";
            strSQL += " INV.INVOICE_REF_NO, ";
            strSQL += " INV.INV_SUPPLIER_PK, ";
            strSQL += " INV.BUSINESS_TYPE, ";
            strSQL += " INV.PROCESS_TYPE, ";
            strSQL += " TO_DATE(INV.INVOICE_DATE, DATEFORMAT) INVOICE_DATE,";
            strSQL += " (TO_DATE('" + asOnDate + "','dd/MM/yyyy')-TO_DATE(INV.SUPPLIER_DUE_DT,'dd/MM/yyyy')) INVDATE, ";
            strSQL += "  INV.CREATED_DT ";
            strSQL += "  FROM  VENDOR_MST_TBL VND,";
            strSQL += "  INV_SUPPLIER_TBL INV, ";
            strSQL += "  PORT_MST_TBL POL, ";
            strSQL += "  PORT_MST_TBL POD, ";
            strSQL += "  INV_SUPPLIER_TRN_TBL  INVTRNTBL,";
            strSQL += " COUNTRY_MST_TBL COU,";
            strSQL += "location_mst_tbl loc,";

            if (Process == 2)
            {
                strSQL += "JOB_CARD_TRN  JOB_EXP,    ";
                strSQL += "JOB_TRN_PIA   JOB_TRN_PIA,";
            }
            if (Process == 1)
            {
                strSQL += "JOB_CARD_TRN  JOB_EXP,Booking_mst_Tbl book, ";
                strSQL += "JOB_TRN_PIA   JOB_TRN_PIA,";
            }
            strSQL += " USER_MST_TBL   USRTBL";

            strSQL += "   WHERE INV.VENDOR_MST_FK=VND.VENDOR_MST_PK";
            strSQL += "  AND USRTBL.USER_MST_PK = INV.CREATED_BY_FK";

            if (Process == 1)
            {
                strSQL += " AND JOB_EXP.BOOKING_MST_FK=BOOK.BOOKING_MST_PK(+)";
                strSQL += " AND BOOK.PORT_MST_POL_FK=POL.PORT_MST_PK(+) AND BOOK.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ";
            }
            else
            {
                strSQL += " AND job_exp.PORT_MST_POL_FK=POL.PORT_MST_PK(+) AND job_exp.PORT_MST_POD_FK=POD.PORT_MST_PK(+) ";
            }

            strSQL += " and inv.inv_supplier_pk= INVTRNTBL.INV_SUPPLIER_TBL_FK";
            strSQL += " AND INVTRNTBL.JOB_CARD_PIA_FK =  JOB_TRN_PIA.JOB_TRN_PIA_PK ";
            strSQL += " AND JOB_TRN_PIA.JOB_CARD_TRN_FK =JOB_EXP.JOB_CARD_TRN_PK(+)";
            if (Cargotype > 0)
            {
                if (Process == 2 & bizType == 2)
                {
                    strSQL += " and JOB_EXP.CARGO_TYPE=" + Cargotype;
                }
                if (Process == 1 & bizType == 2)
                {
                    strSQL += " and book.cargo_type= " + Cargotype;
                }
            }
            if (bizType > 0)
            {
                strSQL += "  AND JOB_EXP.business_type=" + bizType;
            }

            if (Process > 0)
            {
                strSQL += "  AND JOB_EXP.process_type=" + Process;
            }
            strSQL += " and INVTRNTBL.Element_Approved=1 ";
            strSQL += " and loc.location_mst_pk(+)=USRTBL.DEFAULT_LOCATION_FK";
            strSQL += " and loc.country_mst_fk=cou.country_mst_pk ";

            if (Convert.ToInt32(lngLocationPk) > 0)
            {
                strSQL += " and USRTBL.DEFAULT_LOCATION_FK =" + lngLocationPk;
            }

            if (flag == 0)
            {
                strSQL += " AND 1=2 ";
            }
            strSQL += strCondition;
            try
            {
                //If bizType = 1 Then
                //    strSQL = strSQL.ToUpper().Replace("SEA", "AIR")
                //End If
                //If Process = 2 Then
                //    strSQL = strSQL.ToUpper().Replace("EXP", "IMP")
                //End If

                strSQL = strSQL + ") QRY ";
            }
            catch (Exception ex)
            {
            }
            return strSQL;
        }

        #endregion "GetVendorAgeingGrid"
    }
}