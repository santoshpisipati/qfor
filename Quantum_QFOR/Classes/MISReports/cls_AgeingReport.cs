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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsAgeingReport : CommonFeatures
    {
        #region "Fetch Function"

        /// <summary>
        /// Fetches the ageing.
        /// </summary>
        /// <param name="strCustomerPk">The string customer pk.</param>
        /// <param name="lngLocationPk">The LNG location pk.</param>
        /// <param name="lngCountryPk">The LNG country pk.</param>
        /// <param name="PolPk">The pol pk.</param>
        /// <param name="podpk">The podpk.</param>
        /// <param name="asOnDate">As on date.</param>
        /// <param name="Process">The process.</param>
        /// <param name="bizType">Type of the biz.</param>
        /// <param name="intRow1">The int row1.</param>
        /// <param name="intRow2">The int row2.</param>
        /// <param name="intRow3">The int row3.</param>
        /// <param name="intRow4">The int row4.</param>
        /// <param name="intRow5">The int row5.</param>
        /// <param name="intRow6">The int row6.</param>
        /// <param name="intRow7">The int row7.</param>
        /// <param name="GrpHdr">if set to <c>true</c> [GRP HDR].</param>
        /// <param name="cargotype">The cargotype.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="BaseCurr">The base curr.</param>
        /// <param name="GrpPKs">The GRP p ks.</param>
        /// <param name="JobType">Type of the job.</param>
        /// <param name="InvRefNr">The inv reference nr.</param>
        /// <returns></returns>
        public DataSet FetchAgeing(string strCustomerPk, string lngLocationPk, string lngCountryPk, string PolPk, string podpk, System.DateTime asOnDate, int Process, int bizType, string intRow1, string intRow2,
        string intRow3, string intRow4, string intRow5, string intRow6, string intRow7, bool GrpHdr, Int32 cargotype, Int32 CurrentPage = 0, Int32 TotalPage = 0, Int64 BaseCurr = 0,
        string GrpPKs = "", int JobType = 0, long InvRefNr = 0)
        {
            WorkFlow objWF = new WorkFlow();

            DataSet dsAgn = new DataSet();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            try
            {
                objWF.MyCommand.Parameters.Clear();
                var _with1 = objWF.MyCommand.Parameters;
                //_with1.Add("LOCPK_IN", (string.IsNullOrEmpty(lngLocationPk) ? 0 : lngLocationPk)).Direction = ParameterDirection.Input;
                _with1.Add("COUNTRYPK_IN", (string.IsNullOrEmpty(lngCountryPk) ? "" : lngCountryPk)).Direction = ParameterDirection.Input;
                _with1.Add("CUSTOMERPK_IN", (string.IsNullOrEmpty(strCustomerPk) ? "" : strCustomerPk)).Direction = ParameterDirection.Input;
                _with1.Add("POLPK_IN", (string.IsNullOrEmpty(PolPk) ? "" : PolPk)).Direction = ParameterDirection.Input;
                _with1.Add("PODPK_IN", (string.IsNullOrEmpty(podpk) ? "" : podpk)).Direction = ParameterDirection.Input;
                _with1.Add("AS_ON_DATE_IN", asOnDate).Direction = ParameterDirection.Input;
                _with1.Add("PROCESS_IN", Process).Direction = ParameterDirection.Input;
                _with1.Add("BIZ_TYPE_IN", bizType).Direction = ParameterDirection.Input;
                if (GrpHdr == true)
                {
                    _with1.Add("GRPPK_IN", (string.IsNullOrEmpty(GrpPKs) ? "" : GrpPKs)).Direction = ParameterDirection.Input;
                }
                _with1.Add("BASE_CURR", BaseCurr).Direction = ParameterDirection.Input;
                //Manoharan 04May09: for Basecurrency in this pkg
                _with1.Add("ROW1_IN", (string.IsNullOrEmpty(intRow1) ? "" : intRow1)).Direction = ParameterDirection.Input;
                _with1.Add("ROW2_IN", (string.IsNullOrEmpty(intRow2) ? "" : intRow2)).Direction = ParameterDirection.Input;
                _with1.Add("ROW3_IN", (string.IsNullOrEmpty(intRow3) ? "" : intRow3)).Direction = ParameterDirection.Input;
                _with1.Add("ROW4_IN", (string.IsNullOrEmpty(intRow4) ? "" : intRow4)).Direction = ParameterDirection.Input;
                _with1.Add("ROW5_IN", (string.IsNullOrEmpty(intRow5) ? "" : intRow5)).Direction = ParameterDirection.Input;
                _with1.Add("ROW6_IN", (string.IsNullOrEmpty(intRow6) ? "" : intRow6)).Direction = ParameterDirection.Input;
                _with1.Add("ROW7_IN", (string.IsNullOrEmpty(intRow7) ? "" : intRow7)).Direction = ParameterDirection.Input;
                _with1.Add("CARGO_TYPE_IN", getDefault(cargotype, "")).Direction = ParameterDirection.Input;
                _with1.Add("JOB_TYPE_IN", JobType).Direction = ParameterDirection.Input;
                _with1.Add("INVOICE_REF_PK_IN", getDefault(InvRefNr, "")).Direction = ParameterDirection.Input;
                _with1.Add("COUNTRY_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("LOCATION_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                if (GrpHdr == true)
                {
                    _with1.Add("CUST_GRP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                }
                _with1.Add("CUSTOMER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("PORTPAIR_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Add("INVOICE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                if (GrpHdr == true)
                {
                    dsAgn = objWF.GetDataSet("FETCH_AGEING_PKG", "FETCH_AGEING_CUST_GRP_ALL");
                }
                else
                {
                    dsAgn = objWF.GetDataSet("FETCH_AGEING_PKG", "FETCH_AGEINGALL");
                }
                TotalRecords = dsAgn.Tables[0].Rows.Count;
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
                CreateRelation(dsAgn, GrpHdr);
                return dsAgn;
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

        #endregion "Fetch Function"

        #region "Create Relation"

        /// <summary>
        /// Creates the relation.
        /// </summary>
        /// <param name="dsAgn">The ds agn.</param>
        /// <param name="GrpHdr">if set to <c>true</c> [GRP HDR].</param>
        private void CreateRelation(DataSet dsAgn, bool GrpHdr)
        {
            DataRelation drCountry = null;
            DataRelation drLocation = null;
            DataRelation drCustomer = null;
            DataRelation drPort = null;
            DataRelation drCustGrpHdr = null;
            try
            {
                drCountry = new DataRelation("COUNTRY", dsAgn.Tables[0].Columns["COUNTRY_NAME"], dsAgn.Tables[1].Columns["COUNTRY_NAME"]);

                drLocation = new DataRelation("LOCATION", new DataColumn[] {
                    dsAgn.Tables[1].Columns["COUNTRY_NAME"],
                    dsAgn.Tables[1].Columns["LOCATION_NAME"]
                }, new DataColumn[] {
                    dsAgn.Tables[2].Columns["COUNTRY_NAME"],
                    dsAgn.Tables[2].Columns["LOCATION_NAME"]
                });

                if (GrpHdr == true)
                {
                    drCustGrpHdr = new DataRelation("CUSTGRPHDR", new DataColumn[] {
                        dsAgn.Tables[2].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[2].Columns["LOCATION_NAME"],
                        dsAgn.Tables[2].Columns["GRP_HDR_PK"]
                    }, new DataColumn[] {
                        dsAgn.Tables[3].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[3].Columns["LOCATION_NAME"],
                        dsAgn.Tables[3].Columns["GRP_HDR_PK"]
                    });

                    drCustomer = new DataRelation("CUSTOMER", new DataColumn[] {
                        dsAgn.Tables[3].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[3].Columns["LOCATION_NAME"],
                        dsAgn.Tables[3].Columns["GRP_HDR_PK"],
                        dsAgn.Tables[3].Columns["CUSTOMER_NAME"]
                    }, new DataColumn[] {
                        dsAgn.Tables[4].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[4].Columns["LOCATION_NAME"],
                        dsAgn.Tables[4].Columns["GRP_HDR_PK"],
                        dsAgn.Tables[4].Columns["CUSTOMER_NAME"]
                    });
                    drPort = new DataRelation("PORT", new DataColumn[] {
                        dsAgn.Tables[4].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[4].Columns["LOCATION_NAME"],
                        dsAgn.Tables[4].Columns["GRP_HDR_PK"],
                        dsAgn.Tables[4].Columns["CUSTOMER_NAME"],
                        dsAgn.Tables[4].Columns["POL"],
                        dsAgn.Tables[4].Columns["POD"]
                    }, new DataColumn[] {
                        dsAgn.Tables[5].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[5].Columns["LOCATION_NAME"],
                        dsAgn.Tables[5].Columns["GRP_HDR_PK"],
                        dsAgn.Tables[5].Columns["CUSTOMER_NAME"],
                        dsAgn.Tables[5].Columns["POL"],
                        dsAgn.Tables[5].Columns["POD"]
                    });
                }
                else
                {
                    drCustomer = new DataRelation("CUSTOMER", new DataColumn[] {
                        dsAgn.Tables[2].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[2].Columns["LOCATION_NAME"],
                        dsAgn.Tables[2].Columns["CUSTOMER_NAME"]
                    }, new DataColumn[] {
                        dsAgn.Tables[3].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[3].Columns["LOCATION_NAME"],
                        dsAgn.Tables[3].Columns["CUSTOMER_NAME"]
                    });

                    drPort = new DataRelation("PORT", new DataColumn[] {
                        dsAgn.Tables[3].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[3].Columns["LOCATION_NAME"],
                        dsAgn.Tables[3].Columns["CUSTOMER_NAME"],
                        dsAgn.Tables[3].Columns["POL"],
                        dsAgn.Tables[3].Columns["POD"]
                    }, new DataColumn[] {
                        dsAgn.Tables[4].Columns["COUNTRY_NAME"],
                        dsAgn.Tables[4].Columns["LOCATION_NAME"],
                        dsAgn.Tables[4].Columns["CUSTOMER_NAME"],
                        dsAgn.Tables[4].Columns["POL"],
                        dsAgn.Tables[4].Columns["POD"]
                    });
                }

                drCountry.Nested = true;
                drLocation.Nested = true;
                drCustomer.Nested = true;
                drPort.Nested = true;

                if (GrpHdr == true)
                {
                    drCustGrpHdr.Nested = true;
                }
                dsAgn.Relations.Add(drCountry);
                dsAgn.Relations.Add(drLocation);

                if (GrpHdr == true)
                {
                    dsAgn.Relations.Add(drCustGrpHdr);
                }

                dsAgn.Relations.Add(drCustomer);
                dsAgn.Relations.Add(drPort);
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

        #endregion "Create Relation"

        #region "Get Location"

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
                    strQuery.Append("SELECT '<ALL>' LOCATION_ID,");
                    strQuery.Append("       0 LOCATION_MST_PK,");
                    strQuery.Append("       0 REPORTING_TO_FK,");
                    strQuery.Append("       0 LOCATION_TYPE_FK");
                    strQuery.Append("  FROM DUAL");
                    strQuery.Append(" UNION");
                    strQuery.Append("  SELECT L.LOCATION_ID,");
                    strQuery.Append("       L.LOCATION_MST_PK,");
                    strQuery.Append("       L.REPORTING_TO_FK,");
                    strQuery.Append("       L.LOCATION_TYPE_FK");
                    strQuery.Append("  FROM LOCATION_MST_TBL L");
                    strQuery.Append("  where L.ACTIVE_FLAG = 1");
                    strQuery.Append("  and   L.LOCATION_MST_PK   in");
                    strQuery.Append("  (SELECT L.LOCATION_MST_PK FROM LOCATION_MST_TBL L    ");
                    strQuery.Append("   START WITH L.LOCATION_MST_PK = " + userLocPK);
                    strQuery.Append("   CONNECT BY PRIOR L.LOCATION_MST_PK=L.REPORTING_TO_FK )");

                    dr = objWF.GetDataReader(strQuery.ToString());
                    while (dr.Read())
                    {
                        strReturn += dr["LOCATION_MST_PK"] + "~$";
                    }
                    dr.Close();
                }
                strALL = strReturn;
                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "Get Location"
    }
}