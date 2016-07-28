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
using System;
using System.Collections;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    public class clsDetentionTariff :CommonFeatures
    {
        #region "This function returns the Depot contract tarrif for perticular DepotID and the cargoType"

        //This function returns the Depot contract tarrif for perticular DepotID and the cargoType
        public DataTable FetchDepotData(string fromdate, string Todate = "", string depotID = "", string cargoType = "", string tariffDate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            //This datatable contains the active containers
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Int16 j = default(Int16);
            Int16 k = default(Int16);

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            if ((depotID != null))
            {
                strCondition += " AND dmt.vendor_mst_pk = " + depotID ;
            }

            if ((cargoType != null))
            {
                strCondition += " AND depot.cargo_type = " + cargoType ;
            }

            //Making query with the condition added 

            if (Convert.ToInt32(cargoType )== 1)
            {
                str =  "SELECT";
                str += "        trn.cont_trn_depot_pk,";
                str += "        trn.from_day, ";
                str += "        trn.to_day ";
                str += " FROM   cont_trn_depot_fcl_lcl trn,";
                str += "        cont_main_depot_tbl depot,";
                str += "        vendor_mst_tbl dmt";
                str += " WHERE  1=1 ";
                str += "        AND dmt.vendor_mst_pk = depot.depot_mst_fk";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2  ";
                //str &= vbCrLf & "        to_date('" & fromdate & "','" & dateFormat & "') between to_date('" & fromdate & "','" & dateFormat & "') and valid_to "
                //If Not Todate Is Nothing And Todate <> "" Then
                //    str &= vbCrLf & "        and valid_to <to_date('" & Todate & "','mm/dd/yyyy')"
                //End If
                // str &= vbCrLf & "        TO_DATE('" & tariffDate & "','" & dateFormat & "') BETWEEN depot.valid_from "
                // str &= vbCrLf & "        AND depot.valid_to "   'only for sea...
                //Modified By Mani

                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM " + "AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";

                str += strCondition;
                str += " ORDER BY from_day ";
            }
            else
            {
                str =  "SELECT";
                str += "        trn.cont_trn_depot_pk,";
                str += "        trn.from_day, ";
                str += "        trn.to_day, ";
                str += "        trn.lcl_volume, ";
                str += "        trn.lcl_weight, ";
                str += "        trn.lcl_amount ";

                str += " FROM   cont_trn_depot_fcl_lcl trn,";
                str += "        cont_main_depot_tbl depot,";
                str += "        vendor_mst_tbl dmt";
                str += " WHERE  1=1 ";
                str += "        AND dmt.vendor_mst_pk = depot.depot_mst_fk";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2 ";

                //AND TO_DATE('" & tariffDate & "','" & dateFormat & "') BETWEEN depot.valid_from AND depot.valid_to " 'only for sea...
                //Modified by gopi
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM " + "AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";

                str += strCondition;

                str += " ORDER BY from_day ";

            }

            try
            {
                dtMain = objWF.GetDataTable(str);

                if (Convert.ToInt32(cargoType) == 1)
                {
                    dtContainerType = FetchDepotContainerDataFCL(depotID, cargoType, fromdate);
                    //For loop: Transposes the rows returned by FetchDepotContainerDataFCL into the columns 
                    //of header in dtMain datatable

                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        if (!dtMain.Columns.Contains(dtContainerType.Rows[i][1].ToString()))
                        {
                            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
                            dtMain.Columns.Add(dcCol);
                        }

                    }

                    //This loops for all the THC rates..

                    for (i = 0; i <= dtMain.Rows.Count - 1; i++)
                    {
                        DataRow[] drCollection = dtContainerType.Select("cont_trn_depot_pk=" + dtMain.Rows[i]["cont_trn_depot_pk"]);

                        //This loops through the container types..
                        for (j = 0; j <= drCollection.Length - 1; j++)
                        {
                            //Container types starts from column 4, its index is 3

                            for (k = 3; k <= dtMain.Columns.Count - 1; k++)
                            {
                                if (drCollection[j]["CONTAINER_TYPE_MST_ID"] == dtMain.Columns[k].ColumnName)
                                {
                                    dtMain.Rows[i][k] = drCollection[j]["fcl_current_rate"];
                                }

                            }
                        }
                    }

                }
                return dtMain;
                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        public DataTable FetchDepotDataCont(string fromdate, string Todate = "", string ContractPK = "", string cargoType = "", string tariffDate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            //This datatable contains the active containers
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Int16 j = default(Int16);
            Int16 k = default(Int16);

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            if ((ContractPK != null))
            {
                strCondition += " AND depot.cont_main_depot_pk = " + ContractPK ;
            }

            if ((cargoType != null))
            {
                strCondition += " AND depot.cargo_type = " + cargoType ;
            }

            //Making query with the condition added

            if (Convert.ToInt32(cargoType) == 1)
            {
                str =  "SELECT";
                str += "        trn.cont_trn_depot_pk,";
                str += "        trn.from_day, ";
                str += "        trn.to_day ";
                str += " FROM   cont_trn_depot_fcl_lcl trn,";
                str += "        cont_main_depot_tbl depot";
                str += " WHERE  1=1 ";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2  ";
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM ";
                str += "        AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += strCondition;
                str += " ORDER BY from_day ";
                //modifying by thiyagarajan on 7/2/09
            }
            else
            {
                str =  "SELECT";
                str += "        trn.cont_trn_depot_pk,";
                str += "        trn.from_day, ";
                str += "        trn.to_day, ";
                //str &= vbCrLf & "        upper(trn.lcl_volume), "
                //str &= vbCrLf & "        upper(trn.lcl_weight), "
                //str &= vbCrLf & "        upper(trn.lcl_amount),upper(trn.lcl_rate_palette)" 'added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates

                str += "        trn.lcl_volume, ";
                str += "        trn.lcl_weight, ";
                str += "        trn.lcl_amount,trn.lcl_rate_palette";
                //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates

                str += " FROM   cont_trn_depot_fcl_lcl trn,";
                str += "        cont_main_depot_tbl depot";
                str += " WHERE  1=1 ";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2 ";
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM ";
                str += "        AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += strCondition;
                str += " ORDER BY from_day ";
            }

            try
            {
                dtMain = objWF.GetDataTable(str);

                if (Convert.ToInt32(cargoType) == 1)
                {
                    dtContainerType = FetchDepotContainerDataFCLCont(ContractPK, cargoType, fromdate);
                    //For loop: Transposes the rows returned by FetchDepotContainerDataFCL into the columns 
                    //of header in dtMain datatable

                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        if (!dtMain.Columns.Contains(dtContainerType.Rows[i][1].ToString()))
                        {
                            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
                            dtMain.Columns.Add(dcCol);
                        }

                    }

                    //This loops for all the THC rates..

                    for (i = 0; i <= dtMain.Rows.Count - 1; i++)
                    {
                        DataRow[] drCollection = dtContainerType.Select("cont_trn_depot_pk=" + dtMain.Rows[i]["cont_trn_depot_pk"]);

                        //This loops through the container types..
                        for (j = 0; j <= drCollection.Length - 1; j++)
                        {
                            //Container types starts from column 4, its index is 3

                            for (k = 3; k <= dtMain.Columns.Count - 1; k++)
                            {
                                if (drCollection[j]["CONTAINER_TYPE_MST_ID"] == dtMain.Columns[k].ColumnName)
                                {
                                    dtMain.Rows[i][k] = drCollection[j]["fcl_current_rate"];
                                }

                            }
                        }
                    }

                }
                return dtMain;
                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
        public DataTable FetchDepotDataOth(string fromdate, string Todate = "", string depotID = "", string cargoType = "", string tariffDate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            //This datatable contains the active containers
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Int16 j = default(Int16);
            Int16 k = default(Int16);

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            if ((depotID != null))
            {
                strCondition += " AND dmt.vendor_mst_pk = " + depotID ;
            }

            if ((cargoType != null))
            {
                strCondition += " AND depot.cargo_type = " + cargoType ;
            }

            //Making query with the condition added
            //Gopi  Ok added mode
            if (Convert.ToInt32(cargoType) == 1)
            {
                str =  "SELECT DISTINCT";
                str += "        trn.cont_depot_oth_chg_pk,";
                str += "        trn.cost_element_mst_fk, ";
                str += "        'true' as SEL, ";
                str += "        C.COST_ELEMENT_ID as \"Other Charges\" ";
                str += " FROM   CONT_TRN_DEPOT_OTH_CHG trn,";
                str += "        cont_main_depot_tbl depot,";
                str += "        vendor_mst_tbl dmt, COST_ELEMENT_MST_TBL c ";
                str += " WHERE  1=1 ";
                str += "        AND C.COST_ELEMENT_MST_PK(+) = TRN.COST_ELEMENT_MST_FK ";
                str += "        AND dmt.vendor_mst_pk = depot.depot_mst_fk";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2  ";
                //str &= vbCrLf & "        to_date('" & fromdate & "','" & dateFormat & "') between to_date('" & fromdate & "','" & dateFormat & "') and valid_to "
                //If Not Todate Is Nothing And Todate <> "" Then
                //    str &= vbCrLf & "        and valid_to <to_date('" & Todate & "','mm/dd/yyyy')"
                //End If
                // str &= vbCrLf & "        TO_DATE('" & tariffDate & "','" & dateFormat & "') BETWEEN depot.valid_from "
                // str &= vbCrLf & "        AND depot.valid_to "   'only for sea...
                //Modified By Mani

                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM " + "AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";

                str += strCondition;
                str += "  UNION";
                str += "  SELECT null CONT_DEPOT_OTH_CHG_PK,";
                str += "  C.COST_ELEMENT_MST_PK,";
                str += "  'false' as \"Sel\",";
                str += "  C.COST_ELEMENT_ID as \"Other Charges\" ";
                str += "  FROM COST_ELEMENT_MST_TBL   C,";
                str += "  cont_main_depot_tbl    depot,";
                str += "  CONT_TRN_DEPOT_OTH_CHG CON,";
                str += "  VENDOR_TYPE_MST_TBL    V";
                str += "  WHERE C.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK";
                str += "  AND CON.COST_ELEMENT_MST_FK(+) = C.COST_ELEMENT_MST_PK";
                str += "  AND con.cont_main_depot_fk = depot.cont_main_depot_pk";
                str += "  AND C.BUSINESS_TYPE in (3,2)";
                str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
                str += "  and c.cost_element_mst_pk not in";
                str += "  (SELECT CHG.COST_ELEMENT_MST_FK";
                str += "  FROM CONT_TRN_DEPOT_OTH_CHG CHG,";
                str += "  cont_main_depot_tbl    CCC,";
                str += "  vendor_mst_tbl         VEN";
                str += "  WHERE CHG.CONT_MAIN_DEPOT_FK = CCC.CONT_MAIN_DEPOT_PK";
                str += "  AND CCC.DEPOT_MST_FK = VEN.VENDOR_MST_PK";
                str += "  AND VEN.VENDOR_MST_PK =  " + depotID + "";
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM " + "AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "  AND ccc.cargo_type = 1)";

                // ok
            }
            else
            {
                str =  "SELECT";
                str += "        trn.cont_depot_oth_chg_pk,";
                str += "        trn.cost_element_mst_fk,  ";
                str += "        'true' as SEL,";
                str += "        C.COST_ELEMENT_ID as \"Other Charges\", ";
                str += "        trn.lcl_rate_per_cbm, ";
                str += "        trn.lcl_rate_per_ton ";
                // str &= vbCrLf & "        ,trn.air_rate_per_100kg "
                str += " FROM   CONT_TRN_DEPOT_OTH_CHG trn,";
                str += "        cont_main_depot_tbl depot,";
                str += "         vendor_mst_tbl dmt, COST_ELEMENT_MST_TBL c ";
                str += " WHERE  1=1 ";
                str += "        AND C.COST_ELEMENT_MST_PK(+) = TRN.COST_ELEMENT_MST_FK ";
                str += "        AND dmt.vendor_mst_pk = depot.depot_mst_fk";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2 ";

                //AND TO_DATE('" & tariffDate & "','" & dateFormat & "') BETWEEN depot.valid_from AND depot.valid_to " 'only for sea...
                //Modified by gopi
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM " + "AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";

                str += strCondition;

                str += "  UNION";
                str += "  SELECT null CONT_DEPOT_OTH_CHG_PK,";
                str += "  C.COST_ELEMENT_MST_PK,";
                str += "  'false' as \"Sel\",";
                str += "  C.COST_ELEMENT_ID as \"Other Charges\", ";
                str += "  null lcl_rate_per_cbm,";
                str += "  null lcl_rate_per_ton";
                //str &= vbCrLf & "  ,null air_rate_per_100kg"
                str += "  FROM COST_ELEMENT_MST_TBL   C,";
                str += "  cont_main_depot_tbl    depot,";
                str += "  CONT_TRN_DEPOT_OTH_CHG CON,";
                str += "  VENDOR_TYPE_MST_TBL    V";
                str += "  WHERE C.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK";
                //str &= vbCrLf & "  AND CON.COST_ELEMENT_MST_FK(+) = C.COST_ELEMENT_MST_PK"
                str += "  AND con.cont_main_depot_fk = depot.cont_main_depot_pk";
                str += "  AND C.BUSINESS_TYPE in (3,2)";
                str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
                str += "  and c.cost_element_mst_pk not in";
                str += "  (SELECT CHG.COST_ELEMENT_MST_FK";
                str += "  FROM CONT_TRN_DEPOT_OTH_CHG CHG,";
                str += "  cont_main_depot_tbl    CCC,";
                str += "  vendor_mst_tbl         VEN";
                str += "  WHERE CHG.CONT_MAIN_DEPOT_FK = CCC.CONT_MAIN_DEPOT_PK";
                str += "  AND CCC.DEPOT_MST_FK = VEN.VENDOR_MST_PK";
                str += "  AND VEN.VENDOR_MST_PK =  " + depotID + "";
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM " + "AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "  AND ccc.cargo_type = 2)";
                str += " group by COST_ELEMENT_MST_PK, COST_ELEMENT_ID";
            }

            try
            {
                dtMain = objWF.GetDataTable(str);

                if (Convert.ToInt32(cargoType) == 1)
                {
                    dtContainerType = FetchDepotContainerDataFCLOth(depotID, cargoType, fromdate);
                    //For loop: Transposes the rows returned by FetchDepotContainerDataFCL into the columns 
                    //of header in dtMain datatable

                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        if (!dtMain.Columns.Contains(dtContainerType.Rows[i][1].ToString()))
                        {
                            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
                            dtMain.Columns.Add(dcCol);
                        }

                    }

                    //This loops for all the THC rates..
                    for (i = 0; i <= dtMain.Rows.Count - 1; i++)
                    {
                        if (!(string.IsNullOrEmpty(dtMain.Rows[i][0].ToString()) == true))
                        {
                            DataRow[] drCollection = dtContainerType.Select("cont_depot_oth_chg_pk=" + dtMain.Rows[i]["cont_depot_oth_chg_pk"]);
                            //This loops through the container types..
                            for (j = 0; j <= drCollection.Length - 1; j++)
                            {
                                //Container types starts from column 4, its index is 3
                                for (k = 3; k <= dtMain.Columns.Count - 1; k++)
                                {
                                    if (drCollection[j]["CONTAINER_TYPE_MST_ID"] == dtMain.Columns[k].ColumnName)
                                    {
                                        dtMain.Rows[i][k] = drCollection[j]["Oth_Chg_Per_Container"];
                                    }
                                }
                            }
                        }
                    }

                }
                return dtMain;
                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        public DataTable FetchDepotDataOthCont(string fromdate, string Todate = "", string ContractPK = "", string cargoType = "", string tariffDate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            //This datatable contains the active containers
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Int16 j = default(Int16);
            Int16 k = default(Int16);

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            if ((ContractPK != null))
            {
                strCondition += " AND depot.cont_main_depot_pk = " + ContractPK ;
            }

            if ((cargoType != null))
            {
                strCondition += " AND depot.cargo_type = " + cargoType ;
            }

            //Making query with the condition added
            //Gopi  Ok added mode
            if (Convert.ToInt32(cargoType) == 1)
            {
                str =  "SELECT DISTINCT";
                str += "        trn.cont_depot_oth_chg_pk,";
                str += "        trn.cost_element_mst_fk, ";
                str += "        'true' as \"SEL\", ";
                str += "        C.COST_ELEMENT_ID as \"Other Charges\" ";
                str += " FROM   CONT_TRN_DEPOT_OTH_CHG trn,";
                str += "        cont_main_depot_tbl depot,";
                str += "        COST_ELEMENT_MST_TBL c ";
                str += " WHERE  1=1 ";
                str += "        AND C.COST_ELEMENT_MST_PK(+) = TRN.COST_ELEMENT_MST_FK ";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2  ";
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM ";
                str += "        AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += strCondition;
                str += "  UNION";
                str += "  SELECT null CONT_DEPOT_OTH_CHG_PK,";
                str += "  C.COST_ELEMENT_MST_PK,";
                str += "  'false' as \"Sel\",";
                str += "  C.COST_ELEMENT_ID as \"Other Charges\" ";
                str += "  FROM COST_ELEMENT_MST_TBL   C,";
                str += "  cont_main_depot_tbl    depot,";
                str += "  CONT_TRN_DEPOT_OTH_CHG CON,";
                str += "  VENDOR_TYPE_MST_TBL    V";
                str += "  WHERE C.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK";
                str += "  AND CON.COST_ELEMENT_MST_FK(+) = C.COST_ELEMENT_MST_PK";
                str += "  AND con.cont_main_depot_fk = depot.cont_main_depot_pk";
                str += "  AND C.BUSINESS_TYPE in (3,2)";
                str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
                str += "  and c.cost_element_mst_pk not in";
                str += "  (SELECT CHG.COST_ELEMENT_MST_FK";
                str += "  FROM CONT_TRN_DEPOT_OTH_CHG CHG,";
                str += "  cont_main_depot_tbl    CCC,";
                str += "  vendor_mst_tbl         VEN";
                str += "  WHERE CHG.CONT_MAIN_DEPOT_FK = CCC.CONT_MAIN_DEPOT_PK";
                str += "  AND CCC.DEPOT_MST_FK = VEN.VENDOR_MST_PK";
                str += "  AND CCC.CONT_MAIN_DEPOT_PK =  " + ContractPK + "";
                str += "  AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM ";
                str += "  AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "  AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "  AND ccc.cargo_type = 1)";
                // ok' modifying by thiyagarajan on 7/2/09
            }
            else
            {
                str =  "SELECT";
                str += "        trn.cont_depot_oth_chg_pk,";
                str += "        trn.cost_element_mst_fk,  ";
                str += "        'true' as \"SEL\",";
                str += "        C.COST_ELEMENT_ID as \"Other Charges\", ";
                //str &= vbCrLf & "        upper(trn.lcl_rate_per_cbm), "
                //str &= vbCrLf & "        upper(trn.lcl_rate_per_ton),"
                //str &= vbCrLf & "        upper(trn.lcl_rate_palette)"  'added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates

                str += "        trn.lcl_rate_per_cbm, ";
                str += "        trn.lcl_rate_per_ton,";
                str += "        trn.lcl_rate_palette";
                //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates

                // str &= vbCrLf & "        ,trn.air_rate_per_100kg "
                str += " FROM   CONT_TRN_DEPOT_OTH_CHG trn,";
                str += "        cont_main_depot_tbl depot,";
                str += "         vendor_mst_tbl dmt, COST_ELEMENT_MST_TBL c ";
                str += " WHERE  1=1 ";
                str += "        AND C.COST_ELEMENT_MST_PK(+) = TRN.COST_ELEMENT_MST_FK ";
                str += "        AND dmt.vendor_mst_pk = depot.depot_mst_fk";
                str += "        AND depot.cont_main_depot_pk = trn.cont_main_depot_fk ";
                str += "        AND depot.active=1 ";
                str += "        AND depot.cont_approved = 1";
                str += "        AND depot.business_type = 2 ";
                str += "        AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM ";
                str += "        AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "        AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += strCondition;
                str += "  UNION";
                str += "  SELECT null CONT_DEPOT_OTH_CHG_PK,";
                str += "  C.COST_ELEMENT_MST_PK,";
                str += "  'false' as \"Sel\",";
                str += "  C.COST_ELEMENT_ID as \"Other Charges\", ";
                str += "  null lcl_rate_per_cbm,";
                str += "  null lcl_rate_per_ton,";
                str += "  null lcl_rate_palette ";
                //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                //str &= vbCrLf & "  ,null air_rate_per_100kg"
                str += "  FROM COST_ELEMENT_MST_TBL   C,";
                str += "  cont_main_depot_tbl    depot,";
                str += "  CONT_TRN_DEPOT_OTH_CHG CON,";
                str += "  VENDOR_TYPE_MST_TBL    V";
                str += "  WHERE C.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK";
                //str &= vbCrLf & "  AND CON.COST_ELEMENT_MST_FK(+) = C.COST_ELEMENT_MST_PK"
                str += "  AND con.cont_main_depot_fk = depot.cont_main_depot_pk";
                str += "  AND C.BUSINESS_TYPE in (3,2)";
                str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
                str += "  and c.cost_element_mst_pk not in";
                str += "  (SELECT CHG.COST_ELEMENT_MST_FK";
                str += "  FROM CONT_TRN_DEPOT_OTH_CHG CHG,";
                str += "  cont_main_depot_tbl    CCC,";
                str += "  vendor_mst_tbl         VEN";
                str += "  WHERE CHG.CONT_MAIN_DEPOT_FK = CCC.CONT_MAIN_DEPOT_PK";
                str += "  AND CCC.DEPOT_MST_FK = VEN.VENDOR_MST_PK";
                //str &= vbCrLf & "  AND VEN.VENDOR_MST_PK =  " & depotID & ""
                str += "  AND CCC.CONT_MAIN_DEPOT_PK =  " + ContractPK + "";
                str += "  AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM ";
                str += "  AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "  AND TO_DATE('" + Todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
                str += "  AND ccc.cargo_type = 2)";
                str += " group by COST_ELEMENT_MST_PK, COST_ELEMENT_ID";
            }

            try
            {
                dtMain = objWF.GetDataTable(str);

                if (Convert.ToInt32(cargoType) == 1)
                {
                    dtContainerType = FetchDepotContainerDataFCLOthCont(ContractPK, cargoType, fromdate);
                    //For loop: Transposes the rows returned by FetchDepotContainerDataFCL into the columns 
                    //of header in dtMain datatable

                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        if (!dtMain.Columns.Contains(dtContainerType.Rows[i][1].ToString()))
                        {
                            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
                            dtMain.Columns.Add(dcCol);
                        }

                    }

                    //This loops for all the THC rates..
                    for (i = 0; i <= dtMain.Rows.Count - 1; i++)
                    {
                        if (!(string.IsNullOrEmpty(dtMain.Rows[i][0].ToString()) == true))
                        {
                            DataRow[] drCollection = dtContainerType.Select("cont_depot_oth_chg_pk=" + dtMain.Rows[i]["cont_depot_oth_chg_pk"]);
                            //This loops through the container types..
                            for (j = 0; j <= drCollection.Length - 1; j++)
                            {
                                //Container types starts from column 4, its index is 3
                                for (k = 3; k <= dtMain.Columns.Count - 1; k++)
                                {
                                    if (drCollection[j]["CONTAINER_TYPE_MST_ID"] == dtMain.Columns[k].ColumnName)
                                    {
                                        dtMain.Rows[i][k] = drCollection[j]["Oth_Chg_Per_Container"];
                                    }
                                }
                            }
                        }
                    }

                }
                return dtMain;
                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
        public DataTable FetchDepotContainerDataFCLOth(string depotID = "", string cargoType = "", string fromdate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            //Gopi  //OK

            //Making query with the condition added
            str =  "SELECT";
            str += "        trn.cont_depot_oth_chg_pk, ";
            str += "        cmt.container_type_mst_id,";
            str += "        cont.Oth_Chg_Per_Container ";
            str += " from   CONT_TRN_DEPOT_OTH_CHG trn,";
            str += "        cont_main_depot_tbl depot,";
            //Modified by Snigdharani - 31/10/2008 - Removing v-array
            //str &= vbCrLf & "       TABLE(trn.container_dtl_fcl) (+) cont,"
            str += "        DEPOT_TRN_OTH_CHG_CONT_DET cont,";
            str += "        container_type_mst_tbl cmt, ";
            str += "        vendor_mst_tbl dmt";
            str += " where  1=1 ";
            str += "        AND depot.depot_mst_fk =dmt.vendor_mst_pk";
            str += "        AND trn.cont_main_depot_fk=depot.cont_main_depot_pk";
            str += "        AND trn.cont_depot_oth_chg_pk = cont.cont_depot_oth_chg_fk";
            //Snigdharani
            str += "        AND cont.container_type_mst_fk=cmt.container_type_mst_pk";
            str += "        AND depot.active=1 ";
            str += "        AND depot.cont_approved = 1";
            //str &= vbCrLf & "       AND depot.business_type = 2 AND TO_DATE('" & fromdate & "','" & dateFormat & "') BETWEEN depot.valid_from AND depot.valid_to " 'only for sea...
            str += "        AND depot.business_type = 2 AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN depot.valid_from AND DECODE(depot.valid_to, NULL,NULL_DATE_FORMAT,depot.valid_to)  ";
            //Amit 28-Feb-07
            str += "        AND dmt.vendor_mst_pk = " + depotID ;
            str += "        AND depot.cargo_type = " + cargoType ;
            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Container data for edit"

        public DataTable FetchDepotContainerDataFCL(string depotID = "", string cargoType = "", string fromdate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;



            //Making query with the condition added
            str =  "SELECT";
            str += "        trn.cont_trn_depot_pk, ";
            str += "        cmt.container_type_mst_id,";
            str += "        cont.fcl_current_rate ";
            str += " from   cont_trn_depot_fcl_lcl trn,";
            str += "        cont_main_depot_tbl depot,";
            //str &= vbCrLf & "        TABLE(trn.container_dtl_fcl) (+) cont,"
            //SNIGDHARANI - 30/10/2008 - REMOVONG V-ARRAY
            str += "        CONT_DEPOT_FCL_RATE_TRN CONT, ";
            str += "        container_type_mst_tbl cmt, ";
            str += "        vendor_mst_tbl dmt";
            str += " where  1=1 ";
            str += "        AND depot.depot_mst_fk =dmt.vendor_mst_pk";
            str += "        AND trn.cont_main_depot_fk=depot.cont_main_depot_pk";
            str += "        AND cont.container_type_mst_fk=cmt.container_type_mst_pk";
            str += "        AND CONT.CONT_TRN_DEPOT_FK=TRN.CONT_TRN_DEPOT_PK";
            //SNIGDHARANI
            str += "        AND depot.active=1 ";
            str += "        AND depot.cont_approved = 1";
            //str &= vbCrLf & "       AND depot.business_type = 2 AND TO_DATE('" & fromdate & "','" & dateFormat & "') BETWEEN depot.valid_from AND depot.valid_to " 'only for sea...
            str += "        AND depot.business_type = 2 AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN depot.valid_from AND DECODE(depot.valid_to, NULL,NULL_DATE_FORMAT,depot.valid_to)  ";
            //Amit 28-Feb-07
            str += "        AND dmt.vendor_mst_pk = " + depotID ;
            str += "        AND depot.cargo_type = " + cargoType ;
            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
        public DataTable FetchDepotContainerDataFCLCont(string ContractPK = "", string cargoType = "", string fromdate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            str =  "SELECT";
            str += "        trn.cont_trn_depot_pk, ";
            str += "        cmt.container_type_mst_id,";
            str += "        cont.fcl_current_rate ";
            str += " from   cont_trn_depot_fcl_lcl trn,";
            str += "        cont_main_depot_tbl depot,";
            //str &= vbCrLf & "        TABLE(trn.container_dtl_fcl) (+) cont,"
            //SNIGDHARANI - 03/11/2008 - REMOVING V-ARRAY
            str += "        CONT_DEPOT_FCL_RATE_TRN CONT,";
            str += "        container_type_mst_tbl cmt ";
            str += " where  1=1 ";
            str += "        AND trn.cont_main_depot_fk=depot.cont_main_depot_pk";
            str += "        AND cont.container_type_mst_fk=cmt.container_type_mst_pk";
            str += "        AND CONT.CONT_TRN_DEPOT_FK=TRN.CONT_TRN_DEPOT_PK";
            //SNIGDHARANI
            str += "        AND depot.active=1 ";
            str += "        AND depot.cont_approved = 1";
            str += "        AND depot.business_type = 2 AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN depot.valid_from AND DECODE(depot.valid_to, NULL,NULL_DATE_FORMAT,depot.valid_to)  ";
            //Amit 28-Feb-07
            str += "        AND depot.cont_main_depot_pk = " + ContractPK ;
            str += "        AND depot.cargo_type = " + cargoType ;
            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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


        public DataTable FetchDepotContainerDataFCLOthCont(string ContractPK = "", string cargoType = "", string fromdate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            //Gopi  //OK

            //Making query with the condition added
            str =  "SELECT";
            str += "        trn.cont_depot_oth_chg_pk, ";
            str += "        cmt.container_type_mst_id,";
            str += "        cont.Oth_Chg_Per_Container ";
            str += " from   CONT_TRN_DEPOT_OTH_CHG trn,";
            str += "        cont_main_depot_tbl depot,";
            //Modified by Snigdharani - 31/10/2008 - Removing v-array
            //str &= vbCrLf & "        TABLE(trn.container_dtl_fcl) (+) cont,"
            str += "        DEPOT_TRN_OTH_CHG_CONT_DET cont,";
            str += "        container_type_mst_tbl cmt, ";
            str += "        vendor_mst_tbl dmt";
            str += " where  1=1 ";
            str += "   AND depot.depot_mst_fk =dmt.vendor_mst_pk";
            str += "   AND trn.cont_depot_oth_chg_pk = cont.cont_depot_oth_chg_fk";
            //Snigdharani
            str += "   AND trn.cont_main_depot_fk=depot.cont_main_depot_pk";
            str += "   AND cont.container_type_mst_fk=cmt.container_type_mst_pk";
            str += "   AND depot.active=1 ";
            str += "   AND depot.cont_approved = 1";
            str += "   AND depot.business_type = 2 AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN depot.valid_from AND DECODE(depot.valid_to, NULL,NULL_DATE_FORMAT,depot.valid_to)  ";
            //Amit 28-Feb-07
            str += "   AND depot.cont_main_depot_pk = " + ContractPK ;
            str += "   AND depot.cargo_type = " + cargoType ;

            str += " UNION";
            str += " SELECT NULL TARF_SEA_DEPOT_OTH_CHG_PK,";
            str += " CMT.CONTAINER_TYPE_MST_ID,";
            str += " NULL AS \"OTH_CHG_PER_CONTAINER\" ";
            str += " FROM CONT_MAIN_DEPOT_TBL CMDT,";
            str += " CONT_TRN_DEPOT_FCL_LCL CT,CONT_DEPOT_FCL_RATE_TRN CON,";
            str += " CONTAINER_TYPE_MST_TBL CMT";
            str += " WHERE CMDT.CONT_MAIN_DEPOT_PK = CT.CONT_MAIN_DEPOT_FK";
            str += " AND CON.CONT_TRN_DEPOT_FK=CT.CONT_TRN_DEPOT_PK";
            str += " AND CON.CONTAINER_TYPE_MST_FK=CMT.CONTAINER_TYPE_MST_PK";
            str += " AND CMDT.CONT_MAIN_DEPOT_PK = " + ContractPK ;
            str += " AND CMDT.CARGO_TYPE = " + cargoType ;

            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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



        #region "This function returns the Depot contract tarrif for perticular DetentionID and the cargoType"

        //This function returns the Depot contract tarrif for perticular DepotID and the cargoType
        public DataTable FetchDetentionData(string detentionID = "", string cargoType = "", string contractRefNumber = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            //This datatable contains the active containers
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Int16 j = default(Int16);
            Int16 k = default(Int16);

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;


            //Making query with the condition added

            if (Convert.ToInt32(cargoType) == 1)
            {
                str =  "SELECT";
                str += "       det_lcl_fcl.detention_slab_trn_pk,";
                str += "       det_lcl_fcl.from_day,";
                str += "       det_lcl_fcl.to_day ";

                str += "FROM   detention_slab_main_tbl det_main,";
                str += "       detention_slab_trn_fcl_lcl det_lcl_fcl ";
                str += "       where 1 = 1";
                str += "       and det_main.detention_slab_main_pk = " + detentionID;
                str += "       and det_main.detention_slab_main_pk= det_lcl_fcl.detention_slab_main_fk";
                str += "       and det_main.cargo_type = 1 ";

                str += " ORDER BY from_day ";

            }
            else
            {
                str =  "select ";
                str += "       det_lcl_fcl.detention_slab_trn_pk,";
                str += "       det_lcl_fcl.from_day,";
                str += "       det_lcl_fcl.to_day,";
                str += "       det_lcl_fcl.lcl_volume,";
                str += "       det_lcl_fcl.lcl_weight,";
                str += "       det_lcl_fcl.lcl_amount,";
                str += "       det_lcl_fcl.lcl_rate_palette";
                //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                str += "from   detention_slab_main_tbl det_main,";
                str += "       detention_slab_trn_fcl_lcl det_lcl_fcl ";
                str += "where 1 = 1";
                str += "       and det_main.detention_slab_main_pk=" + detentionID;
                str += "       and det_main.detention_slab_main_pk = det_lcl_fcl.detention_slab_main_fk";
                str += "       and det_main.cargo_type =2 ";

                str += " ORDER BY from_day ";

            }

            try
            {
                dtMain = objWF.GetDataTable(str);

                if (Convert.ToInt32(cargoType) == 1)
                {
                    dtContainerType = FetchDetentionContainerDataFCL(detentionID, cargoType);
                    //For loop: Transposes the rows returned by FetchDepotContainerDataFCL into the columns 
                    //of header in dtMain datatable

                    //For i = 0 To dtContainerType.Rows.Count - 1
                    //    dcCol = New DataColumn(CStr(dtContainerType.Rows(i).Item(1)))
                    //    dtMain.Columns.Add(dcCol)
                    //Next

                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        if (!dtMain.Columns.Contains(dtContainerType.Rows[i][1].ToString()))
                        {
                            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
                            dtMain.Columns.Add(dcCol);
                        }
                    }


                    //This loops for all the THC rates..

                    for (i = 0; i <= dtMain.Rows.Count - 1; i++)
                    {
                        DataRow[] drCollection = dtContainerType.Select("detention_slab_trn_pk=" + dtMain.Rows[i]["detention_slab_trn_pk"]);

                        //This loops through the container types..
                        for (j = 0; j <= drCollection.Length - 1; j++)
                        {
                            //Container types starts from column 4, its index is 3

                            for (k = 3; k <= dtMain.Columns.Count - 1; k++)
                            {
                                if (drCollection[j]["container_type_mst_id"] == dtMain.Columns[k].ColumnName)
                                {
                                    dtMain.Rows[i][k] = drCollection[j]["CONTAINER_RATE"];
                                }

                            }
                        }
                    }

                }

                return dtMain;
                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        public DataTable FetchDetentionDataOth(string detentionID = "", string cargoType = "", string contractRefNumber = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = new DataTable();
            DataTable dtContainerType = new DataTable();
            //This datatable contains the active containers
            DataColumn dcCol = null;
            Int16 i = default(Int16);
            Int16 j = default(Int16);
            Int16 k = default(Int16);
            int count = 0;
            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;


            //Making query with the condition added

            if (Convert.ToInt32(cargoType) == 1)
            {
                str =  " SELECT";
                str += "       trf_trn.tarf_sea_depot_oth_chg_pk, ";
                str += "       trf_trn.cost_element_mst_fk,'true' as Sel, ";
                str += "       c.cost_element_id AS \"Other Charges\" ";
                str += " FROM   detention_slab_main_tbl det_main,";
                str += "       TARIFF_SEA_DEPOT_OTH_CHG trf_trn , cost_element_mst_tbl c ";
                str += "       where 1 = 1";
                str += "       and trf_trn.cost_element_mst_fk = c.cost_element_mst_pk ";
                str += "       and det_main.detention_slab_main_pk = " + detentionID;
                str += "       and det_main.detention_slab_main_pk= trf_trn.detention_slab_main_fk";
                str += "       and det_main.cargo_type = 1 ";
                str += "       and c.business_type in (2,3) ";
                str += " union";
                str += " SELECT DISTINCT null tarf_sea_depot_oth_chg_pk,";
                str += " CC.COST_ELEMENT_MST_PK,";
                str += "  'false' as SEL,";
                str += "  CC.COST_ELEMENT_ID AS \"Other Charges\" ";
                str += "  FROM TARIFF_SEA_DEPOT_OTH_CHG CT,";
                str += "  detention_slab_main_tbl CMDT,";
                //Snigdharani - 31/10/2008 - Removing v-array task.
                //str &= vbCrLf & "  TABLE(CT.CONTAINER_DTL_FCL)(+) CONT,"
                str += "  TRF_DEPOT_OTHCHG_CONT_DET CONT,";
                str += "  CONTAINER_TYPE_MST_TBL CMT,";
                str += "  COST_ELEMENT_MST_TBL CC,";
                str += "  VENDOR_TYPE_MST_TBL V";
                str += "  WHERE Cc.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK(+) ";
                str += "  AND Ct.Detention_Slab_Main_Fk = CMDT.DETENTION_SLAB_MAIN_PK";
                str += "  AND CONT.TARF_SEA_DEPOT_OTH_CHG_FK = Ct.TARF_SEA_DEPOT_OTH_CHG_PK";
                //Snigdharani
                str += "  AND CONT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK";
                str += "  AND CMDT.DETENTION_SLAB_MAIN_PK = CT.DETENTION_SLAB_MAIN_FK";
                str += "  and cc.business_type in (2,3) ";
                str += "  AND CC.COST_ELEMENT_MST_PK NOT IN";
                str += "  (SELECT CON.COST_ELEMENT_MST_FK";
                str += "  FROM detention_slab_main_tbl CMDT, TARIFF_SEA_DEPOT_OTH_CHG CON";
                str += "  WHERE CMDT.DETENTION_SLAB_MAIN_PK = CON.DETENTION_SLAB_MAIN_FK";
                str += "  AND CMDT.DETENTION_SLAB_MAIN_PK = " + detentionID + ")";
                str += "  AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
            }
            else
            {
                str =  " select trf_trn.tarf_sea_depot_oth_chg_pk, ";
                str += "       trf_trn.cost_element_mst_fk, ";
                str += "       'true' as Sel, C.COST_ELEMENT_ID \"Other Charges\",  ";
                str += "       trf_trn.lcl_rate_per_cbm, ";
                str += "       trf_trn.lcl_rate_per_ton, ";
                str += "       trf_trn.lcl_rate_palette";
                //added by parakash chandra on 1/1/2009 for pts: rate palette
                str += " from   detention_slab_main_tbl det_main,";
                str += "       TARIFF_SEA_DEPOT_OTH_CHG trf_trn ,cost_element_mst_tbl c  ";
                str += "  where 1 = 1";
                str += "  and trf_trn.cost_element_mst_fk = c.cost_element_mst_pk ";
                str += "       and det_main.detention_slab_main_pk=" + detentionID;
                str += "       and det_main.detention_slab_main_pk = trf_trn.detention_slab_main_fk";
                str += "       and det_main.cargo_type =2 ";
                str += "       and c.business_type in (2,3) ";
                str += " union";
                str += " SELECT DISTINCT null tarf_sea_depot_oth_chg_pk,";
                str += " CC.COST_ELEMENT_MST_PK,";
                str += " 'false' as SEL,";
                str += "  CC.COST_ELEMENT_ID AS \"Other Charges\",";
                str += "  CT.LCL_RATE_PER_CBM,";
                str += "  CT.LCL_RATE_PER_TON,";
                str += "  CT.lcl_rate_palette";
                //added by parakash chandra on 1/1/2009 for pts: rate palette
                str += "  FROM TARIFF_SEA_DEPOT_OTH_CHG CT,";
                str += "  detention_slab_main_tbl CMDT,";
                //Snigdharani - 31/10/2008 - Removing v-array task.
                //str &= vbCrLf & "  TABLE(CT.CONTAINER_DTL_FCL)(+) CONT,"
                str += "  TRF_DEPOT_OTHCHG_CONT_DET CONT,";
                str += " CONTAINER_TYPE_MST_TBL CMT,";
                str += " COST_ELEMENT_MST_TBL CC,";
                str += " VENDOR_TYPE_MST_TBL V";
                str += " WHERE Cc.VENDOR_TYPE_MST_FK = V.VENDOR_TYPE_PK(+) ";
                str += " AND Ct.Detention_Slab_Main_Fk = CMDT.DETENTION_SLAB_MAIN_PK";
                str += " AND CONT.TARF_SEA_DEPOT_OTH_CHG_FK = Ct.TARF_SEA_DEPOT_OTH_CHG_PK";
                //Snigdharani
                str += " AND CONT.CONTAINER_TYPE_MST_FK = CMT.CONTAINER_TYPE_MST_PK";
                str += " AND CMDT.DETENTION_SLAB_MAIN_PK = CT.DETENTION_SLAB_MAIN_FK";
                str += " and cc.business_type in (2,3) ";
                str += " AND CC.COST_ELEMENT_MST_PK NOT IN";
                str += " (SELECT CON.COST_ELEMENT_MST_FK";
                str += " FROM detention_slab_main_tbl CMDT, TARIFF_SEA_DEPOT_OTH_CHG CON";
                str += " WHERE CMDT.DETENTION_SLAB_MAIN_PK = CON.DETENTION_SLAB_MAIN_FK";
                str += " AND CMDT.DETENTION_SLAB_MAIN_PK = " + detentionID + ")";
                str += " AND V.VENDOR_TYPE_ID = 'WAREHOUSE'";
            }

            try
            {
                dtMain = objWF.GetDataTable(str);

                if (Convert.ToInt32(cargoType) == 1)
                {
                    dtContainerType = FetchDetentionContainerDataFCLOth(detentionID, cargoType);
                    //For loop: Transposes the rows returned by FetchDepotContainerDataFCL into the columns 
                    //of header in dtMain datatable

                    //For i = 0 To dtContainerType.Rows.Count - 1
                    //    dcCol = New DataColumn(CStr(dtContainerType.Rows(i).Item(1)))
                    //    dtMain.Columns.Add(dcCol)
                    //Next

                    for (i = 0; i <= dtContainerType.Rows.Count - 1; i++)
                    {
                        if (!dtMain.Columns.Contains(dtContainerType.Rows[i][1].ToString()))
                        {
                            dcCol = new DataColumn(Convert.ToString(dtContainerType.Rows[i][1]), typeof(decimal));
                            dtMain.Columns.Add(dcCol);
                        }
                    }


                    //This loops for all the THC rates..
                    for (i = 0; i <= dtMain.Rows.Count - 1; i++)
                    {
                        if (!(string.IsNullOrEmpty(dtMain.Rows[i]["tarf_sea_depot_oth_chg_pk"].ToString()) == true))
                        {
                            DataRow[] drCollection = dtContainerType.Select("tarf_sea_depot_oth_chg_pk=" + dtMain.Rows[i]["tarf_sea_depot_oth_chg_pk"]);

                            //This loops through the container types..
                            for (j = 0; j <= drCollection.Length - 1; j++)
                            {
                                //Container types starts from column 4, its index is 3

                                for (k = 3; k <= dtMain.Columns.Count - 1; k++)
                                {
                                    if (drCollection[j]["container_type_mst_id"] == dtMain.Columns[k].ColumnName)
                                    {
                                        dtMain.Rows[i][k] = drCollection[j]["Oth_Chg_Per_Container"];
                                    }

                                }
                            }
                        }
                        else
                        {
                            if (dtContainerType.Rows.Count > 0)
                            {
                                //added by surya prasad
                                count = 0;
                                if (string.IsNullOrEmpty(dtMain.Rows[i]["tarf_sea_depot_oth_chg_pk"].ToString()))
                                {
                                    count += 1;
                                }
                            }
                            for (j = 0; j <= count - 1; j++)
                            {
                                //Container types starts from column 4, its index is 3

                                for (k = 3; k <= dtMain.Columns.Count - 1; k++)
                                {
                                    if (dtContainerType.Rows[j]["container_type_mst_id"] == dtMain.Columns[k].ColumnName)
                                    {
                                        dtMain.Rows[i][k] = DBNull.Value;
                                    }

                                }
                            }
                        }
                    }

                }

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

        #region "Container data for edit"
        public DataTable FetchDetentionContainerDataFCL(string detentionID = "", string cargoType = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;



            //Making query with the condition added
            str =  "SELECT ";
            str += "       det_lcl_fcl.detention_slab_trn_pk,";
            str += "       cmt.container_type_mst_id,";
            str += "       cont.CONTAINER_RATE";
            str += "from   detention_slab_main_tbl det_main,";
            str += "       detention_slab_trn_fcl_lcl det_lcl_fcl,";
            //Snigdharani - 05/11/2008 - Re,oving v-array
            //str &= vbCrLf & "       TABLE(det_lcl_fcl.container_dtl_fcl) (+) CONT,"
            str += "       DETENTION_SLAB_CONT_DTL CONT,";
            str += "       CONTAINER_TYPE_MST_TBL CMT";
            str += "where  ";
            str += "       det_main.detention_slab_main_pk=" + detentionID;
            str += "       and CONT.detention_slab_trn_fk = det_lcl_fcl.detention_slab_trn_pk";
            //Snigdharani
            str += "       and det_main.detention_slab_main_pk = det_lcl_fcl.detention_slab_main_fk";
            str += "       and cont.CONTAINER_TYPE_MST_FK=cmt.container_type_mst_pk";
            str += "       and det_main.cargo_type =1";
            str += "Order By CMT.Preferences";

            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Container data for edit"
        public DataTable FetchDetentionContainerDataFCLOth(string detentionID = "", string cargoType = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            str =  "SELECT Q.* FROM ( ";
            str += " SELECT";
            str += "       trf_trn.tarf_sea_depot_oth_chg_pk,";
            str += "       cmt.container_type_mst_id,";
            str += "       cont.Oth_Chg_Per_Container,";
            str += "cmt.preferences";
            str += "from   detention_slab_main_tbl det_main,";
            str += "       TARIFF_SEA_DEPOT_OTH_CHG trf_trn,";

            str += "  TRF_DEPOT_OTHCHG_CONT_DET CONT,";
            str += "       CONTAINER_TYPE_MST_TBL CMT";
            str += "where  ";
            str += "       det_main.detention_slab_main_pk=" + detentionID;
            str += "       AND CONT.TARF_SEA_DEPOT_OTH_CHG_FK = trf_trn.TARF_SEA_DEPOT_OTH_CHG_PK";
            str += "       and det_main.detention_slab_main_pk = trf_trn.detention_slab_main_fk";
            str += "       and cont.CONTAINER_TYPE_MST_FK=cmt.container_type_mst_pk";
            str += "       and det_main.cargo_type =1";
            str += " union";
            str += " SELECT null tarf_sea_depot_oth_chg_pk,";
            str += " Cmt.CONTAINER_TYPE_MST_ID,";
            str += " null as \"Oth_Chg_Per_Container\",";
            str += "cmt.preferences";
            str += " FROM TARIFF_SEA_DEPOT_OTH_CHG CT,";

            str += "  TRF_DEPOT_OTHCHG_CONT_DET CONT,";
            str += " CONTAINER_TYPE_MST_TBL CMT";
            str += " where cont.CONTAINER_TYPE_MST_FK(+) = cmt.container_type_mst_pk";
            str += " AND CONT.TARF_SEA_DEPOT_OTH_CHG_FK = Ct.TARF_SEA_DEPOT_OTH_CHG_PK(+)";
            str += " AND CMT.CONTAINER_TYPE_MST_PK IN";

            str += " (SELECT CONTDTL.CONTAINER_TYPE_MST_FK";
            str += " FROM detention_slab_trn_fcl_lcl CTN,";
            str += "  detention_slab_main_tbl CMDT,";

            str += "       DETENTION_SLAB_CONT_DTL CONTDTL";
            str += " WHERE CMDT.DETENTION_SLAB_MAIN_PK = CTN.DETENTION_SLAB_MAIN_FK";
            str += " and CONTDTL.detention_slab_trn_fk = CTN.detention_slab_trn_pk";

            str += " AND CMDT.DETENTION_SLAB_MAIN_PK = " + detentionID + " )) Q";
            str += "order by Q.Preferences";
            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

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
        #endregion

        #region "Save Function"

        public ArrayList Save(DataSet M_DataSet, ref DataSet dsChildData, ref DataSet dsChildDataOth, ref bool isEdting, ref string detentionSlabPK, ref string userLocation, ref string tariffRefNumber, ref int employeeID)
        {

            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            
            int intPKVal = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();

            OracleCommand insCommandChlid = new OracleCommand();
            OracleCommand updCommandChild = new OracleCommand();
            // Dim tariffRefNumber As String

            OracleCommand insCommandChlidOth = new OracleCommand();
            OracleCommand updCommandChildOth = new OracleCommand();

            if (isEdting == false)
            {
                tariffRefNumber = GenerateProtocolKey("DETENTION SLAB", Convert.ToInt32(userLocation), employeeID, System.DateTime.Now, "","" ,"" , M_LAST_MODIFIED_BY_FK);
            }


            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;

                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".DETENTION_TARIFF_PKG.DETENTION_SLAB_MAIN_TBL_INS";
                var _with2 = _with1.Parameters;
                //ok
                insCommand.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                insCommand.Parameters.Add("TARIFF_REF_NO_IN", tariffRefNumber).Direction = ParameterDirection.Input;

                //ok
                insCommand.Parameters.Add("TARIFF_DATE_IN", OracleDbType.Date, 20, "TARIFF_DATE").Direction = ParameterDirection.Input;
                insCommand.Parameters["TARIFF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                insCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 20, "VALID_FROM").Direction = ParameterDirection.Input;
                insCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                insCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 20, "VALID_TO").Direction = ParameterDirection.Input;
                insCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                insCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                insCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                insCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                insCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;


                //ok
                insCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                insCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

                //'ok
                insCommand.Parameters.Add("CONT_MAIN_DEPOT_FK_IN", OracleDbType.Int32, 10, "cont_main_depot_fk").Direction = ParameterDirection.Input;
                insCommand.Parameters["CONT_MAIN_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;


                insCommand.Parameters.Add("CREATED_BY_FK_IN", CREATED_BY).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                insCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DETENTION_SLAB_MAIN_PK").Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;



                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".DETENTION_TARIFF_PKG.DETENTION_SLAB_MAIN_TBL_UPD";
                var _with4 = _with3.Parameters;

                //ok
                updCommand.Parameters.Add("DETENTION_SLAB_MAIN_PK_IN", OracleDbType.Int32, 10, "DETENTION_SLAB_MAIN_PK").Direction = ParameterDirection.Input;
                updCommand.Parameters["DETENTION_SLAB_MAIN_PK_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("VENDOR_MST_FK_IN", OracleDbType.Int32, 10, "VENDOR_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["VENDOR_MST_FK_IN"].SourceVersion = DataRowVersion.Current;


                //ok
                updCommand.Parameters.Add("TARIFF_REF_NO_IN", OracleDbType.Varchar2, 20, "TARIFF_REF_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["TARIFF_REF_NO_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("TARIFF_DATE_IN", OracleDbType.Date, 20, "TARIFF_DATE").Direction = ParameterDirection.Input;
                updCommand.Parameters["TARIFF_DATE_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("VALID_FROM_IN", OracleDbType.Date, 20, "VALID_FROM").Direction = ParameterDirection.Input;
                updCommand.Parameters["VALID_FROM_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("VALID_TO_IN", OracleDbType.Date, 20, "VALID_TO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VALID_TO_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("CURRENCY_MST_FK_IN", OracleDbType.Int32, 10, "CURRENCY_MST_FK").Direction = ParameterDirection.Input;
                updCommand.Parameters["CURRENCY_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("CARGO_TYPE_IN", OracleDbType.Int32, 1, "CARGO_TYPE").Direction = ParameterDirection.Input;
                updCommand.Parameters["CARGO_TYPE_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("ACTIVE_IN", OracleDbType.Int32, 1, "ACTIVE").Direction = ParameterDirection.Input;
                updCommand.Parameters["ACTIVE_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommand.Parameters.Add("VERSION_NO_IN", OracleDbType.Int32, 1, "VERSION_NO").Direction = ParameterDirection.Input;
                updCommand.Parameters["VERSION_NO_IN"].SourceVersion = DataRowVersion.Current;


                updCommand.Parameters.Add("CONT_MAIN_DEPOT_FK_IN", OracleDbType.Int32, 10, "cont_main_depot_fk").Direction = ParameterDirection.Input;
                updCommand.Parameters["CONT_MAIN_DEPOT_FK_IN"].SourceVersion = DataRowVersion.Current;

                updCommand.Parameters.Add("LAST_MODIFIED_BY_FK_IN", LAST_MODIFIED_BY).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("CONFIG_PK_IN", ConfigurationPK).Direction = ParameterDirection.Input;

                updCommand.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;
                updCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                


                var _with5 = objWK.MyDataAdapter;

                _with5.InsertCommand = insCommand;
                _with5.InsertCommand.Transaction = TRAN;

                _with5.UpdateCommand = updCommand;
                _with5.UpdateCommand.Transaction = TRAN;

                RecAfct = _with5.Update(M_DataSet);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    tariffRefNumber = " ";
                    //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                    return arrMessage;
                }
                else
                {
                    if (isEdting == false)
                    {
                        detentionSlabPK = Convert.ToString(insCommand.Parameters["RETURN_VALUE"].Value);
                    }
                    else
                    {
                        detentionSlabPK = Convert.ToString(updCommand.Parameters["RETURN_VALUE"].Value);
                        DeleteOthChrg(Convert.ToInt64(detentionSlabPK));
                    }
                    //arrMessage.Add("All Data Saved Successfully")
                }



                var _with6 = insCommandChlid;
                _with6.Connection = objWK.MyConnection;
                _with6.CommandType = CommandType.StoredProcedure;
                _with6.CommandText = objWK.MyUserName + ".DETENTION_TARIFF_PKG.DETENTION_SLAB_TRN_FCL_LCL_INS";
                var _with7 = _with6.Parameters;
                //ok
                insCommandChlid.Parameters.Add("DETENTION_SLAB_MAIN_FK_IN", detentionSlabPK).Direction = ParameterDirection.Input;

                //ok
                insCommandChlid.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 3, "FROM_DAY").Direction = ParameterDirection.Input;
                insCommandChlid.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                insCommandChlid.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 3, "TO_DAY").Direction = ParameterDirection.Input;
                insCommandChlid.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;


                //ok
                insCommandChlid.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
                insCommandChlid.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                insCommandChlid.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
                insCommandChlid.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;
                //ok
                insCommandChlid.Parameters.Add("LCL_AMOUNT_IN", OracleDbType.Int32, 10, "LCL_AMOUNT").Direction = ParameterDirection.Input;
                insCommandChlid.Parameters["LCL_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                insCommandChlid.Parameters.Add("lcl_rate_palette_in", OracleDbType.Int32, 10, "lcl_rate_palette").Direction = ParameterDirection.Input;
                insCommandChlid.Parameters["lcl_rate_palette_in"].SourceVersion = DataRowVersion.Current;
                //end by prakash chandra

                //ok
                insCommandChlid.Parameters.Add("CONTAINER_DTL_FCL_IN", OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
                insCommandChlid.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

                insCommandChlid.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DETENTION_SLAB_MAIN_FK").Direction = ParameterDirection.Output;
                insCommandChlid.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;




                var _with8 = updCommandChild;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.CommandText = objWK.MyUserName + ".DETENTION_TARIFF_PKG.DETENTION_SLAB_TRN_FCL_LCL_UPD";
                var _with9 = _with8.Parameters;

                //ok
                updCommandChild.Parameters.Add("DETENTION_SLAB_TRN_PK_IN", OracleDbType.Int32, 10, "DETENTION_SLAB_TRN_PK").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["DETENTION_SLAB_TRN_PK_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommandChild.Parameters.Add("DETENTION_SLAB_MAIN_FK_IN", detentionSlabPK).Direction = ParameterDirection.Input;

                //ok
                updCommandChild.Parameters.Add("FROM_DAY_IN", OracleDbType.Int32, 10, "FROM_DAY").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["FROM_DAY_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommandChild.Parameters.Add("TO_DAY_IN", OracleDbType.Int32, 10, "TO_DAY").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["TO_DAY_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommandChild.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                //ok
                updCommandChild.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;


                //ok
                updCommandChild.Parameters.Add("LCL_AMOUNT_IN", OracleDbType.Int32, 10, "LCL_AMOUNT").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["LCL_AMOUNT_IN"].SourceVersion = DataRowVersion.Current;
                //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                updCommandChild.Parameters.Add("lcl_rate_palette_in", OracleDbType.Int32, 10, "lcl_rate_palette").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["lcl_rate_palette_in"].SourceVersion = DataRowVersion.Current;
                //end by prakash chandra

                //ok
                updCommandChild.Parameters.Add("CONTAINER_DTL_FCL_IN", OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
                updCommandChild.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

                updCommandChild.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "DETENTION_SLAB_MAIN_FK").Direction = ParameterDirection.Output;
                updCommandChild.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;


                var _with10 = objWK.MyDataAdapter;

                _with10.InsertCommand = insCommandChlid;
                _with10.InsertCommand.Transaction = TRAN;

                _with10.UpdateCommand = updCommandChild;
                _with10.UpdateCommand.Transaction = TRAN;

                RecAfct = _with10.Update(dsChildData.Tables["Data"]);

                if (arrMessage.Count > 0)
                {
                    TRAN.Rollback();
                    tariffRefNumber = " ";
                    //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                    return arrMessage;
                }


                var _with11 = insCommandChlidOth;
                _with11.Connection = objWK.MyConnection;
                _with11.CommandType = CommandType.StoredProcedure;
                _with11.CommandText = objWK.MyUserName + ".DETENTION_TARIFF_PKG.TARIFF_SEA_DEPOT_OTH_CHG_INS";
                var _with12 = _with11.Parameters;

                //Depot Contract Pk
                insCommandChlidOth.Parameters.Add("DETENTION_SLAB_MAIN_FK_IN", Convert.ToInt64(detentionSlabPK)).Direction = ParameterDirection.Input;

                insCommandChlidOth.Parameters.Add("COST_ELEMENT_MST_FK_IN", OracleDbType.Int32, 10, "COST_ELEMENT_MST_FK").Direction = ParameterDirection.Input;
                insCommandChlidOth.Parameters["COST_ELEMENT_MST_FK_IN"].SourceVersion = DataRowVersion.Current;

                insCommandChlidOth.Parameters.Add("CONTAINER_DTL_FCL_IN", OracleDbType.Varchar2, 300, "CONTAINER_DTL_FCL").Direction = ParameterDirection.Input;
                insCommandChlidOth.Parameters["CONTAINER_DTL_FCL_IN"].SourceVersion = DataRowVersion.Current;

                insCommandChlidOth.Parameters.Add("LCL_VOLUME_IN", OracleDbType.Int32, 10, "LCL_VOLUME").Direction = ParameterDirection.Input;
                insCommandChlidOth.Parameters["LCL_VOLUME_IN"].SourceVersion = DataRowVersion.Current;

                insCommandChlidOth.Parameters.Add("LCL_WEIGHT_IN", OracleDbType.Int32, 10, "LCL_WEIGHT").Direction = ParameterDirection.Input;
                insCommandChlidOth.Parameters["LCL_WEIGHT_IN"].SourceVersion = DataRowVersion.Current;

                //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                insCommandChlidOth.Parameters.Add("LCL_RATE_PALETTE_IN", OracleDbType.Int32, 10, "LCL_RATE_PALETTE").Direction = ParameterDirection.Input;
                insCommandChlidOth.Parameters["LCL_RATE_PALETTE_IN"].SourceVersion = DataRowVersion.Current;



                insCommandChlidOth.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                insCommandChlidOth.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                //************************************************************************
                

                var _with13 = objWK.MyDataAdapter;
                _with13.InsertCommand = insCommandChlidOth;
                _with13.InsertCommand.Transaction = TRAN;

                if (dsChildDataOth.Tables.Count > 0)
                {
                    RecAfct = _with13.Update(dsChildDataOth.Tables[0]);
                }

                if (arrMessage.Count > 1)
                {
                    TRAN.Rollback();
                    tariffRefNumber = " ";
                    //added By Prakash chandra on 1/1/2009 for pts: Implementation of Palette wise rates
                    return arrMessage;
                }
                else
                {
                    TRAN.Commit();
                    arrMessage.Add("All Data Saved Successfully");
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
                //Manjunath
            }
            finally
            {
                objWK.CloseConnection();
            }
        }
        #endregion

        #region "Delete Other Chrge"
        public string DeleteOthChrg(long detentionSlabPK)
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("DELETE FROM TRF_DEPOT_OTHCHG_CONT_DET OTHTRN");
            sb.Append(" WHERE OTHTRN.TARF_SEA_DEPOT_OTH_CHG_FK IN");
            sb.Append("       (SELECT TAR.TARF_SEA_DEPOT_OTH_CHG_PK");
            sb.Append("          FROM TARIFF_SEA_DEPOT_OTH_CHG TAR");
            sb.Append("         WHERE TAR.DETENTION_SLAB_MAIN_FK = " + detentionSlabPK + ")");
            Strsql = "DELETE FROM TARIFF_SEA_DEPOT_OTH_CHG  TAR WHERE TAR.DETENTION_SLAB_MAIN_FK = " + detentionSlabPK;
            try
            {
                Objwk.ExecuteCommands(sb.ToString());
                Objwk.ExecuteCommands(Strsql);
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
            return "";
        }
        #endregion

        #region "Fetch main Data"
        public DataSet FetchMainData(string detentionID = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataSet dsMain = null;


            //Making query with the condition added

            str =  "SELECT ";
            str += "       d.detention_slab_main_pk,";
            str += "       d.vendor_mst_fk,";
            str += "       d.tariff_ref_no,";
            str += "       d.tariff_date,";
            str += "       d.valid_from,";
            str += "       d.valid_to,";
            str += "       d.currency_mst_fk,";
            str += "       d.active,";
            str += "       d.version_no,";
            str += "       d.cargo_type,";
            str += "       d.cont_main_depot_fk";
            str += "FROM";
            str += "       detention_slab_main_tbl d";
            str += "WHERE";
            str += "       d.detention_slab_main_pk = " + detentionID;



            try
            {
                dsMain = objWF.GetDataSet(str);
                return dsMain;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Fetch Currency"
        public DataTable FetchCurrencyData(string currencyPK = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = null;


            //Making query with the condition added

            str =  "select ";
            str += "       c.currency_mst_pk,";
            str += "       c.currency_id,";
            str += "       c.currency_name";
            str += "       from";
            str += "       currency_type_mst_tbl c";
            str += "       where";
            str += "       c.currency_mst_pk=" + currencyPK;


            try
            {
                dtMain = objWF.GetDataTable(str);
                return dtMain;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Fetch Depot Description"
        public DataTable FetchDepotDesc(string DepotPK = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtMain = null;


            //Making query with the condition added

            str =  "select";
            str += "       c.vendor_mst_pk,";
            str += "       c.vendor_id,";
            str += "       c.vendor_name";
            str += "       from";
            str += "       vendor_mst_tbl c";
            str += "       where";
            str += "       c.vendor_mst_pk=" + DepotPK;


            try
            {
                dtMain = objWF.GetDataTable(str);
                return dtMain;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Fetch Contract Description"
        //Modified by mani
        public DataTable FetchContractDesc(string fromdate, string todate, string depotID = "", string cargoType = "", string tariffDate = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            //Making query with the condition added
            str =  "SELECT";
            str += "        depot.cont_main_depot_pk, ";
            str += "        depot.contract_no,";
            str += "        depot.valid_from,";
            str += "        depot.valid_to,";
            str += "        depot.currency_mst_fk";
            str += " from   cont_main_depot_tbl depot,";
            str += "        vendor_mst_tbl dmt";
            str += " where  1=1 ";
            str += "        AND depot.depot_mst_fk =dmt.vendor_mst_pk";
            str += "        AND depot.active=1 ";
            str += "        AND depot.cont_approved = 1";
            str += "        AND depot.business_type = 2 ";
            //str &= vbCrLf & "        to_date('" & fromdate & "','" & dateFormat & "') between to_date('" & fromdate & "','" & dateFormat & "') and valid_to "
            //If Not todate Is Nothing And todate <> "" Then
            //    str &= vbCrLf & "        and valid_to <to_date('" & todate & "','mm/dd/yyyy')"
            //End If
            //'str &= vbCrLf & "        TO_DATE('" & tariffDate & "','" & dateFormat & "') BETWEEN depot.valid_from AND depot.valid_to " 'only for sea...
            //Modified By Mani
            str += "  AND TO_DATE('" + fromdate + "','" + dateFormat + "') BETWEEN VALID_FROM " + "AND DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
            str += "        AND TO_DATE('" + todate + "','" + dateFormat + "') <= DECODE(VALID_TO, NULL,NULL_DATE_FORMAT,VALID_TO) ";
            str += "        AND dmt.vendor_mst_pk = " + depotID ;
            str += "        AND depot.cargo_type = " + cargoType ;
            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #region "Fetch Contract Description"
        public DataTable FetchContractDescEdit(string contractID = "")
        {
            string str = null;
            WorkFlow objWF = new WorkFlow();
            DataTable dtContainerData = new DataTable();

            Array arrPolPk = null;
            Array arrPodPk = null;
            string strCondition = string.Empty;

            //Making query with the condition added
            str =  "SELECT";
            str += "        depot.cont_main_depot_pk, ";
            str += "        depot.contract_no,";
            str += "        depot.valid_from,";
            str += "        depot.valid_to,";
            str += "        depot.currency_mst_fk";
            str += " from   cont_main_depot_tbl depot,";
            str += "        vendor_mst_tbl dmt";
            str += " where  1=1 ";
            str += "        AND depot.depot_mst_fk =dmt.vendor_mst_pk";
            str += "        AND depot.cont_main_depot_pk=" + contractID;

            try
            {
                dtContainerData = objWF.GetDataTable(str);
                return dtContainerData;

                //Catch SQLEX As Exception
                //    Throw SQLEX
                //Manjunath  PTS ID:Sep-02  17/09/2011
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
        #region "Enhance search for Contract"
        public string FetchContract(string strCond)
        {

            WorkFlow objWF = new WorkFlow();
            OracleCommand selectCommand = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strDepotPK = null;
            string strSERACH_IN = null;
            string strReq = null;
            string strTariffdate = null;
            //Dim strTodate As Integer
            int CargoType = 0;
            var strNull = DBNull.Value;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            strDepotPK = Convert.ToString(arr.GetValue(2));
            strTariffdate = Convert.ToString(arr.GetValue(3));
            //strTodate = arr(4)
            CargoType = Convert.ToInt32(arr.GetValue(4));
            try
            {
                objWF.OpenConnection();
                selectCommand.Connection = objWF.MyConnection;
                selectCommand.CommandType = CommandType.StoredProcedure;
                selectCommand.CommandText = objWF.MyUserName + ".EN_TARIFF_REF_NO_PKG.GET_DEPOT_CONTRACT";

                var _with14 = selectCommand.Parameters;
                _with14.Add("SEARCH_IN", (!string.IsNullOrEmpty(strSERACH_IN) ? strSERACH_IN : "")).Direction = ParameterDirection.Input;
                _with14.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with14.Add("DEPOK_PK_IN", strDepotPK).Direction = ParameterDirection.Input;
                _with14.Add("FROM_DATE_IN", strTariffdate).Direction = ParameterDirection.Input;
                //.Add("FROM_DATE_IN", OracleDbType.DateTime, 20, "strTariffdate").Direction = ParameterDirection.Input
                //.Add("TO_DATE_IN", strTodate).Direction = ParameterDirection.Input
                _with14.Add("CARGOTYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with14.Add("RETURN_VALUE", OracleDbType.NVarchar2, 1000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                selectCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                selectCommand.ExecuteNonQuery();
                strReturn = Convert.ToString(selectCommand.Parameters["RETURN_VALUE"].Value);
                return strReturn;
                //Manjunath  PTS ID:Sep-02  17/09/2011
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

        #endregion

    }
}