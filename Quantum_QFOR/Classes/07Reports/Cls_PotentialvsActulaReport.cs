#region "Comments"

//'***************************************************************************************************************
//'*  Company Name:
//'*  Project Title           :    QFOR
//'***************************************************************************************************************
//'*  Created By  :    Santosh on 31-May-16
//'*  Module/Project Leader   :    Santosh Pisipati
//'*  Description :
//'*  Module/Form/Class Name  :
//'*  Configuration ID        :
//'***************************************************************************************************************
//'*  Revision History
//'***************************************************************************************************************
//'*  Modified DateTime(DD-MON-YYYY)  Modified By     Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_PotentialvsActulaReport : CommonFeatures
    {
        #region "TO Fetch PotentialInformation"

        /// <summary>
        /// Fetchpotentials the information.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Locationpk">The locationpk.</param>
        /// <param name="Period">The period.</param>
        /// <param name="Basis">The basis.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="COUNTRYPK">The countrypk.</param>
        /// <param name="REGIONPK">The regionpk.</param>
        /// <param name="Fromdate">The fromdate.</param>
        /// <param name="Todate">The todate.</param>
        /// <param name="Totalpage">The totalpage.</param>
        /// <returns></returns>
        public object FetchpotentialInformation(int BizType = 0, string Customerpk = "", string Locationpk = "", int Period = 0, int Basis = 0, string POLPK = "", string PODPK = "", string COUNTRYPK = "", string REGIONPK = "", string Fromdate = null,
        string Todate = null, int Totalpage = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            DataSet MainDS = new DataSet();
            OracleDataAdapter DA = new OracleDataAdapter();
            try
            {
                sb.Append("SELECT DISTINCT LOCATION_MST_PK,");
                sb.Append("              LOCATION_NAME");
                sb.Append("            /* SUM(CUST_TEU) CUST_TEU,");
                sb.Append("            SUM(CUST_CBM) CUST_CBM,");
                sb.Append("           SUM(CUST_MTS) CUST_MTS,");
                sb.Append("          SUM(ACTUAL_TEU) ACTUAL_TEU,");
                sb.Append("           SUM(ACTUAL_WT) ACTUAL_WT,");
                sb.Append("            SUM(VOLUME_IN_CBM) volume*/");
                sb.Append("  FROM(SELECT LOCATION_MST_PK,");
                sb.Append("       LOCATION_NAME,");
                sb.Append("       CUSTOMER_MST_PK,");
                sb.Append("       CUSTOMER_NAME,");
                sb.Append("       SUM(CUST_TEU),");
                sb.Append("       SUM(CUST_CBM),");
                sb.Append("       SUM(CUST_MTS),");
                sb.Append("       SUM(ACTUAL_TEU),");
                sb.Append("       SUM(ACTUAL_WT),");
                sb.Append("       SUM(VOLUME_IN_CBM)");
                sb.Append("  FROM (SELECT DISTINCT LMT.LOCATION_MST_PK,");
                sb.Append("                        LMT.LOCATION_NAME,");
                sb.Append("                        CMT.CUSTOMER_MST_PK,");
                sb.Append("                        CMT.CUSTOMER_NAME,");
                sb.Append("                        POL.PORT_NAME,");
                if (BizType == 2)
                {
                    sb.Append("  (select sum(CPT.CUST_TEU) from CUSTOMER_POTENTIAL_TBL CPT");
                    sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                    if (Period != 0)
                    {
                        sb.Append("        and CPT.cust_period IN(" + Period + ")");
                    }
                    if (Basis == 1)
                    {
                        sb.Append("               and cpt.port_mst_pod_fk is not null");
                    }
                    else if (Basis == 2)
                    {
                        sb.Append("               and cpt.country_mst_fk is not null");
                    }
                    else
                    {
                        sb.Append("               and cpt.REGION_MST_FK is not null");
                    }
                    if (Basis == 1)
                    {
                        sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_TEU,");
                    }
                    else if (Basis == 2)
                    {
                        sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_TEU,");
                    }
                    else
                    {
                        sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_TEU,");
                    }
                }
                else
                {
                    sb.Append("                        '' CUST_TEU,");
                }
                sb.Append("        (select sum(CPT.CUST_CBM) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_CBM,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_CBM,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_CBM,");
                }
                sb.Append("  (select sum(CPT.CUST_MTS) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_MTS,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_MTS,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_MTS,");
                }

                //sb.Append("                        CPT.CUST_TEU,")
                //sb.Append("                        CPT.CUST_CBM,")
                // sb.Append("                        CPT.CUST_MTS,")
                if (BizType == 2)
                {
                    sb.Append("                        CASE");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 1 THEN");
                    sb.Append("                           (SELECT SUM(NVL(BTSF.NO_OF_BOXES, 0) *");
                    sb.Append("                                       NVL(CTMT.TEU_FACTOR, 0))");
                    sb.Append("                              FROM BOOKING_TRN BTSF,");
                    sb.Append("                                   CONTAINER_TYPE_MST_TBL  CTMT");
                    sb.Append("                             WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                    sb.Append("                               AND CTMT.CONTAINER_TYPE_MST_PK =");
                    sb.Append("                                   BTSF.CONTAINER_TYPE_MST_FK)");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 2 THEN");
                    sb.Append("                           0");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 4 THEN");
                    sb.Append("                           0");
                    sb.Append("                          ELSE");
                    sb.Append("                           0");

                    sb.Append("                        END ACTUAL_TEU,");
                }
                else
                {
                    sb.Append("                       '' ACTUAL_TEU,");
                }
                if (BizType == 2)
                {
                    sb.Append("                        CASE");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 1 THEN");
                    sb.Append("                           NVL(BST.NET_WEIGHT, 0) / 1000");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 2 THEN");
                    sb.Append("                           NVL(BST.CHARGEABLE_WEIGHT, 0) / 1000");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 4 THEN");
                    sb.Append("                           (SELECT SUM(NVL(BTSF.WEIGHT_MT, 0))");
                    sb.Append("                              FROM BOOKING_TRN BTSF");
                    sb.Append("                             WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK)");
                    sb.Append("                          ELSE");
                    sb.Append("                           NVL(BST.NET_WEIGHT, 0) / 1000");
                    sb.Append("                        END ACTUAL_WT,");
                }
                else
                {
                    sb.Append("    NVL(BST.Chargeable_Weight, 0) / 1000 ACTUAL_WT,");
                }
                sb.Append("         NVL(BST.VOLUME_IN_CBM, 0) VOLUME_IN_CBM, ");
                sb.Append("          BST.BOOKING_DATE ");
                sb.Append("          FROM BOOKING_MST_TBL        BST,");
                sb.Append("               PORT_MST_TBL           POL,");
                if (Basis == 1)
                {
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 2)
                {
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 3)
                {
                    sb.Append("  REGION_MST_TBL   REG,");
                    sb.Append("  AREA_MST_TBL   AREA,");
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("  PORT_MST_TBL   POD,");
                }

                sb.Append("               CUSTOMER_MST_TBL       CMT,");
                sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("               LOCATION_MST_TBL       LMT");
                //sb.Append("               CUSTOMER_POTENTIAL_TBL CPT,")
                //sb.Append("               COMPETITORS_TBL        CMP")
                sb.Append("         WHERE CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                //sb.Append("           AND CMT.CUSTOMER_MST_PK = CPT.CUSTOMER_MST_FK")
                sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                // sb.Append("           AND CPT.CUSTOMER_POTENTIAL_MST_PK = CMP.CUST_POTEN_MST_FK")
                sb.Append("           AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                if (Basis == 1)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 2)
                {
                    //sb.Append(" AND LMT.Country_Mst_Fk=COUN.COUNTRY_MST_PK")
                    sb.Append("  AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 3)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append(" AND POD.COUNTRY_MST_FK = COUN.COUNTRY_MST_PK");
                    sb.Append(" AND COUN.AREA_MST_FK=AREA.AREA_MST_PK");
                    sb.Append(" AND AREA.Region_Mst_Fk=REG.REGION_MST_PK");
                }

                //sb.Append("           AND CPT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)")
                //If Basis = 1 Then
                //    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)")
                //ElseIf Basis = 2 Then
                //    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK(+)")
                //ElseIf Basis = 3 Then
                //    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK(+)")
                //End If

                sb.Append("           AND BST.STATUS = 2");

                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }
                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }
                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }
                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }
                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                //If Period <> 0 Then
                //    sb.Append("        AND CPT.cust_period IN(" & Period & ")")

                //End If
                if (BizType == 2)
                {
                    sb.Append("  AND BST.Business_Type =2");
                }
                else if (BizType == 1)
                {
                    sb.Append("  AND BST.Business_Type =1");
                }
                if (BizType == 2 & Basis == 3)
                {
                    sb.Append("  AND REG.Business_Type IN(2)");
                }
                else if (BizType == 1 & Basis == 3)
                {
                    sb.Append("     AND REG.Business_Type IN(0)");
                }

                sb.Append("        UNION ALL");
                sb.Append("        SELECT DISTINCT LMT.LOCATION_MST_PK,");
                sb.Append("                        LMT.LOCATION_NAME,");
                sb.Append("                        CMT.CUSTOMER_MST_PK,");
                sb.Append("                        CMT.CUSTOMER_NAME,");
                sb.Append("                        POL.PORT_NAME,");
                if (BizType == 2)
                {
                    sb.Append("                        CPT.CUST_TEU,");
                }
                else
                {
                    sb.Append("                        '' CUST_TEU,");
                }
                sb.Append("                        CPT.CUST_CBM,");
                sb.Append("                        CPT.CUST_MTS,");
                if (BizType == 2)
                {
                    sb.Append("     0       ACTUAL_TEU,");
                }
                else
                {
                    sb.Append("    ''       ACTUAL_TEU,");
                }
                sb.Append("                        0                   ACTUAL_WT,");
                sb.Append("                        0                   VOLUME_IN_CBM, ");
                sb.Append("          BST.BOOKING_DATE ");
                sb.Append("          FROM CUSTOMER_POTENTIAL_TBL CPT,");
                sb.Append("               BOOKING_MST_TBL        BST,");
                sb.Append("               CUSTOMER_MST_TBL       CMT,");
                sb.Append("               PORT_MST_TBL           POL,");
                if (Basis == 1)
                {
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 2)
                {
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                }
                else if (Basis == 3)
                {
                    sb.Append("  REGION_MST_TBL   REG,");
                    sb.Append("  AREA_MST_TBL   AREA,");
                }
                sb.Append("               LOCATION_MST_TBL       LMT,");
                sb.Append("               COMPETITORS_TBL   CMP,");
                sb.Append("                CUSTOMER_CONTACT_DTLS  CCD");
                sb.Append("         WHERE BST.CUST_CUSTOMER_MST_FK(+) = CPT.CUSTOMER_MST_FK");
                sb.Append("           AND CPT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("           AND POL.PORT_MST_PK(+) = CPT.PORT_MST_POL_FK");
                if (Basis == 1)
                {
                    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                }
                else if (Basis == 2)
                {
                    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK");
                }
                else if (Basis == 3)
                {
                    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK");
                }

                sb.Append("           AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (BizType == 2)
                {
                    sb.Append(" AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else
                {
                    sb.Append(" AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }
                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }
                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }
                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }
                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                if (Period != 0)
                {
                    sb.Append("        AND CPT.cust_period IN(" + Period + ")");
                }
                if (BizType == 2)
                {
                    sb.Append("             AND POL.Business_Type IN(2)");
                }
                else
                {
                    sb.Append("             AND POL.Business_Type IN(1)");
                }
                if (BizType == 2)
                {
                    sb.Append("  AND BST.Business_Type =2");
                }
                else if (BizType == 1)
                {
                    sb.Append("  AND BST.Business_Type =1");
                }
                sb.Append("           AND BST.BOOKING_MST_PK  IS NULL)");
                sb.Append("          WHERE 1=1 ");
                if ((Fromdate != null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE) BETWEEN        TO_DATE('" + Fromdate + "') AND ");
                    sb.Append("           TO_DATE('" + Todate + "') ");
                }
                else if ((Fromdate == null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)<=TO_DATE('" + Todate + "')  ");
                }
                else if ((Todate == null) & (Fromdate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)>=TO_DATE('" + Fromdate + "')  ");
                }
                sb.Append(" GROUP BY LOCATION_MST_PK, ");
                sb.Append(" LOCATION_NAME, ");
                sb.Append(" CUSTOMER_MST_PK, ");
                sb.Append(" CUSTOMER_NAME)");
                sb.Append("");
                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "LOCATION");
                ///'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                //' ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                sb.Remove(0, sb.Length);
                sb.Append("SELECT LOCATION_MST_PK,");
                sb.Append("       CUSTOMER_MST_PK,");
                sb.Append("       CUSTOMER_NAME");
                sb.Append("  FROM (SELECT DISTINCT LMT.LOCATION_MST_PK,");
                sb.Append("                        LMT.LOCATION_NAME,");
                sb.Append("                        CMT.CUSTOMER_MST_PK,");
                sb.Append("                        CMT.CUSTOMER_NAME,");
                sb.Append("                        POL.PORT_NAME,");
                if (BizType == 2)
                {
                    //sb.Append("     CPT.CUST_TEU,")
                    sb.Append("  (select sum(CPT.CUST_TEU) from CUSTOMER_POTENTIAL_TBL CPT");
                    sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                    if (Period != 0)
                    {
                        sb.Append("        and CPT.cust_period IN(" + Period + ")");
                    }
                    if (Basis == 1)
                    {
                        sb.Append("               and cpt.port_mst_pod_fk is not null");
                    }
                    else if (Basis == 2)
                    {
                        sb.Append("               and cpt.country_mst_fk is not null");
                    }
                    else
                    {
                        sb.Append("               and cpt.REGION_MST_FK is not null");
                    }
                    if (Basis == 1)
                    {
                        sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_TEU,");
                    }
                    else if (Basis == 2)
                    {
                        sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_TEU,");
                    }
                    else
                    {
                        sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_TEU,");
                    }
                }
                else
                {
                    sb.Append("  '' CUST_TEU,");
                }
                //sb.Append("                        CPT.CUST_CBM,")
                sb.Append("        (select sum(CPT.CUST_CBM) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_CBM,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_CBM,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_CBM,");
                }

                //sb.Append("                        CPT.CUST_MTS,")
                sb.Append("  (select sum(CPT.CUST_MTS) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_MTS,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_MTS,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_MTS,");
                }

                if (BizType == 2)
                {
                    sb.Append("                        CASE");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 1 THEN");
                    sb.Append("                           (SELECT SUM(NVL(BTSF.NO_OF_BOXES, 0) *");
                    sb.Append("                                       NVL(CTMT.TEU_FACTOR, 0))");
                    sb.Append("                              FROM BOOKING_TRN BTSF,");
                    sb.Append("                                   CONTAINER_TYPE_MST_TBL  CTMT");
                    sb.Append("                             WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                    sb.Append("                               AND CTMT.CONTAINER_TYPE_MST_PK =");
                    sb.Append("                                   BTSF.CONTAINER_TYPE_MST_FK)");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 2 THEN");
                    sb.Append("                           0");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 4 THEN");
                    sb.Append("                           0");
                    sb.Append("                          ELSE");
                    sb.Append("                           0");
                    sb.Append("                        END ACTUAL_TEU,");
                }
                else
                {
                    sb.Append("   '' ACTUAL_TEU,");
                }

                if (BizType == 2)
                {
                    sb.Append("                        CASE");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 1 THEN");
                    sb.Append("                           NVL(BST.NET_WEIGHT, 0) / 1000");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 2 THEN");
                    sb.Append("                           NVL(BST.CHARGEABLE_WEIGHT, 0) / 1000");
                    sb.Append("                          WHEN BST.CARGO_TYPE = 4 THEN");
                    sb.Append("                           (SELECT SUM(NVL(BTSF.WEIGHT_MT, 0))");
                    sb.Append("                              FROM BOOKING_TRN BTSF");
                    sb.Append("                             WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK)");
                    sb.Append("                          ELSE");
                    sb.Append("                           NVL(BST.NET_WEIGHT, 0) / 1000");
                    sb.Append("                        END ACTUAL_WT,");
                }
                else
                {
                    sb.Append("    NVL(BST.Chargeable_Weight, 0) / 1000 ACTUAL_WT,");
                }
                sb.Append("         NVL(BST.VOLUME_IN_CBM, 0) VOLUME_IN_CBM, ");
                sb.Append("          BST.BOOKING_DATE ");
                sb.Append("          FROM BOOKING_MST_TBL        BST,");
                sb.Append("               PORT_MST_TBL           POL,");

                if (Basis == 1)
                {
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 2)
                {
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 3)
                {
                    sb.Append("  REGION_MST_TBL   REG,");
                    sb.Append("  AREA_MST_TBL   AREA,");
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                sb.Append("               CUSTOMER_MST_TBL       CMT,");
                sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("               LOCATION_MST_TBL       LMT");
                //sb.Append("               CUSTOMER_POTENTIAL_TBL CPT,")
                //sb.Append("               COMPETITORS_TBL        CMP")
                sb.Append("         WHERE CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                //sb.Append("           AND CMT.CUSTOMER_MST_PK = CPT.CUSTOMER_MST_FK")
                sb.Append("           AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                //sb.Append("           AND CPT.CUSTOMER_POTENTIAL_MST_PK = CMP.CUST_POTEN_MST_FK")
                sb.Append("           AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("           AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");

                if (Basis == 1)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 2)
                {
                    //sb.Append(" AND LMT.Country_Mst_Fk=COUN.COUNTRY_MST_PK")
                    sb.Append("  AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 3)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append("  AND POD.COUNTRY_MST_FK = COUN.COUNTRY_MST_PK");
                    sb.Append(" AND COUN.AREA_MST_FK=AREA.AREA_MST_PK");
                    sb.Append(" AND AREA.Region_Mst_Fk=REG.REGION_MST_PK");
                }

                //sb.Append("           AND CPT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)")
                //If Basis = 1 Then
                //    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)")
                //ElseIf Basis = 2 Then
                //    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK(+)")
                //ElseIf Basis = 3 Then
                //    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK(+)")
                //End If

                //sb.Append("           AND CPT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK")
                sb.Append("           AND BST.STATUS = 2");
                if (BizType == 2)
                {
                    sb.Append("  AND BST.Business_Type =2");
                }
                else if (BizType == 1)
                {
                    sb.Append("  AND BST.Business_Type =1");
                }
                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }
                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }
                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }
                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }
                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                //If Period <> 0 Then
                //    sb.Append("        AND CPT.cust_period IN(" & Period & ")")

                //End If
                if (BizType == 2 & Basis == 3)
                {
                    sb.Append("  AND REG.Business_Type IN(2)");
                }
                else if (BizType == 1 & Basis == 3)
                {
                    sb.Append("     AND REG.Business_Type IN(0)");
                }

                sb.Append("        UNION ALL");
                sb.Append("        SELECT DISTINCT LMT.LOCATION_MST_PK,");
                sb.Append("                        LMT.LOCATION_NAME,");
                sb.Append("                        CMT.CUSTOMER_MST_PK,");
                sb.Append("                        CMT.CUSTOMER_NAME,");
                sb.Append("                        POL.PORT_NAME,");
                if (BizType == 2)
                {
                    sb.Append("     CPT.CUST_TEU,");
                }
                else
                {
                    sb.Append("   '' CUST_TEU,");
                }
                sb.Append("                        CPT.CUST_CBM,");
                sb.Append("                        CPT.CUST_MTS,");
                if (BizType == 2)
                {
                    sb.Append("     0       ACTUAL_TEU,");
                }
                else
                {
                    sb.Append("    ''       ACTUAL_TEU,");
                }
                sb.Append("                        0                   ACTUAL_WT,");
                sb.Append("                        0                   VOLUME_IN_CBM, ");
                sb.Append("          BST.BOOKING_DATE ");
                sb.Append("          FROM CUSTOMER_POTENTIAL_TBL CPT,");
                sb.Append("               BOOKING_MST_TBL        BST,");
                sb.Append("               CUSTOMER_MST_TBL       CMT,");
                sb.Append("               PORT_MST_TBL           POL,");

                if (Basis == 1)
                {
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 2)
                {
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                }
                else if (Basis == 3)
                {
                    sb.Append("  REGION_MST_TBL   REG,");
                }
                sb.Append("               LOCATION_MST_TBL       LMT,");
                sb.Append("               CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("               COMPETITORS_TBL        CMP");
                sb.Append("         WHERE BST.CUST_CUSTOMER_MST_FK(+) = CPT.CUSTOMER_MST_FK");
                sb.Append("           AND CPT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("            AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("           AND POL.PORT_MST_PK(+) = CPT.PORT_MST_POL_FK");

                if (Basis == 1)
                {
                    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                }
                else if (Basis == 2)
                {
                    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK");
                }
                else if (Basis == 3)
                {
                    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK");
                }

                sb.Append("           AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                if (BizType == 2)
                {
                    sb.Append(" AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else
                {
                    sb.Append(" AND CMT.BUSINESS_TYPE IN (1,3)");
                }

                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }
                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }
                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }
                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }
                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                if (Period != 0)
                {
                    sb.Append("        AND CPT.cust_period IN(" + Period + ")");
                }

                if (BizType == 2)
                {
                    sb.Append("             AND POL.Business_Type IN(2)");
                }
                else
                {
                    sb.Append("             AND POL.Business_Type IN(1)");
                }
                if (BizType == 2)
                {
                    sb.Append("  AND BST.Business_Type =2");
                }
                else if (BizType == 1)
                {
                    sb.Append("  AND BST.Business_Type =1");
                }
                sb.Append("           AND BST.BOOKING_MST_PK IS NULL)");

                sb.Append("          WHERE 1=1 ");
                if ((Fromdate != null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE) BETWEEN        TO_DATE('" + Fromdate + "') AND ");
                    sb.Append("           TO_DATE('" + Todate + "') ");
                }
                else if ((Fromdate == null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)<=TO_DATE('" + Todate + "')  ");
                }
                else if ((Todate == null) & (Fromdate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)>=TO_DATE('" + Fromdate + "')  ");
                }

                sb.Append(" GROUP BY LOCATION_MST_PK, LOCATION_NAME, CUSTOMER_MST_PK, CUSTOMER_NAME");
                sb.Append("");
                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                }
                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "CUSTOMER");
                ///''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                ///''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                sb.Remove(0, sb.Length);
                sb.Append("select CUSTOMER_MST_PK,");
                sb.Append("LOCATION_MST_PK,");
                sb.Append("pol_pk,");
                sb.Append("pol_port,");
                sb.Append("pod_pk,");
                sb.Append("pod_port,");
                sb.Append("CUST_TEU,");
                sb.Append("SUM(ACTUAL_TEU),");
                sb.Append("CUST_CBM,");
                sb.Append("SUM(VOLUME_IN_CBM),");
                sb.Append("CUST_MTS,");
                sb.Append("SUM(ACTUAL_WT) FROM(");
                sb.Append("SELECT  CMT.CUSTOMER_MST_PK,");
                // sb.Append("               CMT.Customer_Name,")
                sb.Append("                LMT.LOCATION_MST_PK,");
                sb.Append("                POL.PORT_MST_PK pol_pk,");
                sb.Append("                POL.PORT_NAME pol_port,");
                if (Basis == 1)
                {
                    sb.Append("                POD.PORT_MST_PK pod_pk,");
                    sb.Append("                POD.Port_Name pod_port,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                COUN.country_mst_pk pod_pk,");
                    sb.Append("              COUN.COUNTRY_NAME  pod_port,");
                }
                else if (Basis == 3)
                {
                    sb.Append("                REG.REGION_MST_PK  pod_pk,");
                    sb.Append("                 REG.Region_Name  pod_port,");
                }
                if (BizType == 2)
                {
                    //sb.Append("                CPT.CUST_TEU,")
                    sb.Append("  (select sum(CPT.CUST_TEU) from CUSTOMER_POTENTIAL_TBL CPT");
                    sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                    if (Period != 0)
                    {
                        sb.Append("        and CPT.cust_period IN(" + Period + ")");
                    }
                    if (Basis == 1)
                    {
                        sb.Append("               and cpt.port_mst_pod_fk is not null");
                    }
                    else if (Basis == 2)
                    {
                        sb.Append("               and cpt.country_mst_fk is not null");
                    }
                    else
                    {
                        sb.Append("               and cpt.REGION_MST_FK is not null");
                    }
                    if (Basis == 1)
                    {
                        sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_TEU,");
                    }
                    else if (Basis == 2)
                    {
                        sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_TEU,");
                    }
                    else
                    {
                        sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_TEU,");
                    }
                    sb.Append("                CASE");
                    sb.Append("                  WHEN BST.CARGO_TYPE = 1 THEN");
                    sb.Append("                   (SELECT SUM(NVL(BTSF.NO_OF_BOXES, 0) *");
                    sb.Append("                               NVL(CTMT.TEU_FACTOR, 0))");
                    sb.Append("                      FROM BOOKING_TRN BTSF,");
                    sb.Append("                           CONTAINER_TYPE_MST_TBL  CTMT");
                    sb.Append("                     WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                    sb.Append("                       AND CTMT.CONTAINER_TYPE_MST_PK =");
                    sb.Append("                           BTSF.CONTAINER_TYPE_MST_FK)");
                    sb.Append("                  WHEN BST.CARGO_TYPE = 2 THEN");
                    sb.Append("                   0");
                    sb.Append("                  WHEN BST.CARGO_TYPE = 4 THEN");
                    sb.Append("                   0");
                    sb.Append("                  ELSE");
                    sb.Append("                   0");
                    sb.Append("                END ACTUAL_TEU,");
                }
                else
                {
                    sb.Append("  ''  CUST_TEU,");
                    sb.Append("  ''  ACTUAL_TEU,");
                }

                //sb.Append("                CPT.CUST_CBM,")
                sb.Append("        (select sum(CPT.CUST_CBM) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_CBM,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_CBM,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_CBM,");
                }

                sb.Append("                NVL(BST.VOLUME_IN_CBM, 0) VOLUME_IN_CBM,");
                //sb.Append("                CPT.CUST_MTS,")
                sb.Append("  (select sum(CPT.CUST_MTS) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_MTS,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_MTS,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_MTS,");
                }
                if (BizType == 2)
                {
                    sb.Append("                  CASE");
                    sb.Append("                  WHEN BST.CARGO_TYPE = 1 THEN");
                    sb.Append("                   NVL(BST.NET_WEIGHT, 0) / 1000");
                    sb.Append("                  WHEN BST.CARGO_TYPE = 2 THEN");
                    sb.Append("                   NVL(BST.CHARGEABLE_WEIGHT, 0) / 1000");
                    sb.Append("                  WHEN BST.CARGO_TYPE = 4 THEN");
                    sb.Append("                   (SELECT SUM(NVL(BTSF.WEIGHT_MT, 0))");
                    sb.Append("                      FROM BOOKING_TRN BTSF");
                    sb.Append("                     WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK)");
                    sb.Append("                  ELSE");
                    sb.Append("                   NVL(BST.NET_WEIGHT, 0) / 1000");
                    sb.Append("                END ACTUAL_WT");
                }
                else
                {
                    sb.Append(" NVL(BST.Chargeable_Weight, 0) / 1000 ACTUAL_WT");
                }
                sb.Append("     , BST.BOOKING_DATE ");
                //sb.Append("                NVL(BST.VOLUME_IN_CBM, 0) VOLUME_IN_CBM")
                sb.Append("  FROM BOOKING_MST_TBL        BST,");
                sb.Append("       PORT_MST_TBL           POL,");
                if (Basis == 1)
                {
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 2)
                {
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("  PORT_MST_TBL   POD,");
                }
                else if (Basis == 3)
                {
                    sb.Append("  REGION_MST_TBL   REG,");
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("       PORT_MST_TBL           POD,");
                    sb.Append("      AREA_MST_TBL   AREA,");
                }
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("       LOCATION_MST_TBL       LMT");
                //sb.Append("       CUSTOMER_POTENTIAL_TBL CPT")
                //sb.Append("      COUNTRY_MST_TBL   COUN,")
                // sb.Append("      REGION_MST_TBL   REG,")
                //sb.Append("      AREA_MST_TBL   AREA")
                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                //sb.Append("   AND CMT.CUSTOMER_MST_PK = CPT.CUSTOMER_MST_FK")
                sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("  AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");

                if (Basis == 1)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 2)
                {
                    // sb.Append(" AND LMT.Country_Mst_Fk=COUN.COUNTRY_MST_PK")
                    sb.Append("  AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 3)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append(" AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                    //sb.Append(" AND LMT.Country_Mst_Fk=COUN.COUNTRY_MST_PK")
                    sb.Append(" AND COUN.AREA_MST_FK=AREA.AREA_MST_PK");
                    sb.Append(" AND AREA.Region_Mst_Fk=REG.REGION_MST_PK");
                }

                //sb.Append("   AND CPT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)")

                //If Basis = 1 Then
                //    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)")
                //ElseIf Basis = 2 Then
                //    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK(+)")
                //ElseIf Basis = 3 Then
                //    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK(+)")
                //End If

                sb.Append("   AND BST.STATUS = 2");
                if (BizType == 2)
                {
                    sb.Append("  AND BST.Business_Type =2");
                }
                else if (BizType == 1)
                {
                    sb.Append("  AND BST.Business_Type =1");
                }
                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }
                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }
                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }
                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }
                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                //If Period <> 0 Then
                //    sb.Append("        AND CPT.cust_period IN(" & Period & ")")

                //End If
                if (BizType == 2 & Basis == 3)
                {
                    sb.Append("  AND REG.Business_Type IN(2)");
                }
                else if (BizType == 1 & Basis == 3)
                {
                    sb.Append("     AND REG.Business_Type IN(0)");
                }

                sb.Append("           UNION ALL ");
                sb.Append("         SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
                // sb.Append("               CMT.Customer_Name,")
                sb.Append("                LMT.LOCATION_MST_PK,");
                sb.Append("                POL.Port_Mst_Pk pol_pk,");
                sb.Append("                POL.PORT_NAME pol_port,");
                if (Basis == 1)
                {
                    sb.Append("                POD.PORT_MST_PK pod_pk,");
                    sb.Append("                POD.Port_Name pod_port,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                COUN.country_mst_pk pod_pk,");
                    sb.Append("              COUN.COUNTRY_NAME pod_port,");
                }
                else if (Basis == 3)
                {
                    sb.Append("                REG.REGION_MST_PK  pod_pk,");
                    sb.Append("                 REG.Region_Name  pod_port,");
                }
                if (BizType == 2)
                {
                    sb.Append("                CPT.CUST_TEU,");
                    sb.Append("                 0  ACTUAL_TEU,");
                }
                else
                {
                    sb.Append(" ''   CUST_TEU,");
                    sb.Append(" ''   ACTUAL_TEU,");
                }
                sb.Append("                CPT.CUST_CBM,");
                sb.Append("                0  VOLUME_IN_CBM,");
                sb.Append("                CPT.CUST_MTS,");
                sb.Append("                  0  ACTUAL_WT,");
                sb.Append("          BST.BOOKING_DATE ");
                sb.Append("  FROM CUSTOMER_POTENTIAL_TBL CPT,");
                sb.Append("       BOOKING_MST_TBL        BST,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("        CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("      COUNTRY_MST_TBL   COUN,");
                sb.Append("      REGION_MST_TBL   REG,");
                sb.Append("      AREA_MST_TBL   AREA");
                sb.Append(" WHERE BST.CUST_CUSTOMER_MST_FK(+) = CPT.CUSTOMER_MST_FK");
                sb.Append("   AND CPT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("    AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK(+) = CPT.PORT_MST_POL_FK");

                if (Basis == 1)
                {
                    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                }
                else if (Basis == 2)
                {
                    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK");
                }
                else if (Basis == 3)
                {
                    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK");
                }
                sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");

                if (BizType == 2)
                {
                    sb.Append(" AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else
                {
                    sb.Append(" AND CMT.BUSINESS_TYPE IN (1,3)");
                }

                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }

                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }

                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }

                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }

                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                if (Period != 0)
                {
                    sb.Append("        AND CPT.cust_period IN(" + Period + ")");
                }
                if (BizType == 2)
                {
                    sb.Append("             AND POL.Business_Type IN(2)");
                }
                else
                {
                    sb.Append("             AND POL.Business_Type IN(1)");
                }
                if (BizType == 2)
                {
                    sb.Append("  AND BST.Business_Type =2");
                }
                else if (BizType == 1)
                {
                    sb.Append("  AND BST.Business_Type =1");
                }
                sb.Append("   AND BST.BOOKING_MST_PK IS NULL)");
                sb.Append("          WHERE 1=1 ");
                if ((Fromdate != null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE) BETWEEN        TO_DATE('" + Fromdate + "') AND ");
                    sb.Append("           TO_DATE('" + Todate + "') ");
                }
                else if ((Fromdate == null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)<=TO_DATE('" + Todate + "')  ");
                }
                else if ((Todate == null) & (Fromdate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)>=TO_DATE('" + Fromdate + "')  ");
                }
                sb.Append("   Group By ");
                sb.Append("   CUSTOMER_MST_PK,");
                sb.Append("LOCATION_MST_PK,");
                sb.Append("pol_pk,");
                sb.Append("pol_port,");
                sb.Append("pod_pk,");
                sb.Append("pod_port,");
                sb.Append("CUST_TEU,");
                sb.Append("CUST_CBM,");
                sb.Append("CUST_MTS");
                sb.Append("");
                if (BizType == 1)
                {
                    sb.Replace("SEA", "AIR");
                }

                DA = objWF.GetDataAdapter(sb.ToString());
                DA.Fill(MainDS, "TRANSACTION");

                DataRelation rellocation_Details = new DataRelation("LOCATION", new DataColumn[] { MainDS.Tables[0].Columns["LOCATION_MST_PK"] }, new DataColumn[] { MainDS.Tables[1].Columns["LOCATION_MST_PK"] });
                DataRelation relCustomer_Details = new DataRelation("CUSTOMER", new DataColumn[] { MainDS.Tables[1].Columns["CUSTOMER_MST_PK"] }, new DataColumn[] { MainDS.Tables[2].Columns["CUSTOMER_MST_PK"] });
                rellocation_Details.Nested = true;
                relCustomer_Details.Nested = true;
                MainDS.Relations.Add(rellocation_Details);
                MainDS.Relations.Add(relCustomer_Details);
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

        #endregion "TO Fetch PotentialInformation"

        #region "TOFetch SeaReport"

        /// <summary>
        /// Fetches the sea report.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Locationpk">The locationpk.</param>
        /// <param name="Period">The period.</param>
        /// <param name="Basis">The basis.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="COUNTRYPK">The countrypk.</param>
        /// <param name="REGIONPK">The regionpk.</param>
        /// <param name="Fromdate">The fromdate.</param>
        /// <param name="Todate">The todate.</param>
        /// <returns></returns>
        public object FetchSeaReport(int BizType = 0, string Customerpk = "", string Locationpk = "", int Period = 0, int Basis = 0, string POLPK = "", string PODPK = "", string COUNTRYPK = "", string REGIONPK = "", string Fromdate = null,
        string Todate = null)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            OracleDataAdapter DA = new OracleDataAdapter();
            try
            {
                sb.Append("select location_name,");
                sb.Append("customer_name,");
                sb.Append("pol_pk,");
                sb.Append("pol_port,");
                sb.Append("pod_pk,");
                sb.Append("pod_port,");
                sb.Append("CUST_TEU,");
                sb.Append("SUM(ACTUAL_TEU),");
                sb.Append("CUST_CBM,");
                sb.Append("SUM(ACTUAL_WT),");
                sb.Append("CUST_MTS,");
                sb.Append("SUM(VOLUME_IN_CBM),");
                sb.Append("Period,");
                sb.Append("businesstype FROM(");
                //sb.Append("SUM(VOLUME_IN_CBM) FROM(")
                sb.Append("SELECT  lmt.location_name,");
                sb.Append("                cmt.customer_name,");
                sb.Append("                CMT.CUSTOMER_MST_PK,");
                sb.Append("                LMT.LOCATION_MST_PK,");
                sb.Append("                POL.PORT_MST_PK pol_pk,");
                sb.Append("                POL.PORT_NAME pol_port,");
                if (Basis == 1)
                {
                    sb.Append("                POD.PORT_MST_PK pod_pk,");
                    sb.Append("                POD.Port_Name pod_port,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                COUN.country_mst_pk pod_pk,");
                    sb.Append("              COUN.COUNTRY_NAME  pod_port,");
                }
                else if (Basis == 3)
                {
                    sb.Append("                REG.REGION_MST_PK  pod_pk,");
                    sb.Append("                 REG.Region_Name  pod_port,");
                }
                //sb.Append("                CPT.CUST_TEU,")
                sb.Append("  (select sum(CPT.CUST_TEU) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_TEU,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_TEU,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_TEU,");
                }
                sb.Append("                CASE");
                sb.Append("                  WHEN BST.CARGO_TYPE = 1 THEN");
                sb.Append("                   (SELECT SUM(NVL(BTSF.NO_OF_BOXES, 0) *");
                sb.Append("                               NVL(CTMT.TEU_FACTOR, 0))");
                sb.Append("                      FROM BOOKING_TRN BTSF,");
                sb.Append("                           CONTAINER_TYPE_MST_TBL  CTMT");
                sb.Append("                     WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK");
                sb.Append("                       AND CTMT.CONTAINER_TYPE_MST_PK =");
                sb.Append("                           BTSF.CONTAINER_TYPE_MST_FK)");
                sb.Append("                  WHEN BST.CARGO_TYPE = 2 THEN");
                sb.Append("                   0");
                sb.Append("                  WHEN BST.CARGO_TYPE = 4 THEN");
                sb.Append("                   0");
                sb.Append("                  ELSE");
                sb.Append("                   0");
                sb.Append("                END ACTUAL_TEU,");
                //sb.Append("                CPT.CUST_CBM,")
                sb.Append("        (select sum(CPT.CUST_CBM) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_CBM,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_CBM,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_CBM,");
                }
                sb.Append("                CASE");
                sb.Append("                  WHEN BST.CARGO_TYPE = 1 THEN");
                sb.Append("                   NVL(BST.NET_WEIGHT, 0) / 1000");
                sb.Append("                  WHEN BST.CARGO_TYPE = 2 THEN");
                sb.Append("                   NVL(BST.CHARGEABLE_WEIGHT, 0) / 1000");
                sb.Append("                  WHEN BST.CARGO_TYPE = 4 THEN");
                sb.Append("                   (SELECT SUM(NVL(BTSF.WEIGHT_MT, 0))");
                sb.Append("                      FROM BOOKING_TRN BTSF");
                sb.Append("                     WHERE BTSF.BOOKING_MST_FK = BST.BOOKING_MST_PK)");
                sb.Append("                  ELSE");
                sb.Append("                   NVL(BST.NET_WEIGHT, 0) / 1000");
                sb.Append("                END ACTUAL_WT,");
                //sb.Append("                CPT.CUST_MTS,")
                sb.Append("  (select sum(CPT.CUST_MTS) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_MTS,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_MTS,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_MTS,");
                }
                sb.Append("                NVL(BST.VOLUME_IN_CBM, 0) VOLUME_IN_CBM,");
                sb.Append("     (select DISTINCT DECODE(CPT.Cust_Period,");
                sb.Append("                                       1,");
                sb.Append("                                       'Montly',");
                sb.Append("                                       2,");
                sb.Append("                                       'Quaterly',");
                sb.Append("                                       3,");
                sb.Append("                                       'HalfYearly',");
                sb.Append("                                       4,");
                sb.Append("                                       'Yearly') Cust_Period");
                sb.Append("                  from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("                 where cpt.customer_mst_fk = cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) Period,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) Period,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) Period,");
                }
                sb.Append("               DECODE(CMT.BUSINESS_TYPE ,1,'Air', 2,'sea',3,'Both')businesstype, ");
                sb.Append("               BST.BOOKING_DATE ");
                sb.Append("  FROM BOOKING_MST_TBL        BST,");
                sb.Append("       PORT_MST_TBL           POL,");
                //sb.Append("       PORT_MST_TBL           POD,")
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                // sb.Append("       CUSTOMER_POTENTIAL_TBL CPT,")
                //sb.Append("       COUNTRY_MST_TBL        COUN,")
                //sb.Append("       REGION_MST_TBL         REG,")
                //sb.Append("       AREA_MST_TBL           AREA")
                if (Basis == 1)
                {
                    sb.Append("  PORT_MST_TBL   POD");
                }
                else if (Basis == 2)
                {
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("  PORT_MST_TBL   POD");
                }
                else if (Basis == 3)
                {
                    sb.Append("  REGION_MST_TBL   REG,");
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("     PORT_MST_TBL   POD,");
                    sb.Append("      AREA_MST_TBL   AREA");
                }

                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                //sb.Append("   AND CMT.CUSTOMER_MST_PK = CPT.CUSTOMER_MST_FK")
                sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                if (Basis == 1)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 2)
                {
                    sb.Append("  AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 3)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append(" AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                    sb.Append(" AND COUN.AREA_MST_FK=AREA.AREA_MST_PK");
                    sb.Append(" AND AREA.Region_Mst_Fk=REG.REGION_MST_PK");
                }

                //sb.Append("   AND CPT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)")
                //If Basis = 1 Then
                //    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)")
                //ElseIf Basis = 2 Then
                //    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK(+)")
                //ElseIf Basis = 3 Then
                //    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK(+)")
                //End If

                sb.Append("   AND BST.STATUS = 2");
                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }
                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }
                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }
                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }
                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                //If Period <> 0 Then
                //    sb.Append("        AND CPT.cust_period IN(" & Period & ")")

                //End If
                if (Basis == 3)
                {
                    sb.Append("  AND REG.Business_Type IN(2)");
                }
                sb.Append("            UNION ALL");

                sb.Append("       SELECT DISTINCT lmt.location_name,");
                sb.Append("                cmt.customer_name,");
                sb.Append("                CMT.CUSTOMER_MST_PK,");
                sb.Append("                LMT.LOCATION_MST_PK,");
                sb.Append("                POL.Port_Mst_Pk,");
                sb.Append("                POL.PORT_NAME,");
                if (Basis == 1)
                {
                    sb.Append("                POD.PORT_MST_PK pod_pk,");
                    sb.Append("                POD.Port_Name pod_port,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                COUN.country_mst_pk pod_pk,");
                    sb.Append("              COUN.COUNTRY_NAME pod_port,");
                }
                else if (Basis == 3)
                {
                    sb.Append("                REG.REGION_MST_PK  pod_pk,");
                    sb.Append("                 REG.Region_Name  pod_port,");
                }
                sb.Append("                CPT.CUST_TEU,");
                sb.Append("                0                   ACTUAL_TEU,");
                sb.Append("                CPT.CUST_CBM,");
                sb.Append("                0                   ACTUAL_WT,");
                sb.Append("                CPT.CUST_MTS,");
                sb.Append("                0                   VOLUME_IN_CBM,");
                sb.Append("               DECODE(CPT.Cust_Period, 1, 'Montly', 2,'Quaterly', 3,'HalfYearly',4,'Yearly')Period,");
                sb.Append("               DECODE(CMT.BUSINESS_TYPE ,1,'Air', 2,'sea',3,'Both')businesstype, ");
                sb.Append("               BST.BOOKING_DATE ");
                sb.Append("  FROM CUSTOMER_POTENTIAL_TBL CPT,");
                sb.Append("       BOOKING_MST_TBL        BST,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       COUNTRY_MST_TBL        COUN,");
                sb.Append("       REGION_MST_TBL         REG,");
                sb.Append("       AREA_MST_TBL           AREA");
                sb.Append(" WHERE BST.CUST_CUSTOMER_MST_FK(+) = CPT.CUSTOMER_MST_FK");
                sb.Append("   AND CPT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK(+) = CPT.PORT_MST_POL_FK");
                if (Basis == 1)
                {
                    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                }
                else if (Basis == 2)
                {
                    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK");
                }
                else if (Basis == 3)
                {
                    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK");
                }
                sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }

                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }

                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }

                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }

                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                if (Period != 0)
                {
                    sb.Append("        AND CPT.cust_period IN(" + Period + ")");
                }
                if (BizType == 2)
                {
                    sb.Append("             AND POL.Business_Type IN(2)");
                }
                else
                {
                    sb.Append("             AND POL.Business_Type IN(1)");
                }
                sb.Append("   AND BST.BOOKING_MST_PK IS NULL)");
                sb.Append("  WHERE 1=1 ");
                if ((Fromdate != null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE) BETWEEN        TO_DATE('" + Fromdate + "') AND ");
                    sb.Append("           TO_DATE('" + Todate + "') ");
                }
                else if ((Fromdate == null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)<=TO_DATE('" + Todate + "')  ");
                }
                else if ((Todate == null) & (Fromdate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)>=TO_DATE('" + Fromdate + "')  ");
                }
                sb.Append("   Group By ");
                sb.Append("   location_name,");
                sb.Append("customer_name,");
                sb.Append("pol_pk,");
                sb.Append("pol_port,");
                sb.Append("pod_pk,");
                sb.Append("pod_port,");
                sb.Append("CUST_TEU,");
                sb.Append("CUST_CBM,");
                sb.Append("CUST_MTS,");
                sb.Append("Period,");
                sb.Append("businesstype");
                sb.Append("");
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
                throw;
            }
        }

        #endregion "TOFetch SeaReport"

        #region "TO Fetch Air Report"

        /// <summary>
        /// Fetches the air report.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="Customerpk">The customerpk.</param>
        /// <param name="Locationpk">The locationpk.</param>
        /// <param name="Period">The period.</param>
        /// <param name="Basis">The basis.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="COUNTRYPK">The countrypk.</param>
        /// <param name="REGIONPK">The regionpk.</param>
        /// <param name="Fromdate">The fromdate.</param>
        /// <param name="Todate">The todate.</param>
        /// <returns></returns>
        public object FetchAirReport(int BizType = 0, string Customerpk = "", string Locationpk = "", int Period = 0, int Basis = 0, string POLPK = "", string PODPK = "", string COUNTRYPK = "", string REGIONPK = "", string Fromdate = null,
        string Todate = null)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            OracleDataAdapter DA = new OracleDataAdapter();
            try
            {
                sb.Append("select location_name,");
                sb.Append("customer_name,");
                sb.Append("pol_pk,");
                sb.Append("pol_port,");
                sb.Append("pod_pk,");
                sb.Append("pod_port,");
                sb.Append("CUST_TEU,");
                sb.Append("SUM(ACTUAL_TEU),");
                sb.Append("CUST_CBM,");
                sb.Append("SUM(ACTUAL_WT),");
                sb.Append("CUST_MTS,");
                sb.Append("SUM(VOLUME_IN_CBM),");
                sb.Append("Period,");
                sb.Append("businesstype FROM(");
                //sb.Append("SUM(VOLUME_IN_CBM) FROM(")
                sb.Append("SELECT  LMT.location_name,");
                sb.Append("                CMT.customer_name,");
                sb.Append("                CMT.CUSTOMER_MST_PK,");
                sb.Append("                LMT.LOCATION_MST_PK,");
                sb.Append("                POL.PORT_MST_PK pol_pk,");
                sb.Append("                POL.PORT_NAME pol_port,");
                if (Basis == 1)
                {
                    sb.Append("                POD.PORT_MST_PK pod_pk,");
                    sb.Append("                POD.Port_Name pod_port,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                COUN.country_mst_pk pod_pk,");
                    sb.Append("              COUN.COUNTRY_NAME  pod_port,");
                }
                else if (Basis == 3)
                {
                    sb.Append("                REG.REGION_MST_PK  pod_pk,");
                    sb.Append("                 REG.Region_Name  pod_port,");
                }
                sb.Append("                '' CUST_TEU,");
                sb.Append("                '' ACTUAL_TEU,");
                // sb.Append("                CPT.CUST_CBM,")
                sb.Append("        (select sum(CPT.CUST_CBM) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_CBM,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_CBM,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_CBM,");
                }
                sb.Append("                NVL(BST.Chargeable_Weight, 0) / 1000 ACTUAL_WT,");
                //sb.Append("                CPT.CUST_MTS,")
                sb.Append("  (select sum(CPT.CUST_MTS) from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("               where cpt.customer_mst_fk=cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) CUST_MTS,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) CUST_MTS,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) CUST_MTS,");
                }
                sb.Append("                NVL(BST.VOLUME_IN_CBM, 0) VOLUME_IN_CBM,");
                sb.Append("     (select DISTINCT DECODE(CPT.Cust_Period,");
                sb.Append("                                       1,");
                sb.Append("                                       'Montly',");
                sb.Append("                                       2,");
                sb.Append("                                       'Quaterly',");
                sb.Append("                                       3,");
                sb.Append("                                       'HalfYearly',");
                sb.Append("                                       4,");
                sb.Append("                                       'Yearly') Cust_Period");
                sb.Append("                  from CUSTOMER_POTENTIAL_TBL CPT");
                sb.Append("                 where cpt.customer_mst_fk = cmt.customer_mst_pk");
                if (Period != 0)
                {
                    sb.Append("        and CPT.cust_period IN(" + Period + ")");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk is not null");
                }
                else if (Basis == 2)
                {
                    sb.Append("               and cpt.country_mst_fk is not null");
                }
                else
                {
                    sb.Append("               and cpt.REGION_MST_FK is not null");
                }
                if (Basis == 1)
                {
                    sb.Append("               and cpt.port_mst_pod_fk=POD.PORT_MST_PK) Period,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                and cpt.country_mst_fk=coun.country_mst_pk) Period,");
                }
                else
                {
                    sb.Append("                 AND CPT.Region_Mst_Fk=REG.REGION_MST_PK) Period,");
                }
                sb.Append("               DECODE(CMT.BUSINESS_TYPE ,1,'Air', 2,'sea',3,'Both')businesstype, ");
                sb.Append("               BST.BOOKING_DATE ");
                sb.Append("  FROM BOOKING_AIR_TBL        BST,");
                sb.Append("       PORT_MST_TBL           POL,");
                //sb.Append("       PORT_MST_TBL           POD,")
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                // sb.Append("       CUSTOMER_POTENTIAL_TBL CPT,")
                if (Basis == 1)
                {
                    sb.Append("  PORT_MST_TBL   POD");
                }
                else if (Basis == 2)
                {
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("  PORT_MST_TBL   POD");
                }
                else if (Basis == 3)
                {
                    sb.Append("  REGION_MST_TBL   REG,");
                    sb.Append("  COUNTRY_MST_TBL   COUN,");
                    sb.Append("       PORT_MST_TBL  POD,");
                    sb.Append("      AREA_MST_TBL   AREA");
                }
                //sb.Append("       COUNTRY_MST_TBL        COUN,")
                //sb.Append("       REGION_MST_TBL         REG,")
                //sb.Append("       AREA_MST_TBL           AREA")
                sb.Append(" WHERE CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                // sb.Append("   AND CMT.CUSTOMER_MST_PK = CPT.CUSTOMER_MST_FK")
                sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = BST.PORT_MST_POL_FK");
                if (Basis == 1)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                }
                else if (Basis == 2)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append("  AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                }
                else if (Basis == 3)
                {
                    sb.Append(" AND POD.PORT_MST_PK = BST.PORT_MST_POD_FK");
                    sb.Append(" AND POD.COUNTRY_MST_FK=COUN.COUNTRY_MST_PK");
                    sb.Append(" AND COUN.AREA_MST_FK=AREA.AREA_MST_PK");
                    sb.Append(" AND AREA.Region_Mst_Fk=REG.REGION_MST_PK");
                }

                //sb.Append("   AND CPT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)")
                //If Basis = 1 Then
                //    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)")
                //ElseIf Basis = 2 Then
                //    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK(+)")
                //ElseIf Basis = 3 Then
                //    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK(+)")
                //End If
                sb.Append("   AND BST.STATUS = 2");
                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }
                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }
                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }
                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }
                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                //If Period <> 0 Then
                //    sb.Append("        AND CPT.cust_period IN(" & Period & ")")

                //End If
                if (Basis == 3)
                {
                    sb.Append("  AND REG.Business_Type IN(0)");
                }
                sb.Append("          UNION ALL");

                sb.Append("          SELECT DISTINCT LMT.location_name,");
                sb.Append("                CMT.customer_name,");
                sb.Append("                 CMT.CUSTOMER_MST_PK,");
                sb.Append("                LMT.LOCATION_MST_PK,");
                sb.Append("                POL.Port_Mst_Pk,");
                sb.Append("                POL.PORT_NAME,");
                if (Basis == 1)
                {
                    sb.Append("                POD.PORT_MST_PK pod_pk,");
                    sb.Append("                POD.Port_Name pod_port,");
                }
                else if (Basis == 2)
                {
                    sb.Append("                COUN.country_mst_pk pod_pk,");
                    sb.Append("              COUN.COUNTRY_NAME pod_port,");
                }
                else if (Basis == 3)
                {
                    sb.Append("                REG.REGION_MST_PK  pod_pk,");
                    sb.Append("                 REG.Region_Name  pod_port,");
                }
                sb.Append("                '' CUST_TEU,");
                sb.Append("                '' ACTUAL_TEU,");
                sb.Append("                CPT.CUST_CBM,");
                sb.Append("                0 ACTUAL_WT,");
                sb.Append("                CPT.CUST_MTS,");
                sb.Append("                0 VOLUME_IN_CBM,");
                sb.Append("               DECODE(CPT.Cust_Period, 1, 'Montly', 2,'Quaterly', 3,'HalfYearly',4,'Yearly')Period,");
                sb.Append("               DECODE(CMT.BUSINESS_TYPE ,1,'Air', 2,'sea',3,'Both')businesstype, ");
                sb.Append("                BST.BOOKING_DATE ");
                sb.Append("  FROM CUSTOMER_POTENTIAL_TBL CPT,");
                sb.Append("       BOOKING_AIR_TBL        BST,");
                sb.Append("       CUSTOMER_MST_TBL       CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS  CCD,");
                sb.Append("       PORT_MST_TBL           POL,");
                sb.Append("       PORT_MST_TBL           POD,");
                sb.Append("       LOCATION_MST_TBL       LMT,");
                sb.Append("       COUNTRY_MST_TBL        COUN,");
                sb.Append("       REGION_MST_TBL         REG,");
                sb.Append("       AREA_MST_TBL           AREA");
                sb.Append(" WHERE BST.CUST_CUSTOMER_MST_FK(+) = CPT.CUSTOMER_MST_FK");
                sb.Append("   AND CPT.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK");
                sb.Append("   AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("   AND POL.PORT_MST_PK(+) = CPT.PORT_MST_POL_FK");
                if (Basis == 1)
                {
                    sb.Append("   AND CPT.PORT_MST_POD_FK = POD.PORT_MST_PK");
                }
                else if (Basis == 2)
                {
                    sb.Append("   AND CPT.Country_Mst_Fk=COUN.COUNTRY_MST_PK");
                }
                else if (Basis == 3)
                {
                    sb.Append("   AND CPT.REGION_MST_FK = REG.REGION_MST_PK");
                }
                sb.Append("   AND CCD.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                if (!string.IsNullOrEmpty(Customerpk) & Customerpk != "0")
                {
                    sb.Append("       AND CMT.CUSTOMER_MST_PK IN(" + Customerpk + ")");
                }

                if (!string.IsNullOrEmpty(Locationpk) & Locationpk != "0")
                {
                    sb.Append("       AND LMT.Location_Mst_Pk IN(" + Locationpk + ")");
                }

                if (!string.IsNullOrEmpty(POLPK) & POLPK != "0")
                {
                    sb.Append("        AND POL.PORT_MST_PK IN(" + POLPK + ")");
                }

                if (!string.IsNullOrEmpty(PODPK) & PODPK != "0")
                {
                    sb.Append("        AND POD.Port_Mst_Pk IN(" + PODPK + ")");
                }

                if (!string.IsNullOrEmpty(COUNTRYPK) & COUNTRYPK != "0")
                {
                    sb.Append("        AND COUN.country_mst_pk IN(" + COUNTRYPK + ")");
                }

                if (!string.IsNullOrEmpty(REGIONPK) & REGIONPK != "0")
                {
                    sb.Append("        AND REG.REGION_MST_PK IN(" + REGIONPK + ")");
                }
                if (Period != 0)
                {
                    sb.Append("        AND CPT.cust_period IN(" + Period + ")");
                }
                if (BizType == 2)
                {
                    sb.Append("             AND POL.Business_Type IN(2)");
                }
                else
                {
                    sb.Append("             AND POL.Business_Type IN(1)");
                }

                sb.Append("   AND BST.BOOKING_AIR_PK IS NULL)");
                sb.Append("  WHERE 1=1 ");
                if ((Fromdate != null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE) BETWEEN        TO_DATE('" + Fromdate + "') AND ");
                    sb.Append("           TO_DATE('" + Todate + "') ");
                }
                else if ((Fromdate == null) & (Todate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)<=TO_DATE('" + Todate + "')  ");
                }
                else if ((Todate == null) & (Fromdate != null))
                {
                    sb.Append(" AND   TO_DATE(BOOKING_DATE)>=TO_DATE('" + Fromdate + "')  ");
                }

                sb.Append("   Group By ");
                sb.Append("   location_name,");
                sb.Append("customer_name,");
                sb.Append("pol_pk,");
                sb.Append("pol_port,");
                sb.Append("pod_pk,");
                sb.Append("pod_port,");
                sb.Append("CUST_TEU,");
                sb.Append("CUST_CBM,");
                sb.Append("CUST_MTS,");
                sb.Append("Period,");
                sb.Append("businesstype");
                sb.Append("");
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
                throw;
            }
        }

        #endregion "TO Fetch Air Report"
    }
}