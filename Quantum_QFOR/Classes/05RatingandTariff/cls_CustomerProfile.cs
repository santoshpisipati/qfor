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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class clsCustomerProfile : CommonFeatures
    {
        /// <summary>
        /// The m_ contact_ data set
        /// </summary>
        private DataSet M_Contact_DataSet;

        /// <summary>
        /// The object TRN_ data set
        /// </summary>
        private DataSet ObjTrn_DataSet;

        /// <summary>
        /// Gets or sets the contact data set.
        /// </summary>
        /// <value>
        /// The contact data set.
        /// </value>
        public DataSet ContactDataSet
        {
            get { return M_Contact_DataSet; }
            set { M_Contact_DataSet = value; }
        }

        /// <summary>
        /// Sets the m_dataset.
        /// </summary>
        /// <value>
        /// The m_dataset.
        /// </value>
        public DataSet M_dataset
        {
            set { ObjTrn_DataSet = value; }
        }

        //This property is added by Ashish Arya on 23rd Sept 2011,
        //to get a default backcolor for textboxes, if they are empty
        /// <summary>
        /// Gets the text control back color if empty.
        /// </summary>
        /// <value>
        /// The text control back color if empty.
        /// </value>
        public object TxtCtrlBackColorIfEmpty
        {
            get { return "GrayText"; }
        }

        #region "Fetch Catagories"

        /// <summary>
        /// Fetches the customer catagories.
        /// </summary>
        /// <param name="customerPk">The customer pk.</param>
        /// <param name="blnFlag">if set to <c>true</c> [BLN flag].</param>
        /// <param name="Notify">The notify.</param>
        /// <returns></returns>
        public DataSet fetchCustCatagories(string customerPk = "", bool blnFlag = false, string Notify = "")
        {
            string strSQL = null;
            string strSQLNoti = null;

            if (!string.IsNullOrEmpty(Notify))
            {
                strSQLNoti = "select ROWNUM SR_NO,q.* from (";
            }
            else
            {
                strSQL = "select ROWNUM SR_NO,q.* from (";
            }

            if (blnFlag)
            {
                strSQL += " SELECT 0 CUSTOMER_CATEGORY_MST_PK, ";
                strSQL += " '<ALL>' CUSTOMER_CATEGORY_ID, ";
                strSQL += " '' CUSTOMER_CATEGORY_DESC, ";
                strSQL += " 1 SELECTE";
                strSQL += " FROM DUAL ";
                strSQL += " UNION ";
            }

            if (!string.IsNullOrEmpty(customerPk))
            {
                strSQL += " SELECT CC.CUSTOMER_CATEGORY_MST_PK ,";
                strSQL += " CC.CUSTOMER_CATEGORY_ID ,";
                strSQL += " CC.CUSTOMER_CATEGORY_DESC,";
                strSQL += " 1 SELECTE";
                strSQL += " FROM CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_MST_TBL CM,CUSTOMER_CATEGORY_TRN  CCT";
                strSQL += " WHERE CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK ";
                strSQL += " And CM.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ";
                strSQL += " AND CCT.CUSTOMER_MST_FK=" + customerPk;
                strSQL += " AND CC.ACTIVE_FLAG=1";
                strSQL += " UNION";
            }

            if (string.IsNullOrEmpty(Notify))
            {
                strSQL += " SELECT CC.CUSTOMER_CATEGORY_MST_PK ,";
                strSQL += " CC.CUSTOMER_CATEGORY_ID ,";
                strSQL += " CC.CUSTOMER_CATEGORY_DESC,";
            }

            if (!string.IsNullOrEmpty(customerPk))
            {
                strSQL += " 0 SELECTE";
            }
            else
            {
                strSQL += " (CASE WHEN CC.CUSTOMER_CATEGORY_ID='CONSIGNEE' ";
                strSQL += " OR CC.CUSTOMER_CATEGORY_ID='CUSTOMER' ";
                strSQL += " OR CC.CUSTOMER_CATEGORY_ID='SHIPPER' ";
                strSQL += " OR CC.CUSTOMER_CATEGORY_ID='NOTIFY' ";
                strSQL += " THEN 1 ELSE 0 END) SELECTE ";
            }

            if (string.IsNullOrEmpty(Notify))
            {
                strSQL += " FROM CUSTOMER_CATEGORY_MST_TBL CC";
                strSQL += " WHERE 1=1";
            }

            if (!string.IsNullOrEmpty(customerPk))
            {
                strSQL += " AND CC.CUSTOMER_CATEGORY_MST_PK NOT IN (SELECT CC.CUSTOMER_CATEGORY_MST_PK";
                strSQL += " FROM CUSTOMER_CATEGORY_MST_TBL CC,CUSTOMER_MST_TBL CM,CUSTOMER_CATEGORY_TRN  CCT";
                strSQL += " WHERE(CC.CUSTOMER_CATEGORY_MST_PK = CCT.CUSTOMER_CATEGORY_MST_FK )";
                strSQL += " And CM.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK";
                strSQL += " AND CCT.CUSTOMER_MST_FK=" + customerPk;
                strSQL += " AND CC.ACTIVE_FLAG=1 )";
            }

            if (!string.IsNullOrEmpty(Notify))
            {
                strSQLNoti += " SELECT CC.CUSTOMER_CATEGORY_MST_PK ,";
                strSQLNoti += " CC.CUSTOMER_CATEGORY_ID ,";
                strSQLNoti += " CC.CUSTOMER_CATEGORY_DESC,";
                strSQLNoti += " 0 SELECTE";
                strSQLNoti += " FROM CUSTOMER_CATEGORY_MST_TBL CC";
                strSQLNoti += " WHERE 1=1 AND CC.CUSTOMER_CATEGORY_MST_PK IN ('506','346','347','348','366')  ";
            }

            if (!string.IsNullOrEmpty(Notify))
            {
                strSQLNoti += " AND CC.ACTIVE_FLAG=1 ";
                strSQLNoti += "  ORDER BY PREFERENCE)q";
            }
            else
            {
                strSQL += " AND CC.ACTIVE_FLAG=1 ";
                strSQL += " order by CUSTOMER_CATEGORY_ID)q";
            }

            try
            {
                if (!string.IsNullOrEmpty(Notify))
                {
                    return (new WorkFlow()).GetDataSet(strSQLNoti);
                }
                else
                {
                    return (new WorkFlow()).GetDataSet(strSQL);
                }

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

        #endregion "Fetch Catagories"

        #region "Fetch selectedoption"

        /// <summary>
        /// Fetchselectedoptions the specified customerpk.
        /// </summary>
        /// <param name="customerpk">The customerpk.</param>
        /// <returns></returns>
        public object fetchselectedoption(string customerpk = "")
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append(" select rownum sn,q.*  from (");
                sb.Append("select cmp.port_mst_pod_fk,cmp.country_mst_fk,cmp.region_mst_fk  from customer_potential_tbl cmp ");
                sb.Append("where cmp.customer_mst_fk= " + customerpk + " order by cmp.created_dt desc) q where rownum=1");
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

        #endregion "Fetch selectedoption"

        #region "Fetchpotential"

        /// <summary>
        /// Fetchcustomerpotentials the specified customer pk.
        /// </summary>
        /// <param name="customerPk">The customer pk.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <param name="SearchType">Type of the search.</param>
        /// <returns></returns>
        public DataSet fetchcustomerpotential(string customerPk = "", string POLPK = "", string PODPK = "", string SearchType = "")
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select ROWNUM SLNO,q.* from (");
                sb.Append("select distinct  cmp.port_mst_pol_fk,");
                //sb.Append("       cmp.port_mst_pol_fk,")
                sb.Append("       POL.PORT_NAME POLName,");
                if ((PODPK == null) | string.IsNullOrEmpty(PODPK))
                {
                    if (SearchType.ToUpper() == "COUNTRY")
                    {
                        sb.Append("       cmp.country_mst_fk port_mst_pod_fk,");
                        sb.Append("       coun.country_name PODName,");
                    }
                    else if (SearchType.ToUpper() == "REGION")
                    {
                        sb.Append("       cmp.region_mst_fk port_mst_pod_fk,");
                        sb.Append("      reg.region_name PODName,");
                    }
                    else
                    {
                        sb.Append("       cmp.port_mst_pod_fk port_mst_pod_fk,");
                        sb.Append("       POD.PORT_NAME PODName,");
                    }
                }
                else
                {
                    sb.Append(" case");
                    sb.Append("                  when pod.port_mst_pk is not null then");
                    sb.Append("                   pod.port_mst_pk");
                    sb.Append("                  when pod.port_mst_pk is null and");
                    sb.Append("                       coun.country_mst_pk is not null then");
                    sb.Append("                   coun.country_mst_pk");
                    sb.Append("                  else");
                    sb.Append("                   cmp.region_mst_fk");
                    sb.Append("                end port_mst_pod_fk,");
                    sb.Append("                case");
                    sb.Append("                  when pod.port_mst_pk is not null then");
                    sb.Append("                   pod.port_name");
                    sb.Append("                  when pod.port_mst_pk is null and");
                    sb.Append("                       coun.country_mst_pk is not null then");
                    sb.Append("                   coun.country_name");
                    sb.Append("                  else");
                    sb.Append("                   reg.region_name");
                    sb.Append("                end podname,");
                }

                sb.Append("       cmp.commodity_mst_fk,");
                sb.Append("       comm.commodity_name,");
                if (SearchType.ToUpper() == "REGION" & !string.IsNullOrEmpty(PODPK))
                {
                    sb.Append(" cmp.cust_teu +");
                    sb.Append("                NVL((select sum(c.cust_teu)");
                    sb.Append("                   from customer_potential_tbl c, country_mst_tbl cmt");
                    sb.Append("                  where c.country_mst_fk = cmt.country_mst_pk");
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1");
                    sb.Append("                    and cmt.area_mst_fk in");
                    sb.Append("                        (select a.area_mst_pk");
                    sb.Append("                           from area_mst_tbl a");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("     where a.region_mst_fk = " + PODPK + "))+");
                    }
                    else
                    {
                        sb.Append(" ) ) + ");
                    }
                    //sb.Append("                          where a.region_mst_fk = " & PODPK & "))+")
                    sb.Append("                (select Sum(c.cust_teu)");
                    sb.Append("                   from customer_potential_tbl c, port_mst_tbl POD");
                    sb.Append("                  where POD.PORT_MST_PK = c.port_mst_pod_fk");
                    sb.Append("                    and pod.country_mst_fk in");
                    sb.Append("                        (select cc.country_mst_pk");
                    sb.Append("                           from country_mst_tbl cc,");
                    sb.Append("                                area_mst_tbl    a,");
                    sb.Append("                                region_mst_tbl  r");
                    sb.Append("                          where a.region_mst_fk = r.region_mst_pk");
                    sb.Append("                            and cc.area_mst_fk = a.area_mst_pk");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("     and r.region_mst_pk = " + PODPK + ") ");
                    }
                    else
                    {
                        sb.Append(")");
                    }
                    //sb.Append("                            and r.region_mst_pk = " & PODPK & ") ")
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1),0) cust_teu,");
                }
                else if (SearchType.ToUpper() == "COUNTRY" & !string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("    cmp.cust_teu +");
                    sb.Append("                   NVL((select Sum(c.cust_teu)");
                    sb.Append("                   from customer_potential_tbl c, port_mst_tbl POD");
                    sb.Append("                  where POD.PORT_MST_PK = c.port_mst_pod_fk");
                    sb.Append("                    and pod.country_mst_fk in");
                    sb.Append("                        (select cc.country_mst_pk");
                    sb.Append("                           from country_mst_tbl cc,");
                    sb.Append("                                area_mst_tbl    a,");
                    sb.Append("                                region_mst_tbl  r");
                    sb.Append("                          where a.region_mst_fk = r.region_mst_pk");
                    sb.Append("                            and cc.area_mst_fk = a.area_mst_pk");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("                            and cc.country_mst_pk= " + PODPK + ") ");
                    }
                    else
                    {
                        sb.Append(" )");
                    }
                    //sb.Append("                            and cc.country_mst_pk= " & PODPK & ") ")
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1),0) cust_teu,");
                }
                else
                {
                    sb.Append("       cmp.cust_teu,");
                }
                sb.Append("       DECODE(cmp.cust_period, 1,'Monthly', 2, 'Quaterly',3,'Half Yearly',4,'Yearly')cust_period,");
                if (SearchType.ToUpper() == "REGION" & !string.IsNullOrEmpty(PODPK))
                {
                    sb.Append(" cmp.cust_cbm +");
                    sb.Append("                NVL((select sum(c.cust_cbm)");
                    sb.Append("                   from customer_potential_tbl c, country_mst_tbl cmt");
                    sb.Append("                  where c.country_mst_fk = cmt.country_mst_pk");
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1");
                    sb.Append("                    and cmt.area_mst_fk in");
                    sb.Append("                        (select a.area_mst_pk");
                    sb.Append("                           from area_mst_tbl a");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("   where a.region_mst_fk = " + PODPK + "))+");
                    }
                    else
                    {
                        sb.Append(" ) ) + ");
                    }
                    // sb.Append("                          where a.region_mst_fk = " & PODPK & "))+")
                    sb.Append("                (select Sum(c.cust_cbm)");
                    sb.Append("                   from customer_potential_tbl c, port_mst_tbl POD");
                    sb.Append("                  where POD.PORT_MST_PK = c.port_mst_pod_fk");
                    sb.Append("                    and pod.country_mst_fk in");
                    sb.Append("                        (select cc.country_mst_pk");
                    sb.Append("                           from country_mst_tbl cc,");
                    sb.Append("                                area_mst_tbl    a,");
                    sb.Append("                                region_mst_tbl  r");
                    sb.Append("                          where a.region_mst_fk = r.region_mst_pk");
                    sb.Append("                            and cc.area_mst_fk = a.area_mst_pk");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("     and r.region_mst_pk = " + PODPK + ") ");
                    }
                    else
                    {
                        sb.Append(")");
                    }
                    //sb.Append("                            and r.region_mst_pk = " & PODPK & ") ")
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1),0) cust_cbm,");
                }
                else if (SearchType.ToUpper() == "COUNTRY" & !string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("    cmp.cust_cbm +");
                    sb.Append("                   NVL((select Sum(c.cust_cbm)");
                    sb.Append("                   from customer_potential_tbl c, port_mst_tbl POD");
                    sb.Append("                  where POD.PORT_MST_PK = c.port_mst_pod_fk");
                    sb.Append("                    and pod.country_mst_fk in");
                    sb.Append("                        (select cc.country_mst_pk");
                    sb.Append("                           from country_mst_tbl cc,");
                    sb.Append("                                area_mst_tbl    a,");
                    sb.Append("                                region_mst_tbl  r");
                    sb.Append("                          where a.region_mst_fk = r.region_mst_pk");
                    sb.Append("                            and cc.area_mst_fk = a.area_mst_pk");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("                            and cc.country_mst_pk= " + PODPK + ") ");
                    }
                    else
                    {
                        sb.Append(" )");
                    }
                    //sb.Append("                            and cc.country_mst_pk= " & PODPK & ") ")
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1),0) cust_cbm,");
                }
                else
                {
                    sb.Append("       cmp.cust_cbm,");
                }

                if (SearchType.ToUpper() == "REGION" & !string.IsNullOrEmpty(PODPK))
                {
                    sb.Append(" cmp.cust_mts +");
                    sb.Append("                NVL((select sum(c.cust_mts)");
                    sb.Append("                   from customer_potential_tbl c, country_mst_tbl cmt");
                    sb.Append("                  where c.country_mst_fk = cmt.country_mst_pk");
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1");
                    sb.Append("                    and cmt.area_mst_fk in");
                    sb.Append("                        (select a.area_mst_pk");
                    sb.Append("                           from area_mst_tbl a");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("                          where a.region_mst_fk = " + PODPK + "))+");
                    }
                    else
                    {
                        sb.Append(" ) ) + ");
                    }
                    //sb.Append("                          where a.region_mst_fk = " & PODPK & "))+")
                    sb.Append("                (select Sum(c.cust_mts)");
                    sb.Append("                   from customer_potential_tbl c, port_mst_tbl POD");
                    sb.Append("                  where POD.PORT_MST_PK = c.port_mst_pod_fk");
                    sb.Append("                    and pod.country_mst_fk in");
                    sb.Append("                        (select cc.country_mst_pk");
                    sb.Append("                           from country_mst_tbl cc,");
                    sb.Append("                                area_mst_tbl    a,");
                    sb.Append("                                region_mst_tbl  r");
                    sb.Append("                          where a.region_mst_fk = r.region_mst_pk");
                    sb.Append("                            and cc.area_mst_fk = a.area_mst_pk");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append(" and cc.country_mst_pk= " + PODPK + ") ");
                    }
                    else
                    {
                        sb.Append(" )");
                    }
                    //sb.Append("                            and r.region_mst_pk = " & PODPK & ") ")
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1),0) cust_mts,");
                }
                else if (SearchType.ToUpper() == "COUNTRY" & !string.IsNullOrEmpty(PODPK))
                {
                    sb.Append("    cmp.cust_mts +");
                    sb.Append("                   NVL((select Sum(c.cust_mts)");
                    sb.Append("                   from customer_potential_tbl c, port_mst_tbl POD");
                    sb.Append("                  where POD.PORT_MST_PK = c.port_mst_pod_fk");
                    sb.Append("                    and pod.country_mst_fk in");
                    sb.Append("                        (select cc.country_mst_pk");
                    sb.Append("                           from country_mst_tbl cc,");
                    sb.Append("                                area_mst_tbl    a,");
                    sb.Append("                                region_mst_tbl  r");
                    sb.Append("                          where a.region_mst_fk = r.region_mst_pk");
                    sb.Append("                            and cc.area_mst_fk = a.area_mst_pk");
                    if (!string.IsNullOrEmpty(PODPK))
                    {
                        sb.Append("                            and cc.country_mst_pk= " + PODPK + ") ");
                    }
                    else
                    {
                        sb.Append(" )");
                    }
                    //sb.Append("                            and cc.country_mst_pk= " & PODPK & ") ")
                    sb.Append("                    and c.customer_mst_fk = " + customerPk);
                    sb.Append("                    and c.add_flag = 1),0) cust_mts,");
                }
                else
                {
                    sb.Append("       cmp.cust_mts,");
                }
                sb.Append("       DECODE(cmp.cust_servicable, 1, 'Y', 2,'N')cust_servicable,");
                sb.Append("       cmp.operator_mst_fk,");
                sb.Append("        opr.operator_name,");
                //  sb.Append("       opr.operator_name,")
                sb.Append("       cmp.cust_tos,");
                sb.Append("       cmp.cust_dtention_free,");
                sb.Append("       cmp.cust_cutoff_hrs,");
                sb.Append("       cmp.cust_ref_plug_free,");
                sb.Append("       DECODE(cmp.cust_seasonal, 1, 'Y', 2, 'N')cust_seasonal,");
                sb.Append("       cmp.from_date,");
                sb.Append("       cmp.To_Date,");
                sb.Append("       cmp.cust_cr_days,");
                sb.Append("       cmp.cust_cr_limit,");
                sb.Append("       cmp.cust_other_inf,");
                sb.Append("       cmp.customer_potential_mst_pk,");
                sb.Append("       cmp.customer_mst_fk,");
                sb.Append("      '' CHEFFLAG,");
                sb.Append("      '' FETCHFLAG,");
                sb.Append("      '' DELFLAG");
                sb.Append("  from customer_potential_tbl cmp,");
                sb.Append("       customer_mst_tbl       cust,");
                sb.Append("       port_mst_tbl           POL,");
                sb.Append("       port_mst_tbl           POD,");
                sb.Append("     country_mst_tbl        coun,");
                sb.Append("     region_mst_tbl       reg,");
                sb.Append("       commodity_mst_tbl      comm,");
                sb.Append("       operator_mst_tbl      opr");
                sb.Append(" where cust.customer_mst_pk = cmp.customer_mst_fk");
                sb.Append("   and POL.port_mst_pk(+)= cmp.port_mst_pol_fk");
                sb.Append("   and POD.PORT_MST_PK(+) = cmp.port_mst_pod_fk");
                sb.Append("   and comm.commodity_mst_pk(+) = cmp.commodity_mst_fk");
                sb.Append("   and coun.country_mst_pk(+)=cmp.country_mst_fk");
                sb.Append("   and reg.region_mst_pk(+)=cmp.region_mst_fk");
                sb.Append("   and opr.operator_mst_pk(+)=cmp.operator_mst_fk");
                if (!string.IsNullOrEmpty(POLPK))
                {
                    sb.Append(" and cmp.port_mst_pol_fk=" + POLPK);
                }

                if (!string.IsNullOrEmpty(SearchType))
                {
                    if (SearchType.ToUpper() == "COUNTRY")
                    {
                        if (!string.IsNullOrEmpty(PODPK))
                        {
                            sb.Append(" and cmp.country_mst_fk=" + PODPK);
                        }
                        else
                        {
                            sb.Append(" and cmp.country_mst_fk is not null ");
                        }
                    }
                    if (SearchType.ToUpper() == "REGION")
                    {
                        if (!string.IsNullOrEmpty(PODPK))
                        {
                            sb.Append(" and cmp.region_mst_fk=" + PODPK);
                        }
                        else
                        {
                            sb.Append(" and cmp.region_mst_fk is not  null ");
                        }
                    }

                    if (SearchType.ToUpper() == "POD")
                    {
                        if (!string.IsNullOrEmpty(PODPK))
                        {
                            sb.Append(" and cmp.port_mst_pod_fk=" + PODPK);
                        }
                        else
                        {
                            sb.Append(" and cmp.port_mst_pod_fk is not null ");
                        }
                    }
                }

                //Else
                //    sb.Append(" and cmp.region_mst_fk is not null ")
                //End If
                //If SearchType.ToUpper = "POD" And PODPK <> "" Then
                //    sb.Append(" and cmp.port_mst_pod_fk=" & PODPK)
                //Else
                //    sb.Append(" and cmp.port_mst_pod_fk is not null ")
                //End If

                //End If

                if (!string.IsNullOrEmpty(customerPk))
                {
                    sb.Append("   and cmp.customer_mst_fk=" + customerPk);
                }
                else
                {
                    sb.Append("   and cmp.customer_mst_fk='0' ");
                }
                sb.Append(" )q ");
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException sqlExp)
            {
                //ErrorMessage = sqlExp.Message
                throw sqlExp;
            }
            catch (Exception exp)
            {
                //ErrorMessage = exp.Message
                throw exp;
            }
        }

        /// <summary>
        /// Fetches the competitor.
        /// </summary>
        /// <param name="Customerpotenpk">The customerpotenpk.</param>
        /// <param name="Custmstfk">The custmstfk.</param>
        /// <returns></returns>
        public DataSet FetchCompetitor(string Customerpotenpk = "", string Custmstfk = "")
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select ROWNUM SLNO,q.* from (");
                sb.Append("select distinct  comp.farwarder_id,");
                sb.Append("       comp. farwarder_name,");
                sb.Append("       comp.cmp_teu,");
                sb.Append("        DECODE(comp.cmp_period, 1, 'Monthly', 2, 'Quaterly',3,'Half Yaerly',4,'Yearly')cmp_period,");
                sb.Append("        comp. cmp_cbm,");
                sb.Append("        comp.cmp_mts,");
                sb.Append("        DECODE(comp.cmp_servicable, 1, 'Y', 2, 'N')cmp_servicable,");
                sb.Append("        comp.operator_mst_fk,");
                sb.Append("         '' operator_name,");
                sb.Append("        comp.cmp_tos,");
                sb.Append("        comp.cmp_detention_free,");
                sb.Append("        comp.cmp_cutoff_hrs,");
                sb.Append("        comp.cmp_ref_plug_free,");
                sb.Append("        DECODE(comp.cmp_seasonal, 1, 'Y', 2, 'N')cmp_seasonal,");
                sb.Append("        comp.from_date,");
                sb.Append("        comp.to_date,");
                sb.Append("        comp.cmp_rate,");
                sb.Append("        comp.cmp_cr_days,");
                sb.Append("        comp.cmp_cr_limit,");
                sb.Append("        comp.cmp_oth_inf,");
                sb.Append("        COMP.Competitors_Pk,");
                sb.Append("        ' ' CHEFFLAG,");
                sb.Append("      '' DELFLAG");
                sb.Append("       from ");
                sb.Append("        competitors_tbl comp,");
                sb.Append("        customer_potential_tbl cust,");
                sb.Append("         customer_mst_tbl custmst,");
                sb.Append("         operator_mst_tbl       opr");
                sb.Append("   Where cust.customer_potential_mst_pk=comp.cust_poten_mst_fk");
                sb.Append("   and custmst.customer_mst_pk=cust.customer_mst_fk");
                if (!string.IsNullOrEmpty(Customerpotenpk))
                {
                    sb.Append(" and comp.cust_poten_mst_fk=" + Customerpotenpk);
                }
                else
                {
                    sb.Append(" and comp.cust_poten_mst_fk='0' ");
                }
                sb.Append(" )q ");
                //sb.Append("")
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

        #endregion "Fetchpotential"

        /// <summary>
        /// Fetches the ecomm users.
        /// </summary>
        /// <param name="CUST_REK_FK">The cus t_ re k_ fk.</param>
        /// <returns></returns>
        public DataSet FetchEcommUsers(string CUST_REK_FK = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with1 = objWF.MyDataAdapter;
                _with1.SelectCommand = new OracleCommand();
                _with1.SelectCommand.Connection = objWF.MyConnection;
                _with1.SelectCommand.CommandText = objWF.MyUserName + ".FETCH_ECOMM_GATE_PKG.FETCH_CUSTUSER_DETAILS";

                _with1.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with1.SelectCommand.Parameters.Add("CUST_REG_FK_IN", OracleDbType.Varchar2).Value = getDefault(CUST_REK_FK, "");
                _with1.SelectCommand.Parameters.Add("PL_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with1.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            finally
            {
                objWF.CloseConnection();
            }
            return new DataSet();
        }

        #region "FetchAll"

        /// <summary>
        /// Fetches the cont det.
        /// </summary>
        /// <param name="CustomerFK">The customer fk.</param>
        /// <returns></returns>
        public DataSet FetchContDet(Int32 CustomerFK = 0)
        {
            try
            {
                string strSQL = null;
                strSQL = string.Empty;
                strSQL += "SELECT ROWNUM SR_NO, ";
                strSQL += "CUST_CONTACT_PK, ";
                strSQL += "NAME CName, ";
                strSQL += "ALIAS, ";
                strSQL += "DESIGNATION, ";
                strSQL += "RESPONSIBILITY, ";
                strSQL += "DIR_PHONE, ";
                strSQL += "MOBILE, ";
                strSQL += "FAX, ";
                strSQL += "EMAIL, ";
                strSQL += "VERSION_NO ";
                strSQL += "FROM CUSTOMER_CONTACT_TRN  ";
                strSQL += "WHERE Customer_Mst_Fk =  " + CustomerFK;
                return (new WorkFlow()).GetDataSet(strSQL);
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

        #endregion "FetchAll"

        #region "Fetch Details"

        /// <summary>
        /// Fetches the by procedure.
        /// </summary>
        /// <param name="customerPK">The customer pk.</param>
        /// <returns></returns>
        public DataSet FetchByProcedure(int customerPK = 0)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataTable dt = null;
                DataSet ds = new DataSet();

                objWF.MyCommand.Parameters.Clear();
                var _with2 = objWF.MyCommand.Parameters;
                _with2.Add("CUSTOMER_MST_FK_IN", customerPK).Direction = ParameterDirection.Input;
                _with2.Add("CUST_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dt = objWF.GetDataTable("CUSTOMER_MST_TBL_PKG", "FETCH_DATA");

                ds.Tables.Add(dt);
                return ds;
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        /// <summary>
        /// Fetches the customer wise det.
        /// </summary>
        /// <param name="customerPK">The customer pk.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="ShippedTEU">The shipped teu.</param>
        /// <param name="ShippedVOL">The shipped vol.</param>
        /// <param name="INVOICEDCURR">The invoicedcurr.</param>
        /// <param name="COLLECTIONCURR">The collectioncurr.</param>
        /// <param name="OUTCURR">The outcurr.</param>
        /// <param name="INVOICEDPREV">The invoicedprev.</param>
        /// <param name="COLLECTIONPREV">The collectionprev.</param>
        /// <param name="OUTPREV">The outprev.</param>
        /// <param name="PROFITCURR">The profitcurr.</param>
        /// <param name="PROFITPREV">The profitprev.</param>
        /// <param name="CONFIRMEDREV">The confirmedrev.</param>
        /// <param name="CANCELEDREV">The canceledrev.</param>
        /// <param name="SHIPPEDREV">The shippedrev.</param>
        /// <returns></returns>
        public DataSet FetchCustWiseDet(int customerPK = 0, long BizType = 0, string ShippedTEU = "", string ShippedVOL = "", string INVOICEDCURR = "", string COLLECTIONCURR = "", string OUTCURR = "", string INVOICEDPREV = "", string COLLECTIONPREV = "", string OUTPREV = "",
        string PROFITCURR = "", string PROFITPREV = "", string CONFIRMEDREV = "", string CANCELEDREV = "", string SHIPPEDREV = "")
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataTable dt = null;
                DataSet ds = new DataSet();
                DataSet Invds = new DataSet();
                objWF.MyCommand.Parameters.Clear();
                var _with3 = objWF.MyCommand.Parameters;
                _with3.Add("CUSTOMER_PK_IN", customerPK).Direction = ParameterDirection.Input;
                _with3.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with3.Add("LOGED_IN_CURRENCY", HttpContext.Current.Session["CURRENCY_ID"]).Direction = ParameterDirection.Input;
                _with3.Add("FET_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with3.Add("SHIPPEDTEU_OUT", customerPK).Direction = ParameterDirection.Output;
                _with3.Add("SHIPPEDVOL_OUT", customerPK).Direction = ParameterDirection.Output;
                _with3.Add("INVOICEDCURR_OUT", customerPK).Direction = ParameterDirection.Output;
                _with3.Add("COLLECTIONCURR_OUT", customerPK).Direction = ParameterDirection.Output;
                _with3.Add("OUTCURR_OUT", customerPK).Direction = ParameterDirection.Output;
                _with3.Add("INVOICEDPREV_OUT", customerPK).Direction = ParameterDirection.Output;
                _with3.Add("COLLECTIONPREV_OUT", customerPK).Direction = ParameterDirection.Output;
                _with3.Add("OUTPREV_OUT", customerPK).Direction = ParameterDirection.Output;

                ds = objWF.GetDataSet("CUST_COMP_ANALYSIS", "GET_CUST_WISE_DATA");
                //ds.Tables.Add(dt)
                ShippedTEU = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["SHIPPEDTEU_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["SHIPPEDTEU_OUT"].Value.ToString());
                ShippedVOL = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["SHIPPEDVOL_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["SHIPPEDVOL_OUT"].Value.ToString());

                objWF.MyCommand.Parameters.Clear();
                var _with4 = objWF.MyCommand.Parameters;
                _with4.Add("CUSTOMER_PK_IN", customerPK).Direction = ParameterDirection.Input;
                _with4.Add("BUSINESS_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with4.Add("LOGED_IN_CURRENCY", HttpContext.Current.Session["CURRENCY_ID"]).Direction = ParameterDirection.Input;
                _with4.Add("INVOICEDCURR_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("COLLECTIONCURR_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("OUTCURR_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("INVOICEDPREV_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("COLLECTIONPREV_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("OUTPREV_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("PROFITCURR_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("PROFITPREV_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("CONFIRMEDREV_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("CANCELEDREV_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;
                _with4.Add("SHIPPEDREV_OUT", OracleDbType.Varchar2, 20).Direction = ParameterDirection.Output;

                Invds = objWF.GetDataSet("CUST_COMP_ANALYSIS", "GET_INVCOL_DATA");

                INVOICEDCURR = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["INVOICEDCURR_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["INVOICEDCURR_OUT"].Value.ToString());
                COLLECTIONCURR = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["COLLECTIONCURR_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["COLLECTIONCURR_OUT"].Value.ToString());
                OUTCURR = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["OUTCURR_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["OUTCURR_OUT"].Value.ToString());
                INVOICEDPREV = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["INVOICEDPREV_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["INVOICEDPREV_OUT"].Value.ToString());
                COLLECTIONPREV = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["COLLECTIONPREV_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["COLLECTIONPREV_OUT"].Value.ToString());
                OUTPREV = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["OUTPREV_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["OUTPREV_OUT"].Value.ToString());
                PROFITCURR = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["PROFITCURR_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["PROFITCURR_OUT"].Value.ToString());
                PROFITPREV = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["PROFITPREV_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["PROFITPREV_OUT"].Value.ToString());
                CONFIRMEDREV = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["CONFIRMEDREV_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["CONFIRMEDREV_OUT"].Value.ToString());
                CANCELEDREV = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["CANCELEDREV_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["CANCELEDREV_OUT"].Value.ToString());
                SHIPPEDREV = (string.IsNullOrEmpty(objWF.MyCommand.Parameters["SHIPPEDREV_OUT"].Value.ToString()) ? "" : objWF.MyCommand.Parameters["SHIPPEDREV_OUT"].Value.ToString());
                return ds;
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception sqlExp)
            {
                throw sqlExp;
            }
        }

        #region "Display Profit Grid"

        /// <summary>
        /// Dispalies the profit grid.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="FromDt">From dt.</param>
        /// <param name="ToDt">To dt.</param>
        /// <returns></returns>
        public DataSet DispalyProfitGrid(int CustPk, string FromDt = "", string ToDt = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            string strSQL = null;
            string strSQL1 = null;
            Int16 BizType = default(Int16);
            strSQL1 = "SELECT CMST.BUSINESS_TYPE FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK = " + CustPk;
            BizType = Convert.ToInt16(objWF.ExecuteScaler(strSQL1.ToString()));
            try
            {
                if (BizType == 2 | BizType == 0)
                {
                    strSQL = "SELECT ROWNUM SR_NO , Q.* FROM (SELECT P.* FROM (";
                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Sea' BIZ_TYPE,";
                    strSQL = strSQL + " 'Export' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (CASE WHEN BOOK.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    strSQL = strSQL + "  WHEN BOOK.CARGO_TYPE = 2 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    //strSQL = strSQL & vbCrLf & " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT * 1000, 0)) FROM JOB_TRN_CONT CONT"
                    strSQL = strSQL + " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) END) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3) ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " BOOKING_MST_TBL      BOOK,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    //strSQL = strSQL & vbCrLf & " AND POL.location_mst_fk = " & Loc()
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + " AND BOOK.STATUS <>3 ";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =2 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 1";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }

                    strSQL = strSQL + " UNION ";

                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Sea' BIZ_TYPE,";
                    strSQL = strSQL + " 'Import' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (CASE WHEN J.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    strSQL = strSQL + "  WHEN J.CARGO_TYPE = 2 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    //strSQL = strSQL & vbCrLf & " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT * 1000, 0)) FROM JOB_TRN_CONT CONT"
                    strSQL = strSQL + " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) END) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3) ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND J.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    //strSQL = strSQL & vbCrLf & " AND POL.location_mst_fk = " & Loc()
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =2 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 2";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }

                    strSQL = strSQL + " )P order by TO_DATE(JOBCARD_DATE) DESC, REF_NUMBER DESC) Q";
                }
                else if (BizType == 1)
                {
                    strSQL = "SELECT ROWNUM SR_NO , Q.* FROM (SELECT P.* FROM (";
                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Air' BIZ_TYPE,";
                    strSQL = strSQL + " 'Export' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.Job_Card_Trn_Pk) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.Job_Card_Trn_Pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3)ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " BOOKING_MST_TBL      BOOK,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    //strSQL = strSQL & vbCrLf & " AND POL.location_mst_fk = " & Loc()
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + " AND BOOK.STATUS <>3 ";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =1 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 1";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }

                    strSQL = strSQL + " UNION ";

                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Air' BIZ_TYPE,";
                    strSQL = strSQL + " 'Import' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3)ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND J.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    //strSQL = strSQL & vbCrLf & " AND POL.location_mst_fk = " & Loc()
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =1 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 2";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }
                    strSQL = strSQL + " )P order by TO_DATE(JOBCARD_DATE) DESC, REF_NUMBER DESC) Q";
                }

                if (BizType == 3)
                {
                    strSQL = "SELECT ROWNUM SR_NO , Q.* FROM ( SELECT P.* FROM (";
                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Sea' BIZ_TYPE,";
                    strSQL = strSQL + " 'Export' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (CASE WHEN BOOK.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    strSQL = strSQL + "  WHEN BOOK.CARGO_TYPE = 2 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    //strSQL = strSQL & vbCrLf & " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT * 1000, 0)) FROM JOB_TRN_CONT CONT"
                    strSQL = strSQL + " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) END) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3) ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " BOOKING_MST_TBL      BOOK,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + " AND BOOK.STATUS <>3 ";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =2 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 1";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }

                    strSQL = strSQL + " UNION ";

                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Sea' BIZ_TYPE,";
                    strSQL = strSQL + " 'Import' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (CASE WHEN J.CARGO_TYPE = 1 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    strSQL = strSQL + "  WHEN J.CARGO_TYPE = 2 THEN (SELECT SUM(NVL(CONT.GROSS_WEIGHT, 0))";
                    strSQL = strSQL + " FROM JOB_TRN_CONT CONT WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk)";
                    //strSQL = strSQL & vbCrLf & " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT * 1000, 0)) FROM JOB_TRN_CONT CONT"
                    strSQL = strSQL + " ELSE (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) END) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_SEA(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3) ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND J.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    //strSQL = strSQL & vbCrLf & " AND POL.location_mst_fk = " & Loc()
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =2 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 2";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }

                    strSQL = strSQL + " UNION ";

                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Air' BIZ_TYPE,";
                    strSQL = strSQL + " 'Export' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.Job_Card_Trn_Pk) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.Job_Card_Trn_Pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_EXP_AIR(j.Job_Card_Trn_Pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3)ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " BOOKING_MST_TBL      BOOK,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.BOOKING_MST_FK = BOOK.BOOKING_MST_PK";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND BOOK.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =1 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 1";
                    strSQL = strSQL + " AND BOOK.STATUS <>3 ";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }

                    strSQL = strSQL + " UNION ";

                    strSQL = strSQL + " select J.JOBCARD_REF_NO REF_NUMBER,";
                    strSQL = strSQL + " TO_CHAR(J.JOBCARD_DATE, dateformat) JOBCARD_DATE,";
                    strSQL = strSQL + " 'Air' BIZ_TYPE,";
                    strSQL = strSQL + " 'Import' PROCESS_TYPE,";
                    strSQL = strSQL + " SHIPPER.CUSTOMER_NAME SHIPPER_NAME,";
                    strSQL = strSQL + " CONSIGNEE.CUSTOMER_NAME CONSIGNEE_NAME,";
                    strSQL = strSQL + " DECODE(J.PYMT_TYPE, 1, 'PREPAID', 2, 'COLLECT') PAYMENT_TYPE,";
                    strSQL = strSQL + " POL.PORT_ID POL,";
                    strSQL = strSQL + " POD.PORT_ID POD,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.CHARGEABLE_WEIGHT, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) WEIGHT,";
                    strSQL = strSQL + " (SELECT SUM(NVL(CONT.VOLUME_IN_CBM, 0)) FROM JOB_TRN_CONT CONT";
                    strSQL = strSQL + " WHERE CONT.job_card_TRN_Fk = J.job_card_TRN_pk) VOLUME,";
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURR,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",4) FREIGHT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",6) ACTUAL_COST,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",5) ESTIMATED_PROFIT,";
                    strSQL = strSQL + " FETCH_JCPROFIT_RPT_PKG.FETCH_JOBCARD_IMP_AIR(j.job_card_TRN_pk," + HttpContext.Current.Session["CURRENCY_MST_PK"] + ",3)ACTUAL_PROFIT,";
                    strSQL = strSQL + " EMP.EMPLOYEE_NAME SALES_REPORTER,";
                    //strSQL = strSQL & vbCrLf & " '" & Process & "' AS DEPARTMENT"
                    strSQL = strSQL + " '' AS DEPARTMENT";
                    strSQL = strSQL + " FROM JOB_CARD_TRN J,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     SHIPPER,";
                    strSQL = strSQL + " CUSTOMER_MST_TBL     CONSIGNEE,";
                    strSQL = strSQL + " PORT_MST_TBL         POL,";
                    strSQL = strSQL + " PORT_MST_TBL         POD,";
                    strSQL = strSQL + " EMPLOYEE_MST_TBL     EMP,";
                    strSQL = strSQL + " USER_MST_TBL         USR ";
                    strSQL = strSQL + " WHERE J.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + " AND J.PORT_MST_POD_FK = POD.PORT_MST_PK";
                    strSQL = strSQL + " AND J.PORT_MST_POL_FK = POL.PORT_MST_PK";
                    //strSQL = strSQL & vbCrLf & " AND POL.location_mst_fk = " & Loc()
                    strSQL = strSQL + " AND SHIPPER.REP_EMP_MST_FK = EMP.EMPLOYEE_MST_PK(+)";
                    //strSQL = strSQL & vbCrLf & " AND TO_DATE(JOBCARD_DATE, 'dd/MM/yyyy') BETWEEN (SELECT TRUNC(SysDate,'YEAR') FROM Dual) AND SYSDATE"
                    strSQL = strSQL + " AND J.created_by_fk = USR.user_mst_pk";
                    strSQL = strSQL + "  AND J.BUSINESS_TYPE =1 ";
                    strSQL = strSQL + "  AND J.PROCESS_TYPE = 2";
                    strSQL = strSQL + " AND j.shipper_cust_mst_fk = " + CustPk;
                    if (!string.IsNullOrEmpty(FromDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) >= TO_DATE('" + FromDt + "',DATEFORMAT)";
                    }
                    if (!string.IsNullOrEmpty(ToDt))
                    {
                        strSQL = strSQL + " AND TO_DATE(J.JOBCARD_DATE,DATEFORMAT) <= TO_DATE('" + ToDt + "',DATEFORMAT)";
                    }
                    strSQL = strSQL + " )P order by  TO_DATE(JOBCARD_DATE) DESC,REF_NUMBER DESC) Q ";
                }

                ds = objWF.GetDataSet(strSQL);
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Display Profit Grid"

        #region "FetchOutStand"

        /// <summary>
        /// Fetches the out stand.
        /// </summary>
        /// <param name="CustPk">The customer pk.</param>
        /// <returns></returns>
        public DataSet FetchOutStand(int CustPk)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            string strSQL = null;
            string strSQL1 = null;
            Int16 BizType = default(Int16);
            strSQL1 = "SELECT CMST.BUSINESS_TYPE FROM CUSTOMER_MST_TBL CMST WHERE CMST.CUSTOMER_MST_PK = " + CustPk;
            BizType = Convert.ToInt16(objWF.ExecuteScaler(strSQL1.ToString()));
            try
            {
                if (BizType == 2 | BizType == 0 | BizType == 1)
                {
                    strSQL = "SELECT ROWNUM SR_NO,q.* from (select A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT OUTSTANDING,";
                    strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else CASE WHEN A.OUTSTD_DAYS < 0 THEN 0 ELSE A.OUTSTD_DAYS END end OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type from  (";
                    strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type from (";

                    strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                    //strSQL = strSQL & vbCrLf & " CURR.CURRENCY_ID,"
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,";
                    //strSQL = strSQL & vbCrLf & " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " & HttpContext.Current.Session("currency_mst_pk") & ",CIT.INVOICE_DATE) INVOICE,"
                    strSQL = strSQL + " (CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE)- SUM(NVL(CIT.DISCOUNT_AMT,0))) INVOICE,";
                    strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                    //strSQL = strSQL & vbCrLf & " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " & HttpContext.Current.Session("currency_mst_pk") & ", CIT.INVOICE_DATE)))INVOICE_AMT," ' MODIFIED BY FAHEEM
                    strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE))) - SUM(NVL(CIT.DISCOUNT_AMT,0)) INVOICE_AMT,";
                    strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                    strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                    strSQL = strSQL + " ,cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,book.cargo_type cargo_type";
                    strSQL = strSQL + "  FROM";
                    strSQL = strSQL + " consol_invoice_tbl CIT,";
                    strSQL = strSQL + " consol_invoice_trn_tbl CITT,";
                    strSQL = strSQL + " JOB_CARD_TRN job,";
                    strSQL = strSQL + " BOOKING_MST_TBL book,";
                    strSQL = strSQL + " customer_mst_tbl cust,";
                    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                    strSQL = strSQL + " collections_tbl        col,";
                    strSQL = strSQL + " collections_trn_tbl    colt";
                    strSQL = strSQL + " WHERE";
                    strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";
                    if (BizType == 2 | BizType == 0)
                    {
                        strSQL = strSQL + " AND citt.job_card_fk = job.job_card_TRN_pk";
                        strSQL = strSQL + " AND job.BOOKING_MST_FK = book.BOOKING_MST_PK";
                    }
                    else if (BizType == 1)
                    {
                        strSQL = strSQL + " AND citt.job_card_fk = job.Job_Card_Trn_Pk";
                        strSQL = strSQL + " AND job.BOOKING_MST_FK = book.BOOKING_MST_PK";
                    }
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
                    strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
                    strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                    strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK(+) = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + " AND COLT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO ";
                    strSQL = strSQL + " GROUP BY";
                    strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                    strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)),cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,book.cargo_type)";
                    strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type)A";
                    strSQL = strSQL + "  WHERE (A.INVOICE_AMT - A.OUT_AMOUNT)> 0 ";
                    //Added by Faheem

                    if (BizType == 2)
                    {
                        strSQL = strSQL + "   UNION";
                        strSQL = strSQL + " SELECT A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT,";
                        strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else CASE WHEN A.OUTSTD_DAYS < 0 THEN 0 ELSE A.OUTSTD_DAYS END end OUTSTD_DAYS,a.process_type,a.business_type,a.consol_invoice_pk,a.CHK_INVOICE,cargo_type from  (";
                        strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type from (";

                        strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                        strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                        strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                        strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
                        strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                        //strSQL = strSQL & vbCrLf & " CURR.CURRENCY_ID,"
                        strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,";
                        //strSQL = strSQL & vbCrLf & " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " & HttpContext.Current.Session("currency_mst_pk") & ",CIT.INVOICE_DATE) INVOICE,"
                        strSQL = strSQL + " (CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE)- SUM(NVL(CIT.DISCOUNT_AMT,0))) INVOICE,";
                        strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                        //strSQL = strSQL & vbCrLf & " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " & HttpContext.Current.Session("currency_mst_pk") & ", CIT.INVOICE_DATE)))INVOICE_AMT," ' MODIFIED BY FAHEEM
                        strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE))) - SUM(NVL(CIT.DISCOUNT_AMT,0)) INVOICE_AMT,";
                        strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                        strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                        strSQL = strSQL + " ,cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type cargo_type";
                        strSQL = strSQL + "  FROM";
                        strSQL = strSQL + " consol_invoice_tbl CIT,";
                        strSQL = strSQL + " consol_invoice_trn_tbl CITT,";
                        strSQL = strSQL + " JOB_CARD_TRN   JOB,";
                        strSQL = strSQL + " customer_mst_tbl cust,";
                        strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                        strSQL = strSQL + " collections_tbl        col,";
                        strSQL = strSQL + " collections_trn_tbl    colt";
                        strSQL = strSQL + " WHERE";
                        strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";
                        strSQL = strSQL + " AND JOB.SHIPPER_CUST_MST_FK = cust.customer_mst_pk(+)";
                        strSQL = strSQL + " AND CITT.JOB_CARD_FK = JOB.job_card_TRN_pk";
                        strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                        strSQL = strSQL + " AND JOB.SHIPPER_CUST_MST_FK = " + CustPk;
                        strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                        strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK(+) = COLT.COLLECTIONS_TBL_FK";
                        strSQL = strSQL + " AND COLT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO ";
                        strSQL = strSQL + " GROUP BY";
                        strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                        strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)),cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type)";
                        strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type)A";
                        //strSQL = strSQL & vbCrLf & "  WHERE (A.INVOICE_AMT - A.OUT_AMOUNT)> 0 "
                        strSQL = strSQL + "  )q WHERE OUTSTANDING> 0  order by OUTSTANDING desc,JOBCARD_DATE desc,JOBCARD_REF_NO desc";
                    }
                    else if (BizType == 1)
                    {
                        strSQL = strSQL + "   UNION";
                        strSQL = strSQL + " SELECT  A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT,";
                        strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else CASE WHEN A.OUTSTD_DAYS < 0 THEN 0 ELSE A.OUTSTD_DAYS END end OUTSTD_DAYS ,a.process_type,a.business_type,a.consol_invoice_pk,a.CHK_INVOICE,a.cargo_type from  (";
                        strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type from (";

                        strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                        strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                        strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                        strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
                        strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                        //strSQL = strSQL & vbCrLf & " CURR.CURRENCY_ID,"
                        strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,";
                        //strSQL = strSQL & vbCrLf & " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " & HttpContext.Current.Session("currency_mst_pk") & ",CIT.INVOICE_DATE) INVOICE,"
                        strSQL = strSQL + " (CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE)- SUM(NVL(CIT.DISCOUNT_AMT,0))) INVOICE,";
                        strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                        //strSQL = strSQL & vbCrLf & " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " & HttpContext.Current.Session("currency_mst_pk") & ", CIT.INVOICE_DATE)))INVOICE_AMT," ' MODIFIED BY FAHEEM
                        strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE))) - SUM(NVL(CIT.DISCOUNT_AMT,0)) INVOICE_AMT,";
                        strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                        strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                        strSQL = strSQL + " ,cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,JOB.cargo_type cargo_type";
                        strSQL = strSQL + "  FROM";
                        strSQL = strSQL + " consol_invoice_tbl CIT,";
                        strSQL = strSQL + " consol_invoice_trn_tbl CITT,";
                        strSQL = strSQL + " JOB_CARD_TRN   JOB,";
                        strSQL = strSQL + " customer_mst_tbl cust,";
                        strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                        strSQL = strSQL + " collections_tbl        col,";
                        strSQL = strSQL + " collections_trn_tbl    colt";
                        strSQL = strSQL + " WHERE";
                        strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";
                        strSQL = strSQL + " AND CITT.JOB_CARD_FK = JOB.job_card_TRN_pk";
                        strSQL = strSQL + " AND JOB.SHIPPER_CUST_MST_FK = cust.customer_mst_pk(+)";
                        strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";

                        strSQL = strSQL + " AND JOB.SHIPPER_CUST_MST_FK = " + CustPk;
                        strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                        strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK(+) = COLT.COLLECTIONS_TBL_FK";
                        strSQL = strSQL + " AND COLT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO ";
                        strSQL = strSQL + " GROUP BY";
                        strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                        strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)),cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,JOB.cargo_type)";
                        strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type)A";
                        strSQL = strSQL + "  )q WHERE OUTSTANDING> 0  order by OUTSTANDING desc,JOBCARD_DATE desc,JOBCARD_REF_NO desc";
                    }
                    else
                    {
                        strSQL = strSQL + "  )q WHERE OUTSTANDING> 0  order by OUTSTANDING desc,JOBCARD_DATE desc,JOBCARD_REF_NO desc";
                    }
                }

                if (BizType == 3)
                {
                    strSQL = " SELECT ROWNUM SR_NO,Q.* FROM(";
                    strSQL = strSQL + " SELECT * FROM(";
                    strSQL = strSQL + "(SELECT A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT OUTSTANDING,";
                    strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else CASE WHEN A.OUTSTD_DAYS < 0 THEN 0 ELSE A.OUTSTD_DAYS END end OUTSTD_DAYS,a.process_type,a.business_type,a.consol_invoice_pk,a.CHK_INVOICE,cargo_type from  (";
                    strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type from (";

                    strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY')INVOICE_DATE,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                    //strSQL = strSQL & vbCrLf & " CURR.CURRENCY_ID,"
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,";
                    strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
                    strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                    strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
                    strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                    strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                    strSQL = strSQL + " ,cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,book.cargo_type cargo_type";
                    strSQL = strSQL + "  FROM";
                    strSQL = strSQL + " consol_invoice_tbl CIT,";
                    strSQL = strSQL + " consol_invoice_trn_tbl CITT,";

                    strSQL = strSQL + " JOB_CARD_TRN job,";
                    strSQL = strSQL + " BOOKING_MST_TBL book,";

                    strSQL = strSQL + " customer_mst_tbl cust,";
                    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                    strSQL = strSQL + " collections_tbl        col,";
                    strSQL = strSQL + " collections_trn_tbl    colt";
                    strSQL = strSQL + " WHERE";
                    strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";

                    strSQL = strSQL + " AND citt.job_card_fk = job.job_card_TRN_pk";
                    strSQL = strSQL + " AND job.BOOKING_MST_FK = book.BOOKING_MST_PK";

                    strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
                    strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
                    strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                    strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK(+) = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + " AND COLT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO ";
                    strSQL = strSQL + " GROUP BY";
                    strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                    //strSQL = strSQL & vbCrLf & " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))"
                    strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)),cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,book.cargo_type";
                    //'
                    strSQL = strSQL + "   UNION";
                    strSQL = strSQL + "               SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + "                      TO_CHAR(TO_DATE(JOB.JOBCARD_DATE), 'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + "                      CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + "                     TO_CHAR(TO_DATE(CIT.INVOICE_DATE), 'DD/MM/YYYY') INVOICE_DATE,";
                    strSQL = strSQL + "                      TO_CHAR(TO_DATE(CIT.INVOICE_DATE) +";
                    strSQL = strSQL + "                              NVL(CUST.CREDIT_DAYS, 0),";
                    strSQL = strSQL + "                              'DD/MM/YYYY') DUEDATE,";
                    //strSQL = strSQL & vbCrLf & "                      CURR.CURRENCY_ID,"
                    strSQL = strSQL + "    '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,";
                    strSQL = strSQL + "                      CIT.INVOICE_AMT *";
                    strSQL = strSQL + "                       GET_EX_RATE(CIT.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + " , CIT.INVOICE_DATE) INVOICE,";
                    strSQL = strSQL + "                       ((CIT.INVOICE_AMT *";
                    strSQL = strSQL + "                      GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) -";
                    strSQL = strSQL + "                     (NVL(CIT.TOTAL_CREDIT_NOTE_AMT, 0) *";
                    strSQL = strSQL + "                       GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE))) INVOICE_AMT,";
                    strSQL = strSQL + "                       (NVL(COLT.RECD_AMOUNT_HDR_CURR, 0) *";
                    strSQL = strSQL + "                      GET_EX_RATE(COL.CURRENCY_MST_FK,";
                    strSQL = strSQL + "                                    " + HttpContext.Current.Session["currency_mst_pk"] + ",";
                    strSQL = strSQL + "                                    COL.COLLECTIONS_DATE)) OUT_AMOUNT,";
                    strSQL = strSQL + "                       CEIL(NVL(TO_DATE(SYSDATE, 'DD/MM/YYYY') -";
                    strSQL = strSQL + "                               (CIT.INVOICE_DATE + NVL(CUST.CREDIT_DAYS, 0)),";
                    strSQL = strSQL + "                                0)) OUTSTD_DAYS";
                    strSQL = strSQL + " ,cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type cargo_type";
                    strSQL = strSQL + "                 FROM CONSOL_INVOICE_TBL     CIT,";
                    strSQL = strSQL + "                      CONSOL_INVOICE_TRN_TBL CITT,";
                    strSQL = strSQL + "                      JOB_CARD_TRN   JOB,";
                    strSQL = strSQL + "                       CUSTOMER_MST_TBL       CUST,";
                    strSQL = strSQL + "                      CURRENCY_TYPE_MST_TBL  CURR,";
                    strSQL = strSQL + "                       COLLECTIONS_TBL        COL,";
                    strSQL = strSQL + "                       COLLECTIONS_TRN_TBL    COLT";
                    strSQL = strSQL + "                WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK";
                    strSQL = strSQL + "                   AND CITT.JOB_CARD_FK = JOB.job_card_TRN_pk";
                    strSQL = strSQL + "                   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + "                  AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + "                   AND JOB.SHIPPER_CUST_MST_FK = " + CustPk;
                    strSQL = strSQL + "                  AND CIT.INVOICE_DATE <= SYSDATE";
                    strSQL = strSQL + "                  AND COL.COLLECTIONS_TBL_PK(+) = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + "                   AND COLT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO";
                    strSQL = strSQL + "                 GROUP BY JOBCARD_REF_NO,";
                    strSQL = strSQL + "                         JOBCARD_DATE,";
                    strSQL = strSQL + "                         INVOICE_REF_NO,";
                    strSQL = strSQL + "                          CIT.INVOICE_DATE,";
                    strSQL = strSQL + "                          NVL(CUST.CREDIT_DAYS, 0),";
                    strSQL = strSQL + "                          CURRENCY_ID,";
                    strSQL = strSQL + "                         CIT.CURRENCY_MST_FK,";
                    strSQL = strSQL + "                          CIT.INVOICE_AMT,";
                    strSQL = strSQL + "                        CIT.TOTAL_CREDIT_NOTE_AMT,";
                    strSQL = strSQL + "                         COL.CURRENCY_MST_FK,";
                    strSQL = strSQL + "                         COL.COLLECTIONS_DATE,";
                    strSQL = strSQL + "                         NVL(COLT.RECD_AMOUNT_HDR_CURR, 0),";
                    strSQL = strSQL + "                          CEIL(NVL(TO_DATE(SYSDATE, 'DD/MM/YYYY') -";
                    strSQL = strSQL + "                                   ((CIT.INVOICE_DATE) +";
                    strSQL = strSQL + "                                   NVL(CUST.CREDIT_DAYS, 0)),";
                    strSQL = strSQL + "                                  0)),cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type";
                    //'
                    strSQL = strSQL + ")";
                    //strSQL = strSQL & vbCrLf & " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS)A)"
                    strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type)A WHERE (A.INVOICE_AMT - A.OUT_AMOUNT)> 0)";
                    //Modified by Faheem
                    strSQL = strSQL + " UNION ";

                    strSQL = strSQL + "(SELECT  A.JOBCARD_REF_NO,A.JOBCARD_DATE,A.INVOICE_REF_NO,A.INVOICE_DATE,A.DUEDATE,A.CURRENCY_ID,A.INVOICE,A.INVOICE_AMT-A.OUT_AMOUNT,";
                    //strSQL = strSQL & vbCrLf & " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else A.OUTSTD_DAYS end OUTSTD_DAYS from  ("
                    strSQL = strSQL + " case when (A.INVOICE_AMT-A.OUT_AMOUNT) <= 0 then 0 else CASE WHEN A.OUTSTD_DAYS < 0 THEN 0 ELSE A.OUTSTD_DAYS END end OUTSTD_DAYS,a.process_type,a.business_type,a.consol_invoice_pk,a.CHK_INVOICE,cargo_type from  (";
                    strSQL = strSQL + " select JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,sum(OUT_AMOUNT) OUT_AMOUNT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type from (";

                    strSQL = strSQL + " SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(JOB.JOBCARD_DATE),'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + " CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE),'DD/MM/YYYY') INVOICE_DATE,";
                    strSQL = strSQL + " TO_CHAR(TO_DATE(CIT.INVOICE_DATE) + nvl(cust.credit_days, 0),'DD/MM/YYYY') DUEDATE,";
                    //strSQL = strSQL & vbCrLf & " CURR.CURRENCY_ID,"
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,";
                    strSQL = strSQL + " CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ",CIT.INVOICE_DATE) INVOICE,";
                    strSQL = strSQL + " ((CIT.INVOICE_AMT * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) - ";
                    strSQL = strSQL + " (nvl(CIT.TOTAL_CREDIT_NOTE_AMT, 0) * GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)))INVOICE_AMT,";
                    strSQL = strSQL + " (nvl(COLT.RECD_AMOUNT_HDR_CURR, 0) * GET_EX_RATE(Col.CURRENCY_MST_FK," + HttpContext.Current.Session["currency_mst_pk"] + ",Col.Collections_Date)) OUT_AMOUNT,";

                    strSQL = strSQL + " ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - (cit.invoice_date + nvl(cust.credit_days,0)),0)) OUTSTD_DAYS";
                    strSQL = strSQL + " ,cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type cargo_type";
                    strSQL = strSQL + "  FROM";
                    strSQL = strSQL + " consol_invoice_tbl CIT,";
                    strSQL = strSQL + " consol_invoice_trn_tbl CITT,";

                    strSQL = strSQL + " JOB_CARD_TRN job,";
                    strSQL = strSQL + " BOOKING_MST_TBL book,";

                    strSQL = strSQL + " customer_mst_tbl cust,";
                    strSQL = strSQL + " CURRENCY_TYPE_MST_TBL CURR,";
                    strSQL = strSQL + " collections_tbl        col,";
                    strSQL = strSQL + " collections_trn_tbl    colt";
                    strSQL = strSQL + " WHERE";
                    strSQL = strSQL + " cit.consol_invoice_pk = citt.consol_invoice_fk";

                    strSQL = strSQL + " AND citt.job_card_fk = job.Job_Card_Trn_Pk";
                    strSQL = strSQL + " AND job.BOOKING_MST_FK = book.BOOKING_MST_PK";

                    strSQL = strSQL + " AND book.cust_customer_mst_fk = cust.customer_mst_pk(+)";
                    strSQL = strSQL + " AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + " AND book.cust_customer_mst_fk = " + CustPk;
                    strSQL = strSQL + " AND cit.invoice_date <= SYSDATE";
                    strSQL = strSQL + " AND COL.COLLECTIONS_TBL_PK(+) = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + " AND COLT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO ";
                    strSQL = strSQL + " GROUP BY";
                    strSQL = strSQL + " JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,CIT.INVOICE_DATE,nvl(cust.credit_days, 0),CURRENCY_ID,CIT.CURRENCY_MST_FK,";
                    //strSQL = strSQL & vbCrLf & " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)))"
                    strSQL = strSQL + " CIT.INVOICE_AMT,CIT.TOTAL_CREDIT_NOTE_AMT,Col.CURRENCY_MST_FK,Col.Collections_Date,nvl(COLT.RECD_AMOUNT_HDR_CURR, 0),ceil(NVL(TO_DATE(sysdate,'DD/MM/YYYY') - ((cit.invoice_date) + nvl(cust.credit_days,0)),0)),cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type";
                    //'
                    strSQL = strSQL + "   UNION";
                    strSQL = strSQL + "               SELECT JOB.JOBCARD_REF_NO,";
                    strSQL = strSQL + "                      TO_CHAR(TO_DATE(JOB.JOBCARD_DATE), 'DD/MM/YYYY') JOBCARD_DATE,";
                    strSQL = strSQL + "                      CIT.INVOICE_REF_NO,";
                    strSQL = strSQL + "                     TO_CHAR(TO_DATE(CIT.INVOICE_DATE), 'DD/MM/YYYY') INVOICE_DATE,";
                    strSQL = strSQL + "                      TO_CHAR(TO_DATE(CIT.INVOICE_DATE) +";
                    strSQL = strSQL + "                              NVL(CUST.CREDIT_DAYS, 0),";
                    strSQL = strSQL + "                              'DD/MM/YYYY') DUEDATE,";
                    //strSQL = strSQL & vbCrLf & "                      CURR.CURRENCY_ID,"
                    strSQL = strSQL + " '" + HttpContext.Current.Session["CURRENCY_ID"] + "' CURRENCY_ID,";
                    strSQL = strSQL + "                      CIT.INVOICE_AMT *";
                    strSQL = strSQL + "                       GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE) INVOICE,";
                    strSQL = strSQL + "                       ((CIT.INVOICE_AMT *";
                    strSQL = strSQL + "                      GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE)) -";
                    strSQL = strSQL + "                     (NVL(CIT.TOTAL_CREDIT_NOTE_AMT, 0) *";
                    strSQL = strSQL + "                       GET_EX_RATE(CIT.CURRENCY_MST_FK, " + HttpContext.Current.Session["currency_mst_pk"] + ", CIT.INVOICE_DATE))) INVOICE_AMT,";
                    strSQL = strSQL + "                       (NVL(COLT.RECD_AMOUNT_HDR_CURR, 0) *";
                    strSQL = strSQL + "                      GET_EX_RATE(COL.CURRENCY_MST_FK,";
                    strSQL = strSQL + "                                    " + HttpContext.Current.Session["currency_mst_pk"] + ",";
                    strSQL = strSQL + "                                    COL.COLLECTIONS_DATE)) OUT_AMOUNT,";
                    strSQL = strSQL + "                       CEIL(NVL(TO_DATE(SYSDATE, 'DD/MM/YYYY') -";
                    strSQL = strSQL + "                               (CIT.INVOICE_DATE + NVL(CUST.CREDIT_DAYS, 0)),";
                    strSQL = strSQL + "                                0)) OUTSTD_DAYS";
                    strSQL = strSQL + " ,cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type cargo_type";
                    strSQL = strSQL + "                 FROM CONSOL_INVOICE_TBL     CIT,";
                    strSQL = strSQL + "                      CONSOL_INVOICE_TRN_TBL CITT,";
                    strSQL = strSQL + "                      JOB_CARD_TRN   JOB,";
                    strSQL = strSQL + "                       CUSTOMER_MST_TBL       CUST,";
                    strSQL = strSQL + "                      CURRENCY_TYPE_MST_TBL  CURR,";
                    strSQL = strSQL + "                       COLLECTIONS_TBL        COL,";
                    strSQL = strSQL + "                       COLLECTIONS_TRN_TBL    COLT";
                    strSQL = strSQL + "                WHERE CIT.CONSOL_INVOICE_PK = CITT.CONSOL_INVOICE_FK";
                    strSQL = strSQL + "                   AND CITT.JOB_CARD_FK = JOB.job_card_TRN_pk";
                    strSQL = strSQL + "                   AND JOB.SHIPPER_CUST_MST_FK = CUST.CUSTOMER_MST_PK(+)";
                    strSQL = strSQL + "                  AND CURR.CURRENCY_MST_PK = CIT.CURRENCY_MST_FK";
                    strSQL = strSQL + "                   AND JOB.SHIPPER_CUST_MST_FK = " + CustPk;
                    strSQL = strSQL + "                  AND CIT.INVOICE_DATE <= SYSDATE";
                    strSQL = strSQL + "                  AND COL.COLLECTIONS_TBL_PK(+) = COLT.COLLECTIONS_TBL_FK";
                    strSQL = strSQL + "                   AND COLT.INVOICE_REF_NR(+) = CIT.INVOICE_REF_NO";
                    strSQL = strSQL + "                 GROUP BY JOBCARD_REF_NO,";
                    strSQL = strSQL + "                         JOBCARD_DATE,";
                    strSQL = strSQL + "                         INVOICE_REF_NO,";
                    strSQL = strSQL + "                          CIT.INVOICE_DATE,";
                    strSQL = strSQL + "                          NVL(CUST.CREDIT_DAYS, 0),";
                    strSQL = strSQL + "                          CURRENCY_ID,";
                    strSQL = strSQL + "                         CIT.CURRENCY_MST_FK,";
                    strSQL = strSQL + "                          CIT.INVOICE_AMT,";
                    strSQL = strSQL + "                        CIT.TOTAL_CREDIT_NOTE_AMT,";
                    strSQL = strSQL + "                         COL.CURRENCY_MST_FK,";
                    strSQL = strSQL + "                         COL.COLLECTIONS_DATE,";
                    strSQL = strSQL + "                         NVL(COLT.RECD_AMOUNT_HDR_CURR, 0),";
                    strSQL = strSQL + "                          CEIL(NVL(TO_DATE(SYSDATE, 'DD/MM/YYYY') -";
                    strSQL = strSQL + "                                   ((CIT.INVOICE_DATE) +";
                    strSQL = strSQL + "                                   NVL(CUST.CREDIT_DAYS, 0)),";
                    strSQL = strSQL + "                                  0)),cit.process_type,cit.business_type,cit.consol_invoice_pk,cit.CHK_INVOICE,job.cargo_type";
                    //'
                    strSQL = strSQL + ")";
                    strSQL = strSQL + " WHERE INVOICE_AMT >0  ";
                    //Added by Faheem
                    strSQL = strSQL + " group by JOBCARD_REF_NO,JOBCARD_DATE,INVOICE_REF_NO,INVOICE_DATE,DUEDATE,CURRENCY_ID,INVOICE,INVOICE_AMT,OUTSTD_DAYS,process_type,business_type,consol_invoice_pk,CHK_INVOICE,cargo_type)A)";
                    strSQL = strSQL + "  )WHERE OUTSTANDING>0  ORDER BY OUTSTANDING DESC,JOBCARD_DATE desc,JOBCARD_REF_NO DESC )Q ";
                }
                ds = objWF.GetDataSet(strSQL);
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchOutStand"

        /// <summary>
        /// Fetches all.
        /// </summary>
        /// <param name="customerPK">The customer pk.</param>
        /// <param name="businesstype">The businesstype.</param>
        /// <returns></returns>
        public DataSet FetchAll(string customerPK = "", Int32 businesstype = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            strCondition = "AND CCD.CUSTOMER_MST_FK = '" + customerPK + "' AND CMT.CUSTOMER_MST_PK = '" + customerPK + "'";
            //CUSTOMER_MST_PK, CUSTOMER_ID, CUSTOMER_NAME, ACTIVE_FLAG, CREDIT_LIMIT,
            // CREDIT_DAYS, SECURITY_CHK_REQD, ACCOUNT_NO, CREATED_BY_FK, CREATED_DT,
            // LAST_MODIFIED_BY_FK, LAST_MODIFIED_DT, VERSION_NO, BUSINESS_TYPE, CUSTOMER_TYPE_FK, VAT_NO
            //CUSTOMER_MST_FK, ADM_ADDRESS_1, ADM_ADDRESS_2, ADM_ADDRESS_3, ADM_ZIP_CODE, ADM_CONTACT_PERSON,
            //ADM_PHONE_NO_1, ADM_PHONE_NO_2, ADM_FAX_NO, ADM_EMAIL_ID, ADM_URL, ADM_SHORT_NAME, COR_ADDRESS_1,
            // COR_ADDRESS_2, COR_ADDRESS_3, COR_ZIP_CODE, COR_CONTACT_PERSON, COR_PHONE_NO_1, COR_PHONE_NO_2,
            //COR_FAX_NO, COR_EMAIL_ID, COR_URL, COR_SHORT_NAME, BILL_ADDRESS_1, BILL_ADDRESS_2, BILL_ADDRESS_3,
            //BILL_ZIP_CODE, BILL_CONTACT_PERSON, BILL_PHONE_NO_1, BILL_PHONE_NO_2, BILL_FAX_NO, BILL_EMAIL_ID,
            //BILL_URL, BILL_SHORT_NAME, CREATED_BY_FK, CREATED_DT, LAST_MODIFIED_BY_FK, LAST_MODIFIED_DT,
            //VERSION_NO, ADM_LOCATION_MST_FK, COR_CITY, COR_LOCATION_MST_FK, BILL_CITY, BILL_LOCATION_MST_FK,
            //ADM_CITY
            strSQL = " SELECT CMT.CUSTOMER_ID,";
            strSQL = strSQL + "CMT.CUSTOMER_MST_PK, ";
            strSQL = strSQL + "CMT.CUSTOMER_NAME,";
            strSQL = strSQL + "CMT.ACTIVE_FLAG,";
            strSQL = strSQL + "CMT.CREDIT_LIMIT,";
            strSQL = strSQL + "CMT.CREDIT_DAYS,";
            strSQL = strSQL + "CMT.SECURITY_CHK_REQD,";
            strSQL = strSQL + "DECODE(CMT.BUSINESS_TYPE,'0','','1','Air','2','Sea','3','Both') BUSINESS_TYPE,";
            strSQL = strSQL + "CMT.VAT_NO,";
            strSQL = strSQL + "CMT.ACCOUNT_NO,";
            strSQL = strSQL + "CUSTOMER_TYPE_FK,";
            strSQL = strSQL + "CCD.ADM_ADDRESS_1,";
            strSQL = strSQL + "CCD.ADM_ADDRESS_2,";
            strSQL = strSQL + "CCD.ADM_ADDRESS_3,";
            strSQL = strSQL + "CCD.ADM_CITY,";
            strSQL = strSQL + "CCD.ADM_ZIP_CODE,";
            strSQL = strSQL + "CCD.ADM_LOCATION_MST_FK,";
            strSQL = strSQL + "CCD.ADM_CONTACT_PERSON,";
            strSQL = strSQL + "CCD.ADM_PHONE_NO_1,";
            strSQL = strSQL + "CCD.ADM_PHONE_NO_2,";
            strSQL = strSQL + "CCD.ADM_FAX_NO,";
            strSQL = strSQL + "CCD.ADM_EMAIL_ID,";
            strSQL = strSQL + "CCD.ADM_URL,";
            strSQL = strSQL + "CCD.ADM_COUNTRY_MST_FK,";

            strSQL = strSQL + "CCD.COR_ADDRESS_1,";
            strSQL = strSQL + "CCD.COR_ADDRESS_2,";
            strSQL = strSQL + "CCD.COR_ADDRESS_3,";
            strSQL = strSQL + "CCD.COR_CITY,";
            strSQL = strSQL + "CCD.COR_ZIP_CODE,";
            strSQL = strSQL + "CCD.COR_LOCATION_MST_FK,";
            strSQL = strSQL + "CCD.COR_CONTACT_PERSON,";
            strSQL = strSQL + "CCD.COR_PHONE_NO_1,";
            strSQL = strSQL + "CCD.COR_PHONE_NO_2,";
            strSQL = strSQL + "CCD.COR_FAX_NO,";
            strSQL = strSQL + "CCD.COR_EMAIL_ID,";
            strSQL = strSQL + "CCD.COR_URL,";
            strSQL = strSQL + "CCD.COR_COUNTRY_MST_FK,";

            strSQL = strSQL + "CCD.BILL_ADDRESS_1,";
            strSQL = strSQL + "CCD.BILL_ADDRESS_2,";
            strSQL = strSQL + "CCD.BILL_ADDRESS_3,";
            strSQL = strSQL + "CCD.BILL_CITY,";
            strSQL = strSQL + "CCD.BILL_ZIP_CODE,";
            strSQL = strSQL + "CCD.BILL_LOCATION_MST_FK,";
            strSQL = strSQL + "CCD.BILL_CONTACT_PERSON,";
            strSQL = strSQL + "CCD.BILL_PHONE_NO_1,";
            strSQL = strSQL + "CCD.BILL_PHONE_NO_2,";
            strSQL = strSQL + "CCD.BILL_FAX_NO,";
            strSQL = strSQL + "CCD.BILL_EMAIL_ID,";
            strSQL = strSQL + "CCD.BILL_URL,";
            strSQL = strSQL + "CCD.ADM_SHORT_NAME,";
            strSQL = strSQL + "CCD.COR_SHORT_NAME,";
            strSQL = strSQL + "CCD.BILL_SHORT_NAME,";
            strSQL = strSQL + "CCD.BILL_COUNTRY_MST_FK,";

            strSQL = strSQL + " CMT.VERSION_NO,";
            strSQL = strSQL + " CCD.ADM_SALUTATION,";
            //Priya - 28/02/06
            strSQL = strSQL + " CCD.COR_SALUTATION,";
            //Priya - 28/02/06
            strSQL = strSQL + " CCD.BILL_SALUTATION,";
            //Priya - 28/02/06
            strSQL = strSQL + " CMT.REP_EMP_MST_FK,";
            //Priya - 01/03/06
            strSQL = strSQL + " EMP.EMPLOYEE_NAME";
            //Priya - 01/03/06
            strSQL = strSQL + " FROM CUSTOMER_MST_TBL CMT,CUSTOMER_CONTACT_DTLS CCD,EMPLOYEE_MST_TBL EMP ";
            strSQL = strSQL + "WHERE ( 1 = 1) ";
            strSQL = strSQL + "AND CMT.REP_EMP_MST_FK=EMP.EMPLOYEE_MST_PK (+) ";

            strSQL = strSQL + strCondition;

            try
            {
                return objWF.GetDataSet(strSQL);
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

        #endregion "Fetch Details"

        #region "location"

        /// <summary>
        /// Selects the location.
        /// </summary>
        /// <param name="fkLocation">The fk location.</param>
        /// <returns></returns>
        public string SelectLocation(int fkLocation)
        {
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSQL = "select LOCATION_NAME from LOCATION_MST_TBL where LOCATION_MST_PK = " + fkLocation + " ";
                string LocName = null;
                LocName = objWF.ExecuteScaler(strSQL);
                return LocName;
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

        #endregion "location"

        #region "SAVE"

        /// <summary>
        /// Saves the new.
        /// </summary>
        /// <param name="ContactDataSet">The contact data set.</param>
        /// <param name="dsChild">The ds child.</param>
        /// <param name="CUSTOMERID">The customerid.</param>
        /// <param name="CUSTOMERName">Name of the customer.</param>
        /// <param name="activeflag">The activeflag.</param>
        /// <param name="creditcustomer">The creditcustomer.</param>
        /// <param name="VATCODE">The vatcode.</param>
        /// <param name="BType">Type of the b.</param>
        /// <param name="customerType">Type of the customer.</param>
        /// <param name="ACCOUNTNO">The accountno.</param>
        /// <param name="secChk">The sec CHK.</param>
        /// <param name="creditD">The credit d.</param>
        /// <param name="creditLmt">The credit LMT.</param>
        /// <param name="admAddress1">The adm address1.</param>
        /// <param name="admAddress2">The adm address2.</param>
        /// <param name="admaddress3">The admaddress3.</param>
        /// <param name="admcity">The admcity.</param>
        /// <param name="admzipcode">The admzipcode.</param>
        /// <param name="admLocation">The adm location.</param>
        /// <param name="admShort">The adm short.</param>
        /// <param name="admContactPerson">The adm contact person.</param>
        /// <param name="admPhone1">The adm phone1.</param>
        /// <param name="admPhone2">The adm phone2.</param>
        /// <param name="admFax">The adm fax.</param>
        /// <param name="admEmail">The adm email.</param>
        /// <param name="admUrl">The adm URL.</param>
        /// <param name="AdmCountry">The adm country.</param>
        /// <param name="corAddress1">The cor address1.</param>
        /// <param name="corAddress2">The cor address2.</param>
        /// <param name="coraddress3">The coraddress3.</param>
        /// <param name="corcity">The corcity.</param>
        /// <param name="corzipcode">The corzipcode.</param>
        /// <param name="corLocation">The cor location.</param>
        /// <param name="corShort">The cor short.</param>
        /// <param name="corContactPerson">The cor contact person.</param>
        /// <param name="corPhone1">The cor phone1.</param>
        /// <param name="corPhone2">The cor phone2.</param>
        /// <param name="corFax">The cor fax.</param>
        /// <param name="corEmail">The cor email.</param>
        /// <param name="corUrl">The cor URL.</param>
        /// <param name="CorrCountry">The corr country.</param>
        /// <param name="billAddress1">The bill address1.</param>
        /// <param name="billAddress2">The bill address2.</param>
        /// <param name="billaddress3">The billaddress3.</param>
        /// <param name="billcity">The billcity.</param>
        /// <param name="billzipcode">The billzipcode.</param>
        /// <param name="billLocation">The bill location.</param>
        /// <param name="billShort">The bill short.</param>
        /// <param name="billContactPerson">The bill contact person.</param>
        /// <param name="billPhone1">The bill phone1.</param>
        /// <param name="billPhone2">The bill phone2.</param>
        /// <param name="billFax">The bill fax.</param>
        /// <param name="billEmail">The bill email.</param>
        /// <param name="billUrl">The bill URL.</param>
        /// <param name="BillCountry">The bill country.</param>
        /// <param name="CUSTOMERPk">The customer pk.</param>
        /// <param name="AdmSalutation">The adm salutation.</param>
        /// <param name="CorSalutation">The cor salutation.</param>
        /// <param name="BillSalutation">The bill salutation.</param>
        /// <param name="repEmp">The rep emp.</param>
        /// <param name="agentfk">The agentfk.</param>
        /// <param name="defermentNo">The deferment no.</param>
        /// <param name="turnNo">The turn no.</param>
        /// <param name="ColAdd">The col add.</param>
        /// <param name="delAdd">The delete add.</param>
        /// <param name="remarks">The remarks.</param>
        /// <param name="CustRegNr">The customer reg nr.</param>
        /// <param name="Status">The status.</param>
        /// <param name="Curr">The curr.</param>
        /// <param name="Customer_Type">Type of the customer_.</param>
        /// <param name="Ref_Grp_Cust_pk">The ref_ GRP_ cust_pk.</param>
        /// <param name="TempCustPk">The temporary customer pk.</param>
        /// <param name="chkcust">The chkcust.</param>
        /// <param name="Act_Number">The act_ number.</param>
        /// <param name="swift_code">The swift_code.</param>
        /// <param name="Bank_Number">The bank_ number.</param>
        /// <param name="Iban">The iban.</param>
        /// <param name="Branch">The branch.</param>
        /// <param name="ebank_code">The ebank_code.</param>
        /// <param name="Location">The location.</param>
        /// <param name="bankcountry">The bankcountry.</param>
        /// <param name="Bank_Name">Name of the bank_.</param>
        /// <param name="Address">The address.</param>
        /// <param name="CustCategory">The customer category.</param>
        /// <param name="txtCustCategory">The text customer category.</param>
        /// <param name="IntDivPK">The int div pk.</param>
        /// <param name="BranchPK">The branch pk.</param>
        /// <param name="TaxStatus">The tax status.</param>
        /// <param name="rated">The rated.</param>
        /// <param name="VATRegistered">The vat registered.</param>
        /// <param name="GroupCategory">The group category.</param>
        /// <param name="Priority">The priority.</param>
        /// <param name="Criteria">The criteria.</param>
        /// <param name="Category">The category.</param>
        /// <param name="MapStatus">The map status.</param>
        /// <param name="PercentStatus">The percent status.</param>
        /// <param name="Percentvalue">The percentvalue.</param>
        /// <returns></returns>
        public ArrayList SaveNew(DataSet ContactDataSet, DataSet dsChild, string CUSTOMERID = "", string CUSTOMERName = "", Int16 activeflag = 0, Int16 creditcustomer = 0, string VATCODE = "", int BType = 0, int customerType = 0, string ACCOUNTNO = "",
        Int16 secChk = 0, string creditD = "", string creditLmt = "", string admAddress1 = "", string admAddress2 = " ", string admaddress3 = " ", string admcity = " ", string admzipcode = " ", Int16 admLocation = 0, string admShort = " ",
        string admContactPerson = " ", string admPhone1 = " ", string admPhone2 = " ", string admFax = " ", string admEmail = " ", string admUrl = " ", Int16 AdmCountry = 0, string corAddress1 = " ", string corAddress2 = " ", string coraddress3 = " ",
        string corcity = " ", string corzipcode = " ", Int16 corLocation = 0, string corShort = " ", string corContactPerson = " ", string corPhone1 = " ", string corPhone2 = " ", string corFax = " ", string corEmail = " ", string corUrl = "",
        Int16 CorrCountry = 0, string billAddress1 = " ", string billAddress2 = " ", string billaddress3 = " ", string billcity = " ", string billzipcode = " ", Int16 billLocation = 0, string billShort = " ", string billContactPerson = " ", string billPhone1 = " ",
        string billPhone2 = " ", string billFax = " ", string billEmail = " ", string billUrl = " ", Int16 BillCountry = 0, int CUSTOMERPk = 0, Int16 AdmSalutation = 0, Int16 CorSalutation = 0, Int16 BillSalutation = 0, Int16 repEmp = 0,
        string agentfk = "", string defermentNo = "", string turnNo = "", string ColAdd = "", string delAdd = "", string remarks = "", string CustRegNr = "", Int16 Status = 0, Int16 Curr = 0, long Customer_Type = 0,
        long Ref_Grp_Cust_pk = 0, Int16 TempCustPk = 0, Int16 chkcust = 0, string Act_Number = "", string swift_code = "", string Bank_Number = "", string Iban = "", string Branch = "", string ebank_code = "", string Location = "",
        string bankcountry = "", string Bank_Name = "", string Address = "", int CustCategory = 1, string txtCustCategory = "", string IntDivPK = "", string BranchPK = "", int TaxStatus = 1, string rated = "", string VATRegistered = "",
        int GroupCategory = 0, int Priority = 0, int Criteria = 0, int Category = 0, Int16 MapStatus = 0, int PercentStatus = 1, string Percentvalue = "")
        {
            //in the above function Customer_Type ,  Ref_Grp_Cust_pk parameters added by surya prasad on 03-Jan-2009

            int intPKVal = 0;
            long lngI = 0;
            DateTime EnteredBudgetStartDate = default(DateTime);
            Int32 RecAfct = default(Int32);
            System.DBNull StrNull = null;
            long lngBudHdrPK = 0;
            OracleCommand insCommand = new OracleCommand();
            OracleCommand insChildCommand = new OracleCommand();
            System.DateTime EnteredDate = default(System.DateTime);
            Int32 inti = default(Int32);
            int Contact = 0;
            long lngDepotPK = 0;
            WorkFlow objWK = new WorkFlow();
            Cls_CustomerReconciliation objCustomer = new Cls_CustomerReconciliation();
            //Snigdharani
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();

            try
            {
                var _with5 = insCommand;
                _with5.Connection = objWK.MyConnection;
                _with5.CommandType = CommandType.StoredProcedure;
                _with5.CommandText = objWK.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_MST_TBL_INS";
                var _with6 = _with5.Parameters;

                _with6.Add("CUSTOMER_ID_IN", CUSTOMERID);
                _with6.Add("CUSTOMER_NAME_IN", CUSTOMERName);
                _with6.Add("ACTIVE_FLAG_IN", activeflag);
                _with6.Add("CREDIT_CUSTOMER_IN", creditcustomer);
                _with6.Add("CREDIT_LIMIT_IN", (string.IsNullOrEmpty(creditLmt) ? "" : creditLmt));
                _with6.Add("CREDIT_DAYS_IN", (string.IsNullOrEmpty(creditD) ? "" : creditD));
                _with6.Add("SECURITY_CHK_REQD_IN", secChk);
                _with6.Add("ACCOUNT_NO_IN", (string.IsNullOrEmpty(ACCOUNTNO) ? "" : ACCOUNTNO));
                _with6.Add("BUSINESS_TYPE_IN", BType);
                _with6.Add("CUSTOMER_TYPE_FK_IN", customerType);
                _with6.Add("VAT_NO_IN", getDefault(VATCODE, ""));
                _with6.Add("REP_EMP_MST_FK_IN", (repEmp == 0 ? 0 : repEmp));
                // Priya -01-Mar-06
                _with6.Add("CONFIG_PK_IN", M_Configuration_PK);
                _with6.Add("CREATED_BY_FK_IN", CREATED_BY);

                _with6.Add("ADM_ADDRESS_1_IN", admAddress1);
                _with6.Add("ADM_ADDRESS_2_IN", (string.IsNullOrEmpty(admAddress2) ? "" : admAddress2));
                _with6.Add("ADM_ADDRESS_3_IN", (string.IsNullOrEmpty(admaddress3) ? "" : admaddress3));
                _with6.Add("ADM_CITY_IN", (string.IsNullOrEmpty(admcity) ? "" : admcity));
                _with6.Add("ADM_ZIP_CODE_IN", (string.IsNullOrEmpty(admzipcode) ? "" : admzipcode));
                _with6.Add("ADM_LOCATION_MST_FK_IN", (admLocation == 0 ? 0 : admLocation));
                _with6.Add("ADM_CONTACT_PERSON_IN", (string.IsNullOrEmpty(admContactPerson) ? "" : admContactPerson));
                _with6.Add("ADM_PHONE_NO_1_IN", (string.IsNullOrEmpty(admPhone1) ? "" : admPhone1));
                _with6.Add("ADM_PHONE_NO_2_IN", (string.IsNullOrEmpty(admPhone2) ? "" : admPhone2));
                _with6.Add("ADM_FAX_NO_IN", (string.IsNullOrEmpty(admFax) ? "" : admFax));
                _with6.Add("ADM_EMAIL_ID_IN", (string.IsNullOrEmpty(admEmail) ? "" : admEmail));
                _with6.Add("ADM_URL_IN", (string.IsNullOrEmpty(admUrl) ? "" : admUrl));
                _with6.Add("ADM_SHORT_NAME_IN", (string.IsNullOrEmpty(admShort) ? "" : admShort));
                _with6.Add("ADM_COUNTRY_MST_FK_IN", (AdmCountry == 0 ? 0 : AdmCountry));

                _with6.Add("COR_ADDRESS_1_IN", (string.IsNullOrEmpty(corAddress1) ? "" : corAddress1));
                _with6.Add("COR_ADDRESS_2_IN", (string.IsNullOrEmpty(corAddress2) ? "" : corAddress2));
                _with6.Add("COR_ADDRESS_3_IN", (string.IsNullOrEmpty(coraddress3) ? "" : coraddress3));
                _with6.Add("COR_CITY_IN", (string.IsNullOrEmpty(corcity) ? "" : corcity));
                _with6.Add("COR_ZIP_CODE_IN", (string.IsNullOrEmpty(corzipcode) ? "" : corzipcode));
                _with6.Add("COR_LOCATION_MST_FK_IN", (corLocation == 0 ? 0 : corLocation));
                _with6.Add("COR_CONTACT_PERSON_IN", (string.IsNullOrEmpty(corContactPerson) ? "" : corContactPerson));
                _with6.Add("COR_PHONE_NO_1_IN", (string.IsNullOrEmpty(corPhone1) ? "" : corPhone1));
                _with6.Add("COR_PHONE_NO_2_IN", (string.IsNullOrEmpty(corPhone2) ? "" : corPhone2));
                _with6.Add("COR_FAX_NO_IN", (string.IsNullOrEmpty(corFax) ? "" : corFax));
                _with6.Add("COR_EMAIL_ID_IN", (string.IsNullOrEmpty(corEmail) ? "" : corEmail));
                _with6.Add("COR_URL_IN", (string.IsNullOrEmpty(corUrl) ? "" : corUrl));
                _with6.Add("COR_SHORT_NAME_IN", (string.IsNullOrEmpty(corShort) ? "" : corShort));
                _with6.Add("COR_COUNTRY_MST_FK_IN", (CorrCountry == 0 ? 0 : CorrCountry));

                _with6.Add("BILL_ADDRESS_1_IN", (string.IsNullOrEmpty(billAddress1) ? "" : billAddress1));
                _with6.Add("BILL_ADDRESS_2_IN", (string.IsNullOrEmpty(billAddress2) ? "" : billAddress2));
                _with6.Add("BILL_ADDRESS_3_IN", (string.IsNullOrEmpty(billaddress3) ? "" : billaddress3));
                _with6.Add("BILL_CITY_IN", (string.IsNullOrEmpty(billcity) ? "" : billcity));
                _with6.Add("BILL_ZIP_CODE_IN", (string.IsNullOrEmpty(billzipcode) ? "" : billzipcode));
                _with6.Add("BILL_LOCATION_MST_FK_IN", (billLocation == 0 ? 0 : billLocation));
                _with6.Add("BILL_CONTACT_PERSON_IN", (string.IsNullOrEmpty(billContactPerson) ? "" : billContactPerson));
                _with6.Add("BILL_PHONE_NO_1_IN", (string.IsNullOrEmpty(billPhone1) ? "" : billPhone1));
                _with6.Add("BILL_PHONE_NO_2_IN", (string.IsNullOrEmpty(billPhone2) ? "" : billPhone2));
                _with6.Add("BILL_FAX_NO_IN", (string.IsNullOrEmpty(billFax) ? "" : billFax));
                _with6.Add("BILL_EMAIL_ID_IN", (string.IsNullOrEmpty(billEmail) ? "" : billEmail));
                _with6.Add("BILL_URL_IN", (string.IsNullOrEmpty(billUrl) ? "" : billUrl));
                _with6.Add("BILL_SHORT_NAME_IN", (string.IsNullOrEmpty(billShort) ? "" : billShort));
                _with6.Add("ADM_SALUTATION_IN", AdmSalutation);
                //Priya - 28/02/2006
                _with6.Add("COR_SALUTATION_IN", CorSalutation);
                //Priya - 28/02/2006
                _with6.Add("BILL_SALUTATION_IN", BillSalutation);
                //Priya - 28/02/2006
                _with6.Add("CREATEDD_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with6.Add("BILL_COUNTRY_MST_FK_IN", (BillCountry == 0 ? 0 : BillCountry));
                //==========
                _with6.Add("DEFERMENT_NO_IN", getDefault(defermentNo, ""));
                _with6.Add("TURN_NO_IN", getDefault(turnNo, ""));
                _with6.Add("COL_ADDRESS_IN", getDefault(ColAdd, ""));
                _with6.Add("DEL_ADDRESS_IN", getDefault(delAdd, ""));
                _with6.Add("REMARKS_IN", getDefault(remarks, ""));
                _with6.Add("DP_AGENT_MST_FK_IN", getDefault(agentfk, ""));
                _with6.Add("CUST_REG_NO_IN", getDefault(CustRegNr, ""));
                //Venkata 27/12/06
                _with6.Add("STATUS_IN", Status);
                //Manoharan 21Feb07: GAP-USS-QFOR-027
                _with6.Add("CURRENCY_MST_FK_IN", Curr);
                //=========
                _with6.Add("Act_Number_IN", (string.IsNullOrEmpty(Act_Number) ? "" : Act_Number));
                _with6.Add("swift_code_IN", (string.IsNullOrEmpty(swift_code) ? "" : swift_code));
                _with6.Add("Bank_Number_IN", (string.IsNullOrEmpty(Bank_Number) ? "" : Bank_Number));
                _with6.Add("Iban_IN", (string.IsNullOrEmpty(Iban) ? "" : Iban));
                _with6.Add("Branch_IN", (string.IsNullOrEmpty(Branch) ? "" : Branch));
                _with6.Add("ebank_code_IN", (string.IsNullOrEmpty(ebank_code) ? "" : ebank_code));
                _with6.Add("Location_IN", (string.IsNullOrEmpty(Location) ? "" : Location));
                _with6.Add("bankcountry_IN", (string.IsNullOrEmpty(bankcountry) ? "" : bankcountry));
                _with6.Add("Bank_Name_IN", (string.IsNullOrEmpty(Bank_Name) ? "" : Bank_Name));
                _with6.Add("Address_IN", (string.IsNullOrEmpty(Address) ? "" : Address));
                //========
                _with6.Add("CUST_CATEGORY_IN", CustCategory);
                _with6.Add("CATEGORY_IN", (string.IsNullOrEmpty(txtCustCategory) ? "" : txtCustCategory));
                _with6.Add("INT_DIV_IN", getDefault(IntDivPK, ""));
                _with6.Add("BRANCHPK_IN", getDefault(BranchPK, ""));
                _with6.Add("TAX_STATUS_IN", TaxStatus);
                _with6.Add("RATED_IN", (string.IsNullOrEmpty(rated) ? "" : rated));
                _with6.Add("VAT_REGISTERED_IN", (string.IsNullOrEmpty(VATRegistered) ? "" : VATRegistered));
                //==========Added by SuryaPrasad =============
                _with6.Add("CUSTOMER_TYPE_IN", Customer_Type);
                _with6.Add("REF_GROUP_CUST_PK_IN", (Ref_Grp_Cust_pk == 0 ? 0 : Ref_Grp_Cust_pk));
                //======Group Category====
                _with6.Add("GROUP_CATEGORY_IN", GroupCategory);
                //===
                //adding by thiyagarajan on 12/5/09
                _with6.Add("CHKCUST_IN", chkcust).Direction = ParameterDirection.Input;
                _with6.Add("PRIORITY_IN", Priority).Direction = ParameterDirection.Input;
                _with6.Add("CRITERIA_IN", Criteria).Direction = ParameterDirection.Input;
                _with6.Add("CATEGORY_TYPE_IN", Category).Direction = ParameterDirection.Input;
                //==========End===============================
                _with6.Add("MAP_STATUS_IN", MapStatus).Direction = ParameterDirection.Input;
                _with6.Add("PERCENTAGE_STATUS_IN", PercentStatus).Direction = ParameterDirection.Input;
                _with6.Add("PERCENTAGE_VALUE_IN", Percentvalue).Direction = ParameterDirection.Input;
                _with6.Add("RETURN_VALUE", CUSTOMERPk).Direction = ParameterDirection.Output;
                insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;

                var _with7 = objWK.MyDataAdapter;
                _with7.InsertCommand = insCommand;
                _with7.InsertCommand.Transaction = TRAN;
                _with7.InsertCommand.ExecuteNonQuery();
                CUSTOMERPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                DtTbl = dsChild.Tables[0];
                var _with8 = insChildCommand;
                _with8.Connection = objWK.MyConnection;
                _with8.CommandType = CommandType.StoredProcedure;
                _with8.Transaction = TRAN;
                _with8.CommandText = objWK.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_CATE_TRN_UPD";
                var _with9 = _with8.Parameters;

                foreach (DataRow DtRw_loopVariable in dsChild.Tables[0].Rows)
                {
                    DtRw = DtRw_loopVariable;
                    _with9.Clear();
                    _with9.Add("CUSTOMER_MST_FK_IN", CUSTOMERPk);
                    _with9.Add("CUSTOMER_CATEGORY_MST_FK_IN", DtRw["CustomerCtPk"]).Direction = ParameterDirection.Input;
                    // insChildCommand.Parameters["CUSTOMER_CATEGORY_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                    _with9.Add("SELECT_IN", DtRw["SELECT"]).Direction = ParameterDirection.Input;
                    //insChildCommand.Parameters["SELECT_IN"].SourceVersion = DataRowVersion.Current
                    _with9.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK);
                    _with9.Add("CONFIG_PK_IN", M_Configuration_PK);
                    _with9.Add("RETURN_VALUE", CUSTOMERPk).Direction = ParameterDirection.Output;
                    insChildCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    insChildCommand.ExecuteNonQuery();
                }
                //With objWK.MyDataAdapter
                //    .InsertCommand = insChildCommand
                //    .InsertCommand.Transaction = TRAN
                //.InsertCommand.ExecuteNonQuery()
                //Contact = Save_Contact(ContactDataSet, objWK, TRAN, CUSTOMERPk)
                //Contact = Save_Contact(objWK, TRAN, CUSTOMERPk);

                TRAN.Commit();
                //Push to financial system if realtime is selected
                Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                ArrayList schDtls = null;
                bool errGen = false;
                if (objSch.GetSchedulerPushType() == true)
                {
                    //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                    //try
                    //{
                    //    schDtls = objSch.FetchSchDtls();
                    //    //'Used to Fetch the Sch Dtls
                    //    objPush.UpdateCustomerMaster(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, Convert.ToString(CUSTOMERPk));
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
                if (Convert.ToInt32(HttpContext.Current.Session["CRecon"]) == 1)
                {
                    ReconcileCustomer(CUSTOMERPk);
                }
                //End If
                if (arrMessage.Count > 0)
                {
                    //TRAN.Rollback()
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("All Data Saved Successfully");
                    return arrMessage;
                }
                //TRAN.Commit()
                //End With
                return arrMessage;
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                //Throw OraExp
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
                //Return 1
                //'added by minakshi on 10-feb-08 for connection close task
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        //Added By :Anand
        //Reason : To Reconcile the Temp customer to a newly created customer
        //DateTime : 19/03/2008
        /// <summary>
        /// Reconciles the customer.
        /// </summary>
        /// <param name="Cust_PK">The cust_ pk.</param>
        public void ReconcileCustomer(int Cust_PK)
        {
            string SqlStr = null;
            string SqlStr1 = null;
            string SqlName = null;
            int BizType = 0;
            int TrnType = 0;
            string CustName = null;
            DataRow dRow = null;
            ArrayList arrTemp = null;
            WorkFlow objWK = new WorkFlow();
            Quantum_QFOR.Cls_CustomerReconciliation objCustomer = new Quantum_QFOR.Cls_CustomerReconciliation();
            objWK.OpenConnection();
            var TCustomerPK = HttpContext.Current.Session["TCustPK"];
            DataSet CustDS = CreateCustDS();
            //SqlStr = ""
            //SqlStr = "update temp_customer_tbl set reconcile_status=2,reconciled_by='" & HttpContext.Current.Session("LOGED_IN_LOC_FK") & "',PERMANENT_CUST_MST_FK='" & Cust_Id & "' where customer_mst_pk='" & TCustomerPK & "'"
            SqlStr = "";
            SqlStr = "select business_type from temp_customer_tbl where customer_mst_pk='" + TCustomerPK + "'";

            SqlStr1 = "";
            SqlStr1 = "select transaction_type from temp_customer_tbl where customer_mst_pk='" + TCustomerPK + "'";

            SqlName = "";
            SqlName = "select CUSTOMER_NAME from temp_customer_tbl where customer_mst_pk='" + TCustomerPK + "'";

            try
            {
                BizType = Convert.ToInt32(objWK.ExecuteScaler(SqlStr));
                TrnType = Convert.ToInt32(objWK.ExecuteScaler(SqlStr1));
                CustName = objWK.ExecuteScaler(SqlName);

                dRow = CustDS.Tables[0].NewRow();

                var _with10 = dRow;
                _with10["T_CUST_PK"] = Convert.ToInt64(TCustomerPK);
                _with10["T_CUST_ID"] = CustName;
                _with10["T_CUST_TRANS_TYPE"] = Convert.ToInt64(TrnType);
                _with10["T_BIZ_TYPE"] = Convert.ToInt64(BizType);
                _with10["P_CUST_PK"] = Convert.ToInt64(Cust_PK);
                _with10["MapOrNew"] = 2;
                CustDS.Tables[0].Rows.Add(dRow);
                //arrTemp = objCustomer.ReconcileCustomer(CustDS);
                HttpContext.Current.Session.Remove("CRecon");
                HttpContext.Current.Session.Remove("TCustPK");
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

        #endregion "SAVE"

        #region "Reconcile DS"

        /// <summary>
        /// Creates the customer ds.
        /// </summary>
        /// <returns></returns>
        private DataSet CreateCustDS()
        {
            DataTable dTab = new DataTable();
            var _with11 = dTab.Columns;
            _with11.Add(new DataColumn("T_CUST_PK"));
            _with11.Add(new DataColumn("T_CUST_ID"));
            //Snigdharani - 06/03/2009 - EBooking Integration
            _with11.Add(new DataColumn("T_CUST_TRANS_TYPE"));
            _with11.Add(new DataColumn("T_BIZ_TYPE"));
            _with11.Add(new DataColumn("P_CUST_PK"));
            _with11.Add(new DataColumn("MapOrNew"));
            DataSet ds = new DataSet();
            ds.Tables.Add(dTab);
            return ds;
        }

        #endregion "Reconcile DS"

        #region "Update"

        /// <summary>
        /// Updates the data.
        /// </summary>
        /// <param name="dsChild">The ds child.</param>
        /// <param name="CUSTOMERPK">The customerpk.</param>
        /// <param name="CUSTOMERID">The customerid.</param>
        /// <param name="CUSTOMERName">Name of the customer.</param>
        /// <param name="activeflag">The activeflag.</param>
        /// <param name="creditcustomer">The creditcustomer.</param>
        /// <param name="VATCODE">The vatcode.</param>
        /// <param name="BType">Type of the b.</param>
        /// <param name="customerType">Type of the customer.</param>
        /// <param name="ACCOUNTNO">The accountno.</param>
        /// <param name="secChk">The sec CHK.</param>
        /// <param name="creditD">The credit d.</param>
        /// <param name="creditLmt">The credit LMT.</param>
        /// <param name="admAddress1">The adm address1.</param>
        /// <param name="admAddress2">The adm address2.</param>
        /// <param name="admaddress3">The admaddress3.</param>
        /// <param name="admcity">The admcity.</param>
        /// <param name="admzipcode">The admzipcode.</param>
        /// <param name="admLocation">The adm location.</param>
        /// <param name="admShort">The adm short.</param>
        /// <param name="admContactPerson">The adm contact person.</param>
        /// <param name="admPhone1">The adm phone1.</param>
        /// <param name="admPhone2">The adm phone2.</param>
        /// <param name="admFax">The adm fax.</param>
        /// <param name="admEmail">The adm email.</param>
        /// <param name="admUrl">The adm URL.</param>
        /// <param name="AdmCountry">The adm country.</param>
        /// <param name="corAddress1">The cor address1.</param>
        /// <param name="corAddress2">The cor address2.</param>
        /// <param name="coraddress3">The coraddress3.</param>
        /// <param name="corcity">The corcity.</param>
        /// <param name="corzipcode">The corzipcode.</param>
        /// <param name="corLocation">The cor location.</param>
        /// <param name="corShort">The cor short.</param>
        /// <param name="corContactPerson">The cor contact person.</param>
        /// <param name="corPhone1">The cor phone1.</param>
        /// <param name="corPhone2">The cor phone2.</param>
        /// <param name="corFax">The cor fax.</param>
        /// <param name="corEmail">The cor email.</param>
        /// <param name="corUrl">The cor URL.</param>
        /// <param name="CorrCountry">The corr country.</param>
        /// <param name="billAddress1">The bill address1.</param>
        /// <param name="billAddress2">The bill address2.</param>
        /// <param name="billaddress3">The billaddress3.</param>
        /// <param name="billcity">The billcity.</param>
        /// <param name="billzipcode">The billzipcode.</param>
        /// <param name="billLocation">The bill location.</param>
        /// <param name="billShort">The bill short.</param>
        /// <param name="billContactPerson">The bill contact person.</param>
        /// <param name="billPhone1">The bill phone1.</param>
        /// <param name="billPhone2">The bill phone2.</param>
        /// <param name="billFax">The bill fax.</param>
        /// <param name="billEmail">The bill email.</param>
        /// <param name="billUrl">The bill URL.</param>
        /// <param name="BillCountry">The bill country.</param>
        /// <param name="versionNo">The version no.</param>
        /// <param name="AdmSalutation">The adm salutation.</param>
        /// <param name="CorSalutation">The cor salutation.</param>
        /// <param name="BillSalutation">The bill salutation.</param>
        /// <param name="repEmp">The rep emp.</param>
        /// <param name="agentfk">The agentfk.</param>
        /// <param name="defermentNo">The deferment no.</param>
        /// <param name="turnNo">The turn no.</param>
        /// <param name="ColAdd">The col add.</param>
        /// <param name="delAdd">The delete add.</param>
        /// <param name="remarks">The remarks.</param>
        /// <param name="CustRegNr">The customer reg nr.</param>
        /// <param name="Status">The status.</param>
        /// <param name="Curr">The curr.</param>
        /// <param name="Customer_Type">Type of the customer_.</param>
        /// <param name="Ref_Grp_Cust_pk">The ref_ GRP_ cust_pk.</param>
        /// <param name="chkcust">The chkcust.</param>
        /// <param name="Act_Number">The act_ number.</param>
        /// <param name="swift_code">The swift_code.</param>
        /// <param name="Bank_Number">The bank_ number.</param>
        /// <param name="Iban">The iban.</param>
        /// <param name="Branch">The branch.</param>
        /// <param name="ebank_code">The ebank_code.</param>
        /// <param name="Location">The location.</param>
        /// <param name="bankcountry">The bankcountry.</param>
        /// <param name="Bank_Name">Name of the bank_.</param>
        /// <param name="Address">The address.</param>
        /// <param name="CustCategory">The customer category.</param>
        /// <param name="txtCustCategory">The text customer category.</param>
        /// <param name="IntDivPK">The int div pk.</param>
        /// <param name="BranchPK">The branch pk.</param>
        /// <param name="TaxStatus">The tax status.</param>
        /// <param name="rated">The rated.</param>
        /// <param name="VATRegistered">The vat registered.</param>
        /// <param name="GroupCategory">The group category.</param>
        /// <param name="Priority">The priority.</param>
        /// <param name="Criteria">The criteria.</param>
        /// <param name="Category">The category.</param>
        /// <param name="MapStatus">The map status.</param>
        /// <param name="EcommDS">The ecomm ds.</param>
        /// <param name="PercentStatus">The percent status.</param>
        /// <param name="PercentValue">The percent value.</param>
        /// <returns></returns>
        public ArrayList UpdateData(DataSet dsChild, int CUSTOMERPK = 0, string CUSTOMERID = "", string CUSTOMERName = "", string activeflag = "", Int16 creditcustomer = 0, string VATCODE = "", int BType = 0, int customerType = 0, string ACCOUNTNO = "",
        string secChk = "", string creditD = "", string creditLmt = "", string admAddress1 = "", string admAddress2 = "", string admaddress3 = "", string admcity = "", string admzipcode = "", Int16 admLocation = 0, string admShort = "",
        string admContactPerson = "", string admPhone1 = "", string admPhone2 = "", string admFax = "", string admEmail = "", string admUrl = "", Int16 AdmCountry = 0, string corAddress1 = "", string corAddress2 = "", string coraddress3 = "",
        string corcity = "", string corzipcode = "", Int16 corLocation = 0, string corShort = "", string corContactPerson = "", string corPhone1 = "", string corPhone2 = "", string corFax = "", string corEmail = "", string corUrl = "",
        Int16 CorrCountry = 0, string billAddress1 = "", string billAddress2 = "", string billaddress3 = "", string billcity = "", string billzipcode = "", Int16 billLocation = 0, string billShort = "", string billContactPerson = "", string billPhone1 = "",
        string billPhone2 = "", string billFax = "", string billEmail = "", string billUrl = "", Int16 BillCountry = 0, string versionNo = "", Int16 AdmSalutation = 0, Int16 CorSalutation = 0, Int16 BillSalutation = 0, Int16 repEmp = 0,
        string agentfk = "", string defermentNo = "", string turnNo = "", string ColAdd = "", string delAdd = "", string remarks = "", string CustRegNr = "", Int16 Status = 0, Int16 Curr = 0, long Customer_Type = 0,
        long Ref_Grp_Cust_pk = 0, Int32 chkcust = 0, string Act_Number = "", string swift_code = "", string Bank_Number = "", string Iban = "", string Branch = "", string ebank_code = "", string Location = "", string bankcountry = "",
        string Bank_Name = "", string Address = "", int CustCategory = 1, string txtCustCategory = "", string IntDivPK = "", string BranchPK = "", int TaxStatus = 1, string rated = "", string VATRegistered = "", int GroupCategory = 0,
        int Priority = 0, int Criteria = 0, int Category = 0, Int16 MapStatus = 0, object EcommDS = null, int PercentStatus = 1, string PercentValue = "")
        {
            // 'Manoharan 21Feb07: GAP-USS-QFOR-027
            //in the above function Customer_Type ,  Ref_Grp_Cust_pk parameters added by surya prasad on 03-Jan-2009
            int intPKVal = 0;
            long lngI = 0;
            DateTime EnteredBudgetStartDate = default(DateTime);
            Int32 RecAfct = default(Int32);
            System.DBNull StrNull = null;
            long lngBudHdrPK = 0;
            OracleCommand insChildCommand = new OracleCommand();
            System.DateTime EnteredDate = default(System.DateTime);
            Int32 inti = default(Int32);
            int Contact = 0;
            long lngDepotPK = 0;
            OracleCommand updCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int configKey = 0;
            M_Configuration_PK = 24;

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                //DtTbl = M_Dataset.Tables(0)
                var _with12 = updCommand;
                _with12.Connection = objWK.MyConnection;
                _with12.CommandType = CommandType.StoredProcedure;
                _with12.CommandText = objWK.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_MST_TBL_UPD";

                var _with13 = _with12.Parameters;
                //CUSTOMER_MST_PK_IN
                _with13.Add("CUSTOMER_MST_PK_IN", CUSTOMERPK);
                _with13.Add("CUSTOMER_ID_IN", CUSTOMERID);
                _with13.Add("CUSTOMER_NAME_IN", CUSTOMERName);
                _with13.Add("ACTIVE_FLAG_IN", activeflag);
                _with13.Add("CREDIT_CUSTOMER_IN", creditcustomer);
                _with13.Add("CREDIT_LIMIT_IN", (string.IsNullOrEmpty(creditLmt) ? 0 : Convert.ToDouble(creditLmt.ToString())));
                _with13.Add("CREDIT_DAYS_IN", (string.IsNullOrEmpty(creditD) ? "" : creditD));
                _with13.Add("SECURITY_CHK_REQD_IN", secChk);
                _with13.Add("ACCOUNT_NO_IN", (string.IsNullOrEmpty(ACCOUNTNO) ? "" : ACCOUNTNO));
                _with13.Add("BUSINESS_TYPE_IN", BType);
                _with13.Add("CUSTOMER_TYPE_FK_IN", customerType);
                _with13.Add("VAT_NO_IN", getDefault(VATCODE, ""));
                _with13.Add("REP_EMP_MST_FK_IN", (repEmp == 0 ? 0 : repEmp));
                //Priya - 01-Mar-06
                _with13.Add("ADM_SALUTATION_IN", AdmSalutation);
                //Priya - 28/02/2006
                _with13.Add("COR_SALUTATION_IN", CorSalutation);
                //Priya - 28/02/2006
                _with13.Add("BILL_SALUTATION_IN", BillSalutation);
                //Priya - 28/02/2006
                _with13.Add("CONFIG_PK_IN", M_Configuration_PK);
                _with13.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK);
                _with13.Add("ADM_ADDRESS_1_IN", admAddress1);
                _with13.Add("ADM_ADDRESS_2_IN", (string.IsNullOrEmpty(admAddress2) ? "" : admAddress2));
                _with13.Add("ADM_ADDRESS_3_IN", (string.IsNullOrEmpty(admaddress3) ? "" : admaddress3));
                _with13.Add("ADM_CITY_IN", (string.IsNullOrEmpty(admcity) ? "" : admcity));
                _with13.Add("ADM_ZIP_CODE_IN", (string.IsNullOrEmpty(admzipcode) ? "" : admzipcode));
                _with13.Add("ADM_LOCATION_MST_FK_IN", (admLocation == 0 ? 0 : admLocation));
                _with13.Add("ADM_CONTACT_PERSON_IN", (string.IsNullOrEmpty(admContactPerson) ? "" : admContactPerson));
                _with13.Add("ADM_PHONE_NO_1_IN", (string.IsNullOrEmpty(admPhone1) ? "" : admPhone1));
                _with13.Add("ADM_PHONE_NO_2_IN", (string.IsNullOrEmpty(admPhone2) ? "" : admPhone2));
                _with13.Add("ADM_FAX_NO_IN", (string.IsNullOrEmpty(admFax) ? "" : admFax));
                _with13.Add("ADM_EMAIL_ID_IN", (string.IsNullOrEmpty(admEmail) ? "" : admEmail));
                _with13.Add("ADM_URL_IN", (string.IsNullOrEmpty(admUrl) ? "" : admUrl));
                _with13.Add("ADM_SHORT_NAME_IN", (string.IsNullOrEmpty(admShort) ? "" : admShort));
                _with13.Add("ADM_COUNTRY_MST_FK_IN", (AdmCountry == 0 ? 0 : AdmCountry));

                _with13.Add("COR_ADDRESS_1_IN", (string.IsNullOrEmpty(corAddress1) ? "" : corAddress1));
                _with13.Add("COR_ADDRESS_2_IN", (string.IsNullOrEmpty(corAddress2) ? "" : corAddress2));
                _with13.Add("COR_ADDRESS_3_IN", (string.IsNullOrEmpty(coraddress3) ? "" : coraddress3));
                _with13.Add("COR_CITY_IN", (string.IsNullOrEmpty(corcity) ? "" : corcity));
                _with13.Add("COR_ZIP_CODE_IN", (string.IsNullOrEmpty(corzipcode) ? "" : corzipcode));
                _with13.Add("COR_LOCATION_MST_FK_IN", (corLocation == 0 ? 0 : corLocation));
                _with13.Add("COR_CONTACT_PERSON_IN", (string.IsNullOrEmpty(corContactPerson) ? "" : corContactPerson));
                _with13.Add("COR_PHONE_NO_1_IN", (string.IsNullOrEmpty(corPhone1) ? "" : corPhone1));
                _with13.Add("COR_PHONE_NO_2_IN", (string.IsNullOrEmpty(corPhone2) ? "" : corPhone2));
                _with13.Add("COR_FAX_NO_IN", (string.IsNullOrEmpty(corFax) ? "" : corFax));
                _with13.Add("COR_EMAIL_ID_IN", (string.IsNullOrEmpty(corEmail) ? "" : corEmail));
                _with13.Add("COR_URL_IN", (string.IsNullOrEmpty(corUrl) ? "" : corUrl));
                _with13.Add("COR_SHORT_NAME_IN", (string.IsNullOrEmpty(corShort) ? "" : corShort));
                _with13.Add("COR_COUNTRY_MST_FK_IN", (CorrCountry == 0 ? 0 : CorrCountry));

                _with13.Add("BILL_ADDRESS_1_IN", (string.IsNullOrEmpty(billAddress1) ? "" : billAddress1));
                _with13.Add("BILL_ADDRESS_2_IN", (string.IsNullOrEmpty(billAddress2) ? "" : billAddress2));
                _with13.Add("BILL_ADDRESS_3_IN", (string.IsNullOrEmpty(billaddress3) ? "" : billaddress3));
                _with13.Add("BILL_CITY_IN", (string.IsNullOrEmpty(billcity) ? "" : billcity));
                _with13.Add("BILL_ZIP_CODE_IN", (string.IsNullOrEmpty(billzipcode) ? "" : billzipcode));
                _with13.Add("BILL_LOCATION_MST_FK_IN", (billLocation == 0 ? 0 : billLocation));
                _with13.Add("BILL_CONTACT_PERSON_IN", (string.IsNullOrEmpty(billContactPerson) ? "" : billContactPerson));
                _with13.Add("BILL_PHONE_NO_1_IN", (string.IsNullOrEmpty(billPhone1) ? "" : billPhone1));
                _with13.Add("BILL_PHONE_NO_2_IN", (string.IsNullOrEmpty(billPhone2) ? "" : billPhone2));
                _with13.Add("BILL_FAX_NO_IN", (string.IsNullOrEmpty(billFax) ? "" : billFax));
                _with13.Add("BILL_EMAIL_ID_IN", (string.IsNullOrEmpty(billEmail) ? "" : billEmail));
                _with13.Add("BILL_URL_IN", (string.IsNullOrEmpty(billUrl) ? "" : billUrl));
                _with13.Add("BILL_SHORT_NAME_IN", (string.IsNullOrEmpty(billShort) ? "" : billShort));
                _with13.Add("BILL_COUNTRY_MST_FK_IN", (BillCountry == 0 ? 0 : BillCountry));
                //==========
                _with13.Add("DEFERMENT_NO_IN", getDefault(defermentNo, ""));
                _with13.Add("TURN_NO_IN", getDefault(turnNo, ""));
                _with13.Add("COL_ADDRESS_IN", getDefault(ColAdd, ""));
                _with13.Add("DEL_ADDRESS_IN", getDefault(delAdd, ""));
                _with13.Add("REMARKS_IN", getDefault(remarks, ""));
                _with13.Add("DP_AGENT_MST_FK_IN", getDefault(agentfk, ""));
                _with13.Add("CUST_REG_NO_IN", getDefault(CustRegNr, ""));
                //Venkata 27/12/06
                _with13.Add("STATUS_IN", Status);
                //Manoharan 21Feb07: GAP-USS-QFOR-027
                _with13.Add("CURRENCY_MST_FK_IN", Curr);
                //=========
                _with13.Add("LAST_MODIFIEDD_BY_FK_IN", M_LAST_MODIFIED_BY_FK);

                _with13.Add("VERSION_NO_IN", versionNo).Direction = ParameterDirection.Input;
                //==========Added by SuryaPrasad =============
                _with13.Add("CUSTOMER_TYPE_IN", Customer_Type);
                _with13.Add("REF_GROUP_CUST_PK_IN", (Ref_Grp_Cust_pk == 0 ? 0 : Ref_Grp_Cust_pk));

                //======Group Category====
                _with13.Add("GROUP_CATEGORY_IN", GroupCategory);

                //adding by thiyagarajan on 12/5/09
                _with13.Add("CHKCUST_IN", chkcust).Direction = ParameterDirection.Input;

                //============================================
                _with13.Add("Act_Number_IN", (string.IsNullOrEmpty(Act_Number) ? "" : Act_Number));
                _with13.Add("swift_code_IN", (string.IsNullOrEmpty(swift_code) ? "" : swift_code));
                _with13.Add("Bank_Number_IN", (string.IsNullOrEmpty(Bank_Number) ? "" : Bank_Number));
                _with13.Add("Iban_IN", (string.IsNullOrEmpty(Iban) ? "" : Iban));
                _with13.Add("Branch_IN", (string.IsNullOrEmpty(Branch) ? "" : Branch));
                _with13.Add("ebank_code_IN", (string.IsNullOrEmpty(ebank_code) ? "" : ebank_code));
                _with13.Add("Location_IN", (string.IsNullOrEmpty(Location) ? "" : Location));
                _with13.Add("bankcountry_IN", (string.IsNullOrEmpty(bankcountry) ? "" : bankcountry));
                _with13.Add("Bank_Name_IN", (string.IsNullOrEmpty(Bank_Name) ? "" : Bank_Name));
                _with13.Add("Address_IN", (string.IsNullOrEmpty(Address) ? "" : Address));
                //========
                _with13.Add("CUST_CATEGORY_IN", CustCategory);
                _with13.Add("CATEGORY_IN", (string.IsNullOrEmpty(txtCustCategory) ? "" : txtCustCategory));
                _with13.Add("INT_DIV_IN", getDefault(IntDivPK, ""));
                _with13.Add("BRANCHPK_IN", getDefault(BranchPK, ""));
                _with13.Add("TAX_STATUS_IN", TaxStatus);
                _with13.Add("RATED_IN", (string.IsNullOrEmpty(rated) ? "" : rated));
                _with13.Add("VAT_REGISTERED_IN", (string.IsNullOrEmpty(VATRegistered) ? "" : VATRegistered));
                //============================================
                _with13.Add("PRIORITY_IN", Priority).Direction = ParameterDirection.Input;
                _with13.Add("CRITERIA_IN", Criteria).Direction = ParameterDirection.Input;
                _with13.Add("CATEGORY_TYPE_IN", Category).Direction = ParameterDirection.Input;
                //==========End===============================
                _with13.Add("MAP_STATUS_IN", MapStatus).Direction = ParameterDirection.Input;
                _with13.Add("PERCENTAGE_STATUS_IN", PercentStatus).Direction = ParameterDirection.Input;
                _with13.Add("PERCENTAGE_VALUE_IN", PercentValue).Direction = ParameterDirection.Input;
                _with13.Add("RETURN_VALUE", admLocation).Direction = ParameterDirection.Output;

                var _with14 = objWK.MyDataAdapter;
                _with14.UpdateCommand = updCommand;
                _with14.UpdateCommand.Transaction = TRAN;
                _with14.UpdateCommand.ExecuteNonQuery();

                DtTbl = dsChild.Tables[0];
                var _with15 = insChildCommand;
                _with15.Connection = objWK.MyConnection;
                _with15.CommandType = CommandType.StoredProcedure;
                _with15.CommandText = objWK.MyUserName + ".CUSTOMER_MST_TBL_PKG.CUSTOMER_CATE_TRN_UPD";
                _with15.Transaction = TRAN;
                var _with16 = _with15.Parameters;

                foreach (DataRow DtRw_loopVariable in dsChild.Tables[0].Rows)
                {
                    DtRw = DtRw_loopVariable;
                    _with16.Clear();
                    _with16.Add("CUSTOMER_MST_FK_IN", CUSTOMERPK);
                    _with16.Add("CUSTOMER_CATEGORY_MST_FK_IN", DtRw["CustomerCtPk"]).Direction = ParameterDirection.Input;
                    // insChildCommand.Parameters["CUSTOMER_CATEGORY_MST_FK_IN"].SourceVersion = DataRowVersion.Current
                    _with16.Add("SELECT_IN", DtRw["SELECT"]).Direction = ParameterDirection.Input;
                    //insChildCommand.Parameters["SELECT_IN"].SourceVersion = DataRowVersion.Current
                    _with16.Add("LAST_MODIFIED_BY_FK_IN", M_CREATED_BY_FK);
                    _with16.Add("CONFIG_PK_IN", M_Configuration_PK);
                    _with16.Add("RETURN_VALUE", CUSTOMERPK).Direction = ParameterDirection.Output;
                    insChildCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    insChildCommand.ExecuteNonQuery();
                }
                //With objWK.MyDataAdapter
                //    .InsertCommand = insChildCommand
                //    .InsertCommand.Transaction = TRAN
                //Contact = Save_Contact(objWK, TRAN, CUSTOMERPK);
                if ((EcommDS != null))
                {
                    UpdEcommUser(objWK, TRAN, (DataSet)EcommDS);
                }
                TRAN.Commit();
                //Push to financial system if realtime is selected
                Quantum_QFOR.cls_Scheduler objSch = new Quantum_QFOR.cls_Scheduler();
                ArrayList schDtls = null;
                bool errGen = false;
                if (objSch.GetSchedulerPushType() == true)
                {
                    //QFSIService.serFinApp objPush = new QFSIService.serFinApp();
                    //try
                    //{
                    //    schDtls = objSch.FetchSchDtls();
                    //    //'Used to Fetch the Sch Dtls
                    //    objPush.UpdateCustomerMaster(schDtls[10], schDtls[2], schDtls[6], schDtls[4], errGen, Convert.ToString(CUSTOMERPK));
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

                //End With

                return arrMessage;
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                //Throw OraExp
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();
                return arrMessage;
                //'added by minakshi on 10-feb-08 for connection close task
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Update"

        /// <summary>
        /// Upds the ecomm user.
        /// </summary>
        /// <param name="objWK">The object wk.</param>
        /// <param name="TRAN">The tran.</param>
        /// <param name="EcommDS">The ecomm ds.</param>
        /// <returns></returns>
        public ArrayList UpdEcommUser(WorkFlow objWK, OracleTransaction TRAN, DataSet EcommDS)
        {
            objWK.MyConnection = TRAN.Connection;
            try
            {
                if (EcommDS.Tables[0].Rows.Count > 0)
                {
                    for (int i = 0; i <= EcommDS.Tables[0].Rows.Count - 1; i++)
                    {
                        var _with20 = objWK.MyCommand;
                        _with20.Transaction = TRAN;
                        _with20.CommandType = CommandType.StoredProcedure;
                        _with20.CommandText = objWK.MyUserName + ".FETCH_ECOMM_GATE_PKG.UPDUSERSTATUS";
                        _with20.Parameters.Clear();
                        _with20.Parameters.Add("CUST_REG_FK_IN", EcommDS.Tables[0].Rows[i]["REGN_NR_PK"]).Direction = ParameterDirection.Input;
                        _with20.Parameters.Add("USER_ID_IN", EcommDS.Tables[0].Rows[i]["USER_ID"]).Direction = ParameterDirection.Input;
                        _with20.Parameters.Add("IS_ACTIVE_IN", EcommDS.Tables[0].Rows[i]["STATUS"]).Direction = ParameterDirection.Input;
                        _with20.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10).Direction = ParameterDirection.Output;
                        _with20.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
            }
            return new ArrayList();
        }

        #region "Fill the Temp Customer Info"

        /// <summary>
        /// Fills the form with temporary customer information.
        /// </summary>
        /// <param name="T_Cust_Pk">The t_ cust_ pk.</param>
        /// <returns></returns>
        public DataSet FillTheFormWithTempCustInfo(string T_Cust_Pk = "0")
        {
            string strSQL = "";
            //Snigdharani - 04/03/2009 - E-Booking Integration
            if (Convert.ToInt32(HttpContext.Current.Session["EBOOKING_AVAILABLE"]) == 1)
            {
                strSQL = "SELECT TCT.CUSTOMER_MST_PK, ";
                strSQL += " TCT.CUSTOMER_ID, ";
                strSQL += " TCT.CUSTOMER_NAME, ";
                strSQL += " TCT.ACTIVE_FLAG, ";
                strSQL += " TCT.CREDIT_LIMIT, ";
                strSQL += " TCT.CREDIT_DAYS, ";
                strSQL += " TCT.SECURITY_CHK_REQD, ";
                strSQL += " TCT.ACCOUNT_NO, ";
                strSQL += " TCT.BUSINESS_TYPE, ";
                strSQL += " TCT.CUSTOMER_TYPE_FK, ";
                strSQL += " TCT.VAT_NO, ";
                strSQL += " TCCD.ADM_ADDRESS_1, ";
                strSQL += " TCCD.ADM_ADDRESS_2, ";
                strSQL += " TCCD.ADM_ADDRESS_3, ";
                strSQL += " TCCD.ADM_ZIP_CODE, ";
                strSQL += " TCCD.ADM_CONTACT_PERSON, ";
                strSQL += " TCCD.ADM_PHONE_NO_1, ";
                strSQL += " TCCD.ADM_PHONE_NO_2, ";
                strSQL += " TCCD.ADM_FAX_NO, ";
                strSQL += " TCCD.ADM_EMAIL_ID, ";
                strSQL += " TCCD.ADM_SALUTATION, ";
                strSQL += " TCCD.ADM_SHORT_NAME, ";
                strSQL += " TCCD.ADM_LOCATION_MST_FK, ";
                strSQL += " ADMLOC.LOCATION_ID ADM_LOC_ID, ";
                strSQL += " ADMLOC.LOCATION_NAME ADM_LOC_NAME, ";
                strSQL += " TCCD.ADM_CITY, ";
                strSQL += " TCCD.COR_ADDRESS_1, ";
                strSQL += " TCCD.COR_ADDRESS_2, ";
                strSQL += " TCCD.COR_ADDRESS_3, ";
                strSQL += " TCCD.COR_ZIP_CODE, ";
                strSQL += " TCCD.COR_CONTACT_PERSON, ";
                strSQL += " TCCD.COR_PHONE_NO_1, ";
                strSQL += " TCCD.COR_FAX_NO, ";
                strSQL += " TCCD.COR_EMAIL_ID, ";
                strSQL += " TCCD.COR_SHORT_NAME, ";
                strSQL += " TCCD.COR_LOCATION_MST_FK, ";
                strSQL += " CORLOC.LOCATION_ID COR_LOC_ID, ";
                strSQL += " CORLOC.LOCATION_NAME COR_LOC_NAME, ";
                strSQL += " TCCD.COR_CITY,  TCCD.ADM_COUNTRY_MST_FK ADM_COUNTRY_MST_FK,  COUNTRY.COUNTRY_ID COUNTRYAID, COUNTRY.COUNTRY_NAME COUNTRYANAME ";
                strSQL += " FROM TEMP_CUSTOMER_TBL TCT, TEMP_CUSTOMER_CONTACT_DTLS TCCD, ";
                strSQL += " LOCATION_MST_TBL ADMLOC, LOCATION_MST_TBL CORLOC , COUNTRY_MST_TBL COUNTRY  ";
                strSQL += " WHERE TCT.CUSTOMER_MST_PK = TCCD.CUSTOMER_MST_FK ";
                strSQL += " AND TCCD.ADM_LOCATION_MST_FK = ADMLOC.LOCATION_MST_PK(+) ";
                strSQL += " AND TCCD.COR_LOCATION_MST_FK = CORLOC.LOCATION_MST_PK(+) ";
                strSQL += " AND TCCD.ADM_COUNTRY_MST_FK = COUNTRY.COUNTRY_MST_PK(+) ";
                strSQL += " AND  TCT.CUSTOMER_MST_PK = " + T_Cust_Pk;
            }
            else
            {
                strSQL = "SELECT TCT.CUSTOMER_ID CUST_ID, ";
                strSQL += " TCT.CUSTOMER_NAME CUST_NAME, ";
                strSQL += " TCCD.ADM_ADDRESS_1 ADDRESS, ";
                strSQL += " TCCD.ADM_PHONE_NO_1 PHONE, ";
                strSQL += " TCCD.ADM_FAX_NO FAX, ";
                strSQL += " TCCD.ADM_SALUTATION SALUTATION, ";
                strSQL += " TCCD.ADM_CONTACT_PERSON CONTACT_PERSON, ";
                strSQL += " TCCD.ADM_EMAIL_ID EMAIL ";
                strSQL += " FROM TEMP_CUSTOMER_TBL TCT, ";
                strSQL += "      TEMP_CUSTOMER_CONTACT_DTLS TCCD ";
                strSQL += " WHERE TCT.CUSTOMER_MST_PK = TCCD.CUSTOMER_MST_FK ";
                strSQL += " AND     TCT.CUSTOMER_MST_PK = " + T_Cust_Pk;
            }
            try
            {
                return (new WorkFlow()).GetDataSet(strSQL);
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

        #endregion "Fill the Temp Customer Info"

        /// <summary>
        /// Sends the mail.
        /// </summary>
        /// <param name="MailId">The mail identifier.</param>
        /// <param name="CUSTOMERID">The customerid.</param>
        /// <param name="pwd">The password.</param>
        /// <param name="custname">The custname.</param>
        /// <returns></returns>
        public object SendMail(string MailId, string CUSTOMERID, string pwd, string custname)
        {
            object functionReturnValue = null;
            System.Web.Mail.MailMessage objMail = new System.Web.Mail.MailMessage();
            //Dim Mailsend As String = ConfigurationSettings.AppSettings("MailServer")
            string EAttach = null;
            string dsMail = null;
            Int32 intCnt = default(Int32);
            string strbody = null;
            try
            {
                //MakeHTMLFormat(strbody, CUSTOMERID, pwd)
                //****************************** External*********************************
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserver"] = "smtpout.secureserver.net";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpserverport"] = 25;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusing"] = 2;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/smtpauthenticate"] = 1;
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendusername"] = "support_temp@quantum-bso.com";
                objMail.Fields["http://schemas.microsoft.com/cdo/configuration/sendpassword"] = "test123";
                objMail.BodyFormat = System.Web.Mail.MailFormat.Text;
                //or MailFormat.Text
                objMail.To = MailId;
                objMail.From = "support_temp@quantum-bso.com";
                objMail.Subject = "Customer Registration";
                //objMail.Body = strbody.ToString()
                strbody = " Dear  " + custname;
                strbody += "   ";
                strbody += " Thanks for the E-Booking Customer Registration.";
                strbody += " Please find the USER ID and Password for your reference.";
                strbody += " USER ID  : " + CUSTOMERID;
                strbody += " Password : " + pwd;
                strbody += " Thanks ";
                strbody += " " + HttpContext.Current.Session["USER_ID"];
                strbody += " Customer Service Desk.";
                strbody += " This is an auto generated e-Mail. Please do not reply to this e-Mail-ID.";
                objMail.Body = strbody;
                //objMail.Body = "Dear " & CUSTOMERID & ","
                //objMail.Body &= "Your ID :" & CUSTOMERID & " Password : " & pwd
                //objMail.Body &= "Your USER ID and Password sent to Administrative Email ID "
                System.Web.Mail.SmtpMail.SmtpServer = "smtpout.secureserver.net";
                System.Web.Mail.SmtpMail.Send(objMail);
                objMail = null;
                return "All Data Saved Successfully.";
                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return "All Data Saved Successfully.Due to some problem Mail has not been sent";
                return functionReturnValue;
            }
            return functionReturnValue;
        }

        /// <summary>
        /// Makes the HTML format.
        /// </summary>
        /// <param name="strhtml">The STRHTML.</param>
        /// <param name="custid">The custid.</param>
        /// <param name="pwd">The password.</param>
        private void MakeHTMLFormat(System.Text.StringBuilder strhtml, string custid, string pwd)
        {
            strhtml.Append("<html><body>");
            strhtml.Append("<p><b>Dear Customer<br>");
            strhtml.Append("Your UserID : " + custid + " <br><br><br><br>Your Password : " + pwd + " <br><br><br><br> Your USER ID and Password sent to Administrative Email ID<br><br></b></p>");
            strhtml.Append("</body></html>");
        }

        #region "Customer Add details popup"

        /// <summary>
        /// Gets the competitiordetails.
        /// </summary>
        /// <param name="customerpk">The customerpk.</param>
        /// <returns></returns>
        public DataSet getCompetitiordetails(int customerpk = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("select ROWNUM SLNO,q.* from (");
            sb.Append("select cmp.cust_poten_mst_fk,");
            //'sb.Append(",cmp.farwarder_pk,")
            sb.Append("cmp.farwarder_id ,");
            sb.Append("cmp.farwarder_name,");
            sb.Append("cmp.cmp_teu,");
            sb.Append("  DECODE(cmp.cmp_period, 0, 'Monthly', 1, 'Quaterly',2,'Half Yearly',3,'Yearly')cmp_period,");
            sb.Append("cmp.cmp_cbm,");
            sb.Append("cmp.cmp_mts,");
            sb.Append("  DECODE(cmp.cmp_servicable, 0, 'Y', 1, 'N')cmp_servicable,");
            sb.Append("cmp.operator_mst_fk carrier,");
            sb.Append("supp.operator_name carriername ,");
            //sb.Append("cmp.airline_mst_fk carrier,")
            //sb.Append("air.airline_name carrier ,")
            sb.Append("cmp.cmp_tos,");
            sb.Append("cmp.cmp_detention_free,");
            sb.Append("cmp.cmp_cutoff_hrs,");
            sb.Append("cmp.cmp_ref_plug_free,");
            sb.Append("  DECODE(cmp.cmp_seasonal, 0, 'Y', 1, 'N')cmp_seasonal,");
            sb.Append(" To_CHAR(cmp.from_date,'DD/MM/YYYY')from_date,");
            sb.Append(" To_CHAR(cmp.to_date,'DD/MM/YYYY')to_date,");
            //sb.Append("cmp.to_date,")
            sb.Append("cmp.cmp_rate,");
            sb.Append("cmp.cmp_cr_days,");
            sb.Append("cmp.cmp_cr_limit,");
            sb.Append("cmp.cmp_oth_inf,");
            sb.Append("cmp.competitors_pk,");
            sb.Append(" '' CHEFLAG,");
            sb.Append(" '' DELFLAF");
            sb.Append("     from competitors_tbl cmp,operator_mst_tbl supp,airline_mst_tbl air,customer_potential_tbl cust");
            sb.Append("     where supp.operator_mst_pk(+)=cmp.operator_mst_fk");
            sb.Append("     and cust.customer_potential_mst_pk=cmp.cust_poten_mst_fk");
            sb.Append("      and  air.airline_mst_pk(+)=cmp.airline_mst_fk");
            sb.Append("      and  cmp.cust_poten_mst_fk=" + customerpk);
            sb.Append(" ) q ");
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

        /// <summary>
        /// Getcustomeraddetailses the specified custpk.
        /// </summary>
        /// <param name="Custpk">The custpk.</param>
        /// <param name="Polpk">The polpk.</param>
        /// <param name="Podpk">The podpk.</param>
        /// <param name="Commpk">The commpk.</param>
        /// <param name="potentialpk">The potentialpk.</param>
        /// <param name="flag">The flag.</param>
        /// <returns></returns>
        public object getcustomeraddetails(int Custpk = 0, int Polpk = 0, int Podpk = 0, int Commpk = 0, int potentialpk = 0, int flag = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("select cust.customer_potential_mst_pk,");
            sb.Append("       cust.customer_mst_fk,");
            sb.Append("       cust.cust_teu,");
            sb.Append("       cust.cust_period,");
            sb.Append("       cust.cust_cbm,");
            sb.Append("       cust.cust_mts,");
            sb.Append("       cust.cust_servicable,");
            sb.Append("       cust.operator_mst_fk,");
            sb.Append("       supp.operator_name,");
            sb.Append("       cust.airline_mst_fk,");
            sb.Append("       air.airline_name,");
            sb.Append("       cust.cust_tos,");
            sb.Append("       cust.cust_dtention_free,");
            sb.Append("       cust.cust_cutoff_hrs,");
            sb.Append("       cust.cust_ref_plug_free,");
            sb.Append("       cust.cust_cr_days,");
            sb.Append("       cust.cust_cr_limit,");
            sb.Append("       cust.cust_seasonal,");
            sb.Append("       cust.from_date,");
            sb.Append("       cust.to_date,");
            sb.Append("       cust.cust_other_inf,");
            sb.Append("       cust.port_mst_pol_fk,");
            sb.Append("        POL.port_name polname,");
            sb.Append("       cust.port_mst_pod_fk,");
            sb.Append("        POD.PORT_NAME podname,");
            sb.Append("       cust.country_mst_fk,");
            sb.Append("       coun.country_name,");
            sb.Append("       cust.region_mst_fk,");
            sb.Append("       reg.region_name,");
            sb.Append("       cust.commodity_mst_fk,");
            sb.Append("       comm.commodity_name,");
            sb.Append("       cust.ADD_FLAG,");
            sb.Append("       cust.INCLUDE_FLAG");
            sb.Append("       from");
            sb.Append("   customer_potential_tbl cust,customer_mst_tbl custmst,operator_mst_tbl supp,");
            sb.Append("   airline_mst_tbl air,port_mst_tbl POL,port_mst_tbl POD, country_mst_tbl coun,region_mst_tbl reg,");
            sb.Append("    commodity_mst_tbl comm");
            sb.Append("   where custmst.customer_mst_pk=cust.customer_mst_fk");
            sb.Append("   and supp.operator_mst_pk(+)=cust.operator_mst_fk");
            sb.Append("   and   air.airline_mst_pk(+)=cust.airline_mst_fk");
            sb.Append("   and POL.port_mst_pk(+) = cust.port_mst_pol_fk");
            sb.Append("   and POD.PORT_MST_PK(+)=cust.port_mst_pod_fk");
            sb.Append("   and coun.country_mst_pk(+)=cust.country_mst_fk");
            sb.Append("   and reg.region_mst_pk(+)=cust.region_mst_fk");
            sb.Append("   and comm.commodity_mst_pk(+)=cust.commodity_mst_fk");
            sb.Append("   and cust.customer_mst_fk=" + Custpk);
            if (Polpk > 0)
            {
                sb.Append("   and cust.port_mst_pol_fk=" + Polpk);
            }

            if (flag == 2)
            {
                if (Podpk > 0)
                {
                    sb.Append("   and cust.country_mst_fk=" + Podpk);
                }
            }
            else if (flag == 3)
            {
                if (Podpk > 0)
                {
                    sb.Append("   and cust.region_mst_fk=" + Podpk);
                }
            }
            else
            {
                if (Podpk > 0)
                {
                    sb.Append("   and cust.port_mst_pod_fk=" + Podpk);
                }
            }
            if (Commpk > 0)
            {
                sb.Append("   and cust.commodity_mst_fk=" + Commpk);
            }
            if (potentialpk > 0)
            {
                sb.Append("   and cust.customer_potential_mst_pk=" + potentialpk);
            }
            sb.Append("");
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

        /// <summary>
        /// Deletecustomers the specified potentialpk.
        /// </summary>
        /// <param name="Potentialpk">The potentialpk.</param>
        public void Deletecustomer(Int32 Potentialpk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand DelCommand = new OracleCommand();

            try
            {
                var _with21 = DelCommand;
                _with21.Parameters.Clear();
                _with21.Transaction = TRAN;
                _with21.Connection = objWK.MyConnection;
                _with21.CommandType = CommandType.StoredProcedure;
                _with21.CommandText = objWK.MyUserName + ".customer_add_details_pkg.CustomerPoten_Delete";

                _with21.Parameters.Add("Cust_Poten_pk_In", Potentialpk).Direction = ParameterDirection.Input;
                _with21.Parameters["Cust_Poten_pk_In"].SourceVersion = DataRowVersion.Current;
                //.Parameters.Add("DELETED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input
                _with21.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with21.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                _with21.ExecuteNonQuery();
                TRAN.Commit();
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
        /// Deletes the competitor.
        /// </summary>
        /// <param name="competitorpk">The competitorpk.</param>
        public void DeleteCompetitor(Int32 competitorpk = 0)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand DelCommand = new OracleCommand();
            try
            {
                var _with22 = DelCommand;
                _with22.Parameters.Clear();
                _with22.Transaction = TRAN;
                _with22.Connection = objWK.MyConnection;
                _with22.CommandType = CommandType.StoredProcedure;
                _with22.CommandText = objWK.MyUserName + ".customer_add_details_pkg.COMPETITORS_DELETE";

                _with22.Parameters.Add("COMP_PK_IN", competitorpk).Direction = ParameterDirection.Input;
                _with22.Parameters["COMP_PK_IN"].SourceVersion = DataRowVersion.Current;
                //.Parameters.Add("DELETED_BY_FK_IN", M_Last_Modified_By_FK).Direction = ParameterDirection.Input
                _with22.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                _with22.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                _with22.ExecuteNonQuery();
                TRAN.Commit();
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
        /// Countcompetitors the specified potentialpk.
        /// </summary>
        /// <param name="potentialpk">The potentialpk.</param>
        /// <returns></returns>
        public object countcompetitor(Int32 potentialpk)
        {
            System.Text.StringBuilder strsql = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();
            Int32 arrcount = 0;
            try
            {
                strsql.Append(" select count(*) from competitors_tbl cmp where cmp.cust_poten_mst_fk = " + potentialpk + "");
                arrcount = Convert.ToInt32(objWF.ExecuteScaler(strsql.ToString()));
                return arrcount;
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
        /// Savecustomers the specified dscustomer.
        /// </summary>
        /// <param name="dscustomer">The dscustomer.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="customerpk">The customerpk.</param>
        /// <returns></returns>
        public ArrayList savecustomer(DataSet dscustomer, DataSet dsGrid, Int16 customerpk)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction TRAN = null;
            Int32 RecAfct = default(Int32);
            int CustPotentialPk = 0;
            long custpk = 0;
            objWK.OpenConnection();
            TRAN = objWK.MyConnection.BeginTransaction();
            OracleCommand InsertCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            try
            {
                CustPotentialPk = Convert.ToInt32(dscustomer.Tables["Customer"].Rows[0]["CUSTOMER_POTENTIAL_MST_PK"]);
            }
            catch (Exception ex)
            {
            }
            try
            {
                var _with23 = InsertCommand;
                _with23.Parameters.Clear();
                _with23.Connection = objWK.MyConnection;
                _with23.CommandType = CommandType.StoredProcedure;
                _with23.CommandText = objWK.MyUserName + ".customer_add_details_pkg.CUSTOMER_ADD_DETAILS_INS";
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_TEU"].ToString()))
                {
                    _with23.Parameters.Add("CUST_TEU_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_TEU_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_TEU"])).Direction = ParameterDirection.Input;
                }
                //.Parameters.Add("CUST_TEU_IN", CLng(dscustomer.Tables("Customer").Rows(0).Item("CUST_TEU"))).Direction = ParameterDirection.Input
                _with23.Parameters.Add("CUST_PERIOD_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_PERIOD"])).Direction = ParameterDirection.Input;
                _with23.Parameters.Add("CUST_CBM_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CBM"])).Direction = ParameterDirection.Input;
                _with23.Parameters.Add("CUST_MTS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_MTS"])).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_SERVICABLE"].ToString()))
                {
                    _with23.Parameters.Add("CUST_SERVICABLE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_SERVICABLE_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_SERVICABLE"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["OPERATOR_MST_FK"].ToString()))
                {
                    _with23.Parameters.Add("OPERATOR_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["AIRLINE_MST_FK"].ToString()))
                {
                    _with23.Parameters.Add("AIRLINE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("AIRLINE_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["AIRLINE_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_TOS"].ToString()))
                {
                    _with23.Parameters.Add("CUST_TOS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_TOS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_TOS"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_DETENTION_FREE"].ToString()))
                {
                    _with23.Parameters.Add("CUST_DTENTION_FREE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_DTENTION_FREE_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_DETENTION_FREE"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_CUTOFF_HRS"].ToString()))
                {
                    _with23.Parameters.Add("CUST_CUTOFF_HRS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_CUTOFF_HRS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CUTOFF_HRS"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_REF_PLUG_FREE"].ToString()))
                {
                    _with23.Parameters.Add("CUST_REF_PLUG_FREE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_REF_PLUG_FREE_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_REF_PLUG_FREE"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_DAYS"].ToString()))
                {
                    _with23.Parameters.Add("CUST_CR_DAYS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_CR_DAYS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_DAYS"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_LIMIT"].ToString()))
                {
                    _with23.Parameters.Add("CUST_CR_LIMIT_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_CR_LIMIT_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_LIMIT"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_SEASONAL"].ToString()))
                {
                    _with23.Parameters.Add("CUST_SEASONAL_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_SEASONAL_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_SEASONAL"])).Direction = ParameterDirection.Input;
                }
                if (dscustomer.Tables["Customer"].Rows[0]["FROM_DATE"] == "{0:dateFormat}")
                {
                    _with23.Parameters.Add("FROM_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("FROM_DATE_IN", Convert.ToString(dscustomer.Tables["Customer"].Rows[0]["FROM_DATE"])).Direction = ParameterDirection.Input;
                }
                if (dscustomer.Tables["Customer"].Rows[0]["TO_DATE"] == "{0:dateFormat}")
                {
                    _with23.Parameters.Add("TO_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("TO_DATE_IN", Convert.ToString(dscustomer.Tables["Customer"].Rows[0]["TO_DATE"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_OTHER_INF"].ToString()))
                {
                    _with23.Parameters.Add("CUST_OTHER_INF_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("CUST_OTHER_INF_IN", Convert.ToString(dscustomer.Tables["Customer"].Rows[0]["CUST_OTHER_INF"])).Direction = ParameterDirection.Input;
                }

                _with23.Parameters.Add("CREATED_BY_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CREATED_BY_FK"])).Direction = ParameterDirection.Input;
                _with23.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["LAST_MODIFIED_BY_FK"])).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POL_FK"].ToString()))
                {
                    _with23.Parameters.Add("PORT_MST_POL_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POD_FK"].ToString()))
                {
                    _with23.Parameters.Add("PORT_MST_POD_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["COUNTRY_MST_FK"].ToString()))
                {
                    _with23.Parameters.Add("COUNTRY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("COUNTRY_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["COUNTRY_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["REGION_MST_FK"].ToString()))
                {
                    _with23.Parameters.Add("REGION_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("REGION_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["REGION_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["COMMODITY_MST_FK"].ToString()))
                {
                    _with23.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with23.Parameters.Add("COMMODITY_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
                }
                _with23.Parameters.Add("CUSTOMER_MST_FK_IN", customerpk).Direction = ParameterDirection.Input;
                _with23.Parameters.Add("ADD_FLAG_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["ADD_FLAG"])).Direction = ParameterDirection.Input;
                _with23.Parameters.Add("INCLUDE_FLAG_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["INCLUDE_FLAG"])).Direction = ParameterDirection.Input;
                _with23.Parameters.Add("RETURN_VALUE", OracleDbType.Varchar2, 100, "RETURN_VALUE").Direction = ParameterDirection.Output;

                var _with24 = updCommand;
                updCommand.Parameters.Clear();
                _with24.Connection = objWK.MyConnection;
                _with24.CommandType = CommandType.StoredProcedure;
                _with24.CommandText = objWK.MyUserName + ".customer_add_details_pkg.CUSTOMER_ADD_DETAILS_UPD";

                _with24.Parameters.Add("CUSTOMER_POTENTIAL_MST_PK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUSTOMER_POTENTIAL_MST_PK"])).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_TEU"].ToString()))
                {
                    _with24.Parameters.Add("CUST_TEU_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_TEU_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_TEU"])).Direction = ParameterDirection.Input;
                }
                //.Parameters.Add("CUST_TEU_IN", CLng(dscustomer.Tables("Customer").Rows(0).Item("CUST_TEU"))).Direction = ParameterDirection.Input
                _with24.Parameters.Add("CUST_PERIOD_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_PERIOD"])).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("CUST_CBM_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CBM"])).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("CUST_MTS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_MTS"])).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_SERVICABLE"].ToString()))
                {
                    _with24.Parameters.Add("CUST_SERVICABLE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_SERVICABLE_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_SERVICABLE"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["OPERATOR_MST_FK"].ToString()))
                {
                    _with24.Parameters.Add("OPERATOR_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("OPERATOR_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["OPERATOR_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["AIRLINE_MST_FK"].ToString()))
                {
                    _with24.Parameters.Add("AIRLINE_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("AIRLINE_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["AIRLINE_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_TOS"].ToString()))
                {
                    _with24.Parameters.Add("CUST_TOS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_TOS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_TOS"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_DETENTION_FREE"].ToString()))
                {
                    _with24.Parameters.Add("CUST_DTENTION_FREE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_DTENTION_FREE_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_DETENTION_FREE"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_CUTOFF_HRS"].ToString()))
                {
                    _with24.Parameters.Add("CUST_CUTOFF_HRS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_CUTOFF_HRS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CUTOFF_HRS"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_REF_PLUG_FREE"].ToString()))
                {
                    _with24.Parameters.Add("CUST_REF_PLUG_FREE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_REF_PLUG_FREE_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_REF_PLUG_FREE"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_DAYS"].ToString()))
                {
                    _with24.Parameters.Add("CUST_CR_DAYS_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_CR_DAYS_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_DAYS"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_LIMIT"].ToString()))
                {
                    _with24.Parameters.Add("CUST_CR_LIMIT_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_CR_LIMIT_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_CR_LIMIT"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_SEASONAL"].ToString()))
                {
                    _with24.Parameters.Add("CUST_SEASONAL_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_SEASONAL_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["CUST_SEASONAL"])).Direction = ParameterDirection.Input;
                }

                if (dscustomer.Tables["Customer"].Rows[0]["FROM_DATE"] == "{0:dateFormat}")
                {
                    _with24.Parameters.Add("FROM_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("FROM_DATE_IN", Convert.ToString(dscustomer.Tables["Customer"].Rows[0]["FROM_DATE"])).Direction = ParameterDirection.Input;
                }

                if (dscustomer.Tables["Customer"].Rows[0]["TO_DATE"] == "{0:dateFormat}")
                {
                    _with24.Parameters.Add("TO_DATE_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("TO_DATE_IN", Convert.ToString(dscustomer.Tables["Customer"].Rows[0]["TO_DATE"])).Direction = ParameterDirection.Input;
                }

                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["CUST_OTHER_INF"].ToString()))
                {
                    _with24.Parameters.Add("CUST_OTHER_INF_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("CUST_OTHER_INF_IN", Convert.ToString(dscustomer.Tables["Customer"].Rows[0]["CUST_OTHER_INF"])).Direction = ParameterDirection.Input;
                }

                _with24.Parameters.Add("LAST_MODIFIED_BY_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["LAST_MODIFIED_BY_FK"])).Direction = ParameterDirection.Input;
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POL_FK"].ToString()))
                {
                    _with24.Parameters.Add("PORT_MST_POL_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("PORT_MST_POL_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POL_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POD_FK"].ToString()))
                {
                    _with24.Parameters.Add("PORT_MST_POD_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("PORT_MST_POD_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["PORT_MST_POD_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["COUNTRY_MST_FK"].ToString()))
                {
                    _with24.Parameters.Add("COUNTRY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("COUNTRY_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["COUNTRY_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["REGION_MST_FK"].ToString()))
                {
                    _with24.Parameters.Add("REGION_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("REGION_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["REGION_MST_FK"])).Direction = ParameterDirection.Input;
                }
                if (string.IsNullOrEmpty(dscustomer.Tables["Customer"].Rows[0]["COMMODITY_MST_FK"].ToString()))
                {
                    _with24.Parameters.Add("COMMODITY_MST_FK_IN", "").Direction = ParameterDirection.Input;
                }
                else
                {
                    _with24.Parameters.Add("COMMODITY_MST_FK_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["COMMODITY_MST_FK"])).Direction = ParameterDirection.Input;
                }
                _with24.Parameters.Add("CUSTOMER_MST_FK_IN", customerpk).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("ADD_FLAG_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["ADD_FLAG"])).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("INCLUDE_FLAG_IN", Convert.ToInt64(dscustomer.Tables["Customer"].Rows[0]["INCLUDE_FLAG"])).Direction = ParameterDirection.Input;
                _with24.Parameters.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                var _with25 = objWK.MyDataAdapter;
                if (CustPotentialPk == 0)
                {
                    _with25.InsertCommand = InsertCommand;
                    _with25.InsertCommand.Transaction = TRAN;
                    RecAfct = _with25.InsertCommand.ExecuteNonQuery();
                    custpk = Convert.ToInt64(InsertCommand.Parameters["RETURN_VALUE"].Value);
                }
                else
                {
                    _with25.UpdateCommand = updCommand;
                    _with25.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with25.UpdateCommand.ExecuteNonQuery();
                    custpk = Convert.ToInt64(updCommand.Parameters["RETURN_VALUE"].Value);
                }
                if (RecAfct > 0)
                {
                    arrMessage = SaveCompetitorDetails((int)custpk, dsGrid, TRAN);
                    if (arrMessage.Count == 0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All data saved successfully");
                        arrMessage.Add(custpk);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                else
                {
                    arrMessage.Add("Error");
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException oraEx)
            {
                TRAN.Rollback();
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
        }

        /// <summary>
        /// Saves the competitor details.
        /// </summary>
        /// <param name="custpk">The custpk.</param>
        /// <param name="dsGrid">The ds grid.</param>
        /// <param name="TRAN">The tran.</param>
        /// <returns></returns>
        public ArrayList SaveCompetitorDetails(int custpk = 0, DataSet dsGrid = null, OracleTransaction TRAN = null)
        {
            Int32 RecAfct = default(Int32);
            Int32 i = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            int competitorpk = 0;
            OracleCommand updCommand = new OracleCommand();
            objWK.MyConnection = TRAN.Connection;
            try
            {
                competitorpk = Convert.ToInt32(dsGrid.Tables[0].Rows[0]["competitors_pk"]);
            }
            catch (Exception ex)
            {
            }

            try
            {
                if (dsGrid.Tables[0].Rows.Count > 0)
                {
                    for (i = 0; i <= dsGrid.Tables[0].Rows.Count - 1; i++)
                    {
                        var _with26 = insCommand;
                        insCommand.Parameters.Clear();
                        _with26.Connection = objWK.MyConnection;
                        _with26.CommandType = CommandType.StoredProcedure;
                        _with26.CommandText = objWK.MyUserName + ".customer_add_details_pkg.COMPETITOR_ADD_DETAILS_INS";
                        var _with27 = _with26.Parameters;
                        _with27.Add("CUSTOMER_POTEN_MST_FK_IN", custpk).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["farwarder_id"].ToString()))
                        {
                            _with27.Add("farwarder_id_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("farwarder_id_IN", dsGrid.Tables[0].Rows[i]["farwarder_id"]).Direction = ParameterDirection.Input;
                        }
                        _with27.Add("farwarder_name_IN", dsGrid.Tables[0].Rows[i]["farwarder_name"]).Direction = ParameterDirection.Input;

                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_teu"].ToString()))
                        {
                            _with27.Add("cmp_teu_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_teu_IN", dsGrid.Tables[0].Rows[i]["cmp_teu"]).Direction = ParameterDirection.Input;
                        }
                        //.Add("cmp_teu_IN", dsGrid.Tables(0).Rows(i).Item("cmp_teu")).Direction = ParameterDirection.Input
                        _with27.Add("cmp_period_IN", dsGrid.Tables[0].Rows[i]["cmp_period"]).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_servicable"].ToString()))
                        {
                            _with27.Add("cmp_servicable_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_servicable_IN", dsGrid.Tables[0].Rows[i]["cmp_servicable"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["carrier"].ToString()))
                        {
                            _with27.Add("operator_mst_fk_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("operator_mst_fk_IN", dsGrid.Tables[0].Rows[i]["carrier"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cbm"].ToString()))
                        {
                            _with27.Add("cmp_cbm_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_cbm_IN", dsGrid.Tables[0].Rows[i]["cmp_cbm"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_mts"].ToString()))
                        {
                            _with27.Add("cmp_mts_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_mts_IN", dsGrid.Tables[0].Rows[i]["cmp_mts"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_tos"].ToString()))
                        {
                            _with27.Add("cmp_tos_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_tos_IN", dsGrid.Tables[0].Rows[i]["cmp_tos"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_detention_free"].ToString()))
                        {
                            _with27.Add("cmp_detention_free_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_detention_free_IN", dsGrid.Tables[0].Rows[i]["cmp_detention_free"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cutoff_hrs"].ToString()))
                        {
                            _with27.Add("cmp_cutoff_hrs_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_cutoff_hrs_IN", dsGrid.Tables[0].Rows[i]["cmp_cutoff_hrs"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_ref_plug_free"].ToString()))
                        {
                            _with27.Add("cmp_ref_plug_free_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_ref_plug_free_IN", dsGrid.Tables[0].Rows[i]["cmp_ref_plug_free"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_seasonal"].ToString()))
                        {
                            _with27.Add("cmp_seasonal_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_seasonal_IN", dsGrid.Tables[0].Rows[i]["cmp_seasonal"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["from_date"].ToString()))
                        {
                            _with27.Add("from_date_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("from_date_IN", Convert.ToDateTime(Convert.ToString(dsGrid.Tables[0].Rows[i]["from_date"]))).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["to_date"].ToString()))
                        {
                            _with27.Add("to_date_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("to_date_IN", Convert.ToDateTime(Convert.ToString(dsGrid.Tables[0].Rows[i]["to_date"]))).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_rate"].ToString()))
                        {
                            _with27.Add("cmp_rate_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_rate_IN", dsGrid.Tables[0].Rows[i]["cmp_rate"]).Direction = ParameterDirection.Input;
                        }
                        //.Add("cmp_rate_IN", dsGrid.Tables(0).Rows(i).Item("cmp_rate")).Direction = ParameterDirection.Input
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cr_days"].ToString()))
                        {
                            _with27.Add("cmp_cr_days_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_cr_days_IN", dsGrid.Tables[0].Rows[i]["cmp_cr_days"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cr_limit"].ToString()))
                        {
                            _with27.Add("cmp_cr_limit_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_cr_limit_IN", dsGrid.Tables[0].Rows[i]["cmp_cr_limit"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_oth_inf"].ToString()))
                        {
                            _with27.Add("cmp_oth_inf_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("cmp_oth_inf_IN", dsGrid.Tables[0].Rows[i]["cmp_oth_inf"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["carrier"].ToString()))
                        {
                            _with27.Add("airline_mst_fk_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with27.Add("airline_mst_fk_IN", dsGrid.Tables[0].Rows[i]["carrier"]).Direction = ParameterDirection.Input;
                        }
                        _with27.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;
                        var _with28 = updCommand;
                        updCommand.Parameters.Clear();
                        _with28.Connection = objWK.MyConnection;
                        _with28.CommandType = CommandType.StoredProcedure;
                        _with28.CommandText = objWK.MyUserName + ".customer_add_details_pkg.COMPETITOR_ADD_DETAILS_UPD";
                        var _with29 = _with28.Parameters;
                        _with29.Add("COMP_PK_IN", dsGrid.Tables[0].Rows[i]["competitors_pk"]).Direction = ParameterDirection.Input;
                        _with29.Add("CUSTOMER_POTEN_MST_FK_IN", custpk).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["farwarder_id"].ToString()))
                        {
                            _with29.Add("farwarder_id_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("farwarder_id_IN", dsGrid.Tables[0].Rows[i]["farwarder_id"]).Direction = ParameterDirection.Input;
                        }
                        _with29.Add("farwarder_name_IN", dsGrid.Tables[0].Rows[i]["farwarder_name"]).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_teu"].ToString()))
                        {
                            _with29.Add("cmp_teu_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_teu_IN", dsGrid.Tables[0].Rows[i]["cmp_teu"]).Direction = ParameterDirection.Input;
                        }
                        //.Add("cmp_teu_IN", dsGrid.Tables(0).Rows(i).Item("cmp_teu")).Direction = ParameterDirection.Input
                        _with29.Add("cmp_period_IN", dsGrid.Tables[0].Rows[i]["cmp_period"]).Direction = ParameterDirection.Input;
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_servicable"].ToString()))
                        {
                            _with29.Add("cmp_servicable_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_servicable_IN", dsGrid.Tables[0].Rows[i]["cmp_servicable"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["carrier"].ToString()))
                        {
                            _with29.Add("operator_mst_fk_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("operator_mst_fk_IN", dsGrid.Tables[0].Rows[i]["carrier"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cbm"].ToString()))
                        {
                            _with29.Add("cmp_cbm_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_cbm_IN", dsGrid.Tables[0].Rows[i]["cmp_cbm"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_mts"].ToString()))
                        {
                            _with29.Add("cmp_mts_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_mts_IN", dsGrid.Tables[0].Rows[i]["cmp_mts"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_tos"].ToString()))
                        {
                            _with29.Add("cmp_tos_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_tos_IN", dsGrid.Tables[0].Rows[i]["cmp_tos"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_detention_free"].ToString()))
                        {
                            _with29.Add("cmp_detention_free_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_detention_free_IN", dsGrid.Tables[0].Rows[i]["cmp_detention_free"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cutoff_hrs"].ToString()))
                        {
                            _with29.Add("cmp_cutoff_hrs_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_cutoff_hrs_IN", dsGrid.Tables[0].Rows[i]["cmp_cutoff_hrs"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_ref_plug_free"].ToString()))
                        {
                            _with29.Add("cmp_ref_plug_free_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_ref_plug_free_IN", dsGrid.Tables[0].Rows[i]["cmp_ref_plug_free"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_seasonal"].ToString()))
                        {
                            _with29.Add("cmp_seasonal_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_seasonal_IN", dsGrid.Tables[0].Rows[i]["cmp_seasonal"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["from_date"].ToString()))
                        {
                            _with29.Add("from_date_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("from_date_IN", Convert.ToDateTime(Convert.ToString(dsGrid.Tables[0].Rows[i]["from_date"]))).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["to_date"].ToString()))
                        {
                            _with29.Add("to_date_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("to_date_IN", Convert.ToDateTime(Convert.ToString(dsGrid.Tables[0].Rows[i]["to_date"]))).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_rate"].ToString()))
                        {
                            _with29.Add("cmp_rate_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_rate_IN", dsGrid.Tables[0].Rows[i]["cmp_rate"]).Direction = ParameterDirection.Input;
                        }
                        //.Add("cmp_rate_IN", dsGrid.Tables(0).Rows(i).Item("cmp_rate")).Direction = ParameterDirection.Input
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cr_days"].ToString()))
                        {
                            _with29.Add("cmp_cr_days_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_cr_days_IN", dsGrid.Tables[0].Rows[i]["cmp_cr_days"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_cr_limit"].ToString()))
                        {
                            _with29.Add("cmp_cr_limit_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_cr_limit_IN", dsGrid.Tables[0].Rows[i]["cmp_cr_limit"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["cmp_oth_inf"].ToString()))
                        {
                            _with29.Add("cmp_oth_inf_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("cmp_oth_inf_IN", dsGrid.Tables[0].Rows[i]["cmp_oth_inf"]).Direction = ParameterDirection.Input;
                        }
                        if (string.IsNullOrEmpty(dsGrid.Tables[0].Rows[i]["carrier"].ToString()))
                        {
                            _with29.Add("airline_mst_fk_IN", "").Direction = ParameterDirection.Input;
                        }
                        else
                        {
                            _with29.Add("airline_mst_fk_IN", dsGrid.Tables[0].Rows[i]["carrier"]).Direction = ParameterDirection.Input;
                        }
                        _with29.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        var _with30 = objWK.MyDataAdapter;
                        if (competitorpk == 0)
                        {
                            _with30.InsertCommand = insCommand;
                            _with30.InsertCommand.Transaction = TRAN;
                            RecAfct = _with30.InsertCommand.ExecuteNonQuery();
                            //custpk = CType(InsertCommand.Parameters["RETURN_VALUE"].Value, Long)
                        }
                        else
                        {
                            _with30.UpdateCommand = updCommand;
                            _with30.UpdateCommand.Transaction = TRAN;
                            RecAfct = _with30.UpdateCommand.ExecuteNonQuery();
                            //custpk = CType(InsertCommand.Parameters["RETURN_VALUE"].Value, Long)
                        }
                        if (RecAfct == 0)
                        {
                            arrMessage.Add("Error");
                            return arrMessage;
                        }
                    }
                }
                return arrMessage;
            }
            catch (OracleException oraEx)
            {
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Customer Add details popup"

        #region "Check the Sales Executive"

        /// <summary>
        /// Checks the sales executable.
        /// </summary>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <param name="SalesExeType">Type of the sales executable.</param>
        /// <returns></returns>
        public int CheckSalesExe(int CustomerPK, Int16 SalesExeType)
        {
            WorkFlow objWF = new WorkFlow();
            string strsql = null;
            try
            {
                if (SalesExeType == 1)
                {
                    strsql = "SELECT NVL(CMT.REP_EMP_MST_FK,0) FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=" + CustomerPK;
                }
                else
                {
                    strsql = "SELECT NVL(CMT.REQ_SALES_EXE,0) FROM CUSTOMER_MST_TBL CMT WHERE CMT.CUSTOMER_MST_PK=" + CustomerPK;
                }
                return Convert.ToInt32(objWF.ExecuteScaler(strsql));
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

        #endregion "Check the Sales Executive"

        /// <summary>
        /// Fetches the shipper details.
        /// </summary>
        /// <param name="CustomerPk">The customer pk.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchShipperDetails(long CustomerPk = 0, long Flag = 1)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataSet dsAll = null;
                var _with31 = objWF.MyCommand.Parameters;
                _with31.Add("CUSTOMER_MST_PK_IN", CustomerPk).Direction = ParameterDirection.Input;
                _with31.Add("FLAG_IN", Flag).Direction = ParameterDirection.Input;
                ///''''''''''''''''''''''''
                _with31.Add("SHIPPER_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with31.Add("CONSIGNEE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with31.Add("NOTIFY1_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with31.Add("NOTIFY2_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("CUSTOMER_MST_TBL_PKG", "FETCH_SHIPPER_DETAILS");
                return dsAll;
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
        /// Fetches the affiliate customers.
        /// </summary>
        /// <param name="CustomerPk">The customer pk.</param>
        /// <param name="Flag">The flag.</param>
        /// <returns></returns>
        public DataSet FetchAffiliateCustomers(long CustomerPk = 0, long Flag = 1)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataSet dsAll = null;
                var _with32 = objWF.MyCommand.Parameters;
                _with32.Add("CUSTOMER_MST_PK_IN", CustomerPk).Direction = ParameterDirection.Input;
                _with32.Add("FLAG_IN", Flag).Direction = ParameterDirection.Input;
                ///''''''''''''''''''''''''
                _with32.Add("AFFILIATE_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                dsAll = objWF.GetDataSet("CUSTOMER_MST_TBL_PKG", "FETCH_AFFILIATE_CUSTOMERS");
                return dsAll;
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

        #region "SAVE Temporery Customer"

        /// <summary>
        /// Temporaries the customersave.
        /// </summary>
        /// <param name="ID">The identifier.</param>
        /// <param name="Name">The name.</param>
        /// <param name="BType">Type of the b.</param>
        /// <param name="Category">The category.</param>
        /// <param name="TransactionType">Type of the transaction.</param>
        /// <param name="CREATED_BY">The create d_ by.</param>
        /// <param name="CREATED_FROM">The create d_ from.</param>
        /// <param name="EMPPK">The emppk.</param>
        /// <param name="CUSTOMERPk">The customer pk.</param>
        /// <param name="Address">The address.</param>
        /// <param name="Salutation">The salutation.</param>
        /// <param name="ContectPreson">The contect preson.</param>
        /// <param name="Phone">The phone.</param>
        /// <param name="Fax">The fax.</param>
        /// <param name="Email">The email.</param>
        /// <param name="CountryPk">The country pk.</param>
        /// <param name="Mobile">The mobile.</param>
        /// <param name="Address2">The address2.</param>
        /// <param name="Address3">The address3.</param>
        /// <param name="city">The city.</param>
        /// <returns></returns>
        public ArrayList TempCustomersave(string ID = "", string Name = "", int BType = 0, int Category = 0, string TransactionType = "", int CREATED_BY = 0, string CREATED_FROM = "", int EMPPK = 0, int CUSTOMERPk = 0, string Address = "",
        string Salutation = "", string ContectPreson = "", string Phone = "", string Fax = "", string Email = "", int CountryPk = 0, string Mobile = "", string Address2 = "", string Address3 = "", string city = "")
        {
            OracleCommand insCommand = new OracleCommand();
            OracleCommand insChildCommand = new OracleCommand();
            WorkFlow objWK = new WorkFlow();
            Quantum_QFOR.Cls_CustomerReconciliation objCustomer = new Quantum_QFOR.Cls_CustomerReconciliation();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            try
            {
                if (CUSTOMERPk == 0)
                {
                    var _with33 = insCommand;
                    _with33.Connection = objWK.MyConnection;
                    _with33.CommandType = CommandType.StoredProcedure;
                    _with33.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_PKG.TEMP_CUSTOMER_INS";
                    _with33.Parameters.Clear();
                    var _with34 = _with33.Parameters;
                    _with34.Add("TEMP_ID_IN", ID);
                    _with34.Add("NAME_IN", Name);
                    _with34.Add("BIZ_TYPE_IN", BType);
                    _with34.Add("CATEGORY_IN", Category);
                    _with34.Add("TRANSACTIONTYPE_IN", TransactionType);
                    _with34.Add("CREATED_BY_IN", CREATED_BY);
                    _with34.Add("CREATED_FROM_IN", CREATED_FROM);
                    _with34.Add("EMPLOYEE_PK_IN", EMPPK);
                    _with34.Add("RETURN_VALUE", CUSTOMERPk).Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    var _with35 = objWK.MyDataAdapter;
                    _with35.InsertCommand = insCommand;
                    _with35.InsertCommand.Transaction = TRAN;
                    _with35.InsertCommand.ExecuteNonQuery();
                    CUSTOMERPk = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                    var _with36 = insCommand;
                    _with36.CommandType = CommandType.StoredProcedure;
                    _with36.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_PKG.TEMP_CUSTOMER_CONTACT_INS";
                    _with36.Parameters.Clear();
                    var _with37 = _with36.Parameters;
                    _with37.Add("ADM_SALUTATION_IN", Salutation);
                    _with37.Add("ADM_PHONE_NO_1_IN", Phone);
                    _with37.Add("ADM_MOBILE_NO_IN", Mobile);
                    _with37.Add("ADM_ADDRESS_1_IN", Address);
                    _with37.Add("ADM_ADDRESS_2_IN", Address2);
                    _with37.Add("ADM_ADDRESS_3_IN", Address3);
                    _with37.Add("ADM_CITY_IN", city);
                    _with37.Add("ADM_COUNTRY_PK_IN", CountryPk);
                    _with37.Add("ADM_CONTACT_PERSON_IN", ContectPreson);
                    _with37.Add("ADM_FAX_NO_IN", Fax);
                    _with37.Add("ADM_EMAIL_ID_IN", Email);
                    _with37.Add("RETURN_VALUE", CUSTOMERPk).Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    var _with38 = objWK.MyDataAdapter;
                    _with38.InsertCommand = insCommand;
                    _with38.InsertCommand.Transaction = TRAN;
                    _with38.InsertCommand.ExecuteNonQuery();
                    if (arrMessage.Count > 0)
                    {
                        return arrMessage;
                    }
                    else
                    {
                        arrMessage.Add("All Data Saved Successfully");
                        TRAN.Commit();
                        return arrMessage;
                    }
                    return arrMessage;
                }
                else
                {
                    var _with39 = insCommand;
                    _with39.Connection = objWK.MyConnection;
                    _with39.CommandType = CommandType.StoredProcedure;
                    _with39.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_PKG.TEMP_CUSTOMER_UPD";
                    _with39.Parameters.Clear();
                    var _with40 = _with39.Parameters;
                    _with40.Add("TEMP_PK_IN", CUSTOMERPk);
                    _with40.Add("TEMP_ID_IN", ID);
                    _with40.Add("NAME_IN", Name);
                    _with40.Add("EMPLOYEE_PK_IN", EMPPK);
                    _with40.Add("RETURN_VALUE", CUSTOMERPk).Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    var _with41 = objWK.MyDataAdapter;
                    _with41.InsertCommand = insCommand;
                    _with41.InsertCommand.Transaction = TRAN;
                    _with41.InsertCommand.ExecuteNonQuery();
                    var _with42 = insCommand;
                    _with42.CommandType = CommandType.StoredProcedure;
                    _with42.CommandText = objWK.MyUserName + ".TEMP_CUSTOMER_PKG.TEMP_CUSTOMER_CONTACT_UPD";
                    _with42.Parameters.Clear();
                    var _with43 = _with42.Parameters;
                    _with43.Add("TEMP_PK_IN", CUSTOMERPk);
                    _with43.Add("ADM_SALUTATION_IN", Salutation);
                    _with43.Add("ADM_PHONE_NO_1_IN", Phone);
                    _with43.Add("ADM_MOBILE_NO_IN", Mobile);
                    _with43.Add("ADM_ADDRESS_1_IN", Address);
                    _with43.Add("ADM_ADDRESS_2_IN", Address2);
                    _with43.Add("ADM_ADDRESS_3_IN", Address3);
                    _with43.Add("ADM_CITY_IN", city);
                    _with43.Add("ADM_COUNTRY_PK_IN", CountryPk);
                    _with43.Add("ADM_CONTACT_PERSON_IN", ContectPreson);
                    _with43.Add("ADM_FAX_NO_IN", Fax);
                    _with43.Add("ADM_EMAIL_ID_IN", Email);
                    _with43.Add("RETURN_VALUE", CUSTOMERPk).Direction = ParameterDirection.Output;
                    insCommand.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    var _with44 = objWK.MyDataAdapter;
                    _with44.InsertCommand = insCommand;
                    _with44.InsertCommand.Transaction = TRAN;
                    _with44.InsertCommand.ExecuteNonQuery();
                    if (arrMessage.Count > 0)
                    {
                        return arrMessage;
                    }
                    else
                    {
                        arrMessage.Add("Customer details updated successfully");
                        TRAN.Commit();
                        return arrMessage;
                    }
                    return arrMessage;
                }

                //Manjunath  PTS ID:Sep-02  17/09/2011
            }
            catch (OracleException OraExp)
            {
                arrMessage.Add(OraExp.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                arrMessage.Add(ex.Message);
                TRAN.Rollback();

                return arrMessage;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #region "FetchAll"

        /// <summary>
        /// Fetch_s the temp_ customer.
        /// </summary>
        /// <param name="CustomerPK">The customer pk.</param>
        /// <returns></returns>
        public DataSet Fetch_Temp_Customer(Int32 CustomerPK = 0)
        {
            try
            {
                WorkFlow objWF = new WorkFlow();
                DataSet ds = null;
                string strVal = null;

                objWF.MyCommand.Parameters.Clear();
                var _with45 = objWF.MyCommand.Parameters;
                _with45.Add("CUSTOMER_PK_IN", CustomerPK).Direction = ParameterDirection.Input;

                _with45.Add("LOAD_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("TEMP_CUSTOMER_PKG", "FETCH_TEMP_CUSTOMER");

                return ds;
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

        #endregion "FetchAll"

        #endregion "SAVE Temporery Customer"
    }
}