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
using System.Text;
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_QuotationStatusRpt : CommonFeatures
    {
        #region "Fetch Quotations"

        //QuoStatus is Added By prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report

        /// <summary>
        /// Fetches the quotation.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="QuoStatus">The quo status.</param>
        /// <param name="Cargo_Type">Type of the cargo_.</param>
        /// <param name="Port_Cont_Pk">The port_ cont_ pk.</param>
        /// <param name="CommodityPk">The commodity pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="Todt">The todt.</param>
        /// <returns></returns>
        public DataSet FetchQuotation(Int32 BizType, Int32 QuoStatus, Int32 Cargo_Type, string Port_Cont_Pk, Int32 CommodityPk, Int32 CustPk, string Fromdt, string Todt)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            Int32 commodity = default(Int32);
            try
            {
                strsql.Append(" select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                strsql.Append(" qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");

                //Sea
                if (BizType == 2)
                {
                    strsql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    //Hided By By prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report
                    //strsql.Append(" 'Confirm' Status  from quotation_sea_tbl qto,quotation_trn_sea_fcl_lcl trn ")
                    //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                    //strsql.Append(" and qto.status=2")
                    //Added By prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report
                    //Added by Ajay PTS ID AUG-024
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    //strsql.Append("qto.QUOTATION_DATE CreatedDt,qto.QUOTATION_DATE+VALID_FOR ExpiryDt,")
                    strsql.Append("to_date(qto.QUOTATION_DATE, 'dd/MM/yyyy') AS \"CreatedDt\",to_date(qto.QUOTATION_DATE+VALID_FOR,'dd/MM/yyyy') AS \"ExpiryDt\",");
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Updated by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4)");
                    }
                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //Added by Ajay PTS ID AUG-024
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        //Ended by Ajay
                        strsql.Append(" where qto.status=3");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    if (Port_Cont_Pk.Length > 0 & Port_Cont_Pk != "n" & Port_Cont_Pk != "0")
                    {
                        strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk,trn.container_type_mst_fk) IN (" + Port_Cont_Pk + ")");
                    }

                    strsql.Append(" and qto.cargo_type=" + Cargo_Type);
                    if (HttpContext.Current.Session["user_id"] != "admin")
                    {
                        strsql.Append(" and qto.created_by_fk= " + HttpContext.Current.Session["USER_PK"]);
                    }
                    //strsql.Append(" and qto.status <> 1 ")

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    strsql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }

                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }

                    //Air
                }
                else if (BizType == 1)
                {
                    strsql.Append(" (select opr.airline_id from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    //Hided By  prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report
                    //strsql.Append(" 'Confirm' Status  from quotation_sea_tbl qto,QUOT_GEN_TRN_AIR_TBL trn ")
                    //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                    //strsql.Append(" and qto.status=2")
                    //Added By prakash Chandra on 29/09/08 for pts : Cancelled Quotation Report
                    //Added by Ajay PTS ID AUG-024
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    //strsql.Append("qto.QUOTATION_DATE CreatedDt,qto.QUOTATION_DATE+VALID_FOR ExpiryDt,")
                    strsql.Append("to_date(qto.QUOTATION_DATE, 'dd/MM/yyyy') AS \"CreatedDt\",to_date(qto.QUOTATION_DATE+VALID_FOR,'dd/MM/yyyy') AS \"ExpiryDt\",");
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Updated by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4)");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=3");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    if (Port_Cont_Pk.Length > 0 & Port_Cont_Pk != "n" & Port_Cont_Pk != "0")
                    {
                        strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" + Port_Cont_Pk + ")");
                    }
                    strsql.Append(" and qto.quotation_type = " + Cargo_Type);

                    if (HttpContext.Current.Session["user_id"] != "admin")
                    {
                        strsql.Append(" and qto.created_by_fk= " + HttpContext.Current.Session["USER_PK"]);
                    }
                    //strsql.Append(" and qto.status <> 1 ")

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    if (Cargo_Type == 0)
                    {
                        strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");
                    }
                    else
                    {
                        strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");
                    }

                    if (CommodityPk > 0)
                    {
                        if (Cargo_Type == 1)
                        {
                            strsql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                        }
                        else
                        {
                            strsql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                        }
                    }

                    if (Cargo_Type == 1)
                    {
                        strsql.Replace("QUOTATION_DTL_TBL", "QUOTATION_DTL_TBL");
                    }
                    strsql.Replace("BOOKING_TRN", "BOOKING_TRN");
                    strsql.Replace("sea", "air");

                    //'Sea & Air
                }
                else
                {
                    strsql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    //Added by Ajay PTS ID AUG-024
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    //strsql.Append("qto.QUOTATION_DATE CreatedDt,qto.QUOTATION_DATE+VALID_FOR ExpiryDt,")
                    strsql.Append("to_date(qto.QUOTATION_DATE, 'dd/MM/yyyy') AS \"CreatedDt\",to_date(qto.QUOTATION_DATE+VALID_FOR,'dd/MM/yyyy') AS \"ExpiryDt\",");
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Updated by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4)");
                    }
                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //Added by Ajay PTS ID AUG-024
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        //Ended by Ajay
                        strsql.Append(" where qto.status=3");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    if (Port_Cont_Pk.Length > 0 & Port_Cont_Pk != "n" & Port_Cont_Pk != "0")
                    {
                        strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk,trn.container_type_mst_fk) IN (" + Port_Cont_Pk + ")");
                    }
                    //strsql.Append(" and qto.cargo_type=" & Cargo_Type)
                    if (HttpContext.Current.Session["user_id"] != "admin")
                    {
                        strsql.Append(" and qto.created_by_fk= " + HttpContext.Current.Session["USER_PK"]);
                    }
                    //strsql.Append(" and qto.status <> 1 ")

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    strsql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }

                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" UNION ");
                    //'
                    strsql.Append(" select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    strsql.Append(" qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    strsql.Append(" (select opr.airline_name from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    //Added by Ajay PTS ID AUG-024
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CreatedDt,qto.QUOTATION_DATE+VALID_FOR ExpiryDt,");
                    //strsql.Append("to_date(qto.QUOTATION_DATE, 'dd/MM/yyyy') AS ""CreatedDt"",to_date(qto.QUOTATION_DATE+VALID_FOR,'dd/MM/yyyy') AS ""ExpiryDt"",")
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Updated by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4)");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //Added by Ajay PTS ID AUG-024
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        //Ended by ajay
                        strsql.Append(" where  qto.status=3");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    if (Port_Cont_Pk.Length > 0 & Port_Cont_Pk != "n" & Port_Cont_Pk != "0")
                    {
                        strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" + Port_Cont_Pk + ")");
                    }
                    //strsql.Append(" and qto.quotation_type = " & Cargo_Type)

                    if (HttpContext.Current.Session["user_id"] != "admin")
                    {
                        strsql.Append(" and qto.created_by_fk= " + HttpContext.Current.Session["USER_PK"]);
                    }
                    //strsql.Append(" and qto.status <> 1 ")

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");

                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    }
                    strsql.Append(" UNION ");
                    //'
                    strsql.Append(" select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    strsql.Append(" qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    strsql.Append(" (select opr.airline_name from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    //Added by Ajay PTS ID AUG-024
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    //strsql.Append("qto.QUOTATION_DATE CreatedDt,qto.QUOTATION_DATE+VALID_FOR ExpiryDt,")
                    strsql.Append("to_date(qto.QUOTATION_DATE, 'dd/MM/yyyy') AS \"CreatedDt\",to_date(qto.QUOTATION_DATE+VALID_FOR,'dd/MM/yyyy') AS \"ExpiryDt\",");
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Updated by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4)");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=3");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn  ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    if (Port_Cont_Pk.Length > 0 & Port_Cont_Pk != "n" & Port_Cont_Pk != "0")
                    {
                        strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" + Port_Cont_Pk + ")");
                    }
                    //strsql.Append(" and qto.quotation_type = " & Cargo_Type)

                    if (HttpContext.Current.Session["user_id"] != "admin")
                    {
                        strsql.Append(" and qto.created_by_fk= " + HttpContext.Current.Session["USER_PK"]);
                    }
                    //strsql.Append(" and qto.status <> 1 ")

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");

                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    }
                    //strsql.Replace("booking_trn_sea_fcl_lcl", "booking_trn_air")
                }
                if (BizType != 0)
                {
                    strsql.Append(" order by custid,quotrefno ");
                }
                return objWK.GetDataSet(strsql.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Quotations"

        //Added by Ajay PTS ID AUG-024

        #region "Fetch Quotations For Search"

        //QuoStatus is Added By prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report

        /// <summary>
        /// Fetches the quotation search.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="QuoStatus">The quo status.</param>
        /// <param name="Cargo_Type">Type of the cargo_.</param>
        /// <param name="Port_Cont_Pk">The port_ cont_ pk.</param>
        /// <param name="CommodityPk">The commodity pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="SHOW">The show.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <returns></returns>
        public DataSet FetchQuotationSearch(Int32 BizType, Int32 QuoStatus, Int32 Cargo_Type, string Port_Cont_Pk, Int32 CommodityPk, Int32 CustPk, string Fromdt, string Todt, string SHOW, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 flag = 0, Int32 POLPK = 0, Int32 PODPK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            StringBuilder strsql1 = new StringBuilder();
            StringBuilder strsql2 = new StringBuilder();
            StringBuilder Sql = new StringBuilder();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            string Biz = null;
            string strCondition1 = null;
            string Val = null;
            Int32 commodity = default(Int32);
            Int32 start = default(Int32);

            Fromdt = Fromdt.Trim();
            Todt = Todt.Trim();

            try
            {
                if (BizType == 0)
                {
                    Biz = "ALL";
                }
                else if (BizType == 1)
                {
                    Biz = "AIR";
                }
                else if (BizType == 2)
                {
                    Biz = "SEA";
                }

                //Val = SHOW
                //Sea
                if (BizType == 2)
                {
                    Sql.Append(" SELECT Count(*) from(select rownum SLNO,q.* from(select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    Sql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    Sql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    Sql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    if (QuoStatus == 1)
                    {
                        Sql.Append(" case when qto.status=4 then");
                        Sql.Append(" 'Used' when qto.status=2 then");
                        Sql.Append(" 'Confirmed' when qto.status=1 then");
                        Sql.Append(" 'Active' when qto.status=3 then");
                        Sql.Append(" 'Cancelled' end status, ");
                        //Cancelled'
                        Sql.Append("  0 QUOTATION_TYPE ");
                        Sql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 2)
                    {
                        Sql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 3)
                    {
                        Sql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }

                    if (QuoStatus == 4)
                    {
                        Sql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 5)
                    {
                        Sql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    Sql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk,trn.container_type_mst_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        Sql.Append(" and qto.cargo_type=" + Cargo_Type);
                    }
                    Sql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        Sql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    Sql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        Sql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }

                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    if (flag == 0)
                    {
                        Sql.Append(" AND 1=2");
                    }
                    //Air
                }
                else if (BizType == 1)
                {
                    Sql.Append(" SELECT Count(*) from(select rownum SLNO,q.* from(select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    Sql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    Sql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    Sql.Append(" (select opr.airline_id from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    if (QuoStatus == 1)
                    {
                        Sql.Append(" case when qto.status=4 then");
                        Sql.Append(" 'Used' when qto.status=2 then");
                        Sql.Append(" 'Confirmed' when qto.status=1 then");
                        Sql.Append(" 'Active' when qto.status=3 then");
                        Sql.Append(" 'Cancelled' end status, ");
                        Sql.Append("  QTO.QUOTATION_TYPE ");
                        Sql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }

                    if (QuoStatus == 2)
                    {
                        Sql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 3)
                    {
                        Sql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 4)
                    {
                        Sql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 5)
                    {
                        Sql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    Sql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        Sql.Append(" and qto.quotation_type = " + (Cargo_Type == 1 ? 0 : 1));
                    }
                    Sql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        Sql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    Sql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");
                    if (CommodityPk > 0)
                    {
                        Sql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    }
                    if (flag == 0)
                    {
                        Sql.Append(" AND 1=2");
                    }
                    //'Sea & Air
                }
                else
                {
                    Sql.Append(" SELECT Count(*) from(select rownum SLNO,q.* from(select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    Sql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    Sql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    Sql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    if (QuoStatus == 1)
                    {
                        Sql.Append(" case when qto.status=4 then");
                        Sql.Append(" 'Used' when qto.status=2 then");
                        Sql.Append(" 'Confirmed' when qto.status=1 then");
                        Sql.Append(" 'Active' when qto.status=3 then");
                        Sql.Append(" 'Cancelled' end status, ");
                        //Cancelled'
                        Sql.Append("  0 QUOTATION_TYPE ");
                        Sql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 2)
                    {
                        Sql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 3)
                    {
                        Sql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }

                    if (QuoStatus == 4)
                    {
                        Sql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 5)
                    {
                        Sql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    Sql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk,trn.container_type_mst_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        Sql.Append(" and qto.cargo_type=" + Cargo_Type);
                    }
                    Sql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        Sql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    Sql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        Sql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }

                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    if (flag == 0)
                    {
                        Sql.Append(" AND 1=2");
                    }
                    Sql.Append(" UNION ");
                    //'
                    Sql.Append(" select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    Sql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    Sql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    Sql.Append(" (select opr.airline_id from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    if (QuoStatus == 1)
                    {
                        Sql.Append(" case when qto.status=4 then");
                        Sql.Append(" 'Used' when qto.status=2 then");
                        Sql.Append(" 'Confirmed' when qto.status=1 then");
                        Sql.Append(" 'Active' when qto.status=3 then");
                        Sql.Append(" 'Cancelled' end status, ");
                        Sql.Append("  QTO.QUOTATION_TYPE ");
                        Sql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }

                    if (QuoStatus == 2)
                    {
                        Sql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        Sql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 3)
                    {
                        Sql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 4)
                    {
                        Sql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 5)
                    {
                        Sql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        Sql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    Sql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        Sql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        Sql.Append(" and qto.quotation_type = " + (Cargo_Type == 1 ? 0 : 1));
                    }
                    Sql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        Sql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        Sql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    Sql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");
                    if (CommodityPk > 0)
                    {
                        Sql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    }
                    if (flag == 0)
                    {
                        Sql.Append(" AND 1=2");
                    }
                }
                //Ended by Ajay
                Sql.Append(")q)");

                TotalRecords = Convert.ToInt32(objWK.ExecuteScaler(Sql.ToString()));
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

                //Sea
                if (BizType == 2)
                {
                    strsql.Append(" SELECT * from(select rownum SLNO,q.* from(select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    strsql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");

                    strsql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    strsql.Append(" '" + Biz + "' BIZTYPE,");
                    strsql.Append(" DECODE(QTO.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGOTYPE,");
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk=trn.commodity_group_fk)Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");

                    if (QuoStatus == 1)
                    {
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status, ");
                        strsql.Append("  0 QUOTATION_TYPE ");
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk,trn.container_type_mst_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        strsql.Append(" and qto.cargo_type=" + Cargo_Type);
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    strsql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }

                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    if (flag == 0)
                    {
                        strsql.Append(" AND 1=2");
                    }
                    strsql.Append(" order by CREATEDDT DESC ");
                    strsql.Append(")q)where SLNO Between '" + start + "' and '" + last + "'");

                    //Air
                }
                else if (BizType == 1)
                {
                    strsql.Append(" SELECT * from(select rownum SLNO,q.* from(select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    strsql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    strsql.Append(" (select opr.airline_id from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    strsql.Append(" '" + Biz + "' BIZTYPE,");
                    strsql.Append(" DECODE(qto.quotation_type,1,'ULD',0,'KGS') CARGOTYPE,");
                    commodity = CommodityPk;
                    if (Cargo_Type == 1)
                    {
                        strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk=TRN.COMMODITY_GROUP_FK)Commodity,");
                    }
                    else
                    {
                        strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk=QTO.commodity_group_mst_fk)Commodity,");
                    }

                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");

                    if (QuoStatus == 1)
                    {
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status, ");
                        strsql.Append(" Qto.QUOTATION_TYPE ");
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn , USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn , USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn , USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        Sql.Append(" and qto.quotation_type = " + (Cargo_Type == 1 ? 0 : 1));
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");
                    strsql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    if (flag == 0)
                    {
                        strsql.Append(" AND 1=2");
                    }
                    strsql.Append(" order by CREATEDDT DESC ");
                    strsql.Append(")q)where SLNO Between '" + start + "' and '" + last + "'");
                    //'Sea & Air
                }
                else
                {
                    strsql.Append(" SELECT * from(select rownum SLNO,q.* from(select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    strsql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    strsql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    strsql.Append(" 'SEA' BIZTYPE,");
                    strsql.Append(" DECODE(QTO.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGOTYPE,");
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk=trn.commodity_group_fk)Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");
                    if (QuoStatus == 1)
                    {
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status, ");
                        strsql.Append("  0 QUOTATION_TYPE ");
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }

                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        strsql.Append(" and qto.cargo_type=" + Cargo_Type);
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    strsql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }
                    if (flag == 0)
                    {
                        strsql.Append(" AND 1=2");
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" UNION ");
                    //'
                    strsql.Append(" select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    strsql.Append(" qto.QUOTATION_MST_PK QuotPk, qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    strsql.Append(" (select opr.airline_name from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    strsql.Append(" 'AIR' BIZTYPE,");
                    strsql.Append("DECODE(qto.quotation_type,1,'ULD',0,'KGS') CARGOTYPE,");
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk=qto.commodity_group_mst_fk)Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");
                    if (QuoStatus == 1)
                    {
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status, ");
                        strsql.Append("  QTO.QUOTATION_TYPE ");
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT ");
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }

                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        Sql.Append(" and qto.quotation_type = " + (Cargo_Type == 1 ? 0 : 1));
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");

                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    }

                    if (flag == 0)
                    {
                        strsql.Append(" AND 1=2");
                    }
                    strsql.Append(" order by CREATEDDT DESC ");
                    strsql.Append(")q)where SLNO Between '" + start + "' and '" + last + "'");
                }
                return objWK.GetDataSet(strsql.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Quotations For Search"

        #region "Fetch Quotations For GrandVal"

        //QuoStatus is Added By prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report

        /// <summary>
        /// Fetches the quotation grand value.
        /// </summary>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="QuoStatus">The quo status.</param>
        /// <param name="Cargo_Type">Type of the cargo_.</param>
        /// <param name="Port_Cont_Pk">The port_ cont_ pk.</param>
        /// <param name="CommodityPk">The commodity pk.</param>
        /// <param name="CustPk">The customer pk.</param>
        /// <param name="Fromdt">The fromdt.</param>
        /// <param name="Todt">The todt.</param>
        /// <param name="SHOW">The show.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="POLPK">The polpk.</param>
        /// <param name="PODPK">The podpk.</param>
        /// <returns></returns>
        public DataSet FetchQuotationGrandVal(Int32 BizType, Int32 QuoStatus, Int32 Cargo_Type, string Port_Cont_Pk, Int32 CommodityPk, Int32 CustPk, string Fromdt, string Todt, string SHOW, Int32 CurrentPage = 0,
        Int32 TotalPage = 0, Int32 flag = 0, Int32 POLPK = 0, Int32 PODPK = 0)
        {
            WorkFlow objWK = new WorkFlow();
            StringBuilder strsql = new StringBuilder();
            StringBuilder strsql1 = new StringBuilder();
            StringBuilder strsql2 = new StringBuilder();
            StringBuilder Sql = new StringBuilder();
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            string Biz = null;
            string strCondition1 = null;
            string Val = null;
            Int32 commodity = default(Int32);
            Int32 start = default(Int32);
            Array Arr = null;

            Fromdt = Fromdt.Trim();
            Todt = Todt.Trim();

            try
            {
                if (BizType == 0)
                {
                    Biz = "ALL";
                }
                else if (BizType == 1)
                {
                    Biz = "AIR";
                }
                else if (BizType == 2)
                {
                    Biz = "SEA";
                }

                strsql.Append(" select rownum SLNO,q.* from(select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                strsql.Append(" qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");

                //Sea
                if (BizType == 2)
                {
                    strsql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    //Added by Ajay PTS ID AUG-024
                    strsql.Append(" '" + Biz + "' BIZTYPE,");
                    //strsql.Append(" '" & Val & "' CARGOTYPE,")
                    strsql.Append(" DECODE(QTO.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGOTYPE,");
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");
                    //Ended by Ajay
                    //Hided By By prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report
                    //strsql.Append(" 'Confirm' Status  from quotation_sea_tbl qto,quotation_trn_sea_fcl_lcl trn ")
                    //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                    //strsql.Append(" and qto.status=2")
                    //Added By prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report
                    //Added by Ajay PTS ID AUG-024
                    //commodity = CommodityPk
                    //strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" & commodity & "')Commodity,")
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Changed by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn ,USER_MST_TBL UMT");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        //Added by Ajay PTS ID AUG-024
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        //Ended by Ajay
                        strsql.Append(" where qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        strsql.Append(" and qto.cargo_type=" + Cargo_Type);
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    //strsql.Append(" and qto.status <> 1 ")

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    strsql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }

                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    //strsql.Append(")q)")
                    strsql.Append(" order by CREATEDDT DESC ");
                    strsql.Append(")q");

                    //Air
                }
                else if (BizType == 1)
                {
                    strsql.Append(" (select opr.airline_id from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    //Added by Ajay PTS ID AUG-024
                    strsql.Append(" '" + Biz + "' BIZTYPE,");
                    //strsql.Append(" '" & Val & "' CARGOTYPE,")
                    strsql.Append(" DECODE(qto.quotation_type,1,'ULD',0,'KGS') CARGOTYPE,");
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");
                    //Ended by Ajay
                    //Hided By  prakash Chandra on 29/09/08 for pts : Cancelled Quotation  Report
                    //strsql.Append(" 'Confirm' Status  from quotation_sea_tbl qto,QUOT_GEN_TRN_AIR_TBL trn ")
                    //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                    //strsql.Append(" and qto.status=2")
                    //Added By prakash Chandra on 29/09/08 for pts : Cancelled Quotation Report
                    //Added by Ajay PTS ID AUG-024
                    //commodity = CommodityPk
                    //strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" & commodity & "')Commodity,")
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Changed by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        //Added by Ajay PTS ID AUG-024
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        //Ended by Ajay
                        strsql.Append(" where  qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    //strsql.Append(" and qto.quotation_type = " & Cargo_Type)

                    //If HttpContext.Current.Session("user_id") <> "admin" Then
                    //    strsql.Append(" and qto.created_by_fk= " & HttpContext.Current.Session("USER_PK"))
                    //End If
                    //strsql.Append(" and qto.status <> 1 ")
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        strsql.Append(" and qto.quotation_type = " + (Cargo_Type == 1 ? 0 : 1));
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");
                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    }
                    //Added by Ajay PTS ID AUG-024
                    //strsql.Append(")q)")
                    strsql.Append(" order by CREATEDDT DESC ");
                    strsql.Append(")q");
                    //Ended by Ajay
                    //'Sea & Air
                }
                else
                {
                    strsql.Append(" (select opr.operator_name from operator_mst_tbl opr where opr.operator_mst_pk=trn.CARRIER_MST_FK)oprid, ");
                    //Added by Ajay PTS ID AUG-024
                    strsql.Append(" 'SEA' BIZTYPE,");
                    //strsql.Append(" '" & Val & "' CARGOTYPE,")
                    strsql.Append(" DECODE(QTO.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGOTYPE,");
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Changed by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT ");
                        //Added by Ajay PTS ID AUG-024
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        //Ended by Ajay
                        strsql.Append(" where qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn,USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_sea_fcl_lcl bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=2");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    //'strsql.Append(" and qto.cargo_type=" & Cargo_Type)
                    //If HttpContext.Current.Session("user_id") <> "admin" Then
                    //    strsql.Append(" and qto.created_by_fk= " & HttpContext.Current.Session("USER_PK"))
                    //End If

                    //strsql.Append(" and qto.status <> 1 ")
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        strsql.Append(" and qto.cargo_type=" + Cargo_Type);
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);
                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    strsql.Append(" and trn.QUOTATION_MST_FK = qto.QUOTATION_MST_PK ");
                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and trn.commodity_group_fk = " + CommodityPk);
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (flag == 0)
                    {
                        strsql.Append(" AND 1=2");
                    }
                    //Ended by Ajay
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" UNION ");
                    //'
                    strsql.Append(" select distinct (select cust.customer_name from customer_mst_tbl cust  where cust.customer_mst_pk=qto.customer_mst_fk)custid, ");
                    strsql.Append(" qto.quotation_ref_no quotrefno,(select pol.port_name from port_mst_tbl pol where pol.port_mst_pk=trn.port_mst_pol_fk)polid, ");
                    strsql.Append(" (select pod.port_name from port_mst_tbl pod where pod.port_mst_pk=trn.port_mst_pod_fk)podid, ");
                    strsql.Append(" (select opr.airline_name from airline_mst_tbl  opr where opr.airline_mst_pk = trn.CARRIER_MST_FK) oprid,");
                    //Added by Ajay PTS ID AUG-024
                    strsql.Append(" 'AIR' BIZTYPE,");
                    //strsql.Append(" '" & Val & "' CARGOTYPE,")
                    strsql.Append("DECODE(qto.quotation_type,1,'ULD',0,'KGS') CARGOTYPE,");
                    commodity = CommodityPk;
                    strsql.Append("(select cmg.commodity_group_code from COMMODITY_GROUP_MST_TBL cmg where cmg.commodity_group_pk='" + commodity + "')Commodity,");
                    strsql.Append("qto.QUOTATION_DATE CREATEDDT,qto.QUOTATION_DATE+VALID_FOR EXPIRYDT,");
                    //Ended by Ajay
                    if (QuoStatus == 1)
                    {
                        //Changed by Ajay PTS ID AUG-024
                        strsql.Append(" case when qto.status=4 then");
                        strsql.Append(" 'Used' when qto.status=2 then");
                        strsql.Append(" 'Confirmed' when qto.status=1 then");
                        strsql.Append(" 'Active' when qto.status=3 then");
                        strsql.Append(" 'Cancelled' end status ");
                        //Cancelled'
                        //Ended by Ajay
                        strsql.Append(" from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status in (1,2,3,4) AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }

                    if (QuoStatus == 2)
                    {
                        strsql.Append(" 'Used' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where qto.status=4 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 3)
                    {
                        strsql.Append(" 'Cancelled' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        //Added by Ajay PTS ID AUG-024
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        //Ended by Ajay
                        strsql.Append(" where  qto.status=3 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (QuoStatus == 4)
                    {
                        strsql.Append(" 'Active' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=1 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    if (QuoStatus == 5)
                    {
                        strsql.Append(" 'Confirmed' Status  from QUOTATION_MST_TBL qto,QUOTATION_DTL_TBL trn, USER_MST_TBL UMT  ");
                        //strsql.Append(" where (select count(*) from booking_trn_air bkg where bkg.trans_ref_no like qto.quotation_ref_no)=0 ")
                        strsql.Append(" where  qto.status=2 AND QTO.CREATED_BY_FK = UMT.USER_MST_PK AND QTO.BIZ_TYPE=1");
                    }
                    //Ended by Ajay
                    //End by prakash Chandra
                    //If Port_Cont_Pk.Length > 0 And Port_Cont_Pk <> "n" And Port_Cont_Pk <> "0" Then
                    //    strsql.Append(" and (trn.port_mst_pol_fk, trn.port_mst_pod_fk) IN (" & Port_Cont_Pk & ")")
                    //End If
                    //'strsql.Append(" and qto.quotation_type = " & Cargo_Type)

                    //If HttpContext.Current.Session("user_id") <> "admin" Then
                    //    strsql.Append(" and qto.created_by_fk= " & HttpContext.Current.Session("USER_PK"))
                    //End If
                    //strsql.Append(" and qto.status <> 1 ")
                    if (POLPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pol_fk=" + POLPK);
                    }
                    if (PODPK > 0)
                    {
                        strsql.Append(" and trn.port_mst_pod_fk=" + PODPK);
                    }
                    if (Cargo_Type > 0)
                    {
                        Sql.Append(" and qto.quotation_type = " + (Cargo_Type == 1 ? 0 : 1));
                    }
                    strsql.Append(" and UMT.DEFAULT_LOCATION_FK=" + HttpContext.Current.Session["LOGED_IN_LOC_FK"]);

                    if (CustPk > 0)
                    {
                        strsql.Append(" and qto.customer_mst_fk=" + CustPk);
                    }
                    if (Fromdt.Length > 0 & Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date between to_date('" + Fromdt + "','" + dateFormat + "') and to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    else if (Fromdt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date>=to_date('" + Fromdt + "','" + dateFormat + "')");
                    }
                    else if (Todt.Length > 0)
                    {
                        strsql.Append(" and qto.quotation_date<=to_date('" + Todt + "','" + dateFormat + "')");
                    }
                    strsql.Append(" and qto.QUOTATION_MST_PK=trn.QUOTATION_MST_FK");

                    if (CommodityPk > 0)
                    {
                        strsql.Append(" and qto.commodity_group_mst_fk = " + CommodityPk);
                    }
                    //Added by Ajay PTS ID AUG-024
                    if (flag == 0)
                    {
                        strsql.Append(" AND 1=2");
                    }
                    //Ended by Ajay

                    strsql.Append(" order by CREATEDDT DESC ");
                    strsql.Append(")Q");
                    //strsql.Replace("booking_trn_sea_fcl_lcl", "booking_trn_air")
                }
                //If BizType <> 0 Then
                //    strsql.Append(" order by CREATEDDT DESC ")
                //End If
                //strsql.Append(" order by CREATEDDT DESC ")
                return objWK.GetDataSet(strsql.ToString());
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
                //'Exception Handling Added by Gangadhar on 15/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Quotations For GrandVal"

        //Ended by Ajay

        #region " Commodity Group "

        /// <summary>
        /// Fetches the commodity GRP.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchCommodityGrp()
        {
            string sql = null;

            sql += " select 0 COMMODITY_GROUP_PK,";
            sql += " 'ALL' COMMODITY_GROUP_CODE, ";
            sql += " ' ' COMMODITY_GROUP_DESC, ";
            sql += " 0 VERSION_NO from dual UNION ";
            sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO ";
            sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            sql += " WHERE CG.ACTIVE_FLAG=1 ";
            sql += " ORDER BY COMMODITY_GROUP_CODE ";

            try
            {
                return (new WorkFlow()).GetDataSet(sql);
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

        #endregion " Commodity Group "
    }
}