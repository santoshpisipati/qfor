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

namespace Quantum_QFOR
{
    public class ClsActiveRates : CommonFeatures
    {
        public System.Text.StringBuilder StrCondition = new System.Text.StringBuilder(5000);

        public string sbNew;
        ///'''''''''''-------------Grid Section-----------------------

        #region "FetchActiveRatingforGrid()"

        public DataSet FetchActiveRatingforGrid(string validFrom, string validTo, string Loc, int commType, string mainType, int BizType, string Locpk = "", string polpk = "", string podpk = "", string Custpk = "",
        Int32 CurrentPage = 0, Int32 TotalPage = 0, Int32 flag = 0)
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            Int32 TotalRecords = default(Int32);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strCondition1 = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow objWF = new WorkFlow();

            if ((Convert.ToInt32(Loc) != 0))
            {
                StrCondition.Append(" and lmt.location_mst_pk =  " + Loc + " ");
            }
            if (flag == 0)
            {
                strCondition1 = " AND 1=2";
            }

            if ((!string.IsNullOrEmpty(polpk) & polpk != "0"))
            {
                StrCondition.Append(" and pol.port_mst_pk =  " + polpk + " ");
            }
            if ((!string.IsNullOrEmpty(podpk) & podpk != "0"))
            {
                StrCondition.Append(" and pod.port_mst_pk =  " + podpk + " ");
            }

            if ((!string.IsNullOrEmpty(Custpk)))
            {
                StrCondition.Append(" and cmt.customer_mst_pk =  " + Custpk + " ");
            }
            sb.Append(StrCondition);

            //SRR SEA FOR FCL
            //SRR SEA FOR FCL
            if ((BizType == 2) & mainType == "SRR" & commType == 1)
            {
                forGridSeaSRRFCL(validFrom, validTo);
            }

            //SRR SEA FOR LCL
            if ((BizType == 2) & mainType == "SRR" & commType == 2)
            {
                forGridSeaSRRLCL(validFrom, validTo);
            }

            //QUATATION SEA FOR FCL
            if ((BizType == 2) & mainType == "QTN" & commType == 1)
            {
                forGridQtnSeaFCL(validFrom, validTo);
            }

            //QUATATION SEA FOR LCL
            if ((BizType == 2) & mainType == "QTN" & commType == 2)
            {
                forGridQtnSeaLCL(validFrom, validTo);
            }

            //QUATATION SEA FOR BBC
            if ((BizType == 2) & mainType == "QTN" & commType == 3)
            {
                forGridQtnSeaBBC(validFrom, validTo);
            }

            //SRR FOR AIR
            if ((BizType == 1) & mainType == "SRR")
            {
                forGridSRRAir(validFrom, validTo);
            }

            //QUATATION FOR AIR
            if ((BizType == 1) & mainType == "QTN")
            {
                forGridQtnAir(validFrom, validTo);
            }

            //SEA FCL BOTH
            if (BizType == 2 & mainType == "BOTH" & commType == 1)
            {
                forGridSeaSRRAndQtnFCL(validFrom, validTo);
            }
            //SEA LCL BOTH
            if (BizType == 2 & mainType == "BOTH" & commType == 2)
            {
                forGridSeaSRRAndQtnLCL(validFrom, validTo);
            }

            //AIR BOTH
            if (BizType == 1 & mainType == "BOTH")
            {
                forGridAirSRRAndQtn(validFrom, validTo);
            }

            //Fcl
            if (BizType == 0 & mainType == "SRR")
            {
                ForGridAirSeaSRR(validFrom, validTo, flag);
            }
            if (BizType == 0 & mainType == "QTN")
            {
                forGridAirSeaQtn(validFrom, validTo, flag);
            }
            if (BizType == 0 & mainType == "BOTH")
            {
                forGridAirSeaBoth(validFrom, validTo);
            }

            try
            {
                System.Text.StringBuilder strCount = new System.Text.StringBuilder();
                System.Text.StringBuilder sqlstr2 = new System.Text.StringBuilder();
                strCount.Append(" SELECT COUNT(*)  from  ");
                strCount.Append(("(" + sbNew.ToString() + ""));
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
                sqlstr2.Append("  (" + sbNew.ToString() + " ");
                sqlstr2.Append(" ) q )) ");
                sqlstr2.Append("   WHERE \"SL_NO\"  BETWEEN " + start + " AND " + last + "");
                strSql = sqlstr2.ToString();
                return objWF.GetDataSet(strSql);
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

        #endregion "FetchActiveRatingforGrid()"

        #region "forGridSeaSRRFCL()" ''''--------------

        public void forGridSeaSRRFCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("     sst.srr_sea_pk as \"REF PK\",");
                sb.Append("    sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_id as \"Location\",");
                sb.Append("       POL.port_id as \"POL\",");
                sb.Append("       POD.port_id as \"POD\",");
                sb.Append("   SUM(c.teu_factor)  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\",");
                sb.Append("       'SEA' \"BIZType\",");
                sb.Append("       'FCL' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl POL,");
                sb.Append("       port_mst_tbl POD, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = POL.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = POD.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   and sst.cargo_type = 1  and sst.status=1  ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                    //sb.Append(" and (sst.srr_date >= to_date('" & validFrom & "','DD/MM/YYYY') and sst.srr_date <= to_date('" & validTo & "','DD/MM/YYYY') )  ")
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("  GROUP BY cmt.customer_name,  sst.srr_sea_pk, ");
                sb.Append("   sst.srr_ref_no, usr.user_id,  lmt.location_id, ");
                sb.Append("   POL.port_id,  POD.port_id,  sst.srr_date, ");
                sb.Append("   sst.valid_to,  sst.status ");
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSeaSRRFCL()" ''''--------------

        #region "forGridSeaSRRLCL()"

        public void forGridSeaSRRLCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select distinct  cmt.customer_name as \"Customer Name\",");
                sb.Append("         sst.srr_sea_pk as \"REF PK\",");
                sb.Append("         sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("       ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\",");
                sb.Append("       'SEA' \"BIZType\",");
                sb.Append("       'LCL' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk  and sst.status=1 and sst.active=1  ");
                sb.Append("   and sst.cargo_type = 2 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and (sst.srr_date >= to_date('" & validFrom & "','DD/MM/YYYY') and sst.srr_date <= to_date('" & validTo & "','DD/MM/YYYY') )  ")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSeaSRRLCL()"

        #region "forGridSeaQtnFCL()"

        public void forGridQtnSeaFCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("     qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("     qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("   SUM(c.teu_factor)  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\",");
                sb.Append("       'SEA' \"BIZType\",");
                sb.Append("       'FCL' \"CargoType\",");
                sb.Append("        0  \"StausType\"");
                sb.Append("   from  ");
                sb.Append("  QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");

                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append("  qtrn.container_type_mst_fk= c.container_type_mst_pk ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=1");
                sb.Append("   AND qtns.BIZ_TYPE=2 ");
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date  <= to_date('" + validTo + "','DD/MM/YYYY') )  ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                sb.Append(StrCondition);
                sb.Append(" GROUP BY CMT.CUSTOMER_NAME,          QTNS.QUOTATION_MST_PK, ");
                sb.Append("  QTNS.QUOTATION_REF_NO,          USR.USER_ID, ");
                sb.Append("  LMT.LOCATION_NAME,          POL.PORT_NAME, ");
                sb.Append("  QTNS.VALID_FOR,  POD.PORT_NAME,  QTNS.QUOTATION_DATE ");
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSeaQtnFCL()"

        #region "forGridSeaQtnLCL()"

        public void forGridQtnSeaLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("     qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("     qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("           ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl .trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\", ");
                sb.Append("       'SEA' \"BIZType\",");
                sb.Append("       'LCL' \"CargoType\",");
                sb.Append("        0   \"StausType\"");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("   ");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt, ");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt");
                sb.Append("   where ");
                sb.Append("   qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK  ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=2 and qtns.status=2  AND QTNS.BIZ_TYPE=2 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date  <= to_date('" + validTo + "','DD/MM/YYYY') )  ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSeaQtnLCL()"

        #region "forGridSeaQtnBBC()"

        public void forGridQtnSeaBBC(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("      qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("      qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_id as \"Location\",");
                sb.Append("          pol.port_id as \"POL\",");
                sb.Append("       pod.port_id as \"POD\", ");
                sb.Append("           ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl .trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\", ");
                sb.Append("       'SEA' \"BIZType\",");
                sb.Append("       'BBC' \"CargoType\",");
                sb.Append("         0  \"StausType\"");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("   ");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt, ");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt");
                sb.Append("   where ");
                sb.Append("   qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK  ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=4 and qtns.status=2 AND QTNS.BIZ_TYPE=2 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (qtns.quotation_date >= to_date('" + validFrom + "','DD/MM/YYYY') and qtns.quotation_date  <= to_date('" + validTo + "','DD/MM/YYYY') )  ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date(qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSeaQtnBBC()"

        #region "forGridAirSRR()"

        public void forGridSRRAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_id as \"Location\",");
                sb.Append("       POL.port_id as \"POL\",");
                sb.Append("       POD.port_id as \"POD\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\",");
                sb.Append("       'AIR' \"BIZType\",");
                sb.Append("       '' \"CargoType\",");
                sb.Append("       SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     POL,");
                sb.Append("       port_mst_tbl     POD,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");

                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = POL.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = POD.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy') and to_date(SRRAIR.Valid_To,'dd/mm/yyyy') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy') and to_date(SRRAIR.Valid_To,'dd/mm/yyyy') or (to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                    //sb.Append(" and  (SRRAIR.SRR_DATE >= to_date('" & validFrom & "','DD/MM/YYYY') and SRRAIR.SRR_DATE <= to_date('" & validTo & "','DD/MM/YYYY') )  ")
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy')   <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( SRRAIR.VALID_TO,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridAirSRR()"

        #region "forGridAirQtn()"

        public void forGridQtnAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO as \"REF NO\",");
                sb.Append("                'Quotation' as \"REF Type\",");
                sb.Append("                usr.user_id as \"User ID\",");
                sb.Append("                lmt.location_id as \"Location\",");
                sb.Append("                POL.port_id as \"POL\",");
                sb.Append("                POD.port_id as \"POD\",");
                sb.Append("                (select count(*)");
                sb.Append("                   from BOOKING_TRN BTA");
                sb.Append("                  where BTA.TRANS_REF_NO = MAIN1.QUOTATION_REF_NO) as \"No of Booking\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Expiry DateTime\",");
                sb.Append("               'AIR' \"BIZType\",");
                sb.Append("              ' ' \"CargoType\",");
                sb.Append("       MAIN1.QUOTATION_TYPE as \"StausType\"");
                sb.Append("  FROM QUOTATION_MST_TBL MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL TRAN,");
                sb.Append("       PORT_MST_TBL      POL,");
                sb.Append("       PORT_MST_TBL      POD,");
                sb.Append("       user_mst_tbl      usr,");
                sb.Append("       location_mst_tbl  lmt,");
                sb.Append("       customer_mst_tbl  cmt ");
                sb.Append("");
                sb.Append(" WHERE ");
                sb.Append("    TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   and cmt.customer_mst_pk = MAIN1.Customer_Mst_Fk and MAIN1.status=2  AND MAIN1.BIZ_TYPE=1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date and main1.quotation_date + main1.valid_for or  to_date('" & validTo & "','DD/MM/YYYY') between MAIN1.quotation_date and main1.quotation_date + main1.valid_for or (MAIN1.quotation_date >= to_date('" & validFrom & "','DD/MM/YYYY') and main1.quotation_date + main1.valid_for <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" and (to_date(MAIN1.quotation_date,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(main1.quotation_date,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridAirQtn()"

        #region "forGridSeaSRRQtnFCL()"

        public void forGridSeaSRRAndQtnFCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("         sst.srr_sea_pk as \"REF PK\",");
                sb.Append("         sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("   c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\",");
                sb.Append("       'SEA' \"BIZType\",");
                sb.Append("      'FCL' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   and sst.cargo_type = 1  and sst.status=1 and sst.active=1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (to_date(sst.srr_date,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.srr_date,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') )  ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("    union   ");

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("     qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("     qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("     c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\",");
                sb.Append("        'SEA' \"BIZType\",");
                sb.Append("         'FCL' \"CargoType\",");
                sb.Append("         0 \"StausType\"");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("      QUOTATION_MST_TBL qtns,");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append(" qtrn.container_type_mst_fk= c.container_type_mst_pk ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=1");
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK AND  qtns.BIZ_TYPE=2 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date(qtns.quotation_date,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSeaSRRQtnFCL()"

        #region "forGridSeaSRRQtnLCL()"

        public void forGridSeaSRRAndQtnLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct  cmt.customer_name as \"Customer Name\",");
                sb.Append("      sst.srr_sea_pk as \"REF PK\",");
                sb.Append("        sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("       ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\",");
                sb.Append("        'SEA' \"BIZType\",");
                sb.Append("         'LCL' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk  and sst.status=1 and sst.active=1  ");
                sb.Append("   and sst.cargo_type = 2 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date(sst.srr_date,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.srr_date,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') )  ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("  union  ");

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("   qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("   qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("           ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl .trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\",");
                sb.Append("        'SEA' \"BIZType\",");
                sb.Append("         'LCL' \"CargoType\",");
                sb.Append("           0 \"StausType\"");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("   ");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt, ");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt");
                sb.Append("   where ");

                sb.Append("   qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK  ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=2 and qtns.status=2 AND qtns.BIZ_TYPE=2 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (to_date(qtns.quotation_date,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'dd/mm/yyyy') <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                sb.Append(StrCondition);

                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridSeaSRRQtnLCL()"

        #region "forGridAirSRRQtn()"

        public void forGridAirSRRAndQtn(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_id as \"Location\",");
                sb.Append("       POL.port_id as \"POL\",");
                sb.Append("       POD.port_id as \"POD\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\",");
                sb.Append("        'AIR' \"BIZType\",");
                sb.Append("         '' \"CargoType\",");
                sb.Append("       SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     POL,");
                sb.Append("       port_mst_tbl     POD,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");

                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = POL.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = POD.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( SRRAIR.VALID_TO,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("union ");

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_MST_PK as \"Ref PK\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO as \"Ref No\",");
                sb.Append("                'Quotation' as \"REF Type\",");
                sb.Append("                usr.user_id as \"User ID\",");
                sb.Append("                lmt.location_id as \"Location\",");
                sb.Append("                pol.port_id as \"AOO\",");
                sb.Append("                pod.port_id as \"AOD\",");
                sb.Append("                (select count(*)");
                sb.Append("                   from BOOKING_TRN BTA");
                sb.Append("                  where BTA.TRANS_REF_NO = MAIN1.QUOTATION_REF_NO) as \"No of Booking\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Expiry DateTime\",");
                sb.Append("                 'AIR' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("       MAIN1.QUOTATION_TYPE as \"StausType\"");
                sb.Append("  FROM QUOTATION_MST_TBL MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL TRAN,");
                sb.Append("       PORT_MST_TBL      POL,");
                sb.Append("       PORT_MST_TBL      POD,");
                sb.Append("       user_mst_tbl      usr,");
                sb.Append("       location_mst_tbl  lmt,");
                sb.Append("       customer_mst_tbl  cmt ");
                sb.Append(" WHERE ");
                sb.Append("    TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   and cmt.customer_mst_pk = MAIN1.Customer_Mst_Fk and MAIN1.status=2 AND MAIN1.BIZ_TYPE=1 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date(MAIN1.quotation_date,'dd/mm/yyyy') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(MAIN1.quotation_date,'dd/mm/yyyy') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'dd/mm/yyyy')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'dd/mm/yyyy')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridAirSRRQtn()"

        #region "forGridAirandSeaSRR"

        public void ForGridAirSeaSRR(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       sst.srr_sea_pk as \"REF PK\",");
                sb.Append("      sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("   c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\",");
                sb.Append("                 'SEA' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   and sst.status=1 and sst.active=1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append(StrCondition2);

                sb.Append("   union  ");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"AOO\",");
                sb.Append("       pod.port_name as \"AOD\",");
                sb.Append("       0  as \"No of TEU'S\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\",");
                sb.Append("                 'AIR' \"BIZType\",");
                sb.Append("                 '' \"CargoType\",");
                sb.Append("       SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     pol,");
                sb.Append("       port_mst_tbl     pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");

                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = pol.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = pod.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(SRRAIR.srr_date,'DD/MM/YYYY') and to_date(SRRAIR.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(SRRAIR.srr_date,'DD/MM/YYYY') and to_date(SRRAIR.valid_to,'DD/MM/YYYY') or (to_date(SRRAIR.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(SRRAIR.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date( SRRAIR.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append(StrCondition2);

                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forGridAirandSeaSRR"

        #region "forGridAirandSeaQtn"

        public void forGridAirSeaQtn(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("  qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("  qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("   case when qtns.BIZ_TYPE=2 then SUM(c.teu_factor) else null end  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\", ");
                sb.Append("                  DECODE(QTNS.BIZ_TYPE,2,'SEA',1,'AIR') \"BIZType\",");
                sb.Append("         DECODE(qtns.cargo_type, 1, 'FCL', 2, 'LCL',4,'BBC') As \"CargoType\",");
                sb.Append("            0  \"StausType\"");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("      QUOTATION_MST_TBL qtns,");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append("  qtrn.container_type_mst_fk= c.container_type_mst_pk(+) ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append(StrCondition2);

                sb.Append(" GROUP BY CMT.CUSTOMER_NAME,  QTNS.QUOTATION_MST_PK, ");
                sb.Append(" QTNS.QUOTATION_REF_NO,  USR.USER_ID,  LMT.LOCATION_NAME, ");
                sb.Append(" POL.PORT_NAME,  QTNS.QUOTATION_DATE,  QTNS.VALID_FOR, ");
                sb.Append(" QTNS.BIZ_TYPE,  QTNS.CARGO_TYPE,  POD.PORT_NAME ");

                sb.Append("  order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forGridAirandSeaQtn"

        #region "forGridAirandSeaBoth"

        public void forGridAirSeaBoth(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("      sst.srr_sea_pk as \"REF PK\",");
                sb.Append("      sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("       c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\",");
                sb.Append("          'SEA' \"BIZType\",");
                sb.Append("         '' \"CargoType\",");
                sb.Append("       sst.status as \"StausType\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   and sst.status=1 and sst.active=1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.srr_date,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                sb.Append("and to_date( sst.valid_to, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                sb.Append(StrCondition);
                sb.Append("    union   ");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("      qtns.QUOTATION_MST_PK as \"REF PK\",");
                sb.Append("      qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("   case when qtns.BIZ_TYPE=2 then c.teu_factor else null end  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\",");
                sb.Append("         DECODE(QTNS.BIZ_TYPE,2,'SEA',1,'AIR') \"BIZType\",");
                sb.Append("        CASE WHEN QTNS.BIZ_TYPE=2 THEN  DECODE(qtns.cargo_type, 1, 'FCL', 2, 'LCL',4,'BBC') WHEN  QTNS.BIZ_TYPE=1 THEN  '' END As \"CargoType\",");
                sb.Append("          0  \"StausType\"");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append(" qtrn.container_type_mst_fk= c.container_type_mst_pk(+) ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");

                sb.Append(StrCondition);
                sb.Append("  union  ");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_air_pk as \"REF PK\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'SRR' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"AOO\",");
                sb.Append("       pod.port_name as \"AOD\",");
                sb.Append("                0  as \"No of TEU'S\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\",");
                sb.Append("          'AIR' \"BIZType\",");
                sb.Append("         '' \"CargoType\",");
                sb.Append("       SRRAIR.Srr_Approved as \"StausType\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     pol,");
                sb.Append("       port_mst_tbl     pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");
                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = pol.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = pod.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and  (to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                sb.Append("and to_date( SRRAIR.VALID_TO, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                sb.Append(StrCondition);
                //sb.Append("union ")
                //sb.Append("select distinct cmt.customer_name as ""Customer Name"",")
                //sb.Append("                MAIN1.QUOTATION_MST_PK as ""Ref PK"",")
                //sb.Append("                MAIN1.QUOTATION_REF_NO as ""Ref No"",")
                //sb.Append("                'Quotation' as ""REF Type"",")
                //sb.Append("                usr.user_id as ""User ID"",")
                //sb.Append("                lmt.location_name as ""Location"",")
                //sb.Append("                pol.port_name as ""AOO"",")
                //sb.Append("                pod.port_name as ""AOD"",")
                //sb.Append("                0  as ""No of TEU'S"",")
                //sb.Append("                (select count(*)")
                //sb.Append("                   from BOOKING_TRN BTA")
                //sb.Append("                  where BTA.TRANS_REF_NO = MAIN1.QUOTATION_REF_NO) as ""No of Booking"",")
                //sb.Append("                to_date(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS ""Created DateTime"",")
                //sb.Append("                to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,")
                //sb.Append("                        'dd/MM/yyyy') AS ""Expiry DateTime"",")
                //sb.Append("          'AIR' ""BIZType"",")
                //sb.Append("         '' ""CargoType"",")
                //sb.Append("       MAIN1.QUOTATION_TYPE as ""StausType""")
                //sb.Append("  FROM QUOTATION_MST_TBL MAIN1,")
                //sb.Append("       QUOTATION_DTL_TBL TRAN,")
                //sb.Append("       PORT_MST_TBL      POL,")
                //sb.Append("       PORT_MST_TBL      POD,")
                //sb.Append("       user_mst_tbl      usr,")
                //sb.Append("       location_mst_tbl  lmt,")
                //sb.Append("       customer_mst_tbl  cmt ")
                //sb.Append("")
                //sb.Append(" WHERE ")
                //sb.Append("    TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK")
                //sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK")
                //sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK ")
                //sb.Append("   and usr.default_location_fk = lmt.location_mst_pk")
                //sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK")
                //sb.Append("   and cmt.customer_mst_pk = MAIN1.Customer_Mst_Fk and MAIN1.status=2 ")
                //If ((validFrom <> "") And (validTo <> "")) Then
                //    sb.Append(" and  (MAIN1.quotation_date >= to_date('" & validFrom & "','DD/MM/YYYY') and main1.quotation_date <= to_date('" & validTo & "','DD/MM/YYYY') ) ")
                //ElseIf Not (validFrom = "") Then
                //    sb.Append(vbCrLf & " AND MAIN1.quotation_date  >= TO_DATE('" & validFrom & "',dateformat) ")
                //ElseIf Not (validTo = "") Then
                //    sb.Append(vbCrLf & " AND MAIN1.quotation_date  <= TO_DATE('" & validTo & "',dateformat) ")
                //End If
                //sb.Append("and to_date( MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')")
                //sb.Append(StrCondition)
                sb.Append("  order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
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

        #endregion "forGridAirandSeaBoth"

        ///'''''''''''-------------Report Section-----------------------

        #region "FetchRatingforReport()"

        public DataSet FetchRatingforReport(string validFrom, string validTo, string Loc, int commType, string mainType, int BizType, string Locpk = "", string polpk = "", string podpk = "", string Custpk = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            string strSql = null;
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            //Dim StrCondition As New System.Text.StringBuilder(5000)
            if ((Convert.ToInt32(Loc) != 0))
            {
                //If (Locpk <> "") Then
                StrCondition.Append(" and lmt.location_mst_pk =  " + Loc + " ");
                // End If
            }
            if ((!string.IsNullOrEmpty(polpk) & polpk != "0"))
            {
                StrCondition.Append(" and pol.port_mst_pk =  " + polpk + " ");
            }
            if ((!string.IsNullOrEmpty(podpk) & podpk != "0"))
            {
                StrCondition.Append(" and pod.port_mst_pk =  " + podpk + " ");
            }

            if ((!string.IsNullOrEmpty(Custpk)))
            {
                StrCondition.Append(" and cmt.customer_mst_pk =  " + Custpk + " ");
            }
            //sb.Append(StrCondition)
            //-----------------------------------------------------------------------------------------------------

            //SRR SEA FOR FCL
            //SRR SEA FOR FCL
            if ((BizType == 2) & mainType == "SRR" & commType == 1)
            {
                forReportSeaSRRFCL(validFrom, validTo);
            }

            //SRR SEA FOR LCL
            if ((BizType == 2) & mainType == "SRR" & commType == 2)
            {
                forReportSeaSRRLCL(validFrom, validTo);
            }

            //QUATATION SEA FOR FCL
            if ((BizType == 2) & mainType == "QTN" & commType == 1)
            {
                ForReportQtnSeaFCL(validFrom, validTo);
            }

            //QUATATION SEA FOR LCL
            if ((BizType == 2) & mainType == "QTN" & commType == 2)
            {
                forReportQtnSeaLCL(validFrom, validTo);
            }

            //Added by Ajay PTS ID AUG-015
            //QUATATION SEA FOR LCL
            if ((BizType == 2) & mainType == "QTN" & commType == 3)
            {
                forReportQtnSeaBBC(validFrom, validTo);
            }
            //Ended by Ajay

            //SRR FOR AIR
            if ((BizType == 1) & mainType == "SRR")
            {
                forReportSRRAir(validFrom, validTo);
                //fnSRRAirG(validFrom, validTo)
            }

            //QUATATION FOR AIR
            if ((BizType == 1) & mainType == "QTN")
            {
                forReportQtnAir(validFrom, validTo);
            }

            //SEA FCL BOTH
            if (BizType == 2 & mainType == "BOTH" & commType == 1)
            {
                forReportSeaSRRAndQtnFCL(validFrom, validTo);
            }
            //SEA LCL BOTH
            if (BizType == 2 & mainType == "BOTH" & commType == 2)
            {
                forReportSeaSRRAndQtnLCL(validFrom, validTo);
            }

            //AIR BOTH
            if (BizType == 1 & mainType == "BOTH")
            {
                forReportAirSRRAndQtn(validFrom, validTo);
            }

            //'Added By Koteshwari on 3/6/2011
            //Commented by Ajay
            //If BizType = 0 And mainType = "SRR" And commType = 1 Then 'Fcl
            //    frmAirSeaSRR(validFrom, validTo)
            //End If
            //If BizType = 0 And mainType = "QTN" And commType = 1 Then
            //    frmAirSeaQua(validFrom, validTo)
            //End If
            //If BizType = 0 And mainType = "BOTH" And commType = 1 Then
            //    frmAirSeaBoth(validFrom, validTo)
            //End If

            //Fcl
            if (BizType == 0 & mainType == "SRR")
            {
                forReportAirSeaSRR(validFrom, validTo);
            }
            if (BizType == 0 & mainType == "QTN")
            {
                forReportAirSeaQtn(validFrom, validTo);
            }
            if (BizType == 0 & mainType == "BOTH")
            {
                forReportAirSeaBoth(validFrom, validTo);
            }

            //If BizType = 0 And mainType = "SRR" And commType = 2 Then
            //    frmAirSeaSRRLcl(validFrom, validTo)
            //End If
            //If BizType = 0 And mainType = "QTN" And commType = 2 Then
            //    frmAirSeaQTNLcl(validFrom, validTo)
            //End If
            //If BizType = 0 And mainType = "BOTH" And commType = 2 Then
            //    frmAirSeaBothLcl(validFrom, validTo)
            //End If
            //Ended by Ajay
            //----------------------------------------------------------------------------------------------------

            try
            {
                return ObjWk.GetDataSet(sbNew.ToString());
                //Manjunath  PTS ID:Sep-02  04/10/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return new DataSet();
        }

        #endregion "FetchRatingforReport()"

        #region "forReportSeaSRRFCL()"

        public void forReportSeaSRRFCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("   SUM(c.teu_factor)  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   and sst.cargo_type = 1  and sst.status=1 ");

                //sb.Append("   and lmt.location_mst_pk = 3")

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and sst.srr_date between to_date('" & validFrom & "','" & dateFormat & "' ) and to_date('" & validTo & " ','" & dateFormat & "')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                //Added by Ajay
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("  GROUP BY cmt.customer_name,  sst.srr_ref_no,  ");
                sb.Append("    usr.user_id, lmt.location_name,  pol.port_name, ");
                sb.Append("    pod.port_name,  sst.srr_date,  sst.valid_to ");
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportSeaSRRFCL()"

        #region "forReportSeaSRRLCL()"

        public void forReportSeaSRRLCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select distinct  cmt.customer_name as \"Customer Name\",sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("       ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk  and sst.status=1 and sst.active=1  ");
                sb.Append("   and sst.cargo_type = 2 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //  sb.Append(" and sst.srr_date between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportSeaSRRLCL()"

        #region "forReportQtnSeaFCL()"

        public void ForReportQtnSeaFCL(string validFrom, string validTo)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("   c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\"     ");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");

                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append("  qtrn.container_type_mst_fk= c.container_type_mst_pk ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=1");
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //  sb.Append(" and qtns.quotation_date between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportQtnSeaFCL()"

        #region "forreportQtnSeaLCL()"

        public void forReportQtnSeaLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("           ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl .trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\"     ");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("   ");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt, ");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt");
                sb.Append("   where ");
                sb.Append("   qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK  ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=2 and qtns.status=2 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forreportQtnSeaLCL()"

        #region "ForReportQtnSeaBBC()"

        public void forReportQtnSeaBBC(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_id as \"Location\",");
                sb.Append("          pol.port_id as \"POL\",");
                sb.Append("       pod.port_id as \"POD\", ");
                sb.Append("           ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl .trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\"     ");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("   ");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt, ");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt");
                sb.Append("   where ");
                sb.Append("   qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK  ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=4 and qtns.status=2 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date(qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "ForReportQtnSeaBBC()"

        #region "forReportSRRAir()"

        public void forReportSRRAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"AOO\",");
                sb.Append("       pod.port_name as \"AOD\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     pol,");
                sb.Append("       port_mst_tbl     pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");

                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = pol.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = pod.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    // sb.Append(" and SRRAIR.SRR_DATE between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or (to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( SRRAIR.VALID_TO,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportSRRAir()"

        #region "forReportQtnAir()"

        public void forReportQtnAir(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO as \"Ref No\",");
                sb.Append("                'Quotation' as \"REF Type\",");
                sb.Append("                usr.user_id as \"User ID\",");
                sb.Append("                lmt.location_name as \"Location\",");
                sb.Append("                pol.port_name as \"AOO\",");
                sb.Append("                pod.port_name as \"AOD\",");
                sb.Append("                (select count(*)");
                sb.Append("                   from BOOKING_TRN BTA");
                sb.Append("                  where BTA.TRANS_REF_NO = MAIN1.QUOTATION_REF_NO) as \"No of Booking\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Expiry DateTime\"");
                sb.Append("");
                sb.Append("  FROM QUOTATION_MST_TBL MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL TRAN,");
                sb.Append("       PORT_MST_TBL      POL,");
                sb.Append("       PORT_MST_TBL      POD,");
                sb.Append("       user_mst_tbl      usr,");
                sb.Append("       location_mst_tbl  lmt,");
                sb.Append("       customer_mst_tbl  cmt ");
                sb.Append("");
                sb.Append(" WHERE ");
                sb.Append("    TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   and cmt.customer_mst_pk = MAIN1.Customer_Mst_Fk and MAIN1.status=2 and MAIN1.BIZ_TYPE = 1 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    // sb.Append(" and MAIN1.quotation_date between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or (to_date(MAIN1.quotation_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportQtnAir()"

        #region "forReportSeaSRRAndQtnFCL()"

        public void forReportSeaSRRAndQtnFCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("   c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   and sst.cargo_type = 1  and sst.status=1 and sst.active=1 ");
                //sb.Append("   and lmt.location_mst_pk = 3")
                sb.Append("   ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and sst.srr_date between to_date('" & validFrom & "','" & dateFormat & "' ) and to_date('" & validTo & " ','" & dateFormat & "')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                //Added by Ajay
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("    union   ");

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("   c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\"     ");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");

                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append(" qtrn.container_type_mst_fk= c.container_type_mst_pk ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=1");
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and qtns.quotation_date between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                //sb.Append("   order by ""REF Type"" desc")
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportSeaSRRAndQtnFCL()"

        #region "forReportSeaSRRAndQtnLCL()"

        public void forReportSeaSRRAndQtnLCL(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct  cmt.customer_name as \"Customer Name\",sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("       ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk  and sst.status=1 and sst.active=1  ");
                sb.Append("   and sst.cargo_type = 2 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    // sb.Append(" and sst.srr_date between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("  union  ");

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("           ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl .trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\"     ");
                sb.Append("   from  ");
                sb.Append("      ");

                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("   ");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt, ");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt");
                sb.Append("   where ");

                sb.Append("   qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK  ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and");
                sb.Append("   qtns.cargo_type=2 and qtns.status=2 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and qtns.quotation_date between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                //sb.Append(" order by ""REF Type"" desc")
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportSeaSRRAndQtnLCL()"

        #region "forReportAirSRRAndQtn()"

        public void forReportAirSRRAndQtn(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"AOO\",");
                sb.Append("       pod.port_name as \"AOD\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     pol,");
                sb.Append("       port_mst_tbl     pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");

                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = pol.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = pod.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    // SRRAIR.Valid_To sb.Append(" and SRRAIR.SRR_DATE between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    sb.Append(" and (to_date('" + validFrom + "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or  to_date('" + validTo + "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or (to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') >= to_date('" + validFrom + "','DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') <= to_date('" + validTo + "','DD/MM/YYYY') ) ) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date(SRRAIR.VALID_TO,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("union ");

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO as \"Ref No\",");
                sb.Append("                'Quotation' as \"REF Type\",");
                sb.Append("                usr.user_id as \"User ID\",");
                sb.Append("                lmt.location_name as \"Location\",");
                sb.Append("                pol.port_name as \"AOO\",");
                sb.Append("                pod.port_name as \"AOD\",");
                sb.Append("                (select count(*)");
                sb.Append("                   from BOOKING_TRN BTA");
                sb.Append("                  where BTA.TRANS_REF_NO = MAIN1.QUOTATION_REF_NO) as \"No of Booking\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Expiry DateTime\"");
                sb.Append("");
                sb.Append("  FROM QUOTATION_MST_TBL MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL TRAN,");
                sb.Append("       PORT_MST_TBL      POL,");
                sb.Append("       PORT_MST_TBL      POD,");
                sb.Append("       user_mst_tbl      usr,");
                sb.Append("       location_mst_tbl  lmt,");
                sb.Append("       customer_mst_tbl  cmt ");
                sb.Append("");
                sb.Append(" WHERE ");
                sb.Append("    TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   and cmt.customer_mst_pk = MAIN1.Customer_Mst_Fk and MAIN1.status=2 and MAIN1.BIZ_TYPE = 1");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //main1.quotation_date + main1.valid_for sb.Append(" and MAIN1.quotation_date between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or (to_date(MAIN1.quotation_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                //sb.Append("  order by ""REF Type"" desc")
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportAirSRRAndQtn()"

        #region "forReportAirSeaSRR"

        public void forReportAirSeaSRR(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("   c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                //Commented by Ajay
                //sb.Append("   and sst.cargo_type = 1")
                //Commented by Ajay
                sb.Append("   and sst.status=1 and sst.active=1 ");

                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and sst.srr_date between to_date('" & validFrom & "','" & dateFormat & "' ) and to_date('" & validTo & " ','" & dateFormat & "')")
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to ,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                //Added by Ajay
                sb.Append("and to_date(sst.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                //Added by Ajay
                sb.Append(StrCondition2);
                //Ended by Ajay
                sb.Append("   union  ");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"AOO\",");
                sb.Append("       pod.port_name as \"AOD\",");
                sb.Append("       0  as \"No of TEU'S\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     pol,");
                sb.Append("       port_mst_tbl     pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");

                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = pol.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = pod.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    // sb.Append(" and SRRAIR.SRR_DATE between to_date('" & validFrom & "','dd/mm/yyyy') and to_date('" & validTo & " ','dd/mm/yyyy')")
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or (to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date(SRRAIR.valid_to,'dd/mm/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append(StrCondition2);
                //sb.Append("  order by ""REF Type"" desc")
                sb.Append("   order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportAirSeaSRR"

        #region "forReportAirSeaQtn"

        public void forReportAirSeaQtn(string validFrom, string validTo, Int32 flag = 1)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            try
            {
                string StrCondition2 = null;
                if (Convert.ToInt32(StrCondition2) == 0)
                {
                    if (flag == 0)
                    {
                        StrCondition2 = " AND 1=2";
                    }
                }

                sb.Append("select distinct cmt.customer_name as \"Customer Name\",qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("    case when qtns.BIZ_TYPE = 2 then SUM(c.teu_factor) else  null end as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\"     ");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append("  qtrn.container_type_mst_fk= c.container_type_mst_pk(+) ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                //Commented by Ajay
                //sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and")
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                //sb.Append("   qtns.cargo_type=1 ")
                //Ended by Ajay
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append(StrCondition2);
                sb.Append(" GROUP BY CMT.CUSTOMER_NAME,  QTNS.QUOTATION_MST_PK, ");
                sb.Append(" QTNS.QUOTATION_REF_NO,  USR.USER_ID,  LMT.LOCATION_NAME, ");
                sb.Append(" POL.PORT_NAME,  QTNS.QUOTATION_DATE,  QTNS.VALID_FOR, ");
                sb.Append(" QTNS.BIZ_TYPE,  QTNS.CARGO_TYPE,  POD.PORT_NAME ");

                //sb.Append("     union ")
                //sb.Append("select distinct cmt.customer_name as ""Customer Name"",")
                //sb.Append("                MAIN1.QUOTATION_REF_NO as ""Ref No"",")
                //sb.Append("                'Quotation' as ""REF Type"",")
                //sb.Append("                usr.user_id as ""User ID"",")
                //sb.Append("                lmt.location_name as ""Location"",")
                //sb.Append("                pol.port_name as ""AOO"",")
                //sb.Append("                pod.port_name as ""AOD"",")
                //sb.Append("                0  as ""No of TEU'S"",")
                //sb.Append("                (select count(*)")
                //sb.Append("                   from BOOKING_TRN BTA")
                //sb.Append("                  where BTA.TRANS_REF_NO = MAIN1.QUOTATION_REF_NO) as ""No of Booking"",")
                //sb.Append("                to_date(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS ""Created DateTime"",")
                //sb.Append("                to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,")
                //sb.Append("                        'dd/MM/yyyy') AS ""Expiry DateTime""")
                //sb.Append("  FROM QUOTATION_MST_TBL MAIN1,")
                //sb.Append("       QUOTATION_DTL_TBL TRAN,")
                //sb.Append("       PORT_MST_TBL      POL,")
                //sb.Append("       PORT_MST_TBL      POD,")
                //sb.Append("       user_mst_tbl      usr,")
                //sb.Append("       location_mst_tbl  lmt,")
                //sb.Append("       customer_mst_tbl  cmt ")
                //sb.Append(" WHERE ")
                //sb.Append("    TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK")
                //sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK")
                //sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK ")
                //sb.Append("   and usr.default_location_fk = lmt.location_mst_pk")
                //sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK")
                //sb.Append("   and cmt.customer_mst_pk = MAIN1.Customer_Mst_Fk and MAIN1.status=2 ")
                //If ((validFrom <> "") And (validTo <> "")) Then
                //    'sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or (to_date(MAIN1.quotation_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                //    sb.Append(vbCrLf & " AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" & validFrom & "',dateformat) ")
                //    sb.Append(vbCrLf & " AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" & validTo & "',dateformat) ")
                //ElseIf Not (validFrom = "") Then
                //    sb.Append(vbCrLf & " AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" & validFrom & "',dateformat) ")
                //ElseIf Not (validTo = "") Then
                //    sb.Append(vbCrLf & " AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" & validTo & "',dateformat) ")
                //End If
                //'Added by Ajay
                //sb.Append("and to_date( MAIN1.quotation_date +  MAIN1.VALID_FOR, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')")
                //'Ended by Ajay
                //sb.Append(StrCondition)
                //sb.Append(StrCondition2)
                sb.Append("  order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportAirSeaQtn"

        #region "forReportAirSeaBoth"

        public void forReportAirSeaBoth(string validFrom, string validTo)
        {
            try
            {
                System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",sst.srr_ref_no as \"REF NO\",");
                sb.Append("       'Special Rate Request' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\",");
                sb.Append("       c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  sst.srr_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("       sst.srr_date as \"Created DateTime\",");
                sb.Append("       sst.valid_to as \"Expiry DateTime\"");
                sb.Append("  from srr_trn_sea_tbl s,");
                sb.Append("       srr_sea_tbl  sst,");
                sb.Append("       port_mst_tbl pol,");
                sb.Append("       port_mst_tbl pod, container_type_mst_tbl c,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt ");
                sb.Append(" where sst.srr_sea_pk = s.srr_sea_fk");
                sb.Append("   and s.port_mst_pol_fk = pol.port_mst_pk  and s.container_type_mst_fk = c.container_type_mst_pk ");
                sb.Append("   and s.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("   and sst.created_by_fk = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = sst.customer_mst_fk ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                //Commented by Ajay'
                //sb.Append("   and sst.cargo_type = 1  and sst.status=1 and sst.active=1 ")
                sb.Append("   and sst.status=1 and sst.active=1 ");
                //Ended by Ajay
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    // sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(sst.srr_date,'DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') or (to_date(sst.srr_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(sst.valid_to,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "','" + dateFormat + "')");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(sst.srr_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "','" + dateFormat + "') ");
                }
                //Added by Ajay
                sb.Append("and to_date( sst.valid_to, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("    union   ");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",qtns.quotation_ref_no as \"REF NO\",");
                sb.Append("       'Quotation' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("          pol.port_name as \"POL\",");
                sb.Append("       pod.port_name as \"POD\", ");
                sb.Append("   c.teu_factor  as \"No of TEU'S\",");
                sb.Append("      ( select count(*) from    BOOKING_TRN fcl  where  qtns.quotation_ref_no= fcl.trans_ref_no)  as \"No of Booking\"  ,");
                sb.Append("         to_date( qtns.quotation_date, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("        to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') AS \"Expiry DateTime\"     ");
                sb.Append("   from  ");
                sb.Append("      QUOTATION_DTL_TBL  qtrn ,");
                sb.Append("    QUOTATION_MST_TBL qtns,");
                sb.Append("       port_mst_tbl           pol,");
                sb.Append("       port_mst_tbl           pod,");
                sb.Append("       location_mst_tbl       lmt,");
                sb.Append("       user_mst_tbl           usr,");
                sb.Append("   customer_mst_tbl cmt,container_type_mst_tbl c ");
                sb.Append("   where ");
                sb.Append(" qtrn.container_type_mst_fk= c.container_type_mst_pk ");
                sb.Append("   and qtrn.port_mst_pol_fk = pol.port_mst_pk");
                sb.Append("   and qtrn.port_mst_pod_fk = pod.port_mst_pk");
                sb.Append("     and qtns.created_by_fk=usr.user_mst_pk");
                sb.Append(" and cmt.customer_mst_pk=qtns.customer_mst_fk and qtns.status=2  ");
                //sb.Append("   and usr.default_location_fk = lmt.location_mst_pk and")
                //Commented by Ajay
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                //sb.Append("   qtns.cargo_type=1")
                //Ended by Ajay
                sb.Append(" and qtrn.QUOTATION_MST_FK=qtns.QUOTATION_MST_PK");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(qtns.quotation_date,'DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') or (to_date(qtns.quotation_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(qtns.quotation_date +  qtns.VALID_FOR,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(qtns.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( qtns.quotation_date +  qtns.VALID_FOR, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("  union  ");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("       SRRAIR.srr_ref_no as \"REF NO\",");
                sb.Append("       'SRR' as \"REF Type\",");
                sb.Append("       usr.user_id as \"User ID\",");
                sb.Append("       lmt.location_name as \"Location\",");
                sb.Append("       pol.port_name as \"AOO\",");
                sb.Append("       pod.port_name as \"AOD\",");
                sb.Append("                0  as \"No of TEU'S\",");
                sb.Append("       (select count(*)");
                sb.Append("          FROM srr_air_tbl SRR, BOOKING_TRN  BTRN");
                sb.Append("         where SRR.SRR_REF_NO = BTRN.TRANS_REF_NO) as \"No of Booking\",");
                sb.Append("       SRRAIR.SRR_DATE as \"Created DateTime\",");
                sb.Append("       SRRAIR.VALID_TO as \"Expiry DateTime\"");
                sb.Append("  FROM srr_trn_air_tbl  SRRTRN,");
                sb.Append("       srr_air_tbl      SRRAIR,");
                sb.Append("       port_mst_tbl     pol,");
                sb.Append("       port_mst_tbl     pod,");
                sb.Append("       location_mst_tbl lmt,");
                sb.Append("       user_mst_tbl     usr,");
                sb.Append("       customer_mst_tbl cmt");
                sb.Append(" where  ");
                sb.Append("    SRRTRN.SRR_AIR_FK = SRRAIR.SRR_AIR_PK");
                sb.Append("      ");
                sb.Append("   and SRRTRN.PORT_MST_POL_FK = pol.port_mst_pk");
                sb.Append("   and SRRTRN.PORT_MST_POD_FK = pod.port_mst_pk");
                sb.Append("      ");
                sb.Append("   and SRRAIR.CREATED_BY_FK = usr.user_mst_pk");
                sb.Append("   and cmt.customer_mst_pk = SRRAIR.CUSTOMER_MST_FK");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND SRRAIR.SRR_APPROVED = 1 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') or (to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(SRRAIR.Valid_To,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(SRRAIR.SRR_DATE,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( SRRAIR.VALID_TO, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("union ");
                sb.Append("select distinct cmt.customer_name as \"Customer Name\",");
                sb.Append("                MAIN1.QUOTATION_REF_NO as \"Ref No\",");
                sb.Append("                'Quotation' as \"REF Type\",");
                sb.Append("                usr.user_id as \"User ID\",");
                sb.Append("                lmt.location_name as \"Location\",");
                sb.Append("                pol.port_name as \"AOO\",");
                sb.Append("                pod.port_name as \"AOD\",");
                sb.Append("                0  as \"No of TEU'S\",");
                sb.Append("                (select count(*)");
                sb.Append("                   from BOOKING_TRN BTA");
                sb.Append("                  where BTA.TRANS_REF_NO = MAIN1.QUOTATION_REF_NO) as \"No of Booking\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE, 'dd/MM/yyyy') AS \"Created DateTime\",");
                sb.Append("                to_date(MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR,");
                sb.Append("                        'dd/MM/yyyy') AS \"Expiry DateTime\"");
                sb.Append("");
                sb.Append("  FROM QUOTATION_MST_TBL MAIN1,");
                sb.Append("       QUOTATION_DTL_TBL TRAN,");
                sb.Append("       PORT_MST_TBL      POL,");
                sb.Append("       PORT_MST_TBL      POD,");
                sb.Append("       user_mst_tbl      usr,");
                sb.Append("       location_mst_tbl  lmt,");
                sb.Append("       customer_mst_tbl  cmt ");
                sb.Append("");
                sb.Append(" WHERE ");
                sb.Append("    TRAN.QUOTATION_MST_FK = MAIN1.QUOTATION_MST_PK");
                sb.Append("   AND POL.PORT_MST_PK = TRAN.PORT_MST_POL_FK");
                sb.Append("   AND POD.PORT_MST_PK = TRAN.PORT_MST_POD_FK ");
                sb.Append("   and usr.default_location_fk = lmt.location_mst_pk");
                sb.Append("   AND USR.USER_MST_PK = MAIN1.CREATED_BY_FK");
                sb.Append("   and cmt.customer_mst_pk = MAIN1.Customer_Mst_Fk and MAIN1.status=2 ");
                if (((!string.IsNullOrEmpty(validFrom)) & (!string.IsNullOrEmpty(validTo))))
                {
                    //sb.Append(" and (to_date('" & validFrom & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or  to_date('" & validTo & "','DD/MM/YYYY') between to_date(MAIN1.quotation_date,'DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') or (to_date(MAIN1.quotation_date,'DD/MM/YYYY') >= to_date('" & validFrom & "','DD/MM/YYYY') and to_date(main1.quotation_date + main1.valid_for,'DD/MM/YYYY') <= to_date('" & validTo & "','DD/MM/YYYY') ) ) ")
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validFrom)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  >= TO_DATE('" + validFrom + "',dateformat) ");
                }
                else if (!(string.IsNullOrEmpty(validTo)))
                {
                    sb.Append(" AND to_date(MAIN1.quotation_date,'DD/MM/YYYY')  <= TO_DATE('" + validTo + "',dateformat) ");
                }
                //Added by Ajay
                sb.Append("and to_date( MAIN1.QUOTATION_DATE + MAIN1.VALID_FOR, 'dd/MM/yyyy') >= to_date(sysdate ,'dd/mm/yyyy')");
                //Ended by Ajay
                sb.Append(StrCondition);
                sb.Append("  order by \"Created DateTime\" desc");
                sbNew = sb.ToString();
                //Manjunath  PTS ID:Sep-02  04/10/2011
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

        #endregion "forReportAirSeaBoth"

        ///'''''''''''-----------------------------
    }
}