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

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    public class Cls_Workflow_Mgr_Report : CommonFeatures
    {
        private long flag;
        public DataSet FetchLocation()
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strsql.Append(" select loc.location_id,loc.location_mst_pk from location_mst_tbl loc where loc.active_flag=1 order by loc.location_id");
            try
            {
                return objwf.GetDataSet(strsql.ToString());
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
           
        }
        public int FetchCustomer(int invpk)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strsql.Append(" select con.customer_mst_fk from consol_invoice_tbl con  where con.consol_invoice_pk = " + invpk);

            try
            {
                return Convert.ToInt32(objwf.ExecuteScaler(strsql.ToString()));
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
           
        }
        public int FetchRulesKey(string refno)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strsql.Append(" select distinct adm.wf_mgr_adm_task_ref_fk from wf_mgr_adm_task_list_tbl ");
            strsql.Append("  adm where adm.wf_mgr_adm_task_ref_nr='" + refno + "' ");
            try
            {
                return Convert.ToInt32(objwf.ExecuteScaler(strsql.ToString()));
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
           
        }
        public DataSet FetchUser()
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strsql.Append(" select users. user_id,users.user_mst_pk from user_mst_tbl users where users.is_activated=1 order by users.user_id");
            try
            {
                return objwf.GetDataSet(strsql.ToString());
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            
        }
        public DataSet FetchActivities(Int32 biztype)
        {
            StringBuilder strsql = new StringBuilder();
            WorkFlow objwf = new WorkFlow();
            strsql.Append("  select act.wf_activity_mst_tbl_pk,");
            strsql.Append(" (case when " + biztype + "=1 then (case when act.wf_activity_name='HBL' or act.wf_activity_name='MBL' then ");
            strsql.Append("  decode(act.wf_activity_name,'HBL','HAWB','MBL','MAWB') else act.wf_activity_name end) ");
            strsql.Append("  ELSE  act.wf_activity_name END)actname   from wf_activity_mst_tbl act ");
            try
            {
                return objwf.GetDataSet(strsql.ToString());
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            
        }
        public DataSet FetchAll(string Doctype, int Biztype, int Process, System.DateTime FromDate, System.DateTime ToDate, string DocRefNo, int Wstatus, string CustomerPk, int UserPk, string LocPk,
        string LocId, string Priority, ref Int32 CurrentPage, ref Int32 TotalPage, int flag, Int32 export)
        {
            StringBuilder SqlMain = new StringBuilder();
            StringBuilder SqlCond = new StringBuilder();
            StringBuilder SqlQuery = new StringBuilder();
            string prior = null;
            string SqlDate = null;
            string enq = null;
            string cust = null;
            string create = null;
            int ch = 0;
            string query = null;
            StringBuilder Sql = new StringBuilder();
            WorkFlow objwf = new WorkFlow();
            string doctypes = null;
            string sql1 = null;
            string sql2 = null;
            DataSet dsmain = new DataSet();
            string FromDateString = FromDate.ToString(dateFormat);
            string ToDateString = ToDate.ToString(dateFormat);
            try
            {
                if (Doctype == "Enquiry_Sea_Exp" & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append("  AND EHDR.ENQUIRY_REF_NO like '%" + DocRefNo + "%'");
                    sql1 = "AND EHDR.ENQUIRY_REF_NO like '%" + DocRefNo + "%'";
                }
                else if (Doctype == "Quotation_Sea_Exp" & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append("  and qhdr.quotation_ref_no like '%" + DocRefNo + "%'");
                    sql1 = "and qhdr.quotation_ref_no like '%" + DocRefNo + "%'";
                }
                else if (Doctype == "Booking_Sea_Exp" & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append("  and bhdr.booking_ref_no like '%" + DocRefNo + "%'");
                }
                else if (Doctype == "JobCard_Sea_Exp" & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append(" AND JHDR.JOBCARD_REF_NO LIKE '%" + DocRefNo + "%'");
                }
                else if ((Doctype == "HBL" | Doctype == "HAWB") & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append(" and HBL.HBL_ref_no like '%" + DocRefNo + "%' ");
                }
                else if ((Doctype == "MBL" | Doctype == "MAWB") & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append(" and MBL.MBL_ref_no like '%" + DocRefNo + "%' ");
                }
                else if (Doctype == "Invoice" & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append("  AND INV.invoice_ref_no like '%" + DocRefNo + "%' ");

                }
                else if (Doctype == "Invoice_Cust_Sea_Exp" & !string.IsNullOrEmpty(DocRefNo))
                {
                    SqlCond.Append("  AND INV.invoice_ref_no like '%" + DocRefNo + "%' ");
                }

                //EHDR.Enquiry_BKG_SEA_PK PK
                switch (Doctype)
                {
                    case "Enquiry_Sea_Exp":
                        doctypes = " EHDR.Enquiry_BKG_SEA_PK PK , EHDR.ENQUIRY_REF_NO REFNO ,";
                        SqlDate = " EHDR.enquiry_date ";
                        ch = 1;
                        enq = "EHDR.Enquiry_BKG_SEA_PK";
                        prior = "EHDR.ENQUIRY_REF_NO ";
                        create = "EHDR.CREATED_BY_FK";
                        cust = "EHDR.customer_mst_fk";

                        break;
                    case "Quotation_Sea_Exp":
                        doctypes = " QHDR.QUOTATION_MST_PK PK, qhdr.quotation_ref_no refno, ";
                        SqlDate = " QHDR.quotation_date ";
                        ch = 2;
                        enq = "QHDR.QUOTATION_MST_PK";
                        prior = "qhdr.quotation_ref_no ";
                        create = "QHDR.CREATED_BY_FK";
                        cust = "QHDR.customer_mst_fk";
                        break;
                    case "Booking_Sea_Exp":
                        doctypes = " bhdr.BOOKING_MST_PK PK , bhdr.booking_ref_no refno, ";
                        SqlDate = " bHDR.booking_date ";
                        ch = 3;
                        prior = "bhdr.booking_ref_no ";
                        create = "BHDR.CREATED_BY_FK";
                        break;
                    case "JobCard_Sea_Exp":
                        doctypes = " JHDR.Job_Card_Trn_Pk PK , JHDR.JOBCARD_REF_NO REFNO, ";
                        SqlDate = " JHDR.JOBCARD_DATE ";
                        ch = 4;
                        prior = "JHDR.JOBCARD_REF_NO ";
                        create = "JHDR.CREATED_BY_FK";
                        break;
                    case "HBL":
                    case "HAWB":
                        doctypes = " HBL.HBL_exp_tbl_pk PK , HBL.HBL_ref_no refno, ";
                        SqlDate = " HBL.HBL_DATE ";
                        prior = "HBL.HBL_ref_no ";
                        ch = 6;
                        create = "HBL.CREATED_BY_FK";
                        break;
                    case "MBL":
                    case "MAWB":
                        doctypes = " MBL.MBL_exp_tbl_pk PK , MBL.MBL_ref_no refno, ";
                        SqlDate = " MBL.MBL_DATE ";
                        ch = 7;
                        prior = "MBL.MBL_ref_no ";
                        create = "MBL.CREATED_BY_FK";
                        break;
                    case "Invoice":
                        doctypes = " INV.consol_invoice_pk PK, INV.INVOICE_REF_NO refno , ";
                        SqlDate = " inv.invoice_date ";
                        prior = "INV.INVOICE_REF_NO ";
                        ch = 4;
                        create = "INV.CREATED_BY_FK";
                        break;
                    case "Invoice_Cust_Sea_Exp":
                        doctypes = " INV.consol_invoice_pk PK, INV.INVOICE_REF_NO refno , ";
                        SqlDate = " inv.invoice_date ";
                        prior = "INV.INVOICE_REF_NO ";
                        ch = 4;
                        create = "INV.CREATED_BY_FK";
                        break;
                }

                if (Wstatus == 1)
                {
                    switch (Doctype)
                    {
                        case "Enquiry_Sea_Exp":
                            Sql.Append(" and EHDR.ENQUIRY_REF_NO is not null ");
                            sql2 = "and EHDR.ENQUIRY_REF_NO is not null";
                            break;
                        case "Quotation_Sea_Exp":
                            Sql.Append(" and qhdr.status in (2,4) ");
                            sql2 = "and qhdr.status in (2,4)";
                            break;
                        case "Booking_Sea_Exp":
                            Sql.Append(" and bhdr.status = 2 ");
                            break;
                        case "JobCard_Sea_Exp":
                            Sql.Append(" and JHDR.job_card_status =2 ");
                            break;
                        case "HBL":
                        case "HAWB":
                            Sql.Append(" and HBL.HBL_ref_no is not null ");
                            break;
                        case "MBL":
                        case "MAWB":
                            Sql.Append(" and MBL.MBL_ref_no is not null ");
                            break;
                        case "Invoice":
                            Sql.Append(" and INV.INVOICE_REF_NO is not null ");
                            break;
                        case "Invoice_Cust_Sea_Exp":
                            Sql.Append(" and INV.INVOICE_REF_NO is not null ");
                            break;
                    }
                }
                else if (Wstatus == 2)
                {
                    switch (Doctype)
                    {
                        case "Enquiry_Sea_Exp":
                            Sql.Append(" and EHDR.ENQUIRY_REF_NO is  null ");
                            sql2 = "and EHDR.ENQUIRY_REF_NO is  null";
                            break;
                        case "Quotation_Sea_Exp":
                            Sql.Append(" and qhdr.status = 1 ");
                            sql2 = "and qhdr.status = 1";
                            break;
                        case "Booking_Sea_Exp":
                            Sql.Append(" and bhdr.status = 1 ");
                            break;
                        case "JobCard_Sea_Exp":
                            Sql.Append(" and JHDR.job_card_status =1 ");
                            break;
                        case "HBL":
                        case "HAWB":
                            Sql.Append(" and HBL.HBL_ref_no is null ");
                            break;
                        case "MBL":
                        case "MAWB":
                            Sql.Append(" and MBL.MBL_ref_no is null ");
                            break;
                        case "Invoice":
                            Sql.Append(" and INV.INVOICE_REF_NO is null ");
                            break;
                        case "Invoice_Cust_Sea_Exp":
                            Sql.Append(" and INV.INVOICE_REF_NO is null ");
                            break;
                    }
                }

                //If Biztype = 2 And Process = 1 Then
                if (Doctype == "Enquiry_Sea_Exp" | Doctype == "Quotation_Sea_Exp")
                {
                    SqlMain.Append(" select distinct " + doctypes);
                    SqlMain.Append(" (select cmt.customer_name from customer_mst_tbl cmt where cmt.customer_mst_pk=EHDR.CUSTOMER_MST_FK) CustID,");
                    SqlMain.Append(" usr.user_id users, (select loc.location_name   from location_mst_tbl loc where loc.location_mst_pk=usr.default_location_fk) locations,");
                    SqlMain.Append(" EHDR.Enquiry_BKG_SEA_PK ENQPK,(CASE WHEN EHDR.ENQUIRY_REF_NO IS NOT NULL THEN 'Completed' ELSE '' END) Enquiry,");
                    SqlMain.Append(" QHDR.quotation_ref_no QTREFNO, QHDR.QUOTATION_MST_PK QTPK,");
                    SqlMain.Append(" (CASE WHEN QHDR.QUOTATION_MST_PK IS NOT NULL THEN ");
                    SqlMain.Append(" decode(QHDR.STATUS,1,'In Process',2,'Completed',3,'Cancelled',4,'Completed')  ELSE  '' END) Quotation,");
                    SqlMain.Append(" null BOOKINGPK,null Booking, null jobpk,null JOBREFNO,null JobCard,null hblPK,null HBLREFNO,null HBL,");
                    SqlMain.Append(" null mblPK,null MBLREFNO,null MBL,null INVPK,null invoice,");
                    SqlMain.Append(" decode((select adm.wf_mgr_adm_task_priority from Wf_Mgr_Adm_Task_List_Tbl adm where adm.wf_mgr_adm_task_ref_nr=" + prior + "),");
                    SqlMain.Append(" 1,'Low',2,'High',3,'Critical') priority ");
                    SqlMain.Append(" from enquiry_bkg_SEA_tbl EHDR,QUOTATION_MST_TBL QHDR,QUOTATION_DTL_TBL q,BOOKING_TRN b,user_mst_tbl usr ");
                    if (Doctype == "Enquiry_Sea_Exp")
                    {
                        SqlMain.Append("where EHDR.enquiry_ref_no=q.trans_ref_no(+) ");
                    }
                    else
                    {
                        SqlMain.Append("   where EHDR.enquiry_ref_no(+)=q.trans_ref_no ");
                    }
                    SqlMain.Append("  and QHDR.QUOTATION_MST_PK(+)=q.QUOTATION_MST_FK and b.trans_ref_no(+)=QHDR.quotation_ref_no ");
                    SqlMain.Append(" and " + create + " =usr.user_mst_pk ");
                    if (UserPk != 0)
                    {
                        SqlMain.Append(" AND usr.USER_MST_PK= " + UserPk);
                    }
                    if (Convert.ToInt32(LocPk) != 0)
                    {
                        SqlMain.Append(" AND usr.DEFAULT_LOCATION_FK = " + LocPk + " ");
                    }
                    try
                    {
                        CustomerPk = Convert.ToString(CustomerPk);
                        if (Convert.ToInt32(CustomerPk )!= 0)
                        {
                            SqlMain.Append(" AND " + cust + "  in (" + CustomerPk + ")");
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                    //If Not CustomerPk = 0 Then
                    //    SqlMain.Append(vbCrLf & " AND " & cust & "  in (" & CustomerPk & ")")
                    //End If
                    SqlMain.Append(" and b.trans_ref_no is null ");
                    SqlMain.Append(sql1);
                    SqlMain.Append(sql2);
                    SqlMain.Append(" AND to_char(" + SqlDate + ") BETWEEN to_date('" + FromDateString + "','" + dateFormat + "') and ");
                    SqlMain.Append(" to_date('" + ToDateString + "','" + dateFormat + "') ");
                    SqlMain.Append(" union ");
                }
                SqlMain.Append("  SELECT distinct  ");
                SqlMain.Append(doctypes + " CMT.customer_name CustID, ");
                SqlMain.Append("  umt.user_id users,(select loc.location_name   from location_mst_tbl loc where loc.location_mst_pk=UMT.DEFAULT_LOCATION_FK) locations ,");
                SqlMain.Append("  EHDR.Enquiry_BKG_SEA_PK ENQPK,(CASE WHEN EHDR.ENQUIRY_REF_NO IS NOT NULL THEN ");
                SqlMain.Append(" 'Completed' ELSE '' END) Enquiry,qhdr.quotation_ref_no QTREFNO, QUOTATION_MST_PK QTPK,");
                SqlMain.Append(" (CASE WHEN QHDR.QUOTATION_MST_PK IS NOT NULL THEN ");
                SqlMain.Append("  decode(QHDR.STATUS,1,'In Process',2,'Completed',3,'Cancelled',4,'Completed')  ELSE  '' END) Quotation,");
                SqlMain.Append("  BHDR.BOOKING_MST_PK BOOKINGPK,");
                SqlMain.Append("  (CASE WHEN bhdr.BOOKING_MST_PK IS NOT NULL THEN ");
                SqlMain.Append("  decode(bHDR.Status,1,'In Process',2,'Completed',3,'Cancelled')  ELSE '' END) Booking,");
                SqlMain.Append("  jhdr.Job_Card_Trn_Pk jobpk ,JHDR.JOBCARD_REF_NO JOBREFNO,");
                SqlMain.Append(" (CASE WHEN  JHDR.Job_Card_Trn_Pk IS NOT NULL and JHDR.JOB_CARD_CLOSED_ON IS NOT NULL  THEN ");
                SqlMain.Append(" 'Completed' else (case when JHDR.Job_Card_Trn_Pk IS  NULL  then '' else 'In Process' end) end) JobCard, HBL_exp_tbl_pk hblPK, HBL.HBL_ref_no HBLREFNO,");
                SqlMain.Append("  (CASE WHEN HBL.HBL_exp_tbl_pk is not null then 'Completed' else '' end) HBL,");
                SqlMain.Append("  MBL_exp_tbl_pk mblPK , MBL.MBL_ref_no MBLRFNO, (CASE WHEN MBL.MBL_exp_tbl_pk is not null then");
                SqlMain.Append("  'Completed' else '' end) MBL,");

                SqlMain.Append("  INV.consol_invoice_pk INVPK, ");
                SqlMain.Append(" ( case when INV.invoice_ref_no is not null then 'Completed' end) invoice, ");
                SqlMain.Append(" decode((select distinct adm.wf_mgr_adm_task_priority from Wf_Mgr_Adm_Task_List_Tbl adm where adm.wf_mgr_adm_task_ref_nr=" + prior + "),");
                SqlMain.Append(" 1,'Low',2,'High',3,'Critical') priority ");

                SqlMain.Append("  FROM MBL_EXP_TBL MBL, HBL_EXP_TBL  HBL,JOB_CARD_TRN  JHDR,");
                SqlMain.Append("  USER_MST_TBL  UMT,BOOKING_MST_TBL BHDR, BOOKING_TRN   BTRN,");
                SqlMain.Append("  QUOTATION_MST_TBL QHDR,QUOTATION_DTL_TBL QTRN,ENQUIRY_BKG_SEA_TBL EHDR,");
                SqlMain.Append("  CUSTOMER_MST_TBL  CMT,consol_invoice_tbl INV , consol_invoice_trn_tbl CTRN ");

                SqlMain.Append(" WHERE BHDR.BOOKING_MST_PK = JHDR.BOOKING_MST_FK(+) AND JHDR.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+) ");
                SqlMain.Append(" AND JHDR.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+) AND BTRN.BOOKING_MST_FK(+) = BHDR.BOOKING_MST_PK ");
                SqlMain.Append(" AND BHDR.CUST_CUSTOMER_MST_FK =  CMT.CUSTOMER_MST_PK(+) AND BTRN.TRANS_REF_NO= QHDR.QUOTATION_REF_NO(+)");
                SqlMain.Append(" AND QTRN.QUOTATION_MST_FK(+) =  QHDR.QUOTATION_MST_PK ");
                SqlMain.Append(" AND QTRN.TRANS_REF_NO= EHDR.ENQUIRY_REF_NO(+) AND " + create + "= UMT.USER_MST_PK  ");
                try
                {
                    CustomerPk = Convert.ToString(CustomerPk);
                    if (Convert.ToInt32(CustomerPk) != 0)
                    {
                        SqlMain.Append(" AND CMT.Customer_Mst_Pk in (" + CustomerPk + ")");
                    }
                }
                catch (Exception ex)
                {
                }
                //If Not CustomerPk = 0 Then
                //    SqlMain.Append(vbCrLf & " AND CMT.Customer_Mst_Pk in (" & CustomerPk & ")")
                //End If
                if (UserPk != 0)
                {
                    SqlMain.Append(" AND UMT.USER_MST_PK= " + UserPk);
                }
                if (Convert.ToInt32(LocPk) != 0)
                {
                    SqlMain.Append(" AND UMT.DEFAULT_LOCATION_FK = " + LocPk + " ");
                }
                //SqlMain.Append(vbCrLf & " AND UMT.USER_MST_PK= " & UserPk)
                SqlMain.Append(" AND INV.CONSOL_INVOICE_PK(+)=CTRN.CONSOL_INVOICE_FK  AND CTRN.JOB_CARD_FK(+)=JHDR.Job_Card_Trn_Pk ");
                SqlMain.Append(" AND to_char(" + SqlDate + ") BETWEEN to_date('" + FromDateString + "','" + dateFormat + "') and ");
                SqlMain.Append(" to_date('" + ToDateString + "','" + dateFormat + "') ");
                SqlMain.Append(SqlCond.ToString());
                SqlMain.Append(Sql.ToString());

                //If flag = 0 Then
                //    SqlMain.Append(" AND 1=2 ")
                //End If

                SqlQuery.Append(" SELECT MAINQ.*  FROM (SELECT ROWNUM AS SlNr, Q.* ");
                SqlQuery.Append(" FROM (SELECT * " + " FROM ( " + SqlMain.ToString());

                SqlQuery.Append(" )) Q ) MAINQ  ");


                if (Biztype == 1 & Process == 1)
                {
                    //SqlMain.Replace("MBL", "MAWB")
                    //SqlMain.Replace("HBL", "HAWB")
                    //SqlMain.Replace("BOOKING_TRN", "booking_trn_air")
                    //SqlMain.Replace("QUOTATION_DTL_TBL", "quotation_trn_air")
                    //SqlMain.Replace("SEA", "AIR")

                    //SqlQuery.Replace("MBL", "MAWB")
                    //SqlQuery.Replace("HBL", "HAWB")
                    //SqlQuery.Replace("BOOKING_TRN", "booking_trn_air")
                    //SqlQuery.Replace("QUOTATION_DTL_TBL", "quotation_trn_air")
                    //SqlQuery.Replace("SEA", "AIR")
                }

                Int32 TotalRecords = default(Int32);
                Int32 last = default(Int32);
                Int32 start = default(Int32);
                TotalRecords = Convert.ToInt32(objwf.ExecuteScaler("select count(*) from ( " + SqlMain.ToString() + " )"));
                //'Get the Total Pages
                //TotalPage = TotalRecords \ RecordsPerPage
                //If TotalRecords Mod RecordsPerPage <> 0 Then
                //    TotalPage += 1
                //End If
                //If CurrentPage > TotalPage Then CurrentPage = 1
                //If TotalRecords = 0 Then CurrentPage = 0

                //'Get the last page and start page
                //last = CurrentPage * RecordsPerPage
                //start = (CurrentPage - 1) * RecordsPerPage + 1

                start = (CurrentPage * RecordsPerPage) + 1;
                last = TotalRecords;
                if (start > last)
                {
                    start = ((CurrentPage - 1) * RecordsPerPage) + 1;
                }
                SqlQuery.Append(" WHERE SlNr BETWEEN " + start + " AND " + last + " ");
                dsmain = objwf.GetDataSet(SqlQuery.ToString());

                int i = 0;
                int K = 0;
                //30:
				if ((dsmain != null))
                {
                    for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                    {
                        for (K = 0; K <= dsmain.Tables[0].Rows.Count - 1; K++)
                        {
                            if (!((dsmain.Tables[0].Rows[i]["REFNO"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["REFNO"].ToString())))
                            {
                                if (!((dsmain.Tables[0].Rows[K]["REFNO"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[K]["REFNO"].ToString())))
                                {
                                    if (dsmain.Tables[0].Rows[K]["REFNO"] == dsmain.Tables[0].Rows[i]["REFNO"] & dsmain.Tables[0].Rows[K]["SLNR"] != dsmain.Tables[0].Rows[i]["SLNR"] & dsmain.Tables[0].Rows[i]["PK"] == dsmain.Tables[0].Rows[K]["PK"])
                                    {
                                        dsmain.Tables[0].Rows[K]["PK"] = Convert.ToString(dsmain.Tables[0].Rows[K]["PK"]) + 100;
                                        dsmain.AcceptChanges();
                                        //goto 30;
                                    }
                                }
                            }
                        }
                    }
                }

                string strenqpk = null;
                string strquopk = null;
                string strjobpk = null;
                string strbookpk = null;
                string strhblpk = null;
                string strmblpk = null;
                string strinvpk = null;
                if ((dsmain != null))
                {
                    for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                    {
                        if (!((dsmain.Tables[0].Rows[i]["ENQPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["ENQPK"].ToString())))
                        {
                            if ((strenqpk == null))
                            {
                                strenqpk = dsmain.Tables[0].Rows[i]["ENQPK"].ToString();
                            }
                            else
                            {
                                strenqpk += "," + dsmain.Tables[0].Rows[i]["ENQPK"];
                            }
                        }

                        if (!((dsmain.Tables[0].Rows[i]["QTPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["QTPK"].ToString())))
                        {
                            if ((strquopk == null))
                            {
                                strquopk = dsmain.Tables[0].Rows[i]["QTPK"].ToString();
                            }
                            else
                            {
                                strquopk += "," + dsmain.Tables[0].Rows[i]["QTPK"];
                            }
                        }

                        if (!((dsmain.Tables[0].Rows[i]["BOOKINGPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["BOOKINGPK"].ToString())))
                        {
                            if ((strbookpk == null))
                            {
                                strbookpk = dsmain.Tables[0].Rows[i]["BOOKINGPK"].ToString();
                            }
                            else
                            {
                                strbookpk += "," + dsmain.Tables[0].Rows[i]["BOOKINGPK"];
                            }
                        }

                        if (!((dsmain.Tables[0].Rows[i]["JOBPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["JOBPK"].ToString())))
                        {
                            if ((strjobpk == null))
                            {
                                strjobpk = dsmain.Tables[0].Rows[i]["JOBPK"].ToString();
                            }
                            else
                            {
                                strjobpk += "," + dsmain.Tables[0].Rows[i]["JOBPK"];
                            }
                        }

                        if (!((dsmain.Tables[0].Rows[i]["HBLPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["HBLPK"].ToString())))
                        {
                            if ((strhblpk == null))
                            {
                                strhblpk = dsmain.Tables[0].Rows[i]["HBLPK"].ToString();
                            }
                            else
                            {
                                strhblpk += "," + dsmain.Tables[0].Rows[i]["HBLPK"];
                            }
                        }

                        if (!((dsmain.Tables[0].Rows[i]["MBLPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["MBLPK"].ToString())))
                        {
                            if ((strmblpk == null))
                            {
                                strmblpk = dsmain.Tables[0].Rows[i]["MBLPK"].ToString();
                            }
                            else
                            {
                                strmblpk += "," + dsmain.Tables[0].Rows[i]["MBLPK"];
                            }
                        }

                        if (!((dsmain.Tables[0].Rows[i]["INVPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["INVPK"].ToString())))
                        {
                            if ((strinvpk == null))
                            {
                                strinvpk = dsmain.Tables[0].Rows[i]["INVPK"].ToString();
                            }
                            else
                            {
                                strinvpk += "," + dsmain.Tables[0].Rows[i]["INVPK"];
                            }
                        }
                    }
                    if (Doctype == "Quotation_Sea_Exp" | Doctype == "Enquiry_Sea_Exp")
                    {
                        for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                        {
                            dsmain.Tables[0].Rows[i]["PK"] = i + 1;
                        }
                        dsmain.AcceptChanges();
                    }
                }
                dsmain.Tables.Add(FetchChild(Biztype, Process, Convert.ToString(UserPk), LocPk, strenqpk, strquopk, strbookpk, strjobpk, strhblpk, strmblpk,
                strinvpk));

                Int32 j = default(Int32);
                int count = 0;
                int chrow = 0;
                int qrow = 0;
                Int32 cho = default(Int32);
                Int32 brow = default(Int32);
                Int32 jrow = default(Int32);
                Int32 hrow = default(Int32);
                Int32 mrow = default(Int32);
                Int32 manual = default(Int32);
                Int32 manualq = default(Int32);
                for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                {
                    count = 0;
                    qrow = 0;
                    chrow = 0;
                    brow = 0;
                    jrow = 0;
                    hrow = 0;
                    mrow = 0;
                    cho = 0;
                    manual = 0;
                    manualq = 0;
                    if (i > 0)
                    {
                        for (K = i - 1; K >= 0; K += -1)
                        {
                            if (dsmain.Tables[0].Rows[i]["refno"] == dsmain.Tables[0].Rows[K]["refno"])
                            {
                                if (!(string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["BOOKINGPK"].ToString()) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[K]["BOOKINGPK"].ToString())))
                                {
                                    cho = 1;
                                }
                            }
                        }
                    }
                    for (j = 0; j <= dsmain.Tables[1].Rows.Count - 1; j++)
                    {
                        if (!((dsmain.Tables[0].Rows[i]["ENQPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["ENQPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "ENQPK" & dsmain.Tables[0].Rows[i]["ENQPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1 & qrow == 0)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    qrow = 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }
                        //manual between enquiry and quotation
                        if (!((dsmain.Tables[0].Rows[i]["ENQPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["ENQPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "Manual" & dsmain.Tables[0].Rows[i]["ENQPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }
                        //end
                        if (!((dsmain.Tables[0].Rows[i]["QTPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["QTPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "QTPK" & dsmain.Tables[0].Rows[i]["QTPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1 & chrow == 0)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    chrow = 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }

                        //manual between quotation and booking
                        if (!((dsmain.Tables[0].Rows[i]["QTPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["QTPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "Manual" & dsmain.Tables[0].Rows[i]["QTPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }
                        //end

                        if (!((dsmain.Tables[0].Rows[i]["BOOKINGPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["BOOKINGPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "BOOKINGPK" & dsmain.Tables[0].Rows[i]["BOOKINGPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1 & brow == 0)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    brow = 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }
                        //manual between booking and jobcard
                        if (!((dsmain.Tables[0].Rows[i]["BOOKINGPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["BOOKINGPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "Manual" & dsmain.Tables[0].Rows[i]["BOOKINGPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }

                        if (!((dsmain.Tables[0].Rows[i]["JOBPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["JOBPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "JOBPK" & dsmain.Tables[0].Rows[i]["JOBPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1 & jrow == 0)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    jrow = 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }
                        if (!((dsmain.Tables[0].Rows[i]["HBLPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["HBLPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "HBLPK" & dsmain.Tables[0].Rows[i]["HBLPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1 & hrow == 0)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    hrow = 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }
                        if (!((dsmain.Tables[0].Rows[i]["MBLPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["MBLPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "MBLPK" & dsmain.Tables[0].Rows[i]["MBLPK"] == dsmain.Tables[1].Rows[j]["PK"] & cho == 0)
                            {
                                if (Convert.ToInt32(dsmain.Tables[1].Rows[j]["FK"]) == -1 & mrow == 0)
                                {
                                    dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                    count = count + 1;
                                    mrow = 1;
                                    dsmain.Tables[1].Rows[j]["SLNR"] = count;
                                }
                            }
                        }
                        if (!((dsmain.Tables[0].Rows[i]["INVPK"] == null) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["INVPK"].ToString())))
                        {
                            if (dsmain.Tables[1].Rows[j]["NAMES"] == "INVPK" & dsmain.Tables[0].Rows[i]["INVPK"] == dsmain.Tables[1].Rows[j]["PK"])
                            {
                                dsmain.Tables[1].Rows[j]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                count = count + 1;
                                dsmain.Tables[1].Rows[j]["SLNR"] = count;
                            }
                        }
                    }
                }
                //To avoid duplicate in a parant row when we generate more than one booking with same quotation no.
                if ((dsmain != null))
                {
                    for (i = 0; i <= dsmain.Tables[1].Rows.Count - 1; i++)
                    {
                        if (Convert.ToInt32(dsmain.Tables[1].Rows[i]["FK"]) == -1)
                        {
                            dsmain.Tables[1].Rows[i].Delete();
                        }
                    }
                    dsmain.AcceptChanges();
                    //To avoid duplicate in a parant row when we generate more than invoice for same JOBCARD or Booking
                    for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                    {
                        for (j = 0; j <= dsmain.Tables[0].Rows.Count - 1; j++)
                        {
                            if (Doctype != "Invoice")
                            {
                                if (!(string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["INVPK"].ToString()) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[j]["INVPK"].ToString())))
                                {
                                    if (!(string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["BOOKINGPK"].ToString()) | string.IsNullOrEmpty(dsmain.Tables[0].Rows[j]["BOOKINGPK"].ToString())))
                                    {
                                        if (dsmain.Tables[0].Rows[i]["INVPK"] != dsmain.Tables[0].Rows[j]["INVPK"] & dsmain.Tables[0].Rows[i]["BOOKINGPK"] == dsmain.Tables[0].Rows[j]["BOOKINGPK"])
                                        {
                                            dsmain.Tables[0].Rows[j]["BOOKINGPK"] = -1;
                                            if (Doctype == "Quotation_Sea_Exp" | Doctype == "Enquiry_Sea_Exp")
                                            {
                                                for (K = 0; K <= dsmain.Tables[1].Rows.Count - 1; K++)
                                                {
                                                    if (!(string.IsNullOrEmpty(dsmain.Tables[1].Rows[K]["PK"].ToString())))
                                                    {
                                                        if (dsmain.Tables[1].Rows[K]["PK"] == dsmain.Tables[0].Rows[j]["INVPK"])
                                                        {
                                                            dsmain.Tables[1].Rows[K]["FK"] = dsmain.Tables[0].Rows[i]["PK"];
                                                        }
                                                    }
                                                }
                                            }
                                            break; // TODO: might not be correct. Was : Exit For
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                    {
                        if (!(string.IsNullOrEmpty(dsmain.Tables[0].Rows[i]["BOOKINGPK"].ToString())))
                        {
                            if (Convert.ToInt32(dsmain.Tables[0].Rows[i]["BOOKINGPK"]) == -1)
                            {
                                dsmain.Tables[0].Rows[i].Delete();
                            }
                        }
                    }
                    Int32 rowc = 0;
                    dsmain.AcceptChanges();
                    for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                    {
                        count = 0;
                        rowc = rowc + 1;
                        dsmain.Tables[0].Rows[i]["SLNR"] = rowc;
                        for (j = 0; j <= dsmain.Tables[1].Rows.Count - 1; j++)
                        {
                            if (dsmain.Tables[0].Rows[i]["PK"] == dsmain.Tables[1].Rows[j]["FK"])
                            {
                                count = count + 1;
                                dsmain.Tables[1].Rows[j]["SLNR"] = count;
                            }
                        }
                    }
                    dsmain.AcceptChanges();
                    if (export == 0)
                    {
                        TotalPage = dsmain.Tables[0].Rows.Count / RecordsPerPage;
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
                        //10:

						i = 0;
                        for (i = 0; i <= dsmain.Tables[0].Rows.Count - 1; i++)
                        {
                            if (!(Convert.ToInt32(dsmain.Tables[0].Rows[i]["SLNR"] )>= start & Convert.ToInt32(dsmain.Tables[0].Rows[i]["SLNR"]) <= last))
                            {
                                //dsmain.Tables(0).Rows(i).Delete()
                                for (j = 0; j <= dsmain.Tables[1].Rows.Count - 1; j++)
                                {
                                    if (dsmain.Tables[0].Rows[i]["PK"] == dsmain.Tables[1].Rows[j]["FK"])
                                    {
                                        dsmain.Tables[1].Rows[j].Delete();
                                    }
                                }
                                dsmain.Tables[0].Rows[i].Delete();
                                dsmain.AcceptChanges();
                                //goto 10;
                            }
                        }
                    }
                    TotalPage = dsmain.Tables[0].Rows.Count / RecordsPerPage;
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
                }
                dsmain.AcceptChanges();
                try
                {
                    DataRelation CONTRel = null;
                    CONTRel = new DataRelation("CONTRelation", dsmain.Tables[0].Columns["PK"], dsmain.Tables[1].Columns["FK"], true);
                    CONTRel.Nested = true;
                    dsmain.Relations.Add(CONTRel);
                }
                catch (Exception ex)
                {
                }
                return dsmain;
            }
            catch (OracleException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
           
        }
        private string GetWFDeadLine(int WF_RulesIntMstFk)
        {
            StringBuilder sb = new StringBuilder(5000);
            return "";
        }

        public DataTable FetchChild(int biztype, int process, string userpk, string locpk, string strenqpk, string strquopk, string strbkgpk, string strjobpk, string strhblpk, string strmblpk,
        string strinvpk)
        {
            WorkFlow objwf = new WorkFlow();
            StringBuilder sqlmain = new StringBuilder();
            string doctype1 = null;
            string doctype2 = null;
            try
            {
                strenqpk = ((strenqpk == null) ? "0" : strenqpk);
                strquopk = ((strquopk == null) ? "0" : strquopk);
                strbkgpk = ((strbkgpk == null) ? "0" : strbkgpk);
                strjobpk = ((strjobpk == null) ? "0" : strjobpk);
                strhblpk = ((strhblpk == null) ? "0" : strhblpk);
                strmblpk = ((strmblpk == null) ? "0" : strmblpk);
                strinvpk = ((strinvpk == null) ? "0" : strinvpk);

                if (biztype == 2)
                {
                    doctype1 = "HBL";
                    doctype2 = "MBL";
                }
                else if (biztype == 1)
                {
                    doctype1 = "HAWB";
                    doctype2 = "MAWB";
                }
                else
                {
                    doctype1 = "HBL/HAWB";
                    doctype2 = "MBL/MAWB";
                }

                sqlmain.Append("SELECT ROWNUM AS SLNR, Q.*");
                sqlmain.Append("  FROM (SELECT 'ENQPK' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               ENQ.ENQUIRY_BKG_SEA_PK PK,");
                sqlmain.Append("               'Enquiry' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = ENQ.CREATED_BY_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = ENQ.CREATED_BY_FK)) LOCID,");
                sqlmain.Append("               ENQ.ENQUIRY_REF_NO DOCREFNO,");
                sqlmain.Append("               ENQ.ENQUIRY_REF_NO DOCREFNOS,");
                sqlmain.Append("               'Completed' STATUS,");
                sqlmain.Append("               NULL STARTTIME,");
                sqlmain.Append("               ENQ.CREATED_DT ENDTIME,");
                sqlmain.Append("               (SELECT DISTINCT (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE,0)||'Days ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)||'Hours ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)||'Mins' ELSE '' END) ");
                //sqlmain.Append("                       DECODE(WK.WF_RULES_INT_DEADLINE_MODE,")
                //sqlmain.Append("                              1,")
                //sqlmain.Append("                              'Mins',")
                //sqlmain.Append("                              2,")
                //sqlmain.Append("                              'hrs',")
                //sqlmain.Append("                              3,")
                //sqlmain.Append("                              'days')")
                sqlmain.Append("                  FROM WORKFLOW_RULES_INT_CONFIG_TBL WK,WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                 WHERE WK.WF_RULES_INT_MST_FK = 35 AND WK.WF_RULES_INT_CONFIG_PK = WL.WORKFLOW_RULES_INT_CONFIG_FK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME(ENQ.CREATED_DT,");
                sqlmain.Append("                                  ENQ.LAST_MODIFIED_DT,");
                sqlmain.Append("                                  2,");
                sqlmain.Append("                                  (SELECT DISTINCT FLOOR(NVL(WK.WF_RULES_INT_DEADLINE,0)*24*60+NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)*60+NVL(WK.WF_RULES_INT_DEADLINE_MINS,0))");
                sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK,WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 35 AND WK.WF_RULES_INT_CONFIG_PK = WL.WORKFLOW_RULES_INT_CONFIG_FK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),1,");
                //sqlmain.Append("                                  (SELECT DISTINCT WK.WF_RULES_INT_DEADLINE_MODE")
                //sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK,WORKFLOW_RULES_INT_APPL_TBL   WL")
                //sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 35 AND WK.WF_RULES_INT_CONFIG_PK = WL.WORKFLOW_RULES_INT_MST_FK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),")
                sqlmain.Append("                                  2)) DELAY,");
                sqlmain.Append("               DECODE((SELECT WKM.WF_RULES_INT_PRIORITY");
                sqlmain.Append("                        FROM WORKFLOW_RULES_INT_MST_TBL WKM");
                sqlmain.Append("                       WHERE WKM.WF_RULES_INT_MST_TBL_PK = 35),");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               1 ORDERS");
                sqlmain.Append("          FROM ENQUIRY_BKG_SEA_TBL ENQ,USER_MST_TBL UMT ");
                sqlmain.Append("         WHERE ENQ.ENQUIRY_BKG_SEA_PK IN ( " + strenqpk + ")");
                sqlmain.Append("          AND ENQ.CREATED_BY_FK = UMT.USER_MST_PK");
                sqlmain.Append("        UNION");
                sqlmain.Append("        SELECT 'Manual' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               ENQ.ENQUIRY_BKG_SEA_PK PK,");
                sqlmain.Append("               'Manual' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = ADM.WF_MGR_ADM_TASK_CUSER_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_ID");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = ADM.WF_MGR_ADM_TASK_CUSER_FK)) LOCID,");
                sqlmain.Append("               TASK.DOC_REF_NO DOCREFNO,");
                sqlmain.Append("               TASK.DOC_REF_NO DOCREFNOS,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN ADM.WF_MGR_ADM_TASK_COMPLETED_ON IS NULL THEN");
                sqlmain.Append("                  'In Process'");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  'Completed'");
                sqlmain.Append("               END) STATUS,");
                sqlmain.Append("               ENQ.CREATED_DT STARTTIME,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN ADM.WF_MGR_ADM_TASK_COMPLETED_ON IS NULL THEN");
                sqlmain.Append("                  NULL");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  ADM.WF_MGR_ADM_TASK_COMPLETED_ON");
                sqlmain.Append("               END) ENDTIME,");
                sqlmain.Append("               (ADM.WF_MGR_ADM_TASK_DEADLINE ||");
                sqlmain.Append("               DECODE(ADM.WF_MGR_ADM_TASK_DLMODE,");
                sqlmain.Append("                       1,");
                sqlmain.Append("                       'Mins',");
                sqlmain.Append("                       2,");
                sqlmain.Append("                       'hrs',");
                sqlmain.Append("                       3,");
                sqlmain.Append("                       'days')) DEADLINE,");
                sqlmain.Append("               GET_WORKFLOW_TIME(ENQ.CREATED_DT,");
                sqlmain.Append("                                 ADM.WF_MGR_ADM_TASK_COMPLETED_ON,");
                sqlmain.Append("                                 (CASE");
                sqlmain.Append("                                   WHEN ADM.WF_MGR_ADM_TASK_COMPLETED_ON IS NULL THEN");
                sqlmain.Append("                                    1");
                sqlmain.Append("                                   ELSE");
                sqlmain.Append("                                    2");
                sqlmain.Append("                                 END),");
                sqlmain.Append("                                 ADM.WF_MGR_ADM_TASK_DEADLINE,");
                sqlmain.Append("                                 ADM.WF_MGR_ADM_TASK_DLMODE,");
                sqlmain.Append("                                 1) DELAY,");
                sqlmain.Append("               DECODE(ADM.WF_MGR_ADM_TASK_PRIORITY,");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               2 ORDERS");
                sqlmain.Append("          FROM ENQUIRY_BKG_SEA_TBL        ENQ,");
                sqlmain.Append("               QUOTATION_DTL_TBL          Q2,");
                sqlmain.Append("               QUOTATION_MST_TBL          Q1,");
                sqlmain.Append("               BOOKING_TRN    CARGO,");
                sqlmain.Append("               WF_MGR_ADM_TASK_LIST_TBL   ADM,");
                sqlmain.Append("               WORKFLOW_MGR_TASK_MSG_TBL  TASK,");
                sqlmain.Append("               WORKFLOW_RULES_EXT_MST_TBL RULEEXT");
                sqlmain.Append("         WHERE ENQ.ENQUIRY_BKG_SEA_PK IN ( " + strenqpk + ")");
                sqlmain.Append("           AND Q2.TRANS_REF_NO(+) = ENQ.ENQUIRY_REF_NO");
                sqlmain.Append("           AND Q1.QUOTATION_MST_PK(+) = Q2.QUOTATION_MST_FK");
                sqlmain.Append("           AND CARGO.TRANS_REF_NO(+) = Q1.QUOTATION_REF_NO");
                sqlmain.Append("           AND TASK.REF_TO(+) = ENQ.ENQUIRY_BKG_SEA_PK");
                sqlmain.Append("           AND ADM.WF_MGR_ADM_TASK_PK(+) = TASK.ADM_TASK_FK");
                sqlmain.Append("           AND RULEEXT.WF_RULES_EXT_MST_TBL_PK(+) = TASK.DOC_REF_NO_PK");
                sqlmain.Append("           AND RULEEXT.WF_RULES_EXT_MANDATORY = 1");
                sqlmain.Append("           AND TASK.ACCEPT = 1");
                sqlmain.Append("        UNION ");
                sqlmain.Append("        SELECT 'QTPK' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               QUO.QUOTATION_MST_PK PK,");
                sqlmain.Append("               'Quotation' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = QUO.CREATED_BY_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = QUO.CREATED_BY_FK)) LOCID,");
                sqlmain.Append("               QUO.QUOTATION_REF_NO DOCREFNO,");
                sqlmain.Append("               QUO.QUOTATION_REF_NO DOCREFNOS,");
                sqlmain.Append("               DECODE(QUO.STATUS,");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'In Process',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'Completed',");
                sqlmain.Append("                      4,");
                sqlmain.Append("                      'Completed') STATUS,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN (SELECT MAX(ADM.WF_MGR_ADM_TASK_COMPLETED_ON)");
                sqlmain.Append("                         FROM WF_MGR_ADM_TASK_LIST_TBL  ADM,");
                sqlmain.Append("                              WORKFLOW_MGR_TASK_MSG_TBL TASK");
                sqlmain.Append("                        WHERE TASK.REF_TO =");
                sqlmain.Append("                              (SELECT MAX(EN.ENQUIRY_BKG_SEA_PK)");
                sqlmain.Append("                                 FROM QUOTATION_DTL_TBL  QQ,");
                sqlmain.Append("                                      ENQUIRY_BKG_SEA_TBL       EN");
                sqlmain.Append("                                WHERE (EN.ENQUIRY_REF_NO = QQ.TRANS_REF_NO)");
                sqlmain.Append("                                  AND QQ.TRANS_REFERED_FROM = 4");
                sqlmain.Append("                                  AND QQ.QUOTATION_MST_FK =");
                sqlmain.Append("                                      QUO.QUOTATION_MST_PK)");
                sqlmain.Append("                          AND ADM.WF_MGR_ADM_TASK_PK(+) = TASK.ADM_TASK_FK) IS NOT NULL THEN");
                sqlmain.Append("                  (SELECT MAX(ADM.WF_MGR_ADM_TASK_COMPLETED_ON)");
                sqlmain.Append("                     FROM WF_MGR_ADM_TASK_LIST_TBL  ADM,");
                sqlmain.Append("                          WORKFLOW_MGR_TASK_MSG_TBL TASK");
                sqlmain.Append("                    WHERE TASK.REF_TO =");
                sqlmain.Append("                          (SELECT EN.ENQUIRY_BKG_SEA_PK");
                sqlmain.Append("                             FROM QUOTATION_DTL_TBL QQ,");
                sqlmain.Append("                                  ENQUIRY_BKG_SEA_TBL       EN");
                sqlmain.Append("                            WHERE (EN.ENQUIRY_REF_NO = QQ.TRANS_REF_NO)");
                sqlmain.Append("                              AND QQ.TRANS_REFERED_FROM = 4");
                sqlmain.Append("                              AND QQ.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK)");
                sqlmain.Append("                      AND ADM.WF_MGR_ADM_TASK_PK(+) = TASK.ADM_TASK_FK)");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  (CASE");
                sqlmain.Append("                 WHEN (SELECT MAX(QQ.TRANS_REF_NO)");
                sqlmain.Append("                         FROM QUOTATION_DTL_TBL QQ");
                sqlmain.Append("                        WHERE QQ.TRANS_REFERED_FROM = 4");
                sqlmain.Append("                          AND QQ.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK) IS NOT NULL THEN");
                sqlmain.Append("                  (SELECT DISTINCT ENQMAIN.CREATED_DT");
                sqlmain.Append("                     FROM QUOTATION_DTL_TBL QTRAN,");
                sqlmain.Append("                          ENQUIRY_BKG_SEA_TBL       ENQMAIN");
                sqlmain.Append("                    WHERE QTRAN.QUOTATION_MST_FK IN (QUO.QUOTATION_MST_PK)");
                sqlmain.Append("                      AND QTRAN.TRANS_REF_NO = ENQMAIN.ENQUIRY_REF_NO)");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  QUO.CREATED_DT");
                sqlmain.Append("               END) END) STARTTIME,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN QUO.STATUS = 2 OR QUO.STATUS = 4 THEN");
                sqlmain.Append("                  QUO.LAST_MODIFIED_DT");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  NULL");
                sqlmain.Append("               END) ENDTIME,");
                sqlmain.Append("               (SELECT DISTINCT (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE,0)||'Days ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)||'Hours ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)||'Mins' ELSE '' END) ");
                //sqlmain.Append("                       DECODE(WK.WF_RULES_INT_DEADLINE_MODE,")
                //sqlmain.Append("                              1,")
                //sqlmain.Append("                              'Mins',")
                //sqlmain.Append("                              2,")
                //sqlmain.Append("                              'hrs',")
                //sqlmain.Append("                              3,")
                //sqlmain.Append("                              'days')")
                sqlmain.Append("                  FROM WORKFLOW_RULES_INT_CONFIG_TBL WK, WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                 WHERE WK.WF_RULES_INT_MST_FK = 36 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME((CASE");
                sqlmain.Append("                                    WHEN (SELECT MAX(ADM.WF_MGR_ADM_TASK_COMPLETED_ON)");
                sqlmain.Append("                                            FROM WF_MGR_ADM_TASK_LIST_TBL  ADM,");
                sqlmain.Append("                                                 WORKFLOW_MGR_TASK_MSG_TBL TASK");
                sqlmain.Append("                                           WHERE TASK.REF_TO =");
                sqlmain.Append("                                                 (SELECT MAX(EN.ENQUIRY_BKG_SEA_PK)");
                sqlmain.Append("                                                    FROM QUOTATION_DTL_TBL  QQ,");
                sqlmain.Append("                                                         ENQUIRY_BKG_SEA_TBL       EN");
                sqlmain.Append("                                                   WHERE (EN.ENQUIRY_REF_NO = QQ.TRANS_REF_NO)");
                sqlmain.Append("                                                     AND QQ.TRANS_REFERED_FROM = 4");
                sqlmain.Append("                                                     AND QQ.QUOTATION_MST_FK =");
                sqlmain.Append("                                                         QUO.QUOTATION_MST_PK)");
                sqlmain.Append("                                             AND ADM.WF_MGR_ADM_TASK_PK(+) = TASK.ADM_TASK_FK) IS NOT NULL THEN");
                sqlmain.Append("                                     (SELECT MAX(ADM.WF_MGR_ADM_TASK_COMPLETED_ON)");
                sqlmain.Append("                                        FROM WF_MGR_ADM_TASK_LIST_TBL  ADM,");
                sqlmain.Append("                                             WORKFLOW_MGR_TASK_MSG_TBL TASK");
                sqlmain.Append("                                       WHERE TASK.REF_TO =");
                sqlmain.Append("                                             (SELECT EN.ENQUIRY_BKG_SEA_PK");
                sqlmain.Append("                                                FROM QUOTATION_DTL_TBL  QQ,");
                sqlmain.Append("                                                     ENQUIRY_BKG_SEA_TBL       EN");
                sqlmain.Append("                                               WHERE (EN.ENQUIRY_REF_NO = QQ.TRANS_REF_NO)");
                sqlmain.Append("                                                 AND QQ.TRANS_REFERED_FROM = 4");
                sqlmain.Append("                                                 AND QQ.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK)");
                sqlmain.Append("                                         AND ADM.WF_MGR_ADM_TASK_PK(+) = TASK.ADM_TASK_FK)");
                sqlmain.Append("                                    ELSE");
                sqlmain.Append("                                     (CASE");
                sqlmain.Append("                                    WHEN (SELECT MAX(QQ.TRANS_REF_NO)");
                sqlmain.Append("                                            FROM QUOTATION_DTL_TBL QQ");
                sqlmain.Append("                                           WHERE QQ.TRANS_REFERED_FROM = 4");
                sqlmain.Append("                                             AND QQ.QUOTATION_MST_FK = QUO.QUOTATION_MST_PK) IS NOT NULL THEN");
                sqlmain.Append("                                     (SELECT DISTINCT ENQMAIN.CREATED_DT");
                sqlmain.Append("                                        FROM QUOTATION_DTL_TBL    QTRAN,");
                sqlmain.Append("                                             ENQUIRY_BKG_SEA_TBL       ENQMAIN");
                sqlmain.Append("                                       WHERE QTRAN.QUOTATION_MST_FK IN (QUO.QUOTATION_MST_PK)");
                sqlmain.Append("                                         AND QTRAN.TRANS_REF_NO = ENQMAIN.ENQUIRY_REF_NO)");
                sqlmain.Append("                                    ELSE");
                sqlmain.Append("                                     QUO.CREATED_DT");
                sqlmain.Append("                                  END) END), QUO.LAST_MODIFIED_DT, QUO.STATUS,");
                sqlmain.Append("                (SELECT DISTINCT FLOOR(NVL(WK.WF_RULES_INT_DEADLINE,0)*24*60+NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)*60+NVL(WK.WF_RULES_INT_DEADLINE_MINS,0))");
                sqlmain.Append("                   FROM WORKFLOW_RULES_INT_CONFIG_TBL WK,WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                  WHERE WK.WF_RULES_INT_MST_FK = 36 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),1,");
                //sqlmain.Append("                (SELECT DISTINCT WK.WF_RULES_INT_DEADLINE_MODE")
                //sqlmain.Append("                   FROM WORKFLOW_RULES_INT_CONFIG_TBL WK,WORKFLOW_RULES_INT_APPL_TBL   WL")
                //sqlmain.Append("                  WHERE WK.WF_RULES_INT_MST_FK = 36 AND WL.WORKFLOW_RULES_INT_MST_FK = WK.WF_RULES_INT_CONFIG_PK AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),")
                sqlmain.Append("                1)) DELAY,");
                sqlmain.Append("               DECODE((SELECT WKM.WF_RULES_INT_PRIORITY");
                sqlmain.Append("                        FROM WORKFLOW_RULES_INT_MST_TBL WKM");
                sqlmain.Append("                       WHERE WKM.WF_RULES_INT_MST_TBL_PK = 36),");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               2 ORDERS");
                sqlmain.Append("          FROM QUOTATION_MST_TBL QUO, USER_MST_TBL UMT");
                sqlmain.Append("         WHERE QUO.QUOTATION_MST_PK IN ( " + strquopk + ")");
                sqlmain.Append("          AND QUO.CREATED_BY_FK = UMT.USER_MST_PK");
                sqlmain.Append("        UNION ");
                sqlmain.Append("        SELECT 'Manual' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               QUO.QUOTATION_MST_PK PK,");
                sqlmain.Append("               'Manual' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = ADM.WF_MGR_ADM_TASK_CUSER_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = ADM.WF_MGR_ADM_TASK_CUSER_FK)) LOCID,");
                sqlmain.Append("               TASK.DOC_REF_NO DOCREFNO,");
                sqlmain.Append("               TASK.DOC_REF_NO DOCREFNOS,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN ADM.WF_MGR_ADM_TASK_COMPLETED_ON IS NULL THEN");
                sqlmain.Append("                  'In Process'");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  'Completed'");
                sqlmain.Append("               END) STATUS,");
                sqlmain.Append("               QUO.LAST_MODIFIED_DT STARTTIME,");
                sqlmain.Append("               ADM.WF_MGR_ADM_TASK_COMPLETED_ON ENDTIME,");
                sqlmain.Append("               (ADM.WF_MGR_ADM_TASK_DEADLINE ||");
                sqlmain.Append("               DECODE(ADM.WF_MGR_ADM_TASK_DLMODE,");
                sqlmain.Append("                       1,");
                sqlmain.Append("                       'Mins',");
                sqlmain.Append("                       2,");
                sqlmain.Append("                       'hrs',");
                sqlmain.Append("                       3,");
                sqlmain.Append("                       'days')) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME(QUO.LAST_MODIFIED_DT,");
                sqlmain.Append("                                  ADM.WF_MGR_ADM_TASK_COMPLETED_ON,");
                sqlmain.Append("                                  (CASE");
                sqlmain.Append("                                    WHEN ADM.WF_MGR_ADM_TASK_COMPLETED_ON IS NULL THEN");
                sqlmain.Append("                                     1");
                sqlmain.Append("                                    ELSE");
                sqlmain.Append("                                     2");
                sqlmain.Append("                                  END),");
                sqlmain.Append("                                  ADM.WF_MGR_ADM_TASK_DEADLINE,");
                sqlmain.Append("                                  ADM.WF_MGR_ADM_TASK_DLMODE,");
                sqlmain.Append("                                  1)) DELAY,");
                sqlmain.Append("               DECODE(ADM.WF_MGR_ADM_TASK_PRIORITY,");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               2 ORDERS");
                sqlmain.Append("          FROM QUOTATION_MST_TBL          QUO,");
                sqlmain.Append("               QUOTATION_DTL_TBL    CARGO,");
                sqlmain.Append("               WF_MGR_ADM_TASK_LIST_TBL   ADM,");
                sqlmain.Append("               WORKFLOW_MGR_TASK_MSG_TBL  TASK,");
                sqlmain.Append("               WORKFLOW_RULES_EXT_MST_TBL RULEEXT");
                sqlmain.Append("         WHERE QUO.QUOTATION_MST_PK IN ( " + strquopk + ")");
                sqlmain.Append("           AND QUO.QUOTATION_REF_NO = CARGO.TRANS_REF_NO(+)");
                sqlmain.Append("           AND ADM.WF_MGR_ADM_TASK_PK(+) = TASK.ADM_TASK_FK");
                sqlmain.Append("           AND TASK.REF_TO(+) = QUO.QUOTATION_MST_PK");
                sqlmain.Append("           AND RULEEXT.WF_RULES_EXT_MST_TBL_PK(+) = TASK.DOC_REF_NO_PK");
                sqlmain.Append("           AND RULEEXT.WF_RULES_EXT_MANDATORY = 1");
                sqlmain.Append("           AND TASK.ACCEPT = 1");
                sqlmain.Append("        UNION");
                sqlmain.Append("        SELECT 'BOOKINGPK' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               BOOKING.BOOKING_MST_PK PK,");
                sqlmain.Append("               'Booking' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = BOOKING.CREATED_BY_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = BOOKING.CREATED_BY_FK)) LOCID,");
                sqlmain.Append("               BOOKING.BOOKING_REF_NO DOCREFNO,");
                sqlmain.Append("               BOOKING.BOOKING_REF_NO DOCREFNOS,");
                sqlmain.Append("               DECODE(BOOKING.STATUS, 1, 'In Process', 2, 'Completed') STATUS,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN (SELECT MAX(T.BOOKING_MST_FK)");
                sqlmain.Append("                         FROM BOOKING_TRN T");
                sqlmain.Append("                        WHERE T.BOOKING_MST_FK = BOOKING.BOOKING_MST_PK");
                sqlmain.Append("                          AND T.TRANS_REFERED_FROM = 1) IS NOT NULL THEN");
                sqlmain.Append("                  (SELECT DISTINCT QMAIN.LAST_MODIFIED_DT");
                sqlmain.Append("                     FROM BOOKING_TRN BTRAN,");
                sqlmain.Append("                          QUOTATION_MST_TBL       QMAIN");
                sqlmain.Append("                    WHERE BTRAN.BOOKING_MST_FK IN (BOOKING.BOOKING_MST_PK)");
                sqlmain.Append("                      AND BTRAN.TRANS_REF_NO = QMAIN.QUOTATION_REF_NO)");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  BOOKING.CREATED_DT");
                sqlmain.Append("               END) STARTTIME,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN BOOKING.STATUS = 2 THEN");
                sqlmain.Append("                  (CASE");
                sqlmain.Append("                 WHEN BOOKING.LAST_MODIFIED_DT IS NULL THEN");
                sqlmain.Append("                  BOOKING.CREATED_DT");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  BOOKING.LAST_MODIFIED_DT");
                sqlmain.Append("               END) ELSE NULL END) ENDTIME,");
                sqlmain.Append("               (SELECT DISTINCT (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE,0)||'Days ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)||'Hours ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)||'Mins' ELSE '' END) ");
                //sqlmain.Append("                       DECODE(WK.WF_RULES_INT_DEADLINE_MODE,")
                //sqlmain.Append("                              1,")
                //sqlmain.Append("                              'Mins',")
                //sqlmain.Append("                              2,")
                //sqlmain.Append("                              'hrs',")
                //sqlmain.Append("                              3,")
                //sqlmain.Append("                              'days')")
                sqlmain.Append("                  FROM WORKFLOW_RULES_INT_CONFIG_TBL WK, WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                 WHERE WK.WF_RULES_INT_MST_FK = 37 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME((CASE");
                sqlmain.Append("                                    WHEN (SELECT MAX(T.BOOKING_MST_FK)");
                sqlmain.Append("                                            FROM BOOKING_TRN T");
                sqlmain.Append("                                           WHERE T.BOOKING_MST_FK = BOOKING.BOOKING_MST_PK");
                sqlmain.Append("                                             AND T.TRANS_REFERED_FROM = 1) IS NOT NULL THEN");
                sqlmain.Append("                                     (SELECT DISTINCT QMAIN.LAST_MODIFIED_DT");
                sqlmain.Append("                                        FROM BOOKING_TRN BTRAN,");
                sqlmain.Append("                                             QUOTATION_MST_TBL       QMAIN");
                sqlmain.Append("                                       WHERE BTRAN.BOOKING_MST_FK IN (BOOKING.BOOKING_MST_PK)");
                sqlmain.Append("                                         AND BTRAN.TRANS_REF_NO = QMAIN.QUOTATION_REF_NO)");
                sqlmain.Append("                                    ELSE");
                sqlmain.Append("                                     BOOKING.CREATED_DT");
                sqlmain.Append("                                  END),");
                sqlmain.Append("                                  (CASE");
                sqlmain.Append("                                    WHEN BOOKING.STATUS = 2 THEN");
                sqlmain.Append("                                     (CASE");
                sqlmain.Append("                                    WHEN BOOKING.LAST_MODIFIED_DT IS NULL THEN");
                sqlmain.Append("                                     BOOKING.CREATED_DT");
                sqlmain.Append("                                    ELSE");
                sqlmain.Append("                                     BOOKING.LAST_MODIFIED_DT");
                sqlmain.Append("                                  END) ELSE NULL END), BOOKING.STATUS,");
                sqlmain.Append("                (SELECT DISTINCT FLOOR(NVL(WK.WF_RULES_INT_DEADLINE,0)*24*60+NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)*60+NVL(WK.WF_RULES_INT_DEADLINE_MINS,0))");
                sqlmain.Append("                   FROM WORKFLOW_RULES_INT_CONFIG_TBL WK,WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                  WHERE WK.WF_RULES_INT_MST_FK = 37 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK ),1,");
                //sqlmain.Append("                (SELECT DISTINCT WK.WF_RULES_INT_DEADLINE_MODE")
                //sqlmain.Append("                   FROM WORKFLOW_RULES_INT_CONFIG_TBL WK ,WORKFLOW_RULES_INT_APPL_TBL   WL")
                //sqlmain.Append("                  WHERE WK.WF_RULES_INT_MST_FK = 37 AND WL.WORKFLOW_RULES_INT_MST_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK), ")
                sqlmain.Append("                1)) DELAY,");
                sqlmain.Append("               DECODE((SELECT WKM.WF_RULES_INT_PRIORITY");
                sqlmain.Append("                        FROM WORKFLOW_RULES_INT_MST_TBL WKM");
                sqlmain.Append("                       WHERE WKM.WF_RULES_INT_MST_TBL_PK = 37),");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               4 ORDERS");
                sqlmain.Append("          FROM BOOKING_MST_TBL BOOKING,USER_MST_TBL UMT");
                sqlmain.Append("         WHERE BOOKING.BOOKING_MST_PK IN ( " + strbkgpk + ")");
                sqlmain.Append("         AND BOOKING.CREATED_BY_FK = UMT.USER_MST_PK ");
                sqlmain.Append(" UNION ");
                sqlmain.Append("        SELECT 'Manual' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               BOOKING.BOOKING_MST_PK PK,");
                sqlmain.Append("               'Manual' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = ADM.WF_MGR_ADM_TASK_CUSER_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = ADM.WF_MGR_ADM_TASK_CUSER_FK)) LOCID,");
                sqlmain.Append("               TASK.DOC_REF_NO DOCREFNO,");
                sqlmain.Append("               TASK.DOC_REF_NO DOCREFNOS,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN ADM.WF_MGR_ADM_TASK_COMPLETED_ON IS NULL THEN");
                sqlmain.Append("                  'In Process'");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  'Completed'");
                sqlmain.Append("               END) STATUS,");
                sqlmain.Append("               BOOKING.LAST_MODIFIED_DT STARTTIME,");
                sqlmain.Append("               ADM.WF_MGR_ADM_TASK_COMPLETED_ON ENDTIME,");
                sqlmain.Append("               (ADM.WF_MGR_ADM_TASK_DEADLINE ||");
                sqlmain.Append("               DECODE(ADM.WF_MGR_ADM_TASK_DLMODE,");
                sqlmain.Append("                       1,");
                sqlmain.Append("                       'Mins',");
                sqlmain.Append("                       2,");
                sqlmain.Append("                       'hrs',");
                sqlmain.Append("                       3,");
                sqlmain.Append("                       'days')) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME(BOOKING.LAST_MODIFIED_DT,");
                sqlmain.Append("                                  ADM.WF_MGR_ADM_TASK_COMPLETED_ON,");
                sqlmain.Append("                                  (CASE");
                sqlmain.Append("                                    WHEN ADM.WF_MGR_ADM_TASK_COMPLETED_ON IS NULL THEN");
                sqlmain.Append("                                     1");
                sqlmain.Append("                                    ELSE");
                sqlmain.Append("                                     2");
                sqlmain.Append("                                  END),");
                sqlmain.Append("                                  ADM.WF_MGR_ADM_TASK_DEADLINE,");
                sqlmain.Append("                                  ADM.WF_MGR_ADM_TASK_DLMODE,");
                sqlmain.Append("                                  1)) DELAY,");
                sqlmain.Append("               DECODE(ADM.WF_MGR_ADM_TASK_PRIORITY,");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               4 ORDERS");
                sqlmain.Append("          FROM BOOKING_MST_TBL            BOOKING,");
                sqlmain.Append("               JOB_CARD_TRN       J,");
                sqlmain.Append("               CONSOL_INVOICE_TRN_TBL     TRN,");
                sqlmain.Append("               WF_MGR_ADM_TASK_LIST_TBL   ADM,");
                sqlmain.Append("               WORKFLOW_MGR_TASK_MSG_TBL  TASK,");
                sqlmain.Append("               WORKFLOW_RULES_EXT_MST_TBL RULEEXT");
                sqlmain.Append("         WHERE BOOKING.BOOKING_MST_PK IN (" + strbkgpk + ")");
                sqlmain.Append("           AND BOOKING.BOOKING_MST_PK = J.BOOKING_MST_FK(+)");
                sqlmain.Append("           AND J.Job_Card_Trn_Pk = TRN.JOB_CARD_FK(+)");
                sqlmain.Append("           AND ADM.WF_MGR_ADM_TASK_PK(+) = TASK.ADM_TASK_FK");
                sqlmain.Append("           AND TASK.REF_TO(+) = BOOKING.BOOKING_MST_PK");
                sqlmain.Append("           AND RULEEXT.WF_RULES_EXT_MST_TBL_PK(+) = TASK.DOC_REF_NO_PK");
                sqlmain.Append("           AND RULEEXT.WF_RULES_EXT_MANDATORY = 1");
                sqlmain.Append("           AND TASK.ACCEPT = 1");
                sqlmain.Append("        UNION ");
                sqlmain.Append("        SELECT 'JOBPK' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               JOB.Job_Card_Trn_Pk PK,");
                sqlmain.Append("               'JobCard' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = JOB.CREATED_BY_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = JOB.CREATED_BY_FK)) LOCID,");
                sqlmain.Append("               JOB.JOBCARD_REF_NO DOCREFNO,");
                sqlmain.Append("               JOB.JOBCARD_REF_NO DOCREFNOS,");
                sqlmain.Append("               DECODE(JOB.JOB_CARD_STATUS, 1, 'In Process', 2, 'Completed') STATUS,");
                sqlmain.Append("               (SELECT BO.LAST_MODIFIED_DT");
                sqlmain.Append("                  FROM BOOKING_MST_TBL BO");
                sqlmain.Append("                 WHERE BO.BOOKING_MST_PK = JOB.BOOKING_MST_FK) STARTTIME,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN JOB.JOB_CARD_STATUS = 2 THEN");
                sqlmain.Append("                  JOB.LAST_MODIFIED_DT");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  NULL");
                sqlmain.Append("               END) ENDTIME,");
                sqlmain.Append("               (SELECT DISTINCT (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE,0)||'Days ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)||'Hours ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)||'Mins' ELSE '' END) ");
                //sqlmain.Append("                       DECODE(WK.WF_RULES_INT_DEADLINE_MODE,")
                //sqlmain.Append("                              1,")
                //sqlmain.Append("                              'Mins',")
                //sqlmain.Append("                              2,")
                //sqlmain.Append("                              'hrs',")
                //sqlmain.Append("                              3,")
                //sqlmain.Append("                              'days')")
                sqlmain.Append("                  FROM WORKFLOW_RULES_INT_CONFIG_TBL WK, WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                 WHERE WK.WF_RULES_INT_MST_FK = 38 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK) DEADLINE,");
                sqlmain.Append("               (CASE");
                sqlmain.Append("                 WHEN JOB.JOB_CARD_STATUS = 2 THEN");
                sqlmain.Append("                  (GET_WORKFLOW_TIME((SELECT BO.LAST_MODIFIED_DT");
                sqlmain.Append("                                       FROM BOOKING_MST_TBL BO");
                sqlmain.Append("                                      WHERE BO.BOOKING_MST_PK =");
                sqlmain.Append("                                            JOB.BOOKING_MST_FK),");
                sqlmain.Append("                                     JOB.LAST_MODIFIED_DT,");
                sqlmain.Append("                                     JOB.JOB_CARD_STATUS,");
                sqlmain.Append("                                     (SELECT DISTINCT FLOOR(NVL(WK.WF_RULES_INT_DEADLINE,0)*24*60+NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)*60+NVL(WK.WF_RULES_INT_DEADLINE_MINS,0))");
                sqlmain.Append("                                        FROM WORKFLOW_RULES_INT_CONFIG_TBL WK, WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                                       WHERE WK.WF_RULES_INT_MST_FK = 38 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),1,");
                //sqlmain.Append("                                     (SELECT DISTINCT WK.WF_RULES_INT_DEADLINE_MODE")
                //sqlmain.Append("                                        FROM WORKFLOW_RULES_INT_CONFIG_TBL WK,WORKFLOW_RULES_INT_APPL_TBL   WL")
                //sqlmain.Append("                                       WHERE WK.WF_RULES_INT_MST_FK = 38 AND WL.WORKFLOW_RULES_INT_MST_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),")
                sqlmain.Append("                                     1))");
                sqlmain.Append("                 ELSE");
                sqlmain.Append("                  NULL");
                sqlmain.Append("               END) DELAY,");
                sqlmain.Append("               DECODE((SELECT WKM.WF_RULES_INT_PRIORITY");
                sqlmain.Append("                        FROM WORKFLOW_RULES_INT_MST_TBL WKM");
                sqlmain.Append("                       WHERE WKM.WF_RULES_INT_MST_TBL_PK = 38),");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               4 ORDERS");
                sqlmain.Append("          FROM JOB_CARD_TRN JOB,USER_MST_TBL UMT");
                sqlmain.Append("         WHERE JOB.Job_Card_Trn_Pk IN ( " + strjobpk + ")");
                sqlmain.Append("          AND JOB.CREATED_BY_FK = UMT.USER_MST_PK ");
                sqlmain.Append("        UNION ");
                sqlmain.Append("        SELECT 'HBLPK' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               HBL.HBL_EXP_TBL_PK PK,");
                sqlmain.Append("               'HBL/HAWB' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = HBL.CREATED_BY_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = HBL.CREATED_BY_FK)) LOCID,");
                sqlmain.Append("               HBL.HBL_REF_NO DOCREFNO,");
                sqlmain.Append("               HBL.HBL_REF_NO DOCREFNO,");
                sqlmain.Append("               'Completed' STATUS,");
                sqlmain.Append("               (SELECT B1.LAST_MODIFIED_DT");
                sqlmain.Append("                  FROM JOB_CARD_TRN JJOB, BOOKING_MST_TBL B1");
                sqlmain.Append("                 WHERE B1.BOOKING_MST_PK = JJOB.BOOKING_MST_FK");
                sqlmain.Append("                   AND JJOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK) STARTTIME,");
                sqlmain.Append("               HBL.LAST_MODIFIED_DT ENDTIME,");
                sqlmain.Append("               (SELECT DISTINCT (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE,0)||'Days ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)||'Hours ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)||'Mins' ELSE '' END) ");
                //sqlmain.Append("                       DECODE(WK.WF_RULES_INT_DEADLINE_MODE,")
                //sqlmain.Append("                              1,")
                //sqlmain.Append("                              'Mins',")
                //sqlmain.Append("                              2,")
                //sqlmain.Append("                              'hrs',")
                //sqlmain.Append("                              3,")
                //sqlmain.Append("                              'days')")
                sqlmain.Append("                  FROM WORKFLOW_RULES_INT_CONFIG_TBL WK, WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                 WHERE WK.WF_RULES_INT_MST_FK = 43 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME((SELECT B1.LAST_MODIFIED_DT");
                sqlmain.Append("                                    FROM JOB_CARD_TRN JJOB,");
                sqlmain.Append("                                         BOOKING_MST_TBL      B1");
                sqlmain.Append("                                   WHERE B1.BOOKING_MST_PK =");
                sqlmain.Append("                                         JJOB.BOOKING_MST_FK");
                sqlmain.Append("                                     AND JJOB.HBL_HAWB_FK =");
                sqlmain.Append("                                         HBL.HBL_EXP_TBL_PK),");
                sqlmain.Append("                                  HBL.LAST_MODIFIED_DT,");
                sqlmain.Append("                                  2,");
                sqlmain.Append("                                  (SELECT DISTINCT FLOOR(NVL(WK.WF_RULES_INT_DEADLINE,0)*24*60+NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)*60+NVL(WK.WF_RULES_INT_DEADLINE_MINS,0))");
                sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK, WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 43 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),1,");
                //sqlmain.Append("                                  (SELECT DISTINCT WK.WF_RULES_INT_DEADLINE_MODE")
                //sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK, WORKFLOW_RULES_INT_APPL_TBL   WL")
                //sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 43 AND WL.WORKFLOW_RULES_INT_MST_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),")
                sqlmain.Append("                                  2)) DELAY,");
                sqlmain.Append("               DECODE((SELECT WKM.WF_RULES_INT_PRIORITY");
                sqlmain.Append("                        FROM WORKFLOW_RULES_INT_MST_TBL WKM");
                sqlmain.Append("                       WHERE WKM.WF_RULES_INT_MST_TBL_PK = 43),");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               6 ORDERS");
                sqlmain.Append("          FROM HBL_EXP_TBL HBL, CONSOL_INVOICE_TRN_TBL TRN, USER_MST_TBL UMT ");
                sqlmain.Append("         WHERE HBL.HBL_EXP_TBL_PK IN (" + strhblpk + ")");
                sqlmain.Append("           AND HBL.JOB_CARD_SEA_EXP_FK = TRN.JOB_CARD_FK(+)");
                sqlmain.Append("         AND  HBL.CREATED_BY_FK = UMT.USER_MST_PK ");
                sqlmain.Append("        UNION ");
                sqlmain.Append("        SELECT 'MBLPK' NAMES,");
                sqlmain.Append("               -1 FK,");
                sqlmain.Append("               MBL.MBL_EXP_TBL_PK PK,");
                sqlmain.Append("               'MBL/MAWB' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = MBL.CREATED_BY_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = MBL.CREATED_BY_FK)) LOCID,");
                sqlmain.Append("               MBL.MBL_REF_NO DOCREFNO,");
                sqlmain.Append("               MBL.MBL_REF_NO DOCREFNO,");
                sqlmain.Append("               'Completed' STATUS,");
                sqlmain.Append("               (SELECT MAX(B2.LAST_MODIFIED_DT)");
                sqlmain.Append("                  FROM JOB_CARD_TRN JJOB, BOOKING_MST_TBL B2");
                sqlmain.Append("                 WHERE B2.BOOKING_MST_PK = JJOB.BOOKING_MST_FK");
                sqlmain.Append("                   AND JJOB.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK) STARTTIME,");
                sqlmain.Append("               MBL.LAST_MODIFIED_DT ENDTIME,");
                sqlmain.Append("               (SELECT DISTINCT (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE,0)||'Days ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)||'Hours ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)||'Mins' ELSE '' END) ");
                //sqlmain.Append("                       DECODE(WK.WF_RULES_INT_DEADLINE_MODE,")
                //sqlmain.Append("                              1,")
                //sqlmain.Append("                              'Mins',")
                //sqlmain.Append("                              2,")
                //sqlmain.Append("                              'hrs',")
                //sqlmain.Append("                              3,")
                //sqlmain.Append("                              'days')")
                sqlmain.Append("                  FROM WORKFLOW_RULES_INT_CONFIG_TBL WK , WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                 WHERE WK.WF_RULES_INT_MST_FK = 44 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME((SELECT MAX(B2.LAST_MODIFIED_DT)");
                sqlmain.Append("                                    FROM JOB_CARD_TRN JJOB,");
                sqlmain.Append("                                         BOOKING_MST_TBL      B2");
                sqlmain.Append("                                   WHERE B2.BOOKING_MST_PK =");
                sqlmain.Append("                                         JJOB.BOOKING_MST_FK");
                sqlmain.Append("                                     AND JJOB.MBL_MAWB_FK =");
                sqlmain.Append("                                         MBL.MBL_EXP_TBL_PK),");
                sqlmain.Append("                                  MBL.LAST_MODIFIED_DT,");
                sqlmain.Append("                                  2,");
                sqlmain.Append("                                  (SELECT DISTINCT FLOOR(NVL(WK.WF_RULES_INT_DEADLINE,0)*24*60+NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)*60+NVL(WK.WF_RULES_INT_DEADLINE_MINS,0))");
                sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK , WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 44 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),1,");
                //sqlmain.Append("                                  (SELECT DISTINCT WK.WF_RULES_INT_DEADLINE_MODE")
                //sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK , WORKFLOW_RULES_INT_APPL_TBL   WL")
                //sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 44 AND WL.WORKFLOW_RULES_INT_MST_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),")
                sqlmain.Append("                                  2)) DELAY,");
                sqlmain.Append("               DECODE((SELECT WKM.WF_RULES_INT_PRIORITY");
                sqlmain.Append("                        FROM WORKFLOW_RULES_INT_MST_TBL WKM");
                sqlmain.Append("                       WHERE WKM.WF_RULES_INT_MST_TBL_PK = 44),");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               7 ORDERS");
                sqlmain.Append("          FROM MBL_EXP_TBL            MBL,");
                sqlmain.Append("               JOB_CARD_TRN   J,");
                sqlmain.Append("               CONSOL_INVOICE_TRN_TBL TRN, USER_MST_TBL UMT");
                sqlmain.Append("         WHERE MBL.MBL_EXP_TBL_PK IN (" + strmblpk + ")");
                sqlmain.Append("           AND MBL.MBL_EXP_TBL_PK(+) = J.MBL_MAWB_FK");
                sqlmain.Append("           AND J.Job_Card_Trn_Pk = TRN.JOB_CARD_FK(+)");
                sqlmain.Append("           AND MBL.CREATED_BY_FK = UMT.USER_MST_PK");
                sqlmain.Append("        UNION ");
                sqlmain.Append("        SELECT 'INVPK' NAMES,");
                sqlmain.Append("               1 FK,");
                sqlmain.Append("               CONSOL.CONSOL_INVOICE_PK PK,");
                sqlmain.Append("               'Invoice' ACTIVITY,");
                sqlmain.Append("               (SELECT USERS.USER_ID");
                sqlmain.Append("                  FROM USER_MST_TBL USERS");
                sqlmain.Append("                 WHERE USERS.USER_MST_PK = CONSOL.CREATED_BY_FK) USERS,");
                sqlmain.Append("               (SELECT LOC.LOCATION_NAME");
                sqlmain.Append("                  FROM LOCATION_MST_TBL LOC");
                sqlmain.Append("                 WHERE LOC.LOCATION_MST_PK IN");
                sqlmain.Append("                       (SELECT US.DEFAULT_LOCATION_FK");
                sqlmain.Append("                          FROM USER_MST_TBL US");
                sqlmain.Append("                         WHERE US.USER_MST_PK = CONSOL.CREATED_BY_FK)) LOCID,");
                sqlmain.Append("               CONSOL.INVOICE_REF_NO DOCREFNO,");
                sqlmain.Append("               CONSOL.INVOICE_REF_NO DOCREFNOS,");
                sqlmain.Append("               'Completed' STATUS,");
                sqlmain.Append("               NULL STARTTIME,");
                sqlmain.Append("               CONSOL.LAST_MODIFIED_DT ENDTIME,");
                sqlmain.Append("               (SELECT DISTINCT (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE,0)||'Days ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)||'Hours ' ELSE '' END) || ");
                sqlmain.Append("                                (CASE WHEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)>0 THEN NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)||'Mins' ELSE '' END) ");
                //sqlmain.Append("                       DECODE(WK.WF_RULES_INT_DEADLINE_MODE,")
                //sqlmain.Append("                              1,")
                //sqlmain.Append("                              'Mins',")
                //sqlmain.Append("                              2,")
                //sqlmain.Append("                              'hrs',")
                //sqlmain.Append("                              3,")
                //sqlmain.Append("                              'days')")
                sqlmain.Append("                  FROM WORKFLOW_RULES_INT_CONFIG_TBL WK , WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                 WHERE WK.WF_RULES_INT_MST_FK = 48 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK) DEADLINE,");
                sqlmain.Append("               (GET_WORKFLOW_TIME((SELECT DISTINCT CONSOL.CREATED_DT");
                sqlmain.Append("                                    FROM CONSOL_INVOICE_TRN_TBL CON,");
                sqlmain.Append("                                         JOB_CARD_TRN   JJOB");
                sqlmain.Append("                                   WHERE CON.CONSOL_INVOICE_FK IN");
                sqlmain.Append("                                         (CONSOL.CONSOL_INVOICE_PK)");
                sqlmain.Append("                                     AND CON.JOB_CARD_FK =");
                sqlmain.Append("                                         JJOB.Job_Card_Trn_Pk),");
                sqlmain.Append("                                  CONSOL.LAST_MODIFIED_DT,");
                sqlmain.Append("                                  2,");
                sqlmain.Append("                                  (SELECT DISTINCT FLOOR(NVL(WK.WF_RULES_INT_DEADLINE,0)*24*60+NVL(WK.WF_RULES_INT_DEADLINE_HOURS,0)*60+NVL(WK.WF_RULES_INT_DEADLINE_MINS,0)) ");
                sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK , WORKFLOW_RULES_INT_APPL_TBL   WL");
                sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 48 AND WL.WORKFLOW_RULES_INT_CONFIG_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),1,");
                //sqlmain.Append("                                  (SELECT DISTINCT WK.WF_RULES_INT_DEADLINE_MODE")
                //sqlmain.Append("                                     FROM WORKFLOW_RULES_INT_CONFIG_TBL WK , WORKFLOW_RULES_INT_APPL_TBL   WL")
                //sqlmain.Append("                                    WHERE WK.WF_RULES_INT_MST_FK = 48 AND WL.WORKFLOW_RULES_INT_MST_FK = WK.WF_RULES_INT_CONFIG_PK  AND WL.WORKFLOW_RULES_INT_LOC_FK = UMT.DEFAULT_LOCATION_FK),")
                sqlmain.Append("                                  2)) DELAY,");
                sqlmain.Append("               DECODE((SELECT WKM.WF_RULES_INT_PRIORITY");
                sqlmain.Append("                        FROM WORKFLOW_RULES_INT_MST_TBL WKM");
                sqlmain.Append("                       WHERE WKM.WF_RULES_INT_MST_TBL_PK = 48),");
                sqlmain.Append("                      1,");
                sqlmain.Append("                      'Low',");
                sqlmain.Append("                      2,");
                sqlmain.Append("                      'High',");
                sqlmain.Append("                      3,");
                sqlmain.Append("                      'Critical') PRIORITY,");
                sqlmain.Append("               5 ORDERS");
                sqlmain.Append("          FROM CONSOL_INVOICE_TBL CONSOL,USER_MST_TBL UMT");
                sqlmain.Append("         WHERE CONSOL.CONSOL_INVOICE_PK IN ( " + strinvpk + ") AND CONSOL.CREATED_BY_FK = UMT.USER_MST_PK ) Q");
                sqlmain.Append(" ORDER BY Q.ORDERS");

                if (biztype == 1 & process == 1)
                {
                    //sqlmain.Replace("hbl", "hawb")
                    //sqlmain.Replace("mbl", "mawb")
                    //sqlmain.Replace("BOOKING_TRN", "booking_trn_air")
                    //sqlmain.Replace("QUOTATION_DTL_TBL", "quotation_trn_air")
                    //sqlmain.Replace("sea", "air")
                }
                return objwf.GetDataTable(sqlmain.ToString());
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
        public string InsertToUserTaskListTable(string strcond)
        {
            WorkFlow objwf = new WorkFlow();
            objwf.OpenConnection();
            OracleTransaction TRAN = null;
            OracleCommand objcmd = new OracleCommand();
            TRAN = objwf.MyConnection.BeginTransaction();
            Array arr = null;
            string activitypk = null;
            int actType = 0;
            int RefNoFk = 0;
            string Refno = null;
            string status = null;
            string priority = null;
            int userpk = 0;
            int refer = 0;
            Int32 admpk = default(Int32);
            try
            {
                arr = strcond.Split('~');
                if (arr.Length > 1)
                    activitypk = Convert.ToString(arr.GetValue(1));
                if (arr.Length > 2)
                    RefNoFk = Convert.ToInt32(Convert.ToString(arr.GetValue(2)));
                if (arr.Length > 3)
                    Refno = Convert.ToString(arr.GetValue(3));
                if (arr.Length > 4)
                    status = Convert.ToString(arr.GetValue(4));
                if (arr.Length > 5)
                    priority = Convert.ToString(arr.GetValue(5));
                if (arr.Length > 6)
                    userpk = Convert.ToInt32(arr.GetValue(6));
                if (arr.Length > 7)
                    refer = Convert.ToInt32(arr.GetValue(7));
                if (arr.Length > 8)
                    admpk = Convert.ToInt32(arr.GetValue(8));

                var _with1 = objcmd;
                _with1.Transaction = TRAN;
                _with1.Connection = objwf.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objwf.MyUserName + ".workflow_rules_entry_pkg.Check_PK";
                _with1.Parameters.Add("activitypk", activitypk).Direction = ParameterDirection.Input;
                _with1.Parameters.Add("RefFk", RefNoFk).Direction = ParameterDirection.Input;
                //.Parameters.Add("Refer", getDefault(refer, 0)).Direction = ParameterDirection.Input
                _with1.Parameters.Add("sel", OracleDbType.Int32, 10, "sel").Direction = ParameterDirection.Output;
                objcmd.ExecuteNonQuery();
                flag = Convert.ToInt64(objcmd.Parameters["sel"].Value);
                if (flag == 0)
                {
                    var _with2 = objwf.MyCommand;
                    _with2.Transaction = TRAN;
                    _with2.Connection = objwf.MyConnection;
                    _with2.CommandType = CommandType.StoredProcedure;
                    _with2.CommandText = objwf.MyUserName + ".workflow_rules_entry_pkg.user_task_list_ins";
                    _with2.Parameters.Add("activitypk", Convert.ToInt32(activitypk)).Direction = ParameterDirection.Input;
                    //.Parameters.Add("Acttype", actType).Direction = ParameterDirection.Input
                    _with2.Parameters.Add("RefFk", RefNoFk).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("Refno", Refno).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("status", status).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("Priority", priority).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("Userpk", userpk).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("Refer", getDefault(refer, 0)).Direction = ParameterDirection.Input;
                    _with2.Parameters.Add("admfk", getDefault(admpk, 0)).Direction = ParameterDirection.Input;
                    objwf.MyCommand.ExecuteNonQuery();
                    TRAN.Commit();
                    return "Saved";
                }
                else
                {
                    return flag.ToString();
                }
                //Manjunath  PTS ID:Sep-02  26/09/2011
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                TRAN.Rollback();
                return "No Saving";
            }
            finally
            {
                objwf.CloseConnection();
            }
        }
    }
}
