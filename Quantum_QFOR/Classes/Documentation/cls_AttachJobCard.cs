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
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_AttachJobCard : CommonFeatures
    {

        #region "List of Members of the Class"
        private Int64 JobCard_PK;
        private string JobCard_Ref_Nr;
        private string M_Line;
        private string M_Customer;
        #endregion
        private string M_Air_Line;

        #region "List of Properties"
        public Int64 JobCard_Mst_Pk
        {
            get { return JobCard_PK; }
            set { JobCard_PK = value; }
        }

        public string JobCard_RefNr
        {
            get { return JobCard_Ref_Nr; }
            set { JobCard_Ref_Nr = value; }
        }

        public string Shipper_Line
        {
            get { return M_Line; }
            set { M_Line = value; }
        }
        public string Customer
        {
            get { return M_Customer; }
            set { M_Customer = value; }
        }
        public string Air_Line
        {
            get { return M_Air_Line; }
            set { M_Air_Line = value; }
        }

        #endregion

        #region "FetchChildJobCards"
        public DataSet FetchChildJobCards(string JobCardPk = "", string POLPK = "", string PODPK = "", string OPRFK = "", string JobCardRefNr = "", string Line = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string commGroup = "",
        string CargoType = "", string DpAgent = "")
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition1 = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition2 = new System.Text.StringBuilder();
            string Condition = null;
            string Condition1 = null;
            string Condition2 = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (JobCardRefNr == "JOB CARD REF NO")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else if (JobCardRefNr == "LINE")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(OPR.OPERATOR_ID) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(CUS.CUSTOMER_NAME) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }

            if (JobCardPk != "null" | !string.IsNullOrEmpty(JobCardPk) | JobCardPk != "Nothing")
            {
                if (JobCardPk.Length > 0)
                {
                    strCondition1.Append( "and JOB.JOB_CARD_TRN_PK not in (" + JobCardPk + " )");
                    strCondition2.Append( "and JOB.JOB_CARD_TRN_PK in (" + JobCardPk + " )");
                }
            }
            Condition = strCondition.ToString();
            Condition1 = strCondition1.ToString();
            Condition2 = strCondition2.ToString();

            strQuery.Append("SELECT Count(*) from JOB_CARD_TRN JOB," );
            strQuery.Append("CUSTOMER_MST_TBL CUS," );
            strQuery.Append("BOOKING_MST_TBL BOO," );
            strQuery.Append("OPERATOR_MST_TBL OPR," );
            strQuery.Append("USER_MST_TBL USRTBL" );
            strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.BOOKING_MST_FK=BOO.BOOKING_MST_PK(+)" );
            strQuery.Append("AND BOO.CARRIER_MST_FK=OPR.OPERATOR_MST_PK(+)" );
            strQuery.Append(" AND BOO.CARGO_TYPE=" + CargoType );
            strQuery.Append(" AND JOB.JOB_CARD_STATUS=1" );
            strQuery.Append("AND JOB.MASTER_JC_FK IS NULL" );
            strQuery.Append("AND BOO.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append("AND BOO.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append("AND OPR.OPERATOR_MST_PK=" + OPRFK );
            strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
            //strQuery.Append("AND JOB.DP_AGENT_MST_FK=" & DpAgent & vbCrLf)
            strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
            // strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK & vbCrLf)
            strQuery.Append(Condition );

            strSQL = strQuery.ToString();

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;


            strQuery.Remove(0, strQuery.Length);
            strQuery.Append(" select * from (" );
            strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM (" );


            if (!string.IsNullOrEmpty(Condition2.ToString()))
            {
                strQuery.Append("SELECT" );
                strQuery.Append("JOB.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK," );
                strQuery.Append("JOB.JOBCARD_REF_NO," );
                strQuery.Append("OPR.OPERATOR_ID," );
                strQuery.Append("CUS.CUSTOMER_NAME," );
                strQuery.Append("    sum(nvl(DECODE(JTC.GROSS_WEIGHT," );
                strQuery.Append("                  0," );
                strQuery.Append("                 (SELECT SUM(PC.NET_WEIGHT)" );
                strQuery.Append("           FROM JOB_TRN_COMMODITY PC" );
                strQuery.Append("            WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK) + " );
                strQuery.Append("         CTMT.CONTAINER_TAREWEIGHT_TONE," );
                strQuery.Append("          JTC.GROSS_WEIGHT),0)) GROSS_WEIGHT," );
                strQuery.Append("       sum(nvl(DECODE(JTC.VOLUME_IN_CBM," );
                strQuery.Append("              0," );
                strQuery.Append("            (SELECT SUM(PC.VOLUME_IN_CBM)" );
                strQuery.Append("               FROM JOB_TRN_COMMODITY PC" );
                strQuery.Append("               WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK)," );
                strQuery.Append("            JTC.VOLUME_IN_CBM),0)) VOLUME_IN_CBM," );
                strQuery.Append(" sum(nvl(JTC.CHARGEABLE_WEIGHT , 0)) as CHARGEABLE_WEIGHT," );
                strQuery.Append("'TRUE' SEL " );
                strQuery.Append("FROM JOB_CARD_TRN JOB," );
                strQuery.Append("  job_trn_cont JTC," );
                strQuery.Append("CONTAINER_TYPE_MST_TBL CTMT," );
                strQuery.Append("CUSTOMER_MST_TBL CUS," );
                strQuery.Append("BOOKING_MST_TBL BOO," );
                strQuery.Append("OPERATOR_MST_TBL OPR," );
                strQuery.Append("USER_MST_TBL USRTBL" );
                strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
                strQuery.Append("AND JOB.BOOKING_MST_FK=BOO.BOOKING_MST_PK(+)" );
                strQuery.Append("AND BOO.CARRIER_MST_FK=OPR.OPERATOR_MST_PK(+)" );
                strQuery.Append("AND BOO.CARGO_TYPE=" + CargoType );
                strQuery.Append(" AND JOB.JOB_CARD_STATUS=1" );
                strQuery.Append("AND JOB.MBL_MAWB_FK IS NULL" );
                strQuery.Append("AND BOO.PORT_MST_POL_FK=" + POLPK );
                strQuery.Append("AND BOO.PORT_MST_POD_FK=" + PODPK );
                strQuery.Append("AND OPR.OPERATOR_MST_PK=" + OPRFK );
                strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );

                strQuery.Append("  AND JTC.JOB_CARD_TRN_FK =JOB.JOB_CARD_TRN_PK " );
                strQuery.Append("  AND JTC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)" );

                //strQuery.Append(" AND JOB.DP_AGENT_MST_FK=" & DpAgent & vbCrLf)
                strQuery.Append(" AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
                //  strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK & vbCrLf)
                strQuery.Append(Condition2 );
                strQuery.Append(Condition );
                strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,");
                strQuery.Append("JOB.JOBCARD_REF_NO," );
                strQuery.Append("OPR.OPERATOR_ID," );
                strQuery.Append("CUS.CUSTOMER_NAME" );
                strQuery.Append(" UNION " );
            }

            strQuery.Append("SELECT" );
            strQuery.Append("JOB.JOB_CARD_TRN_PK JOB_CARD_SEA_EXP_PK," );
            strQuery.Append("JOB.JOBCARD_REF_NO," );
            strQuery.Append("OPR.OPERATOR_ID," );
            strQuery.Append("CUS.CUSTOMER_NAME," );

            strQuery.Append("     sum(nvl(DECODE(JTC.GROSS_WEIGHT," );
            strQuery.Append("                  0," );
            strQuery.Append("                 (SELECT SUM(PC.NET_WEIGHT)" );
            strQuery.Append("           FROM JOB_TRN_COMMODITY PC" );
            strQuery.Append("            WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK) + " );
            strQuery.Append("         CTMT.CONTAINER_TAREWEIGHT_TONE," );
            strQuery.Append("          JTC.GROSS_WEIGHT),0)) GROSS_WEIGHT," );
            strQuery.Append("        sum(nvl(DECODE(JTC.VOLUME_IN_CBM," );
            strQuery.Append("              0," );
            strQuery.Append("            (SELECT SUM(PC.VOLUME_IN_CBM)" );
            strQuery.Append("               FROM JOB_TRN_COMMODITY PC" );
            strQuery.Append("               WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK)," );
            strQuery.Append("            JTC.VOLUME_IN_CBM),0)) VOLUME_IN_CBM," );
            strQuery.Append("   sum(nvl(JTC.CHARGEABLE_WEIGHT,0)) AS CHARGEABLE_WEIGHT," );
            strQuery.Append("'FALSE' SEL " );
            strQuery.Append("FROM JOB_CARD_TRN JOB," );

            strQuery.Append("  job_trn_cont JTC," );
            strQuery.Append("CONTAINER_TYPE_MST_TBL CTMT," );
            strQuery.Append("CUSTOMER_MST_TBL CUS," );
            strQuery.Append("BOOKING_MST_TBL BOO," );
            strQuery.Append("OPERATOR_MST_TBL OPR," );
            strQuery.Append("USER_MST_TBL USRTBL" );
            strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.BOOKING_MST_FK=BOO.BOOKING_MST_PK(+)" );
            strQuery.Append("AND BOO.CARRIER_MST_FK=OPR.OPERATOR_MST_PK(+)" );
            strQuery.Append("AND BOO.CARGO_TYPE=" + CargoType );
            strQuery.Append(" AND JOB.JOB_CARD_STATUS=1" );
            strQuery.Append("AND JOB.MASTER_JC_FK IS NULL" );
            strQuery.Append("AND JOB.MBL_MAWB_FK IS NULL" );


            strQuery.Append("  AND JTC.JOB_CARD_TRN_FK =JOB.JOB_CARD_TRN_PK " );
            strQuery.Append("  AND JTC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)" );

            strQuery.Append("AND BOO.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append("AND BOO.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append("AND OPR.OPERATOR_MST_PK=" + OPRFK );
            strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
            //strQuery.Append("AND JOB.DP_AGENT_MST_FK=" & DpAgent & vbCrLf)
            strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
            //strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " & LoggedIn_Loc_FK & vbCrLf)
            strQuery.Append(Condition1 );
            strQuery.Append(Condition );
            strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,");
            strQuery.Append("JOB.JOBCARD_REF_NO," );
            strQuery.Append("OPR.OPERATOR_ID," );
            strQuery.Append("CUS.CUSTOMER_NAME" );
            strQuery.Append(" order by JOB_CARD_SEA_EXP_PK DESC, JOBCARD_REF_NO DESC) q ) " );
            strQuery.Append(" WHERE SR_NO  Between " + start + " and " + last );
            strSQL = strQuery.ToString();
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
        #endregion

        #region "FetchChildJobCardsImp"
        public DataSet FetchChildJobCardsImp(string JobCardPk = "", string POLPK = "", string PODPK = "", string JobCardRefNr = "", string Line = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string commGroup = "", string CargoType = "",
        string DpAgent = "")
        {

            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition1 = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition2 = new System.Text.StringBuilder();
            string Condition = null;
            string Condition1 = null;
            string Condition2 = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (JobCardRefNr == "JOB CARD REF NO")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else if (JobCardRefNr == "LINE")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(OPR.OPERATOR_ID) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(CUS.CUSTOMER_NAME) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }

            if (JobCardPk != "null" | !string.IsNullOrEmpty(JobCardPk) | JobCardPk != "Nothing")
            {
                if (JobCardPk.Length > 0)
                {
                    strCondition1.Append( "and JOB.JOB_CARD_TRN_PK not in (" + JobCardPk + " )");
                    strCondition2.Append( "and JOB.JOB_CARD_TRN_PK in (" + JobCardPk + " )");
                }
            }
            Condition = strCondition.ToString();
            Condition1 = strCondition1.ToString();
            Condition2 = strCondition2.ToString();

            strQuery.Append(" SELECT Count(*) from JOB_CARD_TRN JOB," );
            strQuery.Append(" CUSTOMER_MST_TBL CUS," );
            strQuery.Append(" OPERATOR_MST_TBL OPR," );
            strQuery.Append(" USER_MST_TBL USRTBL" );
            strQuery.Append(" WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append(" AND JOB.CARRIER_MST_FK=OPR.OPERATOR_MST_PK(+)" );
            strQuery.Append(" AND JOB.CARGO_TYPE=" + CargoType );
            strQuery.Append(" AND JOB.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append(" AND JOB.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append(" AND JOB.COMMODITY_GROUP_FK=" + commGroup );
            strQuery.Append(" AND ((USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK + " and " );
            strQuery.Append(" Job.JC_AUTO_MANUAL = 0) OR (Job.PORT_MST_POD_FK IN " );
            strQuery.Append(" (SELECT T.PORT_MST_FK " );
            strQuery.Append(" FROM LOC_PORT_MAPPING_TRN T " );
            strQuery.Append(" WHERE T.LOCATION_MST_FK = " + LoggedIn_Loc_FK + ") and " );
            strQuery.Append(" Job.JC_AUTO_MANUAL = 1)) " );
            strQuery.Append(" AND JOB.DP_AGENT_MST_FK=" + DpAgent );
            strQuery.Append(" AND Job.CREATED_BY_FK = USRTBL.USER_MST_PK " );
            strQuery.Append(Condition );

            strSQL = strQuery.ToString();


            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;


            strQuery.Remove(0, strQuery.Length);
            strQuery.Append(" select * from (" );
            strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM (" );
            if (!string.IsNullOrEmpty(Condition2.ToString()))
            {
                strQuery.Append("SELECT" );
                strQuery.Append("JOB.JOB_CARD_TRN_PK," );
                strQuery.Append("JOB.JOBCARD_REF_NO," );
                strQuery.Append("OPR.OPERATOR_ID," );
                strQuery.Append("CUS.CUSTOMER_NAME," );
                strQuery.Append("'TRUE' SEL " );
                strQuery.Append("FROM JOB_CARD_TRN JOB," );
                strQuery.Append("CUSTOMER_MST_TBL CUS," );
                strQuery.Append("OPERATOR_MST_TBL OPR," );
                strQuery.Append("USER_MST_TBL USRTBL" );
                strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
                strQuery.Append(" AND JOB.CARGO_TYPE=" + CargoType );
                strQuery.Append("AND JOB.MASTER_JC_SEA_IMP_FK IS NULL" );
                strQuery.Append("AND JOB.CARRIER_MST_FK=OPR.OPERATOR_MST_PK(+)" );
                strQuery.Append("AND JOB.PORT_MST_POL_FK=" + POLPK );
                strQuery.Append("AND JOB.PORT_MST_POD_FK=" + PODPK );
                strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );

                strQuery.Append(" AND ((USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK + " and " );
                strQuery.Append(" Job.JC_AUTO_MANUAL = 0) OR (Job.PORT_MST_POD_FK IN " );
                strQuery.Append(" (SELECT T.PORT_MST_FK " );
                strQuery.Append(" FROM LOC_PORT_MAPPING_TRN T " );
                strQuery.Append(" WHERE T.LOCATION_MST_FK = " + LoggedIn_Loc_FK + ") and " );
                strQuery.Append(" Job.JC_AUTO_MANUAL = 1)) " );
                strQuery.Append(" AND JOB.DP_AGENT_MST_FK=" + DpAgent );
                strQuery.Append(" AND Job.CREATED_BY_FK = USRTBL.USER_MST_PK " );
                strQuery.Append(Condition );
                strQuery.Append(Condition2 );
                strQuery.Append(" UNION " );
            }
            strQuery.Append("SELECT" );
            strQuery.Append("JOB.JOB_CARD_TRN_PK," );
            strQuery.Append("JOB.JOBCARD_REF_NO," );
            strQuery.Append("OPR.OPERATOR_ID," );
            strQuery.Append("CUS.CUSTOMER_NAME," );
            strQuery.Append("'FALSE' SEL " );
            strQuery.Append("FROM JOB_CARD_TRN JOB," );
            strQuery.Append("CUSTOMER_MST_TBL CUS," );
            strQuery.Append("OPERATOR_MST_TBL OPR," );
            strQuery.Append("USER_MST_TBL USRTBL" );
            strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append(" AND JOB.CARGO_TYPE=" + CargoType );
            strQuery.Append("AND JOB.MASTER_JC_SEA_IMP_FK IS NULL" );
            strQuery.Append("AND JOB.CARRIER_MST_FK=OPR.OPERATOR_MST_PK(+)" );
            strQuery.Append("AND JOB.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append("AND JOB.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );

            strQuery.Append(" AND ((USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK + " and " );
            strQuery.Append(" Job.JC_AUTO_MANUAL = 0) OR (Job.PORT_MST_POD_FK IN " );
            strQuery.Append(" (SELECT T.PORT_MST_FK " );
            strQuery.Append(" FROM LOC_PORT_MAPPING_TRN T " );
            strQuery.Append(" WHERE T.LOCATION_MST_FK = " + LoggedIn_Loc_FK + ") and " );
            strQuery.Append(" Job.JC_AUTO_MANUAL = 1)) " );
            strQuery.Append(" AND JOB.DP_AGENT_MST_FK=" + DpAgent );
            strQuery.Append(" AND Job.CREATED_BY_FK = USRTBL.USER_MST_PK " );
            strQuery.Append(Condition );
            strQuery.Append(Condition1 );
            strQuery.Append(" order by JOB_CARD_TRN_PK DESC, JOBCARD_REF_NO DESC) q ) " );
            strQuery.Append(" WHERE SR_NO  Between " + start + " and " + last );
            strSQL = strQuery.ToString();
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
        #endregion

        #region "Fetch JobCardAir Popup Function"
        public DataSet FetchChildJobCardsAir(string JobCardPk = "", string POLPK = "", string PODPK = "", string AIRLINEFK = "", string JobCardRefNr = "", string Line = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string commGroup = "",
        string cargoType = "")
        {


            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition1 = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition2 = new System.Text.StringBuilder();
            string Condition = null;
            string Condition1 = null;
            string Condition2 = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (JobCardRefNr == "JOB CARD REF NO")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else if (JobCardRefNr == "AIRLINE")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(AIRLINE.AIRLINE_ID) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(CUS.CUSTOMER_NAME) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            if (cargoType == "1")
            {
                strCondition.Append( " and BOO.Cargo_Type <> 2");
            }
            if (JobCardPk != "null" | !string.IsNullOrEmpty(JobCardPk) | JobCardPk != "Nothing")
            {
                if (JobCardPk.Length > 0)
                {
                    strCondition1.Append( "and JOB.JOB_CARD_TRN_PK not in (" + JobCardPk + " )");
                    strCondition2.Append( "and JOB.JOB_CARD_TRN_PK in (" + JobCardPk + " )");
                }
            }

            Condition = strCondition.ToString();
            Condition1 = strCondition1.ToString();
            Condition2 = strCondition2.ToString();

            strQuery.Append("SELECT Count(*) from JOB_CARD_TRN JOB," );
            strQuery.Append("CUSTOMER_MST_TBL CUS," );
            strQuery.Append("BOOKING_MST_TBL BOO," );
            strQuery.Append("AIRLINE_MST_TBL AIRLINE," );
            strQuery.Append("USER_MST_TBL USRTBL" );
            strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.BOOKING_MST_FK=BOO.BOOKING_MST_PK(+)" );
            strQuery.Append("AND BOO.CARRIER_MST_FK=AIRLINE.AIRLINE_MST_PK(+)" );
            strQuery.Append(" AND JOB.JOB_CARD_STATUS = 1" );
            strQuery.Append("AND JOB.MBL_MAWB_FK IS NULL" );
            strQuery.Append("AND BOO.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append("AND BOO.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append(" AND AIRLINE.AIRLINE_MST_PK=" + AIRLINEFK );
            strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
            strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
            strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK );
            strQuery.Append(Condition );
            strSQL = strQuery.ToString();
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            strQuery.Remove(0, strQuery.Length);
            strQuery.Append(" select * from (" );
            strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM (" );
            if (!string.IsNullOrEmpty(Condition2.ToString()))
            {
                strQuery.Append("SELECT" );
                strQuery.Append("JOB.JOB_CARD_TRN_PK JOB_CARD_AIR_EXP_PK," );
                strQuery.Append("JOB.JOBCARD_REF_NO," );
                strQuery.Append("AIRLINE.AIRLINE_ID," );
                strQuery.Append("CUS.CUSTOMER_NAME," );
                strQuery.Append("    sum(nvl(DECODE(JTC.GROSS_WEIGHT," );
                strQuery.Append("                  0," );
                strQuery.Append("                 (SELECT SUM(PC.NET_WEIGHT)" );
                strQuery.Append("           FROM JOB_TRN_COMMODITY PC" );
                strQuery.Append("            WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK) + " );
                strQuery.Append("         CTMT.CONTAINER_TAREWEIGHT_TONE," );
                strQuery.Append("          JTC.GROSS_WEIGHT),0)) GROSS_WEIGHT," );
                strQuery.Append("       sum(nvl(DECODE(JTC.VOLUME_IN_CBM," );
                strQuery.Append("              0," );
                strQuery.Append("            (SELECT SUM(PC.VOLUME_IN_CBM)" );
                strQuery.Append("               FROM JOB_TRN_COMMODITY PC" );
                strQuery.Append("               WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK)," );
                strQuery.Append("            JTC.VOLUME_IN_CBM),0)) VOLUME_IN_CBM," );
                strQuery.Append("  sum(nvl(JTC.CHARGEABLE_WEIGHT, 0)) as CHARGEABLE_WEIGHT," );
                strQuery.Append("'TRUE' SEL " );
                strQuery.Append("FROM JOB_CARD_TRN JOB," );
                strQuery.Append("  job_trn_cont JTC," );
                strQuery.Append("CONTAINER_TYPE_MST_TBL CTMT," );
                strQuery.Append("CUSTOMER_MST_TBL CUS," );
                strQuery.Append("BOOKING_MST_TBL BOO," );
                strQuery.Append("AIRLINE_MST_TBL AIRLINE," );
                strQuery.Append("USER_MST_TBL USRTBL" );
                strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
                strQuery.Append("AND JOB.BOOKING_MST_FK=BOO.BOOKING_MST_PK(+)" );
                strQuery.Append("AND BOO.CARRIER_MST_FK=AIRLINE.AIRLINE_MST_PK(+)" );

                strQuery.Append("  AND JTC.JOB_CARD_TRN_FK =JOB.JOB_CARD_TRN_PK " );
                strQuery.Append("  AND JTC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)" );
                strQuery.Append(" AND JOB.JOB_CARD_STATUS = 1" );
                strQuery.Append("AND JOB.MBL_MAWB_FK IS NULL" );
                strQuery.Append("AND BOO.PORT_MST_POL_FK=" + POLPK );
                strQuery.Append("AND BOO.PORT_MST_POD_FK=" + PODPK );
                strQuery.Append(" AND AIRLINE.AIRLINE_MST_PK=" + AIRLINEFK );
                strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
                strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
                strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK );
                strQuery.Append(Condition2 );
                strQuery.Append(Condition );
                strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,");
                strQuery.Append("JOB.JOBCARD_REF_NO," );
                strQuery.Append("AIRLINE.AIRLINE_ID," );
                strQuery.Append("CUS.CUSTOMER_NAME" );
                strQuery.Append(" UNION " );
            }

            strQuery.Append("SELECT" );
            strQuery.Append("JOB.JOB_CARD_TRN_PK JOB_CARD_AIR_EXP_PK," );
            strQuery.Append("JOB.JOBCARD_REF_NO," );
            strQuery.Append("AIRLINE.AIRLINE_ID," );
            strQuery.Append("CUS.CUSTOMER_NAME," );
            strQuery.Append("    sum(nvl(DECODE(JTC.GROSS_WEIGHT," );
            strQuery.Append("                  0," );
            strQuery.Append("                 (SELECT SUM(PC.NET_WEIGHT)" );
            strQuery.Append("           FROM JOB_TRN_COMMODITY PC" );
            strQuery.Append("            WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK) + " );
            strQuery.Append("         CTMT.CONTAINER_TAREWEIGHT_TONE," );
            strQuery.Append("          JTC.GROSS_WEIGHT),0)) GROSS_WEIGHT," );
            strQuery.Append("       sum(nvl(DECODE(JTC.VOLUME_IN_CBM," );
            strQuery.Append("              0," );
            strQuery.Append("            (SELECT SUM(PC.VOLUME_IN_CBM)" );
            strQuery.Append("               FROM JOB_TRN_COMMODITY PC" );
            strQuery.Append("               WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK)," );
            strQuery.Append("            JTC.VOLUME_IN_CBM),0)) VOLUME_IN_CBM," );
            strQuery.Append("  sum(nvl(JTC.CHARGEABLE_WEIGHT,0)) AS CHARGEABLE_WEIGHT," );
            strQuery.Append("'FALSE' SEL " );
            strQuery.Append("FROM JOB_CARD_TRN JOB," );
            strQuery.Append("  job_trn_cont JTC," );
            strQuery.Append("CONTAINER_TYPE_MST_TBL CTMT," );
            strQuery.Append("CUSTOMER_MST_TBL CUS," );
            strQuery.Append("BOOKING_MST_TBL BOO," );
            strQuery.Append("AIRLINE_MST_TBL AIRLINE," );
            strQuery.Append("USER_MST_TBL USRTBL" );
            strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.BOOKING_MST_FK=BOO.BOOKING_MST_PK(+)" );
            strQuery.Append("AND BOO.CARRIER_MST_FK=AIRLINE.AIRLINE_MST_PK(+)" );

            strQuery.Append("  AND JTC.JOB_CARD_TRN_FK =JOB.JOB_CARD_TRN_PK " );
            strQuery.Append("  AND JTC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)" );
            strQuery.Append(" AND JOB.JOB_CARD_STATUS = 1" );
            strQuery.Append("AND JOB.MASTER_JC_FK IS NULL" );
            strQuery.Append("AND JOB.MBL_MAWB_FK IS NULL" );
            strQuery.Append("AND BOO.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append("AND BOO.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append(" AND AIRLINE.AIRLINE_MST_PK=" + AIRLINEFK );
            strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
            strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
            strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK );
            strQuery.Append(Condition1 );
            strQuery.Append(Condition );
            strQuery.Append(" GROUP BY JOB.JOB_CARD_TRN_PK,");
            strQuery.Append("JOB.JOBCARD_REF_NO," );
            strQuery.Append("AIRLINE.AIRLINE_ID," );
            strQuery.Append("CUS.CUSTOMER_NAME" );
            strQuery.Append(" order by JOB_CARD_AIR_EXP_PK DESC, JOBCARD_REF_NO DESC) q ) " );
            strQuery.Append(" WHERE SR_NO  Between " + start + " and " + last );

            strSQL = strQuery.ToString();

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
        #endregion

        #region "Fetch JobCardAirImp Popup Function"
        public DataSet FetchChildJobCardsAirImp(string JobCardPk = "", string POLPK = "", string PODPK = "", string JobCardRefNr = "", string Line = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string SortType = " ASC ", string commGroup = "", string cargoType = "")
        {


            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string strSQL = null;
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition1 = new System.Text.StringBuilder();
            System.Text.StringBuilder strCondition2 = new System.Text.StringBuilder();
            string Condition = null;
            string Condition1 = null;
            string Condition2 = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            if (JobCardRefNr == "JOB CARD REF NO")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(JOB.JOBCARD_REF_NO) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else if (JobCardRefNr == "AIRLINE")
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(AIRLINE.AIRLINE_ID) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                if (Line.Trim().Length > 0)
                {
                    strCondition.Append( " AND UPPER(CUS.CUSTOMER_NAME) LIKE '%" + Line.ToUpper().Replace("'", "''") + "%'");
                }
            }
            if (cargoType == "1")
            {
                strCondition.Append( " and BOO.Cargo_Type <> 2");
            }

            if (JobCardPk != "null" | !string.IsNullOrEmpty(JobCardPk) | JobCardPk != "Nothing")
            {
                if (JobCardPk.Length > 0)
                {
                    strCondition1.Append( "and JOB.JOB_CARD_TRN_PK not in (" + JobCardPk + " )");
                    strCondition2.Append( "and JOB.JOB_CARD_TRN_PK in (" + JobCardPk + " )");
                }
            }
            Condition = strCondition.ToString();
            Condition1 = strCondition1.ToString();
            Condition2 = strCondition2.ToString();

            strQuery.Append("SELECT Count(*) from JOB_CARD_TRN JOB," );
            strQuery.Append("CUSTOMER_MST_TBL CUS," );
            strQuery.Append("AIRLINE_MST_TBL AIRLINE," );
            strQuery.Append("USER_MST_TBL USRTBL" );
            strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.CARRIER_MST_FK=AIRLINE.AIRLINE_MST_PK(+)" );
            strQuery.Append("AND JOB.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append("AND JOB.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
            strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
            strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK );
            strQuery.Append(Condition );
            strSQL = strQuery.ToString();
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strSQL));
            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
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
            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;

            strQuery.Remove(0, strQuery.Length);
            strQuery.Append(" select * from (" );
            strQuery.Append("SELECT ROWNUM SR_NO,q.* FROM (" );
            if (!string.IsNullOrEmpty(Condition2.ToString()))
            {
                strQuery.Append("SELECT" );
                strQuery.Append("JOB.JOB_CARD_TRN_PK," );
                strQuery.Append("JOB.JOBCARD_REF_NO," );
                strQuery.Append("AIRLINE.AIRLINE_ID," );
                strQuery.Append("CUS.CUSTOMER_NAME," );
                strQuery.Append("'TRUE' SEL " );
                strQuery.Append("FROM JOB_CARD_TRN JOB," );
                strQuery.Append("CUSTOMER_MST_TBL CUS," );
                strQuery.Append("AIRLINE_MST_TBL AIRLINE," );
                strQuery.Append("USER_MST_TBL USRTBL" );
                strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
                strQuery.Append("AND JOB.MASTER_JC_AIR_IMP_FK IS NULL" );
                strQuery.Append("AND JOB.CARRIER_MST_FK=AIRLINE.AIRLINE_MST_PK(+)" );
                strQuery.Append("AND JOB.PORT_MST_POL_FK=" + POLPK );
                strQuery.Append("AND JOB.PORT_MST_POD_FK=" + PODPK );
                strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
                strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
                strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK );
                strQuery.Append(Condition );
                strQuery.Append(Condition2 );
                strQuery.Append(" UNION " );
            }
            strQuery.Append("SELECT" );
            strQuery.Append("JOB.JOB_CARD_TRN_PK," );
            strQuery.Append("JOB.JOBCARD_REF_NO," );
            strQuery.Append("AIRLINE.AIRLINE_ID," );
            strQuery.Append("CUS.CUSTOMER_NAME," );
            strQuery.Append("'FALSE' SEL " );
            strQuery.Append("FROM JOB_CARD_TRN JOB," );
            strQuery.Append("CUSTOMER_MST_TBL CUS," );
            strQuery.Append("AIRLINE_MST_TBL AIRLINE," );
            strQuery.Append("USER_MST_TBL USRTBL" );
            strQuery.Append("WHERE JOB.SHIPPER_CUST_MST_FK=CUS.CUSTOMER_MST_PK(+)" );
            strQuery.Append("AND JOB.MASTER_JC_AIR_IMP_FK IS NULL" );
            strQuery.Append("AND JOB.CARRIER_MST_FK=AIRLINE.AIRLINE_MST_PK(+)" );
            strQuery.Append("AND JOB.PORT_MST_POL_FK=" + POLPK );
            strQuery.Append("AND JOB.PORT_MST_POD_FK=" + PODPK );
            strQuery.Append("AND JOB.COMMODITY_GROUP_FK=" + commGroup );
            strQuery.Append("AND USRTBL.USER_MST_PK=JOB.CREATED_BY_FK" );
            strQuery.Append("AND USRTBL.DEFAULT_LOCATION_FK = " + LoggedIn_Loc_FK );
            strQuery.Append(Condition );
            strQuery.Append(Condition1 );
            strQuery.Append(" order by JOB_CARD_TRN_PK DESC, JOBCARD_REF_NO DESC) q ) " );
            strQuery.Append(" WHERE SR_NO  Between " + start + " and " + last );
            strSQL = strQuery.ToString();
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
        #endregion

        #region "Fetch Footer Dtls"
        public DataSet FetchFooterValues(string JCPK = "")
        {
            System.Text.StringBuilder strQuery = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            strQuery.Append("SELECT" );
            strQuery.Append(" sum(nvl(DECODE(JTC.GROSS_WEIGHT," );
            strQuery.Append(" 0," );
            strQuery.Append(" (SELECT SUM(PC.NET_WEIGHT)" );
            strQuery.Append(" FROM JOB_TRN_COMMODITY PC" );
            strQuery.Append(" WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK) +" );
            strQuery.Append(" CTMT.CONTAINER_TAREWEIGHT_TONE," );
            strQuery.Append(" JTC.GROSS_WEIGHT)," );
            strQuery.Append(" 0)) GROSS_WEIGHT," );
            strQuery.Append(" sum(nvl(DECODE(JTC.VOLUME_IN_CBM," );
            strQuery.Append(" 0," );
            strQuery.Append(" (SELECT SUM(PC.VOLUME_IN_CBM)" );
            strQuery.Append(" FROM JOB_TRN_COMMODITY PC" );
            strQuery.Append(" WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK)," );
            strQuery.Append(" JTC.VOLUME_IN_CBM)," );
            strQuery.Append(" 0)) VOLUME_IN_CBM," );
            strQuery.Append(" sum(nvl(JTC.CHARGEABLE_WEIGHT, 0)) as CHARGEABLE_WEIGHT" );
            strQuery.Append(" FROM JOB_CARD_TRN JOB, job_trn_cont JTC, CONTAINER_TYPE_MST_TBL CTMT" );
            strQuery.Append(" WHERE 1 = 1" );
            strQuery.Append(" AND JTC.JOB_CARD_TRN_FK = JOB.JOB_CARD_TRN_PK" );
            strQuery.Append(" AND JTC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+)" );
            strQuery.Append(" and job.job_card_trn_pk in(" + JCPK + ")");
            try
            {
                return objWF.GetDataSet(strQuery.ToString());
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