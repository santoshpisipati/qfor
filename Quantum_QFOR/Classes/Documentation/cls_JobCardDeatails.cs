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
    public class clsJobCardDeatails : CommonFeatures
    {
        #region "For fetching Freight Deatils"

        /// <summary>
        /// Fetches the freight.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <returns></returns>
        public DataSet FetchFreight(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "", int CurrPK = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();

            if ((JCPK == null))
            {
                JCPK = "0";
            }
            if (Convert.ToInt32(Pro) == 2)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append("jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' CONTAINER_TYPE_ID,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("CASE WHEN JC.CARGO_TYPE = 4 THEN  PACK.PACK_TYPE_DESC ");
                    strSQL.Append(" ELSE NVL((SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (SELECT ");
                    strSQL.Append(" DISTINCT JC.PACK_TYPE_FK FROM JOB_TRN_CONT JOB,JOB_TRN_COMMODITY JC WHERE JOB.JOB_TRN_CONT_PK = JC.JOB_TRN_CONT_FK");
                    strSQL.Append(" AND JOB.JOB_TRN_CONT_PK = ' || JTC.JOB_TRN_CONT_PK || ')') FROM DUAL),");
                    strSQL.Append(" PACK.PACK_TYPE_DESC) END Dimention_id,");
                    strSQL.Append(" DECODE(JTC.PACK_COUNT,  0,");
                    strSQL.Append(" (SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strSQL.Append(" WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strSQL.Append(" JTC.PACK_COUNT) QUANTITY,");
                    strSQL.Append("Decode(JC.Pymt_Type, 1, 'Prepaid', 2, 'Collect') Pymt_Type,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.freight_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("JOB_TRN_FD jt,");
                    strSQL.Append("JOB_TRN_CONT            JTC,");
                    strSQL.Append("PACK_TYPE_MST_TBL       PACK,");
                    strSQL.Append("currency_type_mst_tbl cur,");
                    strSQL.Append("Dimention_Unit_Mst_Tbl di");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK(+)=jc.JOB_CARD_TRN_PK");
                    strSQL.Append("AND JC.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strSQL.Append("AND JTC.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    strSQL.Append("and di.dimention_unit_mst_pk(+)=jt.BASIS_FK");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK, JC.Pymt_Type,JTC.PACK_COUNT,JTC.JOB_TRN_CONT_PK, PACK.PACK_TYPE_DESC,");
                    strSQL.Append("JC.CARGO_TYPE, jc.JOBCARD_REF_NO,currency_id,DI.DIMENTION_ID ))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append("jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' CONTAINER_TYPE_ID,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("decode(jt.basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') dimension_ID,");
                    strSQL.Append(" DECODE(JTC.PACK_COUNT,  0,");
                    strSQL.Append(" (SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strSQL.Append(" WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strSQL.Append(" JTC.PACK_COUNT) QUANTITY,");
                    strSQL.Append("Decode(JC.Pymt_Type, 1, 'Prepaid', 2, 'Collect') Pymt_Type,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.freight_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("JOB_TRN_FD jt,");
                    strSQL.Append("JOB_TRN_CONT            JTC,");
                    strSQL.Append("PACK_TYPE_MST_TBL       PACK,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK(+)=jc.JOB_CARD_TRN_PK");
                    strSQL.Append("AND JC.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strSQL.Append("AND JTC.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK, JC.Pymt_Type,JTC.PACK_COUNT,JTC.JOB_TRN_CONT_PK, PACK.PACK_TYPE_DESC,");
                    strSQL.Append("JC.CARGO_TYPE, jc.JOBCARD_REF_NO,currency_id,basis))q)");
                }
            }

            if (Convert.ToInt32(Pro) == 1)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append("jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' CONTAINER_TYPE_ID,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("CASE WHEN JC.CARGO_TYPE = 4 THEN  PACK.PACK_TYPE_DESC ");
                    strSQL.Append(" ELSE NVL((SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (SELECT ");
                    strSQL.Append(" DISTINCT JC.PACK_TYPE_FK FROM JOB_TRN_CONT JOB,JOB_TRN_COMMODITY JC WHERE JOB.JOB_TRN_CONT_PK = JC.JOB_TRN_CONT_FK");
                    strSQL.Append(" AND JOB.JOB_TRN_CONT_PK = ' || JTC.JOB_TRN_CONT_PK || ')') FROM DUAL),");
                    strSQL.Append(" PACK.PACK_TYPE_DESC) END Dimention_id,");
                    strSQL.Append(" DECODE(JTC.PACK_COUNT,  0,");
                    strSQL.Append(" (SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strSQL.Append(" WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strSQL.Append(" JTC.PACK_COUNT) QUANTITY,");
                    strSQL.Append("Decode(JC.Pymt_Type, 1, 'Prepaid', 2, 'Collect') Pymt_Type,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.freight_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("JOB_TRN_FD jt,");
                    strSQL.Append("JOB_TRN_CONT            JTC,");
                    strSQL.Append("PACK_TYPE_MST_TBL       PACK,");
                    strSQL.Append("currency_type_mst_tbl cur,");
                    strSQL.Append("Dimention_Unit_Mst_Tbl di");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK(+) = jc.JOB_CARD_TRN_PK");
                    strSQL.Append("AND JC.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strSQL.Append("AND JTC.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    strSQL.Append("and di.dimention_unit_mst_pk(+)=jt.BASIS_FK");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & MJCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK, JC.Pymt_Type,JTC.PACK_COUNT,JTC.JOB_TRN_CONT_PK, PACK.PACK_TYPE_DESC, ");
                    strSQL.Append("JC.CARGO_TYPE, jc.JOBCARD_REF_NO,currency_id))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append("jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' CONTAINER_TYPE_ID,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("decode(jt.basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') dimension_ID,");
                    strSQL.Append(" DECODE(JTC.PACK_COUNT,  0,");
                    strSQL.Append(" (SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strSQL.Append(" WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strSQL.Append(" JTC.PACK_COUNT) QUANTITY,");
                    strSQL.Append("Decode(JC.Pymt_Type, 1, 'Prepaid', 2, 'Collect') Pymt_Type,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.freight_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("JOB_TRN_FD jt,");
                    strSQL.Append("JOB_TRN_CONT            JTC,");
                    strSQL.Append("PACK_TYPE_MST_TBL       PACK,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK(+)=jc.JOB_CARD_TRN_PK");
                    strSQL.Append("AND JC.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strSQL.Append("AND JTC.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK, JC.Pymt_Type,JTC.PACK_COUNT,JTC.JOB_TRN_CONT_PK, PACK.PACK_TYPE_DESC,");
                    strSQL.Append("JC.CARGO_TYPE,jc.JOBCARD_REF_NO,currency_id,basis))q)");
                }
            }

            string strPsql = null;
            strPsql = strSQL.ToString();
            DataSet DS = new DataSet();
            try
            {
                DS.Tables.Add(objWF.GetDataTable(strPsql));

                DS.Tables.Add(FetchChildForFreight(JCPK, MJCPK, MJCPKAS, Biz, Pro));
                if (Convert.ToInt32(Pro) == 2)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        //                 DataRelation CONTRel = new DataRelation("CONTRelation", {
                        //                     DS.Tables[0].Columns["JOB_CARD_TRN_PK"].ToString(),
                        //                     DS.Tables[0].Columns["QUANTITY"]
                        //                 }, {
                        //                     DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                        //DS.Tables[1].Columns["QUANTITY"]
                        //                 }, true);
                        //DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation relDimensionID = null;
                        relDimensionID = new DataRelation("DimensionID", new DataColumn[] {
                            DS.Tables[0].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[0].Columns["dimension_ID"]
                        }, new DataColumn[] {
                            DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[1].Columns["dimension_ID"]
                        });
                        DS.Relations.Add(relDimensionID);
                        relDimensionID.Nested = true;
                    }
                }

                if (Convert.ToInt32(Pro) == 1)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        //                 DataRelation CONTRel = new DataRelation("CONTRelation", {
                        //                     DS.Tables[0].Columns["JOB_CARD_TRN_PK"],
                        //                     DS.Tables[0].Columns["QUANTITY"]
                        //                 }, {
                        //                     DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                        //DS.Tables[1].Columns["QUANTITY"]
                        //                 }, true);
                        //DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation relDimensionID = null;
                        relDimensionID = new DataRelation("DimensionID", new DataColumn[] {
                            DS.Tables[0].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[0].Columns["dimension_ID"]
                        }, new DataColumn[] {
                            DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[1].Columns["dimension_ID"]
                        });
                        DS.Relations.Add(relDimensionID);
                        relDimensionID.Nested = true;
                    }
                }

                return DS;
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

        #endregion "For fetching Freight Deatils"

        #region "For fetching Other Charges"

        /// <summary>
        /// Fetches the other.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <returns></returns>
        public DataSet FetchOther(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "", int CurrPK = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            if ((JCPK == null))
            {
                JCPK = "0";
            }
            if (Convert.ToInt32(Pro) == 2)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.amount * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("JOB_TRN_OTH_CHRG jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.amount * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_TRN_OTH_CHRG jt,");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
            }

            if (Convert.ToInt32(Pro) == 1)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.amount * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("JOB_TRN_OTH_CHRG jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Rate' AR,");
                    strSQL.Append("'ALL in Rate' AR1,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.amount * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_TRN_OTH_CHRG jt,");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
            }

            string strPsql = null;
            strPsql = strSQL.ToString();
            DataSet DS = new DataSet();
            try
            {
                DS.Tables.Add(objWF.GetDataTable(strPsql));

                DS.Tables.Add(FetchChildForOther(JCPK, MJCPK, MJCPKAS, Biz, Pro));
                if (Convert.ToInt32(Pro) == 2)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                }

                if (Convert.ToInt32(Pro) == 1)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                }

                return DS;
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

        #endregion "For fetching Other Charges"

        #region "For fetching Cost Details"

        /// <summary>
        /// Fetches the cost.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <returns></returns>
        public DataSet FetchCost(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "", int CurrPK = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            if ((JCPK == null))
            {
                JCPK = "0";
            }
            if (Convert.ToInt32(Pro) == 2)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Cost' AR,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.estimated_cost * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.total_cost),2),");
                    strSQL.Append(" 0 Difference");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_COST jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Cost' AR,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.estimated_cost * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.total_cost),2),");
                    strSQL.Append(" 0 Difference");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_COST jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
            }

            if (Convert.ToInt32(Pro) == 1)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Cost' AR,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.estimated_cost * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.total_cost),2),");
                    strSQL.Append(" 0 Difference");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_COST jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'ALL in Cost' AR,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.estimated_cost * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.total_cost),2),");
                    strSQL.Append(" 0 Difference");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_COST jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk=" + CurrPK);
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
            }
            string strPsql = null;
            strPsql = strSQL.ToString();
            DataSet DS = new DataSet();

            try
            {
                DS.Tables.Add(objWF.GetDataTable(strPsql));
                DS.Tables.Add(FetchChildForCost(JCPK, MJCPK, MJCPKAS, Biz, Pro));

                if (Convert.ToInt32(Pro) == 2)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                }

                if (Convert.ToInt32(Pro) == 1)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", DS.Tables[0].Columns["JOB_CARD_TRN_PK"], DS.Tables[1].Columns["JOB_CARD_TRN_PK"], true);
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                }
                return DS;
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

        #endregion "For fetching Cost Details"

        #region "For fetching Purchase Invoice"

        /// <summary>
        /// Fetches the purchase.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <returns></returns>
        public DataSet FetchPurchase(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "", int CurrPK = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            if ((JCPK == null))
            {
                JCPK = "0";
            }
            if (Convert.ToInt32(Pro )== 2)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' Party,");
                    strSQL.Append("'' Inr,");
                    strSQL.Append("'' InD,");
                    strSQL.Append("'' CE,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("'' TaxP,");
                    strSQL.Append("'' TaxA,");
                    strSQL.Append("ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)-ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_PIA jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk = jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' Party,");
                    strSQL.Append("'' Inr,");
                    strSQL.Append("'' InD,");
                    strSQL.Append("'' CE,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("'' TaxP,");
                    strSQL.Append("'' TaxA,");
                    strSQL.Append("ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)-ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_PIA jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk = jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strSQL.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
            }

            if (Convert.ToInt32(Pro) == 1)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' Party,");
                    strSQL.Append("'' Inr,");
                    strSQL.Append("'' InD,");
                    strSQL.Append("'' CE,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("'' TaxP,");
                    strSQL.Append("'' TaxA,");
                    strSQL.Append("ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)-ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_PIA jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk = jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
                else
                {
                    strSQL.Append("SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(");
                    strSQL.Append("SELECT * FROM (");
                    strSQL.Append("SELECT jc.JOB_CARD_TRN_PK,");
                    strSQL.Append(" jc.JOBCARD_REF_NO,");
                    strSQL.Append("'' Party,");
                    strSQL.Append("'' Inr,");
                    strSQL.Append("'' InD,");
                    strSQL.Append("'' CE,");
                    strSQL.Append("cur.currency_id,");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("'' TaxP,");
                    strSQL.Append("'' TaxA,");
                    strSQL.Append("ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2),");
                    strSQL.Append("ROUND(SUM(jt.Invoice_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)-ROUND(SUM(jt.Estimated_amt * GET_EX_RATE(jt.Currency_Mst_Fk," + CurrPK + ",SYSDATE)),2)");
                    strSQL.Append("FROM");
                    strSQL.Append("JOB_CARD_TRN jc,");
                    strSQL.Append(" JOB_TRN_PIA jt,");
                    strSQL.Append("currency_type_mst_tbl cur");
                    strSQL.Append("WHERE");
                    strSQL.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strSQL.Append(" and cur.currency_mst_pk = jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("group by JOB_CARD_TRN_PK,");
                    strSQL.Append("JOBCARD_REF_NO,currency_id))q)");
                }
            }

            string strPsql = null;
            strPsql = strSQL.ToString();
            DataSet DS = new DataSet();
            try
            {
                DS.Tables.Add(objWF.GetDataTable(strPsql));

                DS.Tables.Add(FetchChildForPurchase(JCPK, MJCPK, MJCPKAS, Biz, Pro));
                if (Convert.ToInt32(Pro) == 2)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", new DataColumn[] {
                            DS.Tables[0].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[0].Columns["currency_id"]
                        }, new DataColumn[] {
                            DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[1].Columns["currency_id"]
                        });
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", new DataColumn[] {
                            DS.Tables[0].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[0].Columns["currency_id"]
                        }, new DataColumn[] {
                            DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[1].Columns["currency_id"]
                        });
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                }

                if (Convert.ToInt32(Pro) == 1)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", new DataColumn[] {
                            DS.Tables[0].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[0].Columns["currency_id"]
                        }, new DataColumn[] {
                            DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[1].Columns["currency_id"]
                        });
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", new DataColumn[] {
                            DS.Tables[0].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[0].Columns["currency_id"]
                        }, new DataColumn[] {
                            DS.Tables[1].Columns["JOB_CARD_TRN_PK"],
                            DS.Tables[1].Columns["currency_id"]
                        });
                        DS.Relations.Add(CONTRel);
                        DS.Relations[0].Nested = true;
                    }
                }

                return DS;
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

        #endregion "For fetching Purchase Invoice"

        #region "Fetching Revenue"

        /// <summary>
        /// Fetches the revenue.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <param name="CurrPK">The curr pk.</param>
        /// <returns></returns>
        public DataSet FetchRevenue(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "", int CurrPK = 0)
        {
            StringBuilder strSQL = new StringBuilder();
            WorkFlow objWF = new WorkFlow();
            int CurrencyPk = 0;
            CurrencyPk = CurrPK;
            //If Pro = 2 Then 'Process export
            //If Biz = 2 Then
            //    strSQL.Append(vbCrLf & "SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(")
            //    strSQL.Append(vbCrLf & "SELECT * FROM (")
            //    strSQL.Append(vbCrLf & "SELECT jc.JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & " jc.JOBCARD_REF_NO,")
            //    strSQL.Append(vbCrLf & "'' Ty,")
            //    strSQL.Append(vbCrLf & "'' Debcr,")
            //    strSQL.Append(vbCrLf & "'' DCDate,")
            //    'If DStemp.Tables.Count > 0 Then
            //    '    strSQL.Append(vbCrLf & Sum & " Amount,")
            //    'End If
            //    'strSQL.Append(vbCrLf & "CONSOL_revenue_amt(JOB_CARD_TRN_PK," & CurrencyPk & ") Amount,")
            //    strSQL.Append(vbCrLf & "'' Amt,")
            //    strSQL.Append(vbCrLf & "'' curid,")
            //    strSQL.Append(vbCrLf & "'' EstProf,")
            //    strSQL.Append(vbCrLf & "'' ActProf")
            //    strSQL.Append(vbCrLf & "FROM")
            //    strSQL.Append(vbCrLf & "JOB_CARD_TRN jc")
            //    'strSQL.Append(vbCrLf & "corporate_mst_tbl co,")
            //    'strSQL.Append(vbCrLf & "currency_type_mst_tbl cur")
            //    strSQL.Append(vbCrLf & "WHERE")
            //    'strSQL.Append(vbCrLf & "co.currency_mst_fk=cur.currency_mst_pk")
            //    If MJCPK <> "" Then
            //        'If Master Job Card selected
            //        strSQL.Append(vbCrLf & "  jc.JOB_CARD_TRN_PK in (" & MJCPK & ")")
            //    ElseIf JCPK <> "" Then
            //        'If selcect check boxes in Grid 'Hbl no
            //        strSQL.Append(vbCrLf & " jc.HBL_HAWB_FK in (" & JCPK & ")")
            //    Else
            //        strSQL.Append(vbCrLf & "  jc.MASTER_JC_FK in (" & MJCPKAS & ")")
            //    End If
            //    strSQL.Append(vbCrLf & "group by JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & "JOBCARD_REF_NO))q)")
            //Else
            //    strSQL.Append(vbCrLf & "SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(")
            //    strSQL.Append(vbCrLf & "SELECT * FROM (")
            //    strSQL.Append(vbCrLf & "SELECT jc.JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & " jc.JOBCARD_REF_NO,")
            //    strSQL.Append(vbCrLf & "'' Ty,")
            //    strSQL.Append(vbCrLf & "'' Debcr,")
            //    strSQL.Append(vbCrLf & "'' DCDate,")
            //    'strSQL.Append(vbCrLf & "'' Amount,")
            //    'If DStemp.Tables.Count > 0 Then
            //    '    strSQL.Append(vbCrLf & Sum & " Amount,")
            //    'End If
            //    'strSQL.Append(vbCrLf & "CONSOL_REVENUE_AMT_AIR(JOB_CARD_TRN_PK," & CurrencyPk & ") Amount,")
            //    'strSQL.Append(vbCrLf & "cur.currency_id")
            //    strSQL.Append(vbCrLf & "'' Amt,")
            //    strSQL.Append(vbCrLf & "'' curid,")
            //    strSQL.Append(vbCrLf & "'' EstProf,")
            //    strSQL.Append(vbCrLf & "'' ActProf")
            //    strSQL.Append(vbCrLf & "FROM")
            //    strSQL.Append(vbCrLf & "JOB_CARD_TRN jc")
            //    'strSQL.Append(vbCrLf & "corporate_mst_tbl co,")
            //    'strSQL.Append(vbCrLf & "currency_type_mst_tbl cur")
            //    strSQL.Append(vbCrLf & "WHERE")
            //    'strSQL.Append(vbCrLf & " co.currency_mst_fk=cur.currency_mst_pk")
            //    If MJCPK <> "" Then
            //        'If Master Job Card selected
            //        strSQL.Append(vbCrLf & " jc.JOB_CARD_TRN_PK in (" & MJCPK & ")")
            //    ElseIf JCPK <> "" Then
            //        'If selcect check boxes in Grid 'Hbl no
            //        strSQL.Append(vbCrLf & "  jc.HBL_HAWB_FK in (" & JCPK & ")")
            //    Else
            //        strSQL.Append(vbCrLf & " jc.MASTER_JC_FK in (" & MJCPKAS & ")")
            //    End If
            //    strSQL.Append(vbCrLf & "group by JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & "JOBCARD_REF_NO))q)")
            //End If
            //End If

            //If Pro = 1 Then
            //Process import
            //If Biz = 2 Then ' Buisness type sea
            //    strSQL.Append(vbCrLf & "SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(")
            //    strSQL.Append(vbCrLf & "SELECT * FROM (")
            //    strSQL.Append(vbCrLf & "SELECT jc.JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & " jc.JOBCARD_REF_NO,")
            //    strSQL.Append(vbCrLf & "'' Ty,")
            //    strSQL.Append(vbCrLf & "'' Debcr,")
            //    strSQL.Append(vbCrLf & "'' DCDate,")
            //    strSQL.Append(vbCrLf & "'' Amt,")
            //    strSQL.Append(vbCrLf & "''curid,")
            //    strSQL.Append(vbCrLf & "'' EstProf,")
            //    strSQL.Append(vbCrLf & "'' ActProf")
            //    strSQL.Append(vbCrLf & "FROM")
            //    strSQL.Append(vbCrLf & "JOB_CARD_TRN jc")
            //    strSQL.Append(vbCrLf & "WHERE")
            //    If MJCPK <> "" Then
            //        'If Master Job Card selected
            //        strSQL.Append(vbCrLf & "  jc.JOB_CARD_TRN_PK in (" & MJCPK & ")")
            //    ElseIf JCPK <> "" Then
            //        'If selcect check boxes in Grid 'Hbl no
            //        'strSQL.Append(vbCrLf & " jc.HBL_HAWB_FK in (" & JCPK & ")")
            //    Else
            //        strSQL.Append(vbCrLf & "  jc.MASTER_JC_FK in (" & MJCPKAS & ")")
            //    End If
            //    strSQL.Append(vbCrLf & "group by JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & "JOBCARD_REF_NO))q)")
            //Else
            //    strSQL.Append(vbCrLf & "SELECT * FROM(SELECT ROWNUM SLNO ,Q.*FROM(")
            //    strSQL.Append(vbCrLf & "SELECT * FROM (")
            //    strSQL.Append(vbCrLf & "SELECT jc.JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & " jc.JOBCARD_REF_NO,")
            //    strSQL.Append(vbCrLf & "'' Ty,")
            //    strSQL.Append(vbCrLf & "'' Debcr,")
            //    strSQL.Append(vbCrLf & "'' DCDate,")
            //    strSQL.Append(vbCrLf & "'' Amt,")
            //    strSQL.Append(vbCrLf & "'' curid,")
            //    strSQL.Append(vbCrLf & "'' EstProf,")
            //    strSQL.Append(vbCrLf & "'' ActProf")
            //    strSQL.Append(vbCrLf & "FROM")
            //    strSQL.Append(vbCrLf & "JOB_CARD_TRN jc")
            //    strSQL.Append(vbCrLf & "WHERE")
            //    If MJCPK <> "" Then
            //        'If Master Job Card selected
            //        strSQL.Append(vbCrLf & " jc.JOB_CARD_TRN_PK in (" & MJCPK & ")")
            //    ElseIf JCPK <> "" Then
            //        'If selcect check boxes in Grid 'Hbl no
            //        'strSQL.Append(vbCrLf & "  jc.HBL_HAWB_FK in (" & JCPK & ")")
            //    Else
            //        strSQL.Append(vbCrLf & " jc.MASTER_JC_FK in (" & MJCPKAS & ")")
            //    End If
            //    strSQL.Append(vbCrLf & "group by JOB_CARD_TRN_PK,")
            //    strSQL.Append(vbCrLf & "JOBCARD_REF_NO))q)")
            //End If
            // End If

            //
            //
            //Dim strPsql As String
            //strPsql = strSQL.ToString
            try
            {
                DataSet dsMain = new DataSet();
                DataTable dt = new DataTable();
                DataRow drRow = null;
                DataRow drRev = null;
                DataTable dtRev = new DataTable();
                int k = 0;
                dsMain.Tables.Add("tblTransaction");
                dsMain.Tables["tblTransaction"].Columns.Add("SLNO");
                dsMain.Tables["tblTransaction"].Columns.Add("PK");
                dsMain.Tables["tblTransaction"].Columns.Add("jobcard_ref_no");
                dsMain.Tables["tblTransaction"].Columns.Add("Ty");
                dsMain.Tables["tblTransaction"].Columns.Add("Debcr");
                dsMain.Tables["tblTransaction"].Columns.Add("DCDate");
                dsMain.Tables["tblTransaction"].Columns.Add("curid");
                dsMain.Tables["tblTransaction"].Columns.Add("Amt");
                //dsMain.Tables("tblTransaction").Columns.Add("curid")
                dsMain.Tables["tblTransaction"].Columns.Add("ActProf");
                dsMain.Tables["tblTransaction"].Columns.Add("EstProf");
                dsMain.Tables["tblTransaction"].Columns.Add("ActRev");
                dsMain.Tables["tblTransaction"].Columns.Add("ActCost");
                dsMain.Tables["tblTransaction"].Columns.Add("EstRev");
                dsMain.Tables["tblTransaction"].Columns.Add("EstCost");

                dsMain.Tables.Add("tblRev");
                dsMain.Tables["tblRev"].Columns.Add("JOB_PK1");
                dsMain.Tables["tblRev"].Columns.Add("SLNO");
                dsMain.Tables["tblRev"].Columns.Add("JOB_REF_NO");
                dsMain.Tables["tblRev"].Columns.Add("Type");
                dsMain.Tables["tblRev"].Columns.Add("DOC_REF_NO");
                dsMain.Tables["tblRev"].Columns.Add("DOC_DATE");
                dsMain.Tables["tblRev"].Columns.Add("CUR_ID");
                dsMain.Tables["tblRev"].Columns.Add("DOC_AMT");
                //dsMain.Tables("tblRev").Columns.Add("CUR_ID")

                dtRev = FetchChildForRevenue(JCPK, MJCPK, MJCPKAS, Biz, Pro);
                if (dtRev.Rows.Count > 0)
                {
                    for (k = 0; k <= dtRev.Rows.Count - 1; k++)
                    {
                        drRev = dsMain.Tables["tblRev"].NewRow();
                        drRev["JOB_PK1"] = dtRev.Rows[k]["JOB_PK1"];
                        drRev["SLNO"] = dtRev.Rows[k]["SLNO"];
                        drRev["JOB_REF_NO"] = dtRev.Rows[k]["JOB_REF_NO"];
                        drRev["Type"] = dtRev.Rows[k]["Type"];
                        drRev["DOC_REF_NO"] = dtRev.Rows[k]["DOC_REF_NO"];
                        drRev["DOC_DATE"] = dtRev.Rows[k]["DOC_DATE"];
                        drRev["CUR_ID"] = dtRev.Rows[k]["CUR_ID"];
                        drRev["DOC_AMT"] = dtRev.Rows[k]["DOC_AMT"];
                        //drRev.Item("CUR_ID") = dtRev.Rows(k).Item("CUR_ID")
                        dsMain.Tables["tblRev"].Rows.Add(drRev);
                    }
                }

                dsMain.Tables["tblTransaction"].Columns["PK"].DataType = dsMain.Tables[1].Columns["JOB_PK1"].DataType;
                //Snigdharani >> PTS task to get cost, revenue, profitability for individual job card
                Array strArray = null;
                if ((MJCPK != null))
                {
                    if (!string.IsNullOrEmpty(MJCPK))
                    {
                        strArray = MJCPK.Split(',');
                    }
                }

                int i = 0;
                int j = 0;
                int m = 0;
                if ((MJCPK != null))
                {
                    for (i = 0; i <= strArray.Length - 1; i++)
                    {
                        for (j = 0; j <= strArray.Length - 1; j++)
                        {
                            if (strArray.GetValue(i) == strArray.GetValue(j))
                            {
                                m = m + 1;
                                if (m > 1)
                                {
                                    strArray.SetValue("0", i);
                                }
                            }
                        }
                        m = 0;
                        objWF.MyCommand.Parameters.Clear();
                        if (Convert.ToInt32(Pro) == 2)
                        {
                            if (Convert.ToInt32(Biz) == 2)
                            {
                                var _with1 = objWF.MyCommand.Parameters;
                                _with1.Add("JCPK", strArray.GetValue(i)).Direction = ParameterDirection.Input;
                                //Snigdharani - 05/12/2008 - Added CurrPk for Location based exchange rate task
                                _with1.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                _with1.Add("JOB", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                                dt = objWF.GetDataTable("FETCH_COST_REVENUE_PROFIT", "FETCH_JOB");
                            }
                            else
                            {
                                var _with2 = objWF.MyCommand.Parameters;
                                _with2.Add("JCPK", strArray.GetValue(i)).Direction = ParameterDirection.Input;
                                //Snigdharani - 05/12/2008 - Added CurrPk for Location based exchange rate task
                                _with2.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                _with2.Add("JOB", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                                dt = objWF.GetDataTable("FETCH_COST_REVENUE_PROFIT", "FETCH_JOB");
                            }
                        }
                        if (Convert.ToInt32(Pro) == 1)
                        {
                            if (Convert.ToInt32(Biz) == 2)
                            {
                                var _with3 = objWF.MyCommand.Parameters;
                                _with3.Add("JCPK", strArray.GetValue(i)).Direction = ParameterDirection.Input;
                                //Snigdharani - 05/12/2008 - Added CurrPk for Location based exchange rate task
                                _with3.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                _with3.Add("JOB", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                                dt = objWF.GetDataTable("FETCH_COST_REVENUE_PROFIT", "FETCH_JOB");
                            }
                            else
                            {
                                var _with4 = objWF.MyCommand.Parameters;
                                _with4.Add("JCPK", strArray.GetValue(i)).Direction = ParameterDirection.Input;
                                //Snigdharani - 05/12/2008 - Added CurrPk for Location based exchange rate task
                                _with4.Add("CurrPk", HttpContext.Current.Session["CURRENCY_MST_PK"]).Direction = ParameterDirection.Input;
                                _with4.Add("JOB", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                                dt = objWF.GetDataTable("FETCH_COST_REVENUE_PROFIT", "FETCH_JOB");
                            }
                        }
                        if (dt.Rows.Count > 0)
                        {
                            drRow = dsMain.Tables["tblTransaction"].NewRow();
                            drRow["SLNO"] = dt.Rows[0]["SLNO"];
                            drRow["PK"] = dt.Rows[0]["PK"];
                            drRow["jobcard_ref_no"] = dt.Rows[0]["jobcard_ref_no"];
                            drRow["Ty"] = dt.Rows[0]["Ty"];
                            drRow["Debcr"] = dt.Rows[0]["Debcr"];
                            drRow["DCDate"] = dt.Rows[0]["DCDate"];
                            drRow["curid"] = dt.Rows[0]["curid"];
                            drRow["Amt"] = dt.Rows[0]["Amt"];
                            //drRow.Item("curid") = dt.Rows(0).Item("curid")
                            drRow["ActProf"] = dt.Rows[0]["ActProf"];
                            drRow["EstProf"] = dt.Rows[0]["EstProf"];
                            drRow["ActRev"] = dt.Rows[0]["ActRev"];
                            drRow["ActCost"] = dt.Rows[0]["ActCost"];
                            drRow["EstRev"] = dt.Rows[0]["EstRev"];
                            drRow["EstCost"] = dt.Rows[0]["EstCost"];
                            dsMain.Tables["tblTransaction"].Rows.Add(drRow);
                        }
                    }
                }
                //Snigdharani

                //dsMain.Tables.Add(FetchChildForRevenue(JCPK, MJCPK, MJCPKAS, Biz, Pro))

                if (Convert.ToInt32(Pro) == 2)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", dsMain.Tables["tblTransaction"].Columns["PK"], dsMain.Tables[1].Columns["JOB_PK1"], true);
                        dsMain.Relations.Add(CONTRel);
                        dsMain.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", dsMain.Tables["tblTransaction"].Columns["PK"], dsMain.Tables[1].Columns["JOB_PK1"], true);
                        dsMain.Relations.Add(CONTRel);
                        dsMain.Relations[0].Nested = true;
                    }
                }

                //Process import
                if (Convert.ToInt32(Pro) == 1)
                {
                    // Buisness type sea
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", dsMain.Tables["tblTransaction"].Columns["PK"], dsMain.Tables[1].Columns["JOB_PK1"], true);
                        dsMain.Relations.Add(CONTRel);
                        dsMain.Relations[0].Nested = true;
                    }
                    else
                    {
                        DataRelation CONTRel = new DataRelation("CONTRelation", dsMain.Tables["tblTransaction"].Columns["PK"], dsMain.Tables[1].Columns["JOB_PK1"], true);
                        dsMain.Relations.Add(CONTRel);
                        dsMain.Relations[0].Nested = true;
                    }
                }
                return dsMain;
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

        #endregion "Fetching Revenue"

        #region "For fetching Revenue Child"

        /// <summary>
        /// Fetches the child for revenue.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <returns></returns>
        public DataTable FetchChildForRevenue(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "")
        {
            StringBuilder strSQL = new StringBuilder();
            string StrSQLN = null;
            WorkFlow objWF = new WorkFlow();
            if ((JCPK == null))
            {
                JCPK = "0";
            }
            //Process export
            if (Convert.ToInt32(Pro) == 2)
            {
                //Buisness Type sea
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT");
                    strSQL.Append("JOB_PK1,");
                    strSQL.Append("ROWNUM SLNO,");
                    strSQL.Append("JOB_REF_NO,");
                    strSQL.Append("Type,");
                    strSQL.Append("DOC_REF_NO,");
                    strSQL.Append("DOC_DATE,");
                    strSQL.Append("CUR_ID,");
                    strSQL.Append("DOC_AMT");
                    //strSQL.Append(vbCrLf & "CUR_ID")
                    strSQL.Append("FROM");
                    strSQL.Append("(select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    // strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ic.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ic.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ic.net_payable DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_TRN jc,");
                    strSQL.Append("Inv_Cust_Sea_Exp_Tbl ic,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk=ic.Job_Card_Sea_Exp_Fk and");
                    strSQL.Append("cu.currency_mst_pk=ic.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ia.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ia.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ia.net_inv_amt DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("Inv_Agent_Tbl ia,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk=ia.job_card_fk and");
                    strSQL.Append("cu.currency_mst_pk=ia.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    // strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ci.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ci.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ci.net_receivable DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("consol_invoice_tbl ci, consol_invoice_trn_tbl ctrn,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("ci.consol_invoice_pk = ctrn.consol_invoice_fk");
                    strSQL.Append("and ctrn.job_card_fk = jc.job_card_trn_pk");
                    strSQL.Append("and ci.currency_mst_fk = cu.currency_mst_pk");
                    strSQL.Append("and ci.business_type = 2");
                    strSQL.Append("and ci.process_type=1");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("cc.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(cc.credit_note_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(cc.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("inv_cust_sea_exp_tbl ic,");
                    strSQL.Append("cr_cust_sea_exp_tbl cc,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk = ic.Job_Card_Sea_Exp_Fk");
                    strSQL.Append("and ic.inv_cust_sea_exp_pk = cc.inv_cust_sea_exp_fk");
                    strSQL.Append("and cu.currency_mst_pk = cc.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    // strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("ca.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ca.credit_note_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(ca.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("inv_agent_tbl ias,");
                    strSQL.Append("cr_agent_tbl ca,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk = ias.job_card_fk ");
                    strSQL.Append("and ias.inv_agent_pk = ca.inv_agent_fk");
                    strSQL.Append("and cu.currency_mst_pk = ca.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append(")");
                }
                else
                {
                    //Buisness type Air
                    strSQL.Append("SELECT");
                    strSQL.Append("JOB_PK1,");
                    strSQL.Append("ROWNUM SLNO,");
                    strSQL.Append("JOB_REF_NO,");
                    strSQL.Append("Type,");
                    strSQL.Append("DOC_REF_NO,");
                    strSQL.Append("DOC_DATE,");
                    strSQL.Append("CUR_ID,");
                    strSQL.Append("DOC_AMT");
                    //strSQL.Append(vbCrLf & "CUR_ID")
                    strSQL.Append("FROM");
                    strSQL.Append("(select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ic.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ic.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ic.net_payable DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("Inv_Cust_air_Exp_Tbl ic,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk=ic.Job_Card_Air_Exp_Fk and");
                    strSQL.Append("cu.currency_mst_pk=ic.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ia.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ia.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ia.net_inv_amt DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("Inv_Agent_Tbl ia,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk=ia.job_card_fk and");
                    strSQL.Append("cu.currency_mst_pk=ia.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ci.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ci.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ci.net_receivable DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("consol_invoice_tbl ci, consol_invoice_trn_tbl ctrn,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("ci.consol_invoice_pk = ctrn.consol_invoice_fk");
                    strSQL.Append("and ctrn.job_card_fk = jc.job_card_trn_pk");
                    strSQL.Append("and ci.currency_mst_fk = cu.currency_mst_pk");
                    strSQL.Append("and ci.business_type = 1");
                    strSQL.Append(" and ci.process_type= 2");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("cc.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(cc.credit_note_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(cc.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("inv_cust_air_exp_tbl ic,");
                    strSQL.Append("cr_cust_air_exp_tbl cc,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk = ic.Job_Card_Air_Exp_Fk");
                    strSQL.Append("and ic.inv_cust_air_exp_pk = cc.inv_cust_air_exp_fk");
                    strSQL.Append("and cu.currency_mst_pk = cc.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.job_card_trn_pk JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("ca.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ca.credit_note_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(ca.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from Job_Card_trn jc,");
                    strSQL.Append("inv_agent_tbl ias,");
                    strSQL.Append("cr_agent_tbl ca,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.job_card_trn_pk = ias.job_card_fk ");
                    strSQL.Append("and ias.inv_agent_pk = ca.inv_agent_fk");
                    strSQL.Append("and cu.currency_mst_pk = ca.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.job_card_trn_pk in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strSQL.Append("AND  jc.hbl_hawb_fk in (" + JCPK + ")");
                    }
                    else
                    {
                        strSQL.Append("AND  jc.master_jc_fk in (" + MJCPKAS + ")");
                    }
                    strSQL.Append(")");
                }
            }

            //Process import
            if (Convert.ToInt32(Pro) == 1)
            {
                // Buisness type sea
                if (Convert.ToInt32(Biz) == 2)
                {
                    strSQL.Append("SELECT  ROWNUM SLNO,q.* FROM");
                    strSQL.Append("(SELECT");
                    strSQL.Append("max(JOB_PK1) JOB_PK1,");
                    //strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("max(JOB_REF_NO) JOB_REF_NO,");
                    strSQL.Append("max(Type) Type,");
                    //strSQL.Append(vbCrLf & "max(DOC_REF_NO),")
                    strSQL.Append(" DOC_REF_NO,");
                    strSQL.Append("max(DOC_DATE) DOC_DATE,");
                    strSQL.Append("max(CUR_ID) CUR_ID,");
                    strSQL.Append("sum(DOC_AMT) DOC_AMT");
                    //strSQL.Append(vbCrLf & "max(CUR_ID) CUR_ID")
                    strSQL.Append("FROM");
                    strSQL.Append("(select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    // strSQL.Append(vbCrLf & "ROWNUM SLNO,")
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ic.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ic.invoice_date,dateformat) DOC_DATE,");
                    //strSQL.Append(vbCrLf & "ic.net_payable DOC_AMT,")
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("nvl(ic.invoice_amt, 0) - nvl(ic.discount_amt, 0) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("Inv_Cust_Sea_imp_Tbl ic,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK=ic.Job_Card_Sea_Imp_Fk and");
                    strSQL.Append("cu.currency_mst_pk=ic.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ia.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("ia.invoice_date DOC_DATE,");
                    //strSQL.Append(vbCrLf & "ia.net_inv_amt DOC_AMT,")
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("nvl(ia.net_inv_amt, 0) - nvl(ia.discount_amt, 0) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("inv_agent_tbl ia,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK=ia.job_card_fk and");
                    strSQL.Append("cu.currency_mst_pk=ia.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ci.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ci.invoice_date,dateformat) DOC_DATE,");
                    //strSQL.Append(vbCrLf & "ci.net_receivable DOC_AMT,")
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("nvl(ctrn.Amt_In_Inv_Curr, 0) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("consol_invoice_tbl ci, consol_invoice_trn_tbl ctrn,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("ci.consol_invoice_pk = ctrn.consol_invoice_fk");
                    strSQL.Append("and ctrn.job_card_fk = jc.JOB_CARD_TRN_PK");
                    strSQL.Append("and ci.currency_mst_fk = cu.currency_mst_pk");
                    strSQL.Append("and ci.business_type = 2");
                    strSQL.Append(" and ci.process_type= 2");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("cc.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(cc.credit_note_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(cc.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("inv_cust_sea_imp_tbl ic,");
                    strSQL.Append("cr_cust_sea_imp_tbl cc,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK = ic.Job_Card_Sea_Imp_Fk");
                    strSQL.Append("and ic.inv_cust_sea_imp_pk = cc.inv_cust_sea_imp_fk");
                    strSQL.Append("and cu.currency_mst_pk = cc.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("ca.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("ca.credit_note_date DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(ca.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("inv_agent_tbl ias,");
                    strSQL.Append("cr_agent_tbl ca,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK = ias.job_card_fk ");
                    strSQL.Append("and ias.inv_agent_pk = ca.inv_agent_fk");
                    strSQL.Append("and cu.currency_mst_pk = ca.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    //strSQL.Append(vbCrLf & ") group by JOB_PK1,JOB_REF_NO,Type,DOC_REF_NO,DOC_DATE,DOC_AMT,CUR_ID)q")
                    strSQL.Append(") group by JOB_PK1,JOB_REF_NO,Type,DOC_REF_NO,DOC_DATE,CUR_ID)q");
                }
                else
                {
                    //Buisness type Air
                    strSQL.Append("SELECT");
                    strSQL.Append("JOB_PK1,");
                    strSQL.Append("ROWNUM SLNO,");
                    strSQL.Append("JOB_REF_NO,");
                    strSQL.Append("Type,");
                    strSQL.Append("DOC_REF_NO,");
                    strSQL.Append("DOC_DATE,");
                    strSQL.Append("CUR_ID,");
                    strSQL.Append("DOC_AMT");
                    //strSQL.Append(vbCrLf & "CUR_ID")
                    strSQL.Append("FROM");
                    strSQL.Append("(select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ic.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ic.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ic.net_payable DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("Inv_Cust_air_imp_Tbl ic,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK=ic.Job_Card_Air_Imp_Fk and");
                    strSQL.Append("cu.currency_mst_pk=ic.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ia.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ia.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ia.net_inv_amt DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("inv_agent_tbl ia,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK=ia.job_card_fk and");
                    strSQL.Append("cu.currency_mst_pk=ia.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'INV' as Type,");
                    strSQL.Append("ci.invoice_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ci.invoice_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("ci.net_receivable DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("consol_invoice_tbl ci, consol_invoice_trn_tbl ctrn,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("ci.consol_invoice_pk = ctrn.consol_invoice_fk");
                    strSQL.Append("and ctrn.job_card_fk = jc.JOB_CARD_TRN_PK");
                    strSQL.Append("and ci.currency_mst_fk = cu.currency_mst_pk");
                    strSQL.Append("and ci.business_type =1");
                    strSQL.Append(" and ci.process_type= 2");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("cc.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(cc.credit_note_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(cc.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("inv_cust_air_imp_tbl ic,");
                    strSQL.Append("cr_cust_air_imp_tbl cc,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK = ic.Job_Card_Air_Imp_Fk");
                    strSQL.Append("and ic.inv_cust_air_imp_pk = cc.inv_cust_air_imp_fk");
                    strSQL.Append("and cu.currency_mst_pk = cc.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append("UNION");
                    strSQL.Append("select");
                    strSQL.Append("jc.JOB_CARD_TRN_PK JOB_PK1,");
                    strSQL.Append("jc.jobcard_ref_no JOB_REF_NO,");
                    strSQL.Append("'CRN' as Type,");
                    strSQL.Append("ca.credit_note_ref_no DOC_REF_NO,");
                    strSQL.Append("TO_DATE(ca.credit_note_date,dateformat) DOC_DATE,");
                    strSQL.Append("cu.currency_id CUR_ID,");
                    strSQL.Append("-(ca.credit_note_amt) DOC_AMT");
                    //strSQL.Append(vbCrLf & "cu.currency_id CUR_ID")
                    strSQL.Append("from JOB_CARD_TRN jc,");
                    strSQL.Append("inv_agent_tbl ias,");
                    strSQL.Append("cr_agent_tbl ca,");
                    strSQL.Append("currency_type_mst_tbl cu");
                    strSQL.Append("where");
                    strSQL.Append("jc.JOB_CARD_TRN_PK = ias.job_card_fk ");
                    strSQL.Append("and ias.inv_agent_pk = ca.inv_agent_fk");
                    strSQL.Append("and cu.currency_mst_pk = ca.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        //If Master Job Card selected
                        strSQL.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        //strSQL.Append(vbCrLf & "AND  jc.HBL_HAWB_FK in (" & JCPK & ")")
                    }
                    else
                    {
                        strSQL.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strSQL.Append(")");
                }
            }

            StrSQLN = strSQL.ToString();
            DataTable dt = null;

            try
            {
                dt = objWF.GetDataTable(StrSQLN);
                //Dim dr As DataRow
                //For Each dr In dt.Rows
                //    Dim s As String
                //    s = dr(6).ToString
                //    Sum = Sum + Double.Parse(dr(6))
                //Next

                return dt;
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

        #endregion "For fetching Revenue Child"

        #region "Fetch For  Childs"

        /// <summary>
        /// Fetches the child for freight.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <returns></returns>
        private DataTable FetchChildForFreight(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "")
        {
            StringBuilder strCondition = new StringBuilder();
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            if (Convert.ToInt32(Pro) == 2)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strCondition.Append("SELECT ROWNUM SLNO,");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("jc.JOBCARD_REF_NO,");
                    strCondition.Append("ctmt.container_type_mst_id,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("CASE WHEN JC.CARGO_TYPE = 4 THEN  PACK.PACK_TYPE_DESC ");
                    strCondition.Append("ELSE NVL((SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (SELECT ");
                    strCondition.Append("DISTINCT JC.PACK_TYPE_FK FROM JOB_TRN_CONT JOB,JOB_TRN_COMMODITY JC WHERE JOB.JOB_TRN_CONT_PK = JC.JOB_TRN_CONT_FK");
                    strCondition.Append("AND JOB.JOB_TRN_CONT_PK = ' || JTC.JOB_TRN_CONT_PK || ')') FROM DUAL),");
                    strCondition.Append("PACK.PACK_TYPE_DESC) END Dimention_id,");
                    strCondition.Append("DECODE(JTC.PACK_COUNT,  0,");
                    strCondition.Append("(SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strCondition.Append("WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strCondition.Append("JTC.PACK_COUNT) QUANTITY,");
                    strCondition.Append("Decode(jt.FREIGHT_TYPE,1,'Prepaid',2,'Collect'),");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.FREIGHT_AMT ");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_FD jt,");
                    strCondition.Append("JOB_TRN_CONT JTC,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("DIMENTION_UNIT_MST_TBL di,");
                    strCondition.Append("PACK_TYPE_MST_TBL       PACK,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu,");
                    strCondition.Append("container_type_mst_tbl ctmt");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK(+)=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("and jc.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strCondition.Append("AND fr.freight_element_mst_pk(+)=jt.freight_element_mst_fk");
                    strCondition.Append("AND di.dimention_unit_mst_pk(+)=jt.basis");
                    strCondition.Append("AND JTC.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
                    strCondition.Append("AND cu.currency_mst_pk(+)=jt.currency_mst_fk");
                    strCondition.Append("AND jt.container_type_mst_fk=ctmt.container_type_mst_pk(+)");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strCondition.Append(" ORDER BY ctmt.preferences,FR.PREFERENCE");
                }
                else
                {
                    strCondition.Append("SELECT ROWNUM SLNO,");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("jc.JOBCARD_REF_NO,'' as container_type_mst_id,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("decode(jt.basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') dimension_ID,");
                    strCondition.Append("DECODE(JTC.PACK_COUNT,  0,");
                    strCondition.Append("(SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strCondition.Append("WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strCondition.Append("JTC.PACK_COUNT) QUANTITY,");
                    strCondition.Append("Decode(jt.FREIGHT_TYPE,1,'Prepaid',2,'Collect'),");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.FREIGHT_AMT ");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_FD jt,");
                    strCondition.Append("JOB_TRN_CONT JTC,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK(+)=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("and jc.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strCondition.Append("AND fr.freight_element_mst_pk(+)=jt.freight_element_mst_fk");
                    strCondition.Append("AND cu.currency_mst_pk(+)=jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                        strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strCondition.Append(" ORDER BY FR.PREFERENCE");
                }
            }

            if (Convert.ToInt32(Pro) == 1)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strCondition.Append("SELECT ROWNUM SLNO,");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("jc.JOBCARD_REF_NO,");
                    strCondition.Append("ctmt.container_type_mst_id,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("CASE WHEN JC.CARGO_TYPE = 4 THEN  PACK.PACK_TYPE_DESC ");
                    strCondition.Append("ELSE NVL((SELECT ROWTOCOL('SELECT PT.PACK_TYPE_DESC FROM PACK_TYPE_MST_TBL PT WHERE PT.PACK_TYPE_MST_PK IN (SELECT ");
                    strCondition.Append("DISTINCT JC.PACK_TYPE_FK FROM JOB_TRN_CONT JOB,JOB_TRN_COMMODITY JC WHERE JOB.JOB_TRN_CONT_PK = JC.JOB_TRN_CONT_FK");
                    strCondition.Append("AND JOB.JOB_TRN_CONT_PK = ' || JTC.JOB_TRN_CONT_PK || ')') FROM DUAL),");
                    strCondition.Append("PACK.PACK_TYPE_DESC) END Dimention_id,");
                    strCondition.Append("DECODE(JTC.PACK_COUNT,  0,");
                    strCondition.Append("(SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strCondition.Append("WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strCondition.Append("JTC.PACK_COUNT) QUANTITY,");
                    strCondition.Append("Decode(jt.FREIGHT_TYPE,1,'Prepaid',2,'Collect'),");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.FREIGHT_AMT ");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_FD jt,");
                    strCondition.Append("JOB_TRN_CONT JTC,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("DIMENTION_UNIT_MST_TBL di,");
                    strCondition.Append("PACK_TYPE_MST_TBL       PACK,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu,");
                    strCondition.Append("container_type_mst_tbl ctmt");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK(+)=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("and jc.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strCondition.Append("AND fr.freight_element_mst_pk(+)=jt.freight_element_mst_fk");
                    strCondition.Append("AND di.dimention_unit_mst_pk(+)=jt.basis");
                    strCondition.Append("AND JTC.PACK_TYPE_MST_FK = PACK.PACK_TYPE_MST_PK(+)");
                    strCondition.Append("AND cu.currency_mst_pk(+)=jt.currency_mst_fk");
                    strCondition.Append("AND jt.container_type_mst_fk=ctmt.container_type_mst_pk(+)");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strCondition.Append(" ORDER BY ctmt.preferences,FR.PREFERENCE");
                }
                else
                {
                    strCondition.Append("SELECT ROWNUM SLNO,");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("jc.JOBCARD_REF_NO,'' as container_type_mst_id,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("decode(jt.basis,1,'%',2,'Flat',3,'Kgs',4,'Unit') dimension_ID,");
                    strCondition.Append("DECODE(JTC.PACK_COUNT,  0,");
                    strCondition.Append("(SELECT SUM(PC.PACK_COUNT) FROM JOB_TRN_COMMODITY PC ");
                    strCondition.Append("WHERE PC.JOB_TRN_CONT_FK = JTC.JOB_TRN_CONT_PK),");
                    strCondition.Append("JTC.PACK_COUNT) QUANTITY,");
                    strCondition.Append("Decode(jt.FREIGHT_TYPE,1,'Prepaid',2,'Collect'),");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.FREIGHT_AMT ");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_FD jt,");
                    strCondition.Append("JOB_TRN_CONT JTC,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK(+)=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("and jc.JOB_CARD_TRN_PK = JTC.JOB_CARD_TRN_FK");
                    strCondition.Append("AND fr.freight_element_mst_pk(+)=jt.freight_element_mst_fk");
                    strCondition.Append("AND cu.currency_mst_pk(+)=jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                    {
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                    {
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                    strCondition.Append(" ORDER BY FR.PREFERENCE");
                }
            }

            strSQL = strCondition.ToString();
            DataTable dt = null;

            try
            {
                dt = objWF.GetDataTable(strSQL);
                return dt;
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

        #endregion "Fetch For  Childs"

        #region "FetchChildForOther"

        /// <summary>
        /// Fetches the child for other.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <returns></returns>
        private DataTable FetchChildForOther(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "")
        {
            StringBuilder strCondition = new StringBuilder();

            string strSQL = null;
            WorkFlow objWF = new WorkFlow();
            if (Convert.ToInt32(Pro) == 2)
            {
                if (Convert.ToInt32(Biz) == 2)
                {
                    strCondition.Append("SELECT");
                    strCondition.Append("ROWNUM SLNO,");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("jc.JOBCARD_REF_NO,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.Amount");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_OTH_CHRG jt,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("AND fr.freight_element_mst_pk=jt.freight_element_mst_fk");
                    strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK))
                    {
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK))
                    {
                        strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                }
                else
                {
                    strCondition.Append("SELECT");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("ROWNUM SLNO,");
                    strCondition.Append("jc.JOBCARD_REF_NO,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.Amount");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_OTH_CHRG jt,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("AND fr.freight_element_mst_pk=jt.freight_element_mst_fk");
                    strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK))
                    {
                        //If Master Job Card selected
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK))
                    {
                        //If selcect check boxes in Grid 'Hbl no
                        strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                }
            }

            //Process import
            if (Convert.ToInt32(Pro) == 1)
            {
                // Buisness type sea
                if (Convert.ToInt32(Biz) == 2)
                {
                    strCondition.Append("SELECT");
                    strCondition.Append("ROWNUM SLNO,");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("jc.JOBCARD_REF_NO,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.Amount");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_OTH_CHRG jt,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("AND fr.freight_element_mst_pk=jt.freight_element_mst_fk");
                    strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK))
                    {
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK))
                    {
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                }
                else
                {
                    strCondition.Append("SELECT");
                    strCondition.Append("jc.JOB_CARD_TRN_PK,");
                    strCondition.Append("ROWNUM SLNO,");
                    strCondition.Append("jc.JOBCARD_REF_NO,");
                    strCondition.Append("fr.freight_element_id,");
                    strCondition.Append("fr.freight_element_name,");
                    strCondition.Append("cu.currency_id,");
                    strCondition.Append("jt.Amount");
                    strCondition.Append("FROM");
                    strCondition.Append("JOB_CARD_TRN jc,");
                    strCondition.Append("JOB_TRN_OTH_CHRG jt,");
                    strCondition.Append("FREIGHT_ELEMENT_MST_TBL fr,");
                    strCondition.Append("CURRENCY_TYPE_MST_TBL cu");
                    strCondition.Append("WHERE");
                    strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                    strCondition.Append("AND fr.freight_element_mst_pk=jt.freight_element_mst_fk");
                    strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                    if (!string.IsNullOrEmpty(MJCPK))
                    {
                        strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                    }
                    else if (!string.IsNullOrEmpty(JCPK))
                    {
                    }
                    else
                    {
                        strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                    }
                }
            }

            strSQL = strCondition.ToString();
            DataTable dt = null;

            try
            {
                dt = objWF.GetDataTable(strSQL);
                return dt;
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

        #endregion "FetchChildForOther"

        #region "FetchChildForCost"

        /// <summary>
        /// Fetches the child for cost.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <returns></returns>
        private DataTable FetchChildForCost(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "")
        {
            StringBuilder strCondition = new StringBuilder();
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            try
            {
                if (Convert.ToInt32(Pro) == 2)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("ce.Cost_element_id,");
                        strCondition.Append("cu.currency_id,");
                        strCondition.Append("jt.estimated_cost,");
                        strCondition.Append("jt.total_cost,");
                        strCondition.Append(" 0 Difference");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append("JOB_TRN_COST jt,");
                        strCondition.Append("CURRENCY_TYPE_MST_TBL cu,");
                        strCondition.Append("cost_element_mst_tbl ce");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                            strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                        strCondition.Append(" ORDER BY ce.PREFERENCE");
                    }
                    else
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("ce.Cost_element_id,");
                        strCondition.Append("cu.currency_id,");
                        strCondition.Append("jt.estimated_cost,");
                        strCondition.Append("jt.total_cost,");
                        strCondition.Append(" 0 Difference");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append(" JOB_TRN_COST jt,");
                        strCondition.Append("CURRENCY_TYPE_MST_TBL cu,");
                        strCondition.Append("cost_element_mst_tbl ce");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                            strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                        strCondition.Append(" ORDER BY ce.PREFERENCE");
                    }
                }

                if (Convert.ToInt32(Pro )== 1)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("ce.Cost_element_id,");
                        strCondition.Append("cu.currency_id,");
                        strCondition.Append("jt.estimated_cost,");
                        strCondition.Append("jt.total_cost,");
                        strCondition.Append(" 0 Difference");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append(" JOB_TRN_COST jt,");
                        strCondition.Append("CURRENCY_TYPE_MST_TBL cu,");
                        strCondition.Append("cost_element_mst_tbl ce");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                        strCondition.Append(" ORDER BY ce.PREFERENCE");
                    }
                    else
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("ce.Cost_element_id,");
                        strCondition.Append("cu.currency_id,");
                        strCondition.Append("jt.estimated_cost,");
                        strCondition.Append("jt.total_cost,");
                        strCondition.Append(" 0 Difference");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append("JOB_TRN_COST jt,");
                        strCondition.Append("CURRENCY_TYPE_MST_TBL cu,");
                        strCondition.Append("cost_element_mst_tbl ce");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND cu.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                        strCondition.Append(" ORDER BY ce.PREFERENCE");
                    }
                }

                strSQL = strCondition.ToString();
                DataTable dt = null;
                dt = objWF.GetDataTable(strSQL);
                return dt;
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

        #endregion "FetchChildForCost"

        #region "FetchChildForPurchase"

        /// <summary>
        /// Fetches the child for purchase.
        /// </summary>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <param name="Biz">The biz.</param>
        /// <param name="Pro">The pro.</param>
        /// <returns></returns>
        private DataTable FetchChildForPurchase(string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0", string Biz = "", string Pro = "")
        {
            StringBuilder strCondition = new StringBuilder();
            string strSQL = null;
            WorkFlow objWF = new WorkFlow();

            try
            {
                if (Convert.ToInt32(Pro) == 2)
                {
                    if (Convert.ToInt32(Biz) == 2)
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("vm.vendor_name,");
                        strCondition.Append("jt.invoice_number,");
                        strCondition.Append("TO_DATE(jt.invoice_date,dateformat),");
                        strCondition.Append("ce.cost_element_id,");
                        strCondition.Append("ct.currency_id,");
                        strCondition.Append("jt.invoice_amt,");
                        strCondition.Append("jt.tax_percentage,");
                        strCondition.Append("jt.tax_amt,");
                        strCondition.Append("jt.estimated_amt,");
                        strCondition.Append("jt.invoice_amt-jt.estimated_amt");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append("JOB_TRN_PIA jt,");
                        strCondition.Append("COST_ELEMENT_MST_TBL ce,");
                        strCondition.Append("currency_type_mst_tbl ct,");
                        strCondition.Append("vendor_mst_tbl vm");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND vm.vendor_mst_pk=jt.vendor_mst_fk");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND ct.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                            strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                    }
                    else
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("vm.vendor_name,");
                        strCondition.Append("jt.invoice_number,");
                        strCondition.Append("TO_DATE(jt.invoice_date,dateformat),");
                        strCondition.Append("ce.cost_element_id,");
                        strCondition.Append("ct.currency_id,");
                        strCondition.Append("jt.invoice_amt,");
                        strCondition.Append("jt.tax_percentage,");
                        strCondition.Append("jt.tax_amt,");
                        strCondition.Append("jt.estimated_amt,");
                        strCondition.Append("jt.invoice_amt-jt.estimated_amt");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append("JOB_TRN_PIA jt,");
                        strCondition.Append("COST_ELEMENT_MST_TBL ce,");
                        strCondition.Append("currency_type_mst_tbl ct,");
                        strCondition.Append("vendor_mst_tbl vm");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND vm.vendor_mst_pk=jt.vendor_mst_fk");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND ct.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                            strCondition.Append("AND  jc.HBL_HAWB_FK in (" + JCPK + ")");
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                    }
                }

                if (Convert.ToInt32(Pro) == 1)
                {
                    if (Convert.ToInt32(Biz )== 2)
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("vm.vendor_name,");
                        strCondition.Append("jt.invoice_number,");
                        strCondition.Append("TO_DATE(jt.invoice_date,dateformat),");
                        strCondition.Append("ce.cost_element_id,");
                        strCondition.Append("ct.currency_id,");
                        strCondition.Append("jt.invoice_amt,");
                        strCondition.Append("jt.tax_percentage,");
                        strCondition.Append("jt.tax_amt,");
                        strCondition.Append("jt.estimated_amt,");
                        strCondition.Append("jt.invoice_amt-jt.estimated_amt");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append("JOB_TRN_PIA jt,");
                        strCondition.Append("COST_ELEMENT_MST_TBL ce,");
                        strCondition.Append("currency_type_mst_tbl ct,");
                        strCondition.Append("vendor_mst_tbl vm");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND vm.vendor_mst_pk=jt.vendor_mst_fk");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND ct.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                    }
                    else
                    {
                        strCondition.Append("SELECT");
                        strCondition.Append("jc.JOB_CARD_TRN_PK,");
                        strCondition.Append("ROWNUM SLNO,");
                        strCondition.Append("jc.JOBCARD_REF_NO,");
                        strCondition.Append("vm.vendor_name,");
                        strCondition.Append("jt.invoice_number,");
                        strCondition.Append("TO_DATE(jt.invoice_date,dateformat),");
                        strCondition.Append("ce.cost_element_id,");
                        strCondition.Append("ct.currency_id,");
                        strCondition.Append("jt.invoice_amt,");
                        strCondition.Append("jt.tax_percentage,");
                        strCondition.Append("jt.tax_amt,");
                        strCondition.Append("jt.estimated_amt,");
                        strCondition.Append("jt.invoice_amt-jt.estimated_amt");
                        strCondition.Append("FROM");
                        strCondition.Append("JOB_CARD_TRN jc,");
                        strCondition.Append("JOB_TRN_PIA jt,");
                        strCondition.Append("COST_ELEMENT_MST_TBL ce,");
                        strCondition.Append("currency_type_mst_tbl ct,");
                        strCondition.Append("vendor_mst_tbl vm");
                        strCondition.Append("WHERE");
                        strCondition.Append("jt.JOB_CARD_TRN_FK=jc.JOB_CARD_TRN_PK");
                        strCondition.Append("AND vm.vendor_mst_pk=jt.vendor_mst_fk");
                        strCondition.Append("AND ce.cost_element_mst_pk=jt.cost_element_mst_fk");
                        strCondition.Append("AND ct.currency_mst_pk=jt.currency_mst_fk");
                        if (!string.IsNullOrEmpty(MJCPK) & MJCPK != "0")
                        {
                            strCondition.Append("AND  jc.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                        }
                        else if (!string.IsNullOrEmpty(JCPK) & JCPK != "0")
                        {
                        }
                        else
                        {
                            strCondition.Append("AND  jc.MASTER_JC_FK in (" + MJCPKAS + ")");
                        }
                    }
                }

                strSQL = strCondition.ToString();
                DataTable dt = null;
                dt = objWF.GetDataTable(strSQL);
                return dt;
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

        #endregion "FetchChildForPurchase"

        #region "Revenue Details Calculation for Sea"

        /// <summary>
        /// Gets the revenue details.
        /// </summary>
        /// <param name="actualCost">The actual cost.</param>
        /// <param name="actualRevenue">The actual revenue.</param>
        /// <param name="estimatedCost">The estimated cost.</param>
        /// <param name="estimatedRevenue">The estimated revenue.</param>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <returns></returns>
        public bool GetRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0")
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;

            SQL.Append("SELECT");
            SQL.Append("      sum(ROUND(q.EstimatedCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("            then");
            SQL.Append("                           1");
            SQL.Append("            else");
            SQL.Append("                           (select");
            SQL.Append("                                   exch.exchange_rate");
            SQL.Append("                            from");
            SQL.Append("                                   exchange_rate_trn exch");
            SQL.Append("                            where");
            SQL.Append("                                   q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                   AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end");
            SQL.Append("         ),4))\"Estimated Cost\", ");
            SQL.Append("      sum(ROUND(ActualCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                            q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("           then");
            SQL.Append("                            1");
            SQL.Append("           else");
            SQL.Append("                            (select");
            SQL.Append("                                    exch.exchange_rate");
            SQL.Append("                             from");
            SQL.Append("                                    exchange_rate_trn exch");
            SQL.Append("                             where");
            SQL.Append("                                    q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                    AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end ");
            SQL.Append("    ),4)) \"Actual Cost\" ");
            SQL.Append("FROM");
            SQL.Append("    (SELECT");
            SQL.Append("           job_exp.jobcard_date,");
            SQL.Append("           curr.currency_mst_pk,");
            SQL.Append("           SUM(job_trn_pia.Estimated_Amt) EstimatedCost,");
            SQL.Append("           SUM(job_trn_pia.Invoice_Amt) ActualCost");
            SQL.Append("     FROM");
            SQL.Append("           JOB_TRN_PIA  job_trn_pia,");
            SQL.Append("           currency_type_mst_tbl curr,");
            SQL.Append("           cost_element_mst_tbl cost_ele,");
            SQL.Append("           JOB_CARD_TRN job_exp");
            SQL.Append("     WHERE");
            SQL.Append("           job_trn_pia.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
            SQL.Append("           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedCost = Convert.ToDecimal(oraReader[0]);
                }

                if ((!object.ReferenceEquals(oraReader[1], "")))
                {
                    actualCost = Convert.ToDecimal(oraReader[1]);
                }
            }

            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_fd.freight_amt) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_FD  job_trn_fd,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_fd.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_othr.amount) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_OTH_CHRG  job_trn_othr,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_othr.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = estimatedRevenue + Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            int i = 0;
            int len = 0;
            int JCPK1 = 0;
            string[] temp = null;
            if (!string.IsNullOrEmpty(JCPK))
            {
                temp = JCPK.Split(',');
                len = temp.Length;
            }
            if (len > 0)
            {
                for (i = 0; i <= len - 1; i++)
                {
                    JCPK1 = Convert.ToInt32(temp[i]);
                    try
                    {
                        objWF.MyCommand.Parameters.Clear();
                        var _with5 = objWF.MyCommand.Parameters;

                        _with5.Add("JCPK", JCPK1).Direction = ParameterDirection.Input;
                        _with5.Add("JOB_SEA_EXP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                        oraReader = objWF.GetDataReader("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_EXP_CONS");
                        while (oraReader.Read())
                        {
                            if ((!object.ReferenceEquals(oraReader[0], "")))
                            {
                                actualRevenue += Convert.ToDecimal(oraReader[0]);
                            }
                        }
                    }
                    catch (Exception sqlExp)
                    {
                        throw sqlExp;
                    }
                }
            }
            else
            {
                SQL = new System.Text.StringBuilder();
                SQL.Append("Select sum(ROUND(q.actual_revenue *  ");
                SQL.Append("  (case when ");
                SQL.Append("   q.currency_mst_pk = corp.currency_mst_fk ");
                SQL.Append("         then");
                SQL.Append("        1");
                SQL.Append("         else ");
                SQL.Append("           (select ");
                SQL.Append("               exch.exchange_rate");
                SQL.Append("            from");
                SQL.Append("               exchange_rate_trn exch");
                SQL.Append("            where");
                SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
                SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
                SQL.Append("           )end ");
                SQL.Append("   ),4)) \"Actual Revenue\"");
                SQL.Append("FROM");
                SQL.Append("  ( SELECT");
                SQL.Append("       job_exp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append(" sum (nvl(inv_cust.invoice_amt,0) + nvl(inv_cust.vat_amt,0) - nvl(inv_cust.discount_amt,0) ) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("       inv_cust_sea_exp_tbl inv_cust,");
                SQL.Append("      currency_type_mst_tbl curr,");
                SQL.Append("      JOB_CARD_TRN job_exp");
                SQL.Append("    WHERE");
                SQL.Append("       inv_cust.Job_Card_Sea_Exp_Fk = job_exp.JOB_CARD_TRN_PK");
                SQL.Append("        AND inv_cust.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
                SQL.Append("UNION");
                SQL.Append("   SELECT");
                SQL.Append("       job_exp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append("  sum (nvl(inv_agent.net_inv_amt,0) + nvl(inv_agent.vat_amt,0) - nvl(inv_agent.discount_amt,0) ) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("       inv_agent_tbl inv_agent,");
                SQL.Append("       currency_type_mst_tbl curr,");
                SQL.Append("        JOB_CARD_TRN job_exp");
                SQL.Append("    WHERE");
                SQL.Append("       inv_agent.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
                SQL.Append("        AND inv_agent.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");

                SQL.Append("UNION");

                SQL.Append("   SELECT");
                SQL.Append("       job_exp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append("   -sum(nvl(cr_agent.credit_note_amt,0)) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("       inv_agent_tbl inv_agent,");
                SQL.Append("       cr_agent_tbl  cr_agent,");
                SQL.Append("        currency_type_mst_tbl curr,");
                SQL.Append("       JOB_CARD_TRN job_exp");
                SQL.Append("    WHERE");
                SQL.Append("        cr_agent.inv_agent_fk = inv_agent.inv_agent_pk");
                SQL.Append("        AND inv_agent.job_card_fk = job_exp.JOB_CARD_TRN_PK");
                SQL.Append("         AND cr_agent.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");

                SQL.Append("UNION");

                SQL.Append("   SELECT");
                SQL.Append("       job_exp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append("    -sum(nvl(cr_customer.credit_note_amt,0)) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("     inv_cust_sea_exp_tbl inv_cust,");
                SQL.Append("      cr_cust_sea_exp_tbl  cr_customer,");
                SQL.Append("        currency_type_mst_tbl curr,");
                SQL.Append("      JOB_CARD_TRN job_exp");
                SQL.Append("    WHERE");
                SQL.Append("       cr_customer.inv_cust_sea_exp_fk = inv_cust.inv_cust_sea_exp_pk");
                SQL.Append("       AND inv_cust.Job_Card_Sea_Exp_Fk = job_exp.JOB_CARD_TRN_PK");
                SQL.Append("        AND cr_customer.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");

                SQL.Append(" UNION ");
                SQL.Append(" SELECT JOB_EXP.JOBCARD_DATE,");
                SQL.Append(" CON.CURRENCY_MST_FK,");
                SQL.Append(" SUM(NVL(CONS_TRN.amt_in_inv_curr, 0)) ACTUAL_REVENUE");
                SQL.Append(" FROM CONSOL_INVOICE_TBL     CON,");
                SQL.Append(" CONSOL_INVOICE_TRN_TBL CONS_TRN,");
                SQL.Append(" JOB_CARD_TRN   JOB_EXP");
                SQL.Append(" WHERE CON.CONSOL_INVOICE_PK = CONS_TRN.CONSOL_INVOICE_FK");
                SQL.Append(" AND CONS_TRN.JOB_CARD_FK = JOB_EXP.JOB_CARD_TRN_PK");
                SQL.Append(" AND CON.PROCESS_TYPE = 1");
                SQL.Append(" AND CON.BUSINESS_TYPE = 2");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append(" GROUP BY JOB_EXP.JOBCARD_DATE, CON.CURRENCY_MST_FK");
                SQL.Append(" UNION");
                SQL.Append(" SELECT GCRN.CREDIT_NOTE_DATE,");
                SQL.Append(" GCRN.CURRENCY_MST_FK,");
                SQL.Append(" -SUM(NVL(GCRN.CRN_AMMOUNT, 0)) ACTUAL_REVENUE");
                SQL.Append(" FROM CREDIT_NOTE_TBL GCRN, CREDIT_NOTE_TRN_TBL GCRT");
                SQL.Append(" WHERE GCRN.CRN_TBL_PK = GCRT.CRN_TBL_FK");
                SQL.Append(" AND GCRT.CONSOL_INVOICE_TRN_FK IN");
                SQL.Append(" (SELECT DISTINCT COT.CONSOL_INVOICE_FK");
                SQL.Append(" FROM CONSOL_INVOICE_TRN_TBL COT ,JOB_CARD_TRN   JOB_EXP");
                SQL.Append(" WHERE COT.JOB_CARD_FK = JOB_EXP.JOB_CARD_TRN_PK");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + "))");
                }
                else
                {
                    SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + "))");
                }
                SQL.Append(" AND GCRN.PROCESS_TYPE = 1");
                SQL.Append(" AND GCRN.BIZ_TYPE = 2");
                SQL.Append(" GROUP BY GCRN.CREDIT_NOTE_DATE, GCRN.CURRENCY_MST_FK");
                SQL.Append(" ");
                SQL.Append("  )q,corporate_mst_tbl corp");

                oraReader = objWF.GetDataReader(SQL.ToString());
                while (oraReader.Read())
                {
                    if ((!object.ReferenceEquals(oraReader[0], "")))
                    {
                        actualRevenue = actualRevenue + Convert.ToDecimal(oraReader[0]);
                    }
                }
                oraReader.Close();
            }
            try
            {
                return true;
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

        #endregion "Revenue Details Calculation for Sea"

        #region "Revenue Details Calculation for Sea Imp"

        /// <summary>
        /// Gets the sea imp revenue details.
        /// </summary>
        /// <param name="actualCost">The actual cost.</param>
        /// <param name="actualRevenue">The actual revenue.</param>
        /// <param name="estimatedCost">The estimated cost.</param>
        /// <param name="estimatedRevenue">The estimated revenue.</param>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <returns></returns>
        public bool GetSeaImpRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0")
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;

            SQL.Append("SELECT");
            SQL.Append("      sum(ROUND(q.EstimatedCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("            then");
            SQL.Append("                           1");
            SQL.Append("            else");
            SQL.Append("                           (select");
            SQL.Append("                                   exch.exchange_rate");
            SQL.Append("                            from");
            SQL.Append("                                   exchange_rate_trn exch");
            SQL.Append("                            where");
            SQL.Append("                                   q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                   AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end");
            SQL.Append("         ),4))\"Estimated Cost\", ");
            SQL.Append("      sum(ROUND(ActualCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                            q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("           then");
            SQL.Append("                            1");
            SQL.Append("           else");
            SQL.Append("                            (select");
            SQL.Append("                                    exch.exchange_rate");
            SQL.Append("                             from");
            SQL.Append("                                    exchange_rate_trn exch");
            SQL.Append("                             where");
            SQL.Append("                                    q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                    AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end ");
            SQL.Append("    ),4)) \"Actual Cost\" ");
            SQL.Append("FROM");
            SQL.Append("    (SELECT");
            SQL.Append("           job_imp.jobcard_date,");
            SQL.Append("           curr.currency_mst_pk,");
            SQL.Append("           SUM(job_trn_pia.Estimated_Amt) EstimatedCost,");
            SQL.Append("           SUM(job_trn_pia.Invoice_Amt) ActualCost");
            SQL.Append("     FROM");
            SQL.Append("           JOB_TRN_PIA  job_trn_pia,");
            SQL.Append("           currency_type_mst_tbl curr,");
            SQL.Append("           cost_element_mst_tbl cost_ele,");
            SQL.Append("           JOB_CARD_TRN job_imp");
            SQL.Append("     WHERE");
            SQL.Append("           job_trn_pia.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
            SQL.Append("           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedCost = Convert.ToDecimal(oraReader[0]);
                }

                if ((!object.ReferenceEquals(oraReader[1], "")))
                {
                    actualCost = Convert.ToDecimal(oraReader[1]);
                }
            }

            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_fd.freight_amt) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_FD  job_trn_fd,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_fd.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();
            SQL = new System.Text.StringBuilder();
            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_othr.amount) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_OTH_CHRG  job_trn_othr,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_othr.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = estimatedRevenue + Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            int i = 0;
            int len = 0;
            int JCPK1 = 0;
            string[] temp = null;
            if (!string.IsNullOrEmpty(JCPK))
            {
                temp = JCPK.Split(',');
                len = temp.Length;
            }
            if (len > 0)
            {
                for (i = 0; i <= len - 1; i++)
                {
                    JCPK1 = Convert.ToInt32(temp[i]);
                    try
                    {
                        objWF.MyCommand.Parameters.Clear();
                        var _with6 = objWF.MyCommand.Parameters;

                        _with6.Add("JCPK", JCPK1).Direction = ParameterDirection.Input;
                        _with6.Add("JOB_SEA_IMP_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                        oraReader = objWF.GetDataReader("FETCH_JOB_CARD_REVENUE_DATA", "FETCH_JOB_CARD_SEA_IMP_CONS");
                        while (oraReader.Read())
                        {
                            if ((!object.ReferenceEquals(oraReader[0], "")))
                            {
                                actualRevenue += Convert.ToDecimal(oraReader[0]);
                            }
                        }
                    }
                    catch (Exception sqlExp)
                    {
                        throw sqlExp;
                    }
                }
            }
            else if (!string.IsNullOrEmpty(MJCPKAS))
            {
                SQL = new System.Text.StringBuilder();

                SQL.Append("Select sum(ROUND(q.actual_revenue *  ");
                SQL.Append("  (case when ");
                SQL.Append("   q.currency_mst_pk = corp.currency_mst_fk ");
                SQL.Append("         then");
                SQL.Append("        1");
                SQL.Append("         else ");
                SQL.Append("           (select ");
                SQL.Append("               exch.exchange_rate");
                SQL.Append("            from");
                SQL.Append("               exchange_rate_trn exch");
                SQL.Append("            where");
                SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
                SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
                SQL.Append("           )end ");
                SQL.Append("   ),4)) \"Actual Revenue\"");
                SQL.Append("FROM");
                SQL.Append("  ( SELECT");
                SQL.Append("       job_imp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append(" sum (nvl(inv_cust.invoice_amt,0) + nvl(inv_cust.vat_amt,0) - nvl(inv_cust.discount_amt,0) ) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("       inv_cust_sea_imp_tbl inv_cust,");
                SQL.Append("      currency_type_mst_tbl curr,");
                SQL.Append("      JOB_CARD_TRN job_imp");
                SQL.Append("    WHERE");
                SQL.Append("       inv_cust.Job_Card_Sea_Imp_Fk = job_imp.JOB_CARD_TRN_PK");
                SQL.Append("        AND inv_cust.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
                SQL.Append("UNION");
                SQL.Append("   SELECT");
                SQL.Append("       job_imp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append("  sum (nvl(inv_agent.net_inv_amt,0) + nvl(inv_agent.vat_amt,0) - nvl(inv_agent.discount_amt,0) ) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("       inv_agent_tbl inv_agent,");
                SQL.Append("       currency_type_mst_tbl curr,");
                SQL.Append("        JOB_CARD_TRN job_imp");
                SQL.Append("    WHERE");
                SQL.Append("       inv_agent.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
                SQL.Append("        AND inv_agent.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");

                SQL.Append("UNION");

                SQL.Append("   SELECT");
                SQL.Append("       job_imp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append("   -sum(nvl(cr_agent.credit_note_amt,0)) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("       inv_agent_tbl inv_agent,");
                SQL.Append("       cr_agent_tbl  cr_agent,");
                SQL.Append("        currency_type_mst_tbl curr,");
                SQL.Append("       JOB_CARD_TRN job_imp");
                SQL.Append("    WHERE");
                SQL.Append("        cr_agent.inv_agent_fk = inv_agent.inv_agent_pk");
                SQL.Append("        AND inv_agent.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
                SQL.Append("         AND cr_agent.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");

                SQL.Append("UNION");

                SQL.Append("   SELECT");
                SQL.Append("       job_imp.jobcard_date,");
                SQL.Append("       curr.currency_mst_pk,");
                SQL.Append("    -sum(nvl(cr_customer.credit_note_amt,0)) actual_revenue");
                SQL.Append("    FROM");
                SQL.Append("     inv_cust_sea_imp_tbl inv_cust,");
                SQL.Append("      cr_cust_sea_imp_tbl  cr_customer,");
                SQL.Append("        currency_type_mst_tbl curr,");
                SQL.Append("      JOB_CARD_TRN job_imp");
                SQL.Append("    WHERE");
                SQL.Append("       cr_customer.inv_cust_sea_imp_fk = inv_cust.inv_cust_sea_imp_pk");
                SQL.Append("       AND inv_cust.Job_Card_Sea_Imp_Fk = job_imp.JOB_CARD_TRN_PK");
                SQL.Append("        AND cr_customer.currency_mst_fk =curr.currency_mst_pk");
                if (!string.IsNullOrEmpty(MJCPK))
                {
                    SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
                }
                else
                {
                    SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
                }
                SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
                SQL.Append("  )q,corporate_mst_tbl corp");

                oraReader = objWF.GetDataReader(SQL.ToString());
                while (oraReader.Read())
                {
                    if ((!object.ReferenceEquals(oraReader[0], "")))
                    {
                        actualRevenue = actualRevenue + Convert.ToDecimal(oraReader[0]);
                    }
                }
                oraReader.Close();
            }
            try
            {
                return true;
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

        #endregion "Revenue Details Calculation for Sea Imp"

        #region "Revenue Details Air"

        /// <summary>
        /// Gets the air revenue details.
        /// </summary>
        /// <param name="actualCost">The actual cost.</param>
        /// <param name="actualRevenue">The actual revenue.</param>
        /// <param name="estimatedCost">The estimated cost.</param>
        /// <param name="estimatedRevenue">The estimated revenue.</param>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <returns></returns>
        public bool GetAirRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0")
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;

            SQL.Append("SELECT");
            SQL.Append("      sum(ROUND(q.EstimatedCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("            then");
            SQL.Append("                           1");
            SQL.Append("            else");
            SQL.Append("                           (select");
            SQL.Append("                                   exch.exchange_rate");
            SQL.Append("                            from");
            SQL.Append("                                   exchange_rate_trn exch");
            SQL.Append("                            where");
            SQL.Append("                                   q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                   AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end");
            SQL.Append("         ),4))\"Estimated Cost\", ");
            SQL.Append("      sum(ROUND(ActualCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                            q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("           then");
            SQL.Append("                            1");
            SQL.Append("           else");
            SQL.Append("                            (select");
            SQL.Append("                                    exch.exchange_rate");
            SQL.Append("                             from");
            SQL.Append("                                    exchange_rate_trn exch");
            SQL.Append("                             where");
            SQL.Append("                                    q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                    AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end ");
            SQL.Append("    ),4)) \"Actual Cost\" ");
            SQL.Append("FROM");
            SQL.Append("    (SELECT");
            SQL.Append("           job_exp.jobcard_date,");
            SQL.Append("           curr.currency_mst_pk,");
            SQL.Append("           SUM(job_trn_pia.Estimated_Amt) EstimatedCost,");
            SQL.Append("           SUM(job_trn_pia.Invoice_Amt) ActualCost");
            SQL.Append("     FROM");
            SQL.Append("           JOB_TRN_PIA  job_trn_pia,");
            SQL.Append("           currency_type_mst_tbl curr,");
            SQL.Append("           cost_element_mst_tbl cost_ele,");
            SQL.Append("           JOB_CARD_TRN job_exp");
            SQL.Append("     WHERE");
            SQL.Append("           job_trn_pia.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
            SQL.Append("           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedCost = Convert.ToDecimal(oraReader[0]);
                }

                if ((!object.ReferenceEquals(oraReader[1], "")))
                {
                    actualCost = Convert.ToDecimal(oraReader[1]);
                }
            }

            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_fd.freight_amt) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_FD  job_trn_fd,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_fd.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_othr.amount) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_OTH_CHRG  job_trn_othr,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_othr.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = estimatedRevenue + Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("Select sum(ROUND(q.actual_revenue *  ");
            SQL.Append("  (case when ");
            SQL.Append("   q.currency_mst_pk = corp.currency_mst_fk ");
            SQL.Append("         then");
            SQL.Append("        1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Actual Revenue\"");
            SQL.Append("FROM");
            SQL.Append("  ( SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append(" sum (nvl(inv_cust.invoice_amt,0) + nvl(inv_cust.vat_amt,0) - nvl(inv_cust.discount_amt,0) ) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("       inv_cust_air_exp_tbl inv_cust,");
            SQL.Append("      currency_type_mst_tbl curr,");
            SQL.Append("      JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("       inv_cust.Job_Card_Air_Exp_Fk = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("        AND inv_cust.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
            SQL.Append("UNION");
            SQL.Append("   SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("  sum (nvl(inv_agent.net_inv_amt,0) + nvl(inv_agent.vat_amt,0) - nvl(inv_agent.discount_amt,0) ) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("       inv_agent_tbl inv_agent,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("        JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("       inv_agent.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("        AND inv_agent.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");

            SQL.Append("UNION");

            SQL.Append("   SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("   -sum(nvl(cr_agent.credit_note_amt,0)) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("       inv_agent_tbl inv_agent,");
            SQL.Append("       cr_agent_tbl  cr_agent,");
            SQL.Append("        currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("        cr_agent.inv_agent_fk = inv_agent.inv_agent_pk");
            SQL.Append("        AND inv_agent.JOB_CARD_TRN_FK = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("         AND cr_agent.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");

            SQL.Append("UNION");

            SQL.Append("   SELECT");
            SQL.Append("       job_exp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("    -sum(nvl(cr_customer.credit_note_amt,0)) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("     inv_cust_air_exp_tbl inv_cust,");
            SQL.Append("      cr_cust_air_exp_tbl  cr_customer,");
            SQL.Append("        currency_type_mst_tbl curr,");
            SQL.Append("      JOB_CARD_TRN job_exp");
            SQL.Append("    WHERE");
            SQL.Append("       cr_customer.inv_cust_air_exp_fk = inv_cust.inv_cust_air_exp_pk");
            SQL.Append("       AND inv_cust.Job_Card_Air_Exp_Fk = job_exp.JOB_CARD_TRN_PK");
            SQL.Append("        AND cr_customer.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_exp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
                SQL.Append("AND  job_exp.HBL_HAWB_FK in (" + JCPK + ")");
            }
            else
            {
                SQL.Append("AND  job_exp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
            SQL.Append("  )q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    actualRevenue = actualRevenue + Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            try
            {
                return true;
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

        #endregion "Revenue Details Air"

        #region "Revenue Details Air Imp"

        /// <summary>
        /// Gets the air imp revenue details.
        /// </summary>
        /// <param name="actualCost">The actual cost.</param>
        /// <param name="actualRevenue">The actual revenue.</param>
        /// <param name="estimatedCost">The estimated cost.</param>
        /// <param name="estimatedRevenue">The estimated revenue.</param>
        /// <param name="JCPK">The JCPK.</param>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="MJCPKAS">The mjcpkas.</param>
        /// <returns></returns>
        public bool GetAirImpRevenueDetails(decimal actualCost, decimal actualRevenue, decimal estimatedCost, decimal estimatedRevenue, string JCPK = "0", string MJCPK = "0", string MJCPKAS = "0")
        {
            System.Text.StringBuilder SQL = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();
            OracleDataReader oraReader = null;

            SQL.Append("SELECT");
            SQL.Append("      sum(ROUND(q.EstimatedCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("            then");
            SQL.Append("                           1");
            SQL.Append("            else");
            SQL.Append("                           (select");
            SQL.Append("                                   exch.exchange_rate");
            SQL.Append("                            from");
            SQL.Append("                                   exchange_rate_trn exch");
            SQL.Append("                            where");
            SQL.Append("                                   q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                   AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end");
            SQL.Append("         ),4))\"Estimated Cost\", ");
            SQL.Append("      sum(ROUND(ActualCost * ");
            SQL.Append("      (case when ");
            SQL.Append("                            q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("           then");
            SQL.Append("                            1");
            SQL.Append("           else");
            SQL.Append("                            (select");
            SQL.Append("                                    exch.exchange_rate");
            SQL.Append("                             from");
            SQL.Append("                                    exchange_rate_trn exch");
            SQL.Append("                             where");
            SQL.Append("                                    q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("                                    AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("                            )end ");
            SQL.Append("    ),4)) \"Actual Cost\" ");
            SQL.Append("FROM");
            SQL.Append("    (SELECT");
            SQL.Append("           job_imp.jobcard_date,");
            SQL.Append("           curr.currency_mst_pk,");
            SQL.Append("           SUM(job_trn_pia.Estimated_Amt) EstimatedCost,");
            SQL.Append("           SUM(job_trn_pia.Invoice_Amt) ActualCost");
            SQL.Append("     FROM");
            SQL.Append("           JOB_TRN_PIA  job_trn_pia,");
            SQL.Append("           currency_type_mst_tbl curr,");
            SQL.Append("           cost_element_mst_tbl cost_ele,");
            SQL.Append("           JOB_CARD_TRN job_imp");
            SQL.Append("     WHERE");
            SQL.Append("           job_trn_pia.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("           AND job_trn_pia.cost_element_mst_fk =cost_ele.cost_element_mst_pk");
            SQL.Append("           AND job_trn_pia.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("     GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());

            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedCost = Convert.ToDecimal(oraReader[0]);
                }

                if ((!object.ReferenceEquals(oraReader[1], "")))
                {
                    actualCost = Convert.ToDecimal(oraReader[1]);
                }
            }

            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_fd.freight_amt) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_FD  job_trn_fd,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_fd.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_fd.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("SELECT  ");
            SQL.Append("   sum(ROUND(q.freight_amt * ");
            SQL.Append("   (case when ");
            SQL.Append("           q.currency_mst_pk = corp.currency_mst_fk");
            SQL.Append("         then ");
            SQL.Append("           1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Estimated Revenue\"");
            SQL.Append("FROM");
            SQL.Append("   (SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("       sum(job_trn_othr.amount) freight_amt");
            SQL.Append("    FROM");
            SQL.Append("       JOB_TRN_OTH_CHRG  job_trn_othr,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("       job_trn_othr.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("       AND job_trn_othr.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk)q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    estimatedRevenue = estimatedRevenue + Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            SQL = new System.Text.StringBuilder();

            SQL.Append("Select sum(ROUND(q.actual_revenue *  ");
            SQL.Append("  (case when ");
            SQL.Append("   q.currency_mst_pk = corp.currency_mst_fk ");
            SQL.Append("         then");
            SQL.Append("        1");
            SQL.Append("         else ");
            SQL.Append("           (select ");
            SQL.Append("               exch.exchange_rate");
            SQL.Append("            from");
            SQL.Append("               exchange_rate_trn exch");
            SQL.Append("            where");
            SQL.Append("               q.jobcard_date BETWEEN exch.from_date AND NVL(exch.to_date,round(sysdate - .5))");
            SQL.Append("               AND q.currency_mst_pk = exch.currency_mst_fk and exch.exch_rate_type_fk = 1 ");
            SQL.Append("           )end ");
            SQL.Append("   ),4)) \"Actual Revenue\"");
            SQL.Append("FROM");
            SQL.Append("  ( SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append(" sum (nvl(inv_cust.invoice_amt,0) + nvl(inv_cust.vat_amt,0) - nvl(inv_cust.discount_amt,0) ) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("       inv_cust_air_imp_tbl inv_cust,");
            SQL.Append("      currency_type_mst_tbl curr,");
            SQL.Append("      JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("       inv_cust.Job_Card_Air_Imp_Fk = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("        AND inv_cust.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
            SQL.Append("UNION");
            SQL.Append("   SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("  sum (nvl(inv_agent.net_inv_amt,0) + nvl(inv_agent.vat_amt,0) - nvl(inv_agent.discount_amt,0) ) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("       inv_agent_tbl inv_agent,");
            SQL.Append("       currency_type_mst_tbl curr,");
            SQL.Append("        JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("       inv_agent.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("        AND inv_agent.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
            SQL.Append("UNION");
            SQL.Append("   SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("   -sum(nvl(cr_agent.credit_note_amt,0)) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("       inv_agent_tbl inv_agent,");
            SQL.Append("       cr_agent_tbl  cr_agent,");
            SQL.Append("        currency_type_mst_tbl curr,");
            SQL.Append("       JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("        cr_agent.inv_agent_fk = inv_agent.inv_agent_pk");
            SQL.Append("        AND inv_agent.JOB_CARD_TRN_FK = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("         AND cr_agent.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
            SQL.Append("UNION");
            SQL.Append("   SELECT");
            SQL.Append("       job_imp.jobcard_date,");
            SQL.Append("       curr.currency_mst_pk,");
            SQL.Append("    -sum(nvl(cr_customer.credit_note_amt,0)) actual_revenue");
            SQL.Append("    FROM");
            SQL.Append("     inv_cust_air_imp_tbl inv_cust,");
            SQL.Append("      cr_cust_air_imp_tbl  cr_customer,");
            SQL.Append("        currency_type_mst_tbl curr,");
            SQL.Append("      JOB_CARD_TRN job_imp");
            SQL.Append("    WHERE");
            SQL.Append("       cr_customer.inv_cust_air_imp_fk = inv_cust.inv_cust_air_imp_pk");
            SQL.Append("       AND inv_cust.Job_Card_Air_Imp_Fk = job_imp.JOB_CARD_TRN_PK");
            SQL.Append("        AND cr_customer.currency_mst_fk =curr.currency_mst_pk");
            if (!string.IsNullOrEmpty(MJCPK))
            {
                SQL.Append("AND  job_imp.JOB_CARD_TRN_PK in (" + MJCPK + ")");
            }
            else if (!string.IsNullOrEmpty(JCPK))
            {
            }
            else
            {
                SQL.Append("AND  job_imp.MASTER_JC_FK in (" + MJCPKAS + ")");
            }
            SQL.Append("       GROUP BY jobcard_date,currency_mst_pk");
            SQL.Append("  )q,corporate_mst_tbl corp");

            oraReader = objWF.GetDataReader(SQL.ToString());
            while (oraReader.Read())
            {
                if ((!object.ReferenceEquals(oraReader[0], "")))
                {
                    actualRevenue = actualRevenue + Convert.ToDecimal(oraReader[0]);
                }
            }
            oraReader.Close();

            try
            {
                return true;
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

        #endregion "Revenue Details Air Imp"

        #region "Check FCL LCL"

        /// <summary>
        /// Checks the FCLLCL.
        /// </summary>
        /// <param name="MJCPK">The MJCPK.</param>
        /// <param name="Pro">The pro.</param>
        /// <returns></returns>
        public int CheckFCLLCL(string MJCPK = "0", string Pro = "0")
        {
            //0-LCL,1-FCL
            string strsql = null;
            Int16 RecCount = default(Int16);
            Int16 CargoType = default(Int16);
            WorkFlow objWF = new WorkFlow();

            //Export
            if (Convert.ToInt32(Pro) == 2)
            {
                strsql = "SELECT count(B.cargo_type) FROM BOOKING_MST_TBL B,JOB_CARD_TRN J WHERE";
                strsql += " B.BOOKING_MST_PK=j.BOOKING_MST_FK AND j.JOB_CARD_TRN_PK in (" + MJCPK + ")";
                RecCount = Convert.ToInt16(objWF.ExecuteScaler(strsql));
                if (RecCount > 0)
                {
                    strsql = "SELECT B.cargo_type FROM BOOKING_MST_TBL B,JOB_CARD_TRN J WHERE";
                    strsql += " B.BOOKING_MST_PK=j.BOOKING_MST_FK AND j.JOB_CARD_TRN_PK in (" + MJCPK + ") and rownum=1";
                    CargoType = Convert.ToInt16(objWF.ExecuteScaler(strsql));
                    if (CargoType == 1)
                    {
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }
                }
                else
                {
                    return 0;
                }
                // Import
            }
            else
            {
                strsql = " SELECT count(cargo_type) FROM JOB_CARD_TRN J  WHERE j.JOB_CARD_TRN_PK in (" + MJCPK + ")";
                RecCount = Convert.ToInt16(objWF.ExecuteScaler(strsql));
                if (RecCount > 0)
                {
                    strsql = " SELECT cargo_type FROM JOB_CARD_TRN J  WHERE j.JOB_CARD_TRN_PK in (" + MJCPK + ") and rownum=1";
                    CargoType = Convert.ToInt16(objWF.ExecuteScaler(strsql));
                    if (CargoType == 1)
                    {
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }
                }
                else
                {
                    return 0;
                }
            }
        }

        #endregion "Check FCL LCL"
    }
}