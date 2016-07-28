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
using System.Text;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BarcodeManager : CommonFeatures
    {
        #region "Fetch_Biz_Type"

        /// <summary>
        /// Fetch_s the type of the biz_.
        /// </summary>
        /// <returns></returns>
        public DataSet Fetch_Biz_Type()
        {
            string StrSql = null;
            try
            {
                WorkFlow objWF = new WorkFlow();
                StrSql = "select bdmt.bcd_mst_pk, bdmt.field_name from barcode_data_mst_tbl bdmt where bdmt.data_level=1";
                return objWF.GetDataSet(StrSql);
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

        #endregion "Fetch_Biz_Type"

        #region "Fetch_Module_Type"

        /// <summary>
        /// Fetch_s the type of the module_.
        /// </summary>
        /// <param name="BcfPk">The BCF pk.</param>
        /// <param name="iLevelFlg">The i level FLG.</param>
        /// <returns></returns>
        public DataSet Fetch_Module_Type(int BcfPk, int iLevelFlg)
        {
            string StrSql = null;
            try
            {
                WorkFlow objWF = new WorkFlow();
                StrSql = "select bdmt.bcd_mst_pk, bdmt.field_name from barcode_data_mst_tbl bdmt where bdmt.data_level=" + iLevelFlg + " and bdmt.BCD_MST_FK=" + BcfPk + " order by bdmt.field_name";
                return objWF.GetDataSet(StrSql);
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

        #endregion "Fetch_Module_Type"

        #region "Fetch Grid"

        /// <summary>
        /// Fetch_s the grid.
        /// </summary>
        /// <param name="BcfPk">The BCF pk.</param>
        /// <returns></returns>
        public DataSet Fetch_Grid(int BcfPk)
        {
            StringBuilder strQuery = new StringBuilder();
            try
            {
                WorkFlow objWF = new WorkFlow();
                strQuery.Append("select rownum, q.* from");
                strQuery.Append("       (select bdmt.bcd_mst_pk,");
                strQuery.Append("       bdmt.field_name,");
                strQuery.Append("       bdmt.default_value,");
                strQuery.Append("       bdmt.data_length,");
                strQuery.Append("       case when  bdmt.default_value =1 then 'TRUE' else 'FALSE' end SEL ");
                strQuery.Append("  from barcode_data_mst_tbl bdmt,barcode_doc_data_tbl bddt");
                strQuery.Append(" where bdmt.BCD_MST_FK=" + BcfPk);
                strQuery.Append(" and bdmt.bcd_mst_pk = bddt.bcd_mst_fk(+) ");
                strQuery.Append(" group by bdmt.bcd_mst_pk ,  bdmt.field_name, bdmt.default_value,   bdmt.data_length  order by  default_value desc) q");
                return objWF.GetDataSet(strQuery.ToString());
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

        #endregion "Fetch Grid"
    }
}