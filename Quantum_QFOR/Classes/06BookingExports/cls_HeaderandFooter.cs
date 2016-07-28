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
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    /// 
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class ClsHeaderandFooter : CommonFeatures
    {
        /// <summary>
        /// Fetches the customer details.
        /// </summary>
        /// <param name="CusID">The cus identifier.</param>
        /// <returns></returns>
        public DataSet FetchCustomerDetails(string CusID)
        {
            string strSql = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSql += " select cus.customer_name, ";
                strSql += " con.adm_contact_person, ";
                strSql += " con.adm_phone_no_1, ";
                strSql += " cus.customer_mst_pk ";
                strSql += " from customer_mst_tbl cus,customer_contact_dtls con ";
                strSql += " where cus.customer_mst_pk = con.customer_mst_fk ";
                strSql += " and (cus.customer_id = '" + CusID + "' ";
                strSql += " or cus.customer_NAME LIKE '" + CusID + "%' )";
                strSql += " union ";
                strSql += " select tcus.customer_name,";
                strSql += " tcon.adm_contact_person,";
                strSql += " tcon.adm_phone_no_1,";
                strSql += " tcus.customer_mst_pk";
                strSql += " from temp_customer_tbl tcus,temp_customer_contact_dtls tcon ";
                strSql += " where tcus.customer_mst_pk = tcon.customer_mst_fk ";
                strSql += " and (tcus.customer_id = '" + CusID + "' ";
                strSql += " or tcus.customer_NAME LIKE '" + CusID + "%') ";
                return objWF.GetDataSet(strSql);
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

        /// <summary>
        /// Fetches the customer header details.
        /// </summary>
        /// <param name="Cuspk">The cuspk.</param>
        /// <param name="Quotpk">The quotpk.</param>
        /// <returns></returns>
        public DataSet FetchCustHeaderDetails(string Cuspk, string Quotpk)
        {
            string strSql = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSql = "select distinct q.header_content,q.footer_content" + "from QUOTATION_MST_TBL q" + "where q.customer_mst_fk = " + Cuspk + "and q.QUOTATION_MST_PK = " + Quotpk;

                return objWF.GetDataSet(strSql);
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

        /// <summary>
        /// Fetches the customer header details air.
        /// </summary>
        /// <param name="Cuspk">The cuspk.</param>
        /// <param name="QuotAirpk">The quot airpk.</param>
        /// <returns></returns>
        public DataSet FetchCustHeaderDetailsAir(string Cuspk, string QuotAirpk)
        {
            string strSql = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                strSql = "select distinct q.header_content,q.footer_content" + "from QUOTATION_MST_TBL q" + "where q.customer_mst_fk = " + Cuspk + "and q.QUOTATION_MST_PK = " + QuotAirpk;

                return objWF.GetDataSet(strSql);
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

    }
}