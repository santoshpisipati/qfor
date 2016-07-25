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
    public class Cls_SeaCargoManifest : CommonFeatures
    {
        /// <summary>
        /// The m_ data set
        /// </summary>
        private static DataSet M_DataSet = new DataSet();

        #region "Property"

        /// <summary>
        /// Gets my data set.
        /// </summary>
        /// <value>
        /// My data set.
        /// </value>
        public static DataSet MyDataSet
        {
            get { return M_DataSet; }
        }

        #endregion "Property"

        #region " Enhance search for MBL JOB REF NO"

        /// <summary>
        /// Fetches for MBL job reference.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchForMBLJobRef(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strReq = null;
            string businessType = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            businessType = Convert.ToString(arr.GetValue(2));

            Quantum_QFOR.cls_Enhance_Search obj = new Quantum_QFOR.cls_Enhance_Search();

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_JOB_MAWB_REF_NO_PKG.GET_JOB_MAWB_REF_COMMON";
                var _with1 = SCM.Parameters;
                _with1.Add("SEARCH_IN", ifDBNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with1.Add("LOCATION_MST_FK_IN", ifDBNull(loc)).Direction = ParameterDirection.Input;
                _with1.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with1.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with1.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Ifs the database null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDBNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        /// <summary>
        /// Ifs the date null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDateNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return Convert.ToDateTime(col);
            }
        }

        /// <summary>
        /// Fetches for ves flight.
        /// </summary>
        /// <param name="strCond">The string cond.</param>
        /// <param name="loc">The loc.</param>
        /// <returns></returns>
        public string FetchForVesFlight(string strCond, string loc = "")
        {
            WorkFlow objWF = new WorkFlow();
            OracleCommand SCM = new OracleCommand();
            string strReturn = null;
            Array arr = null;
            string strSERACH_IN = "";
            string strReq = null;
            string businessType = null;
            arr = strCond.Split('~');
            strReq = Convert.ToString(arr.GetValue(0));
            strSERACH_IN = Convert.ToString(arr.GetValue(1));
            businessType = Convert.ToString(arr.GetValue(2));

            Quantum_QFOR.cls_Enhance_Search obj = new Quantum_QFOR.cls_Enhance_Search();

            try
            {
                objWF.OpenConnection();
                SCM.Connection = objWF.MyConnection;
                SCM.CommandType = CommandType.StoredProcedure;
                SCM.CommandText = objWF.MyUserName + ".EN_MBL_VESSEL_PKG.GET_VES_FLIGHT_COMMON";
                var _with2 = SCM.Parameters;
                _with2.Add("SEARCH_IN", ifDNull(strSERACH_IN)).Direction = ParameterDirection.Input;
                _with2.Add("LOCATION_VALUE_IN", ifDBNull(loc)).Direction = ParameterDirection.Input;
                _with2.Add("LOOKUP_VALUE_IN", strReq).Direction = ParameterDirection.Input;
                _with2.Add("BUSINESS_TYPE_IN", businessType).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.NVarchar2, 4000, "RETURN_VALUE").Direction = ParameterDirection.Output;
                SCM.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                SCM.ExecuteNonQuery();
                strReturn = Convert.ToString(SCM.Parameters["RETURN_VALUE"].Value).Trim();
                return strReturn;
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
                SCM.Connection.Close();
            }
        }

        /// <summary>
        /// Ifs the d null.
        /// </summary>
        /// <param name="col">The col.</param>
        /// <returns></returns>
        private object ifDNull(object col)
        {
            if (Convert.ToString(col).Length == 0)
            {
                return "";
            }
            else
            {
                return col;
            }
        }

        #endregion " Enhance search for MBL JOB REF NO"

        #region " Report Details"

        /// <summary>
        /// Fetches the hazardous details.
        /// </summary>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="jobpk">The jobpk.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchHazardousDetails(Int32 Biztype, string jobpk, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            try
            {
                if (Process == "EXPORT")
                {
                    //strSQL.Append(" select Q.imdg_class_code as mdgcode,un_no as unno,PackType,flash_pnt_temp as FlashPoint from ")
                    //strSQL.Append(" (select haz.imdg_class_code ,haz.un_no , ")
                    //strSQL.Append(" (select pack.pack_type_desc from pack_type_mst_tbl pack where pack.pack_type_mst_pk=haz.outer_pack_type_mst_fk)PackType,")
                    //strSQL.Append("  haz.flash_pnt_temp from booking_trn_spl_req haz,job_card_TRN job,booking_mst_tbl bkg,booking_trn lclfcl ")
                    //strSQL.Append(" where job.job_card_trn_pk in(" & jobpk & ") And job.booking_mst_fk = bkg.booking_mst_pk And bkg.booking_mst_pk = lclfcl.booking_mst_fk ")
                    //strSQL.Append("  and haz.booking_trn_fk=lclfcl.booking_trn_pk group by haz.outer_pack_type_mst_fk,imdg_class_code,un_no,flash_pnt_temp)Q ")
                    //If Biztype = 1 Then
                    //    strSQL.Replace("sea", "air")
                    //    strSQL.Replace("booking_trn_air_fcl_lcl", "booking_trn_air")
                    //    strSQL.Replace("booking_trn_air_fk", "booking_air_fk")
                    //End If

                    if (Biztype == 2)
                    {
                        strSQL.Append("             SELECT Q.HBL_EXP_TBL_PK,");
                        strSQL.Append("  Q.CONTAINER_TYPE_MST_ID, ");
                        strSQL.Append("   to_char(Q.MIN_TEMP) AS MIN_TEMP, ");
                        strSQL.Append("    TO_CHAR(Q.MIN_TEMP_UOM) AS MIN_TEMP_UOM,");
                        strSQL.Append("   to_char( Q.MAX_TEMP) As MAX_TEMP ,");
                        strSQL.Append("  TO_CHAR(Q.MAX_TEMP_UOM) AS MAX_TEMP_UOM, ");
                        strSQL.Append("  Q.IMDG_CLASS_CODE AS MDGCODE,");
                        strSQL.Append("   UN_NO AS UNNO,");
                        strSQL.Append("  PACKTYPE, ");
                        strSQL.Append("  FLASH_PNT_TEMP AS FLASHPOINT");

                        strSQL.Append("  FROM (SELECT HAZ.MIN_TEMP,");
                        strSQL.Append("               HAZ.MIN_TEMP_UOM,");
                        strSQL.Append("             HAZ.MAX_TEMP,");
                        strSQL.Append("          HAZ.MAX_TEMP_UOM,");
                        strSQL.Append("         HAZ.IMDG_CLASS_CODE,");
                        strSQL.Append("        HAZ.UN_NO,");
                        strSQL.Append("        (SELECT PACK.PACK_TYPE_DESC");
                        strSQL.Append("          FROM PACK_TYPE_MST_TBL PACK");
                        strSQL.Append("         WHERE PACK.PACK_TYPE_MST_PK = HAZ.OUTER_PACK_TYPE_MST_FK) PACKTYPE,");
                        strSQL.Append("      HAZ.FLASH_PNT_TEMP,");
                        strSQL.Append("        JOB.JOB_CARD_TRN_PK AS HBL_EXP_TBL_PK,");

                        strSQL.Append("         CASE WHEN JOB.CARGO_TYPE=1 THEN ");
                        strSQL.Append("      CTMT.CONTAINER_TYPE_MST_ID ");
                        strSQL.Append("       WHEN JOB.CARGO_TYPE=4 THEN ");
                        strSQL.Append("         CMT.COMMODITY_NAME ");
                        strSQL.Append("    Else ");
                        strSQL.Append("      (SELECT REPLACE(ROWTOCOL('SELECT DISTINCT CMD.COMMODITY_NAME || '';'' FROM JOB_TRN_CONT JOB_CONT, ");
                        strSQL.Append("         JOB_TRN_COMMODITY JCD,COMMODITY_MST_TBL CMD WHERE JOB_CONT.JOB_TRN_CONT_PK = JCD.JOB_TRN_CONT_FK ");
                        strSQL.Append("            AND JCD.COMMODITY_MST_FK = CMD.COMMODITY_MST_PK AND JOB_CONT.JOB_TRN_CONT_PK = ' ||");
                        strSQL.Append("                    JTC.JOB_TRN_CONT_PK),");
                        strSQL.Append("        ';,',");
                        strSQL.Append("     ';')");
                        strSQL.Append("     FROM DUAL)");
                        strSQL.Append("       END CONTAINER_TYPE_MST_ID");

                        // strSQL.Append("      CTMT.CONTAINER_TYPE_MST_ID")
                        strSQL.Append("           FROM JOB_TRN_SPL_REQ    HAZ,");
                        strSQL.Append("           JOB_CARD_TRN           JOB,");
                        strSQL.Append("            CONTAINER_TYPE_MST_TBL CTMT,");
                        strSQL.Append(" COMMODITY_MST_TBL CMT,");
                        strSQL.Append("                HBL_EXP_TBL HBL,");
                        strSQL.Append("                 JOB_TRN_CONT JTC");
                        strSQL.Append("         WHERE JOB.JOB_CARD_TRN_PK IN (" + jobpk + ")");
                        strSQL.Append("     AND HAZ.JOB_TRN_CONT_FK=JTC.JOB_TRN_CONT_PK");
                        strSQL.Append("    AND JTC.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK");
                        //strSQL.Append("     AND HAZ.BOOKING_TRN_FK = LCLFCL.BOOKING_TRN_PK")
                        strSQL.Append("    AND CTMT.CONTAINER_TYPE_MST_PK(+) = JTC.CONTAINER_TYPE_MST_FK");
                        strSQL.Append("       AND HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK(+)");
                        strSQL.Append("   AND JTC.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");

                        strSQL.Append(" ORDER BY  CTMT.PREFERENCES) Q ");
                    }
                    else
                    {
                        strSQL.Append("           SELECT Q.HBL_EXP_TBL_PK,");
                        strSQL.Append("       CONTAINER_TYPE_MST_ID, ");
                        strSQL.Append("   to_char(Q.MIN_TEMP) AS MIN_TEMP,");
                        strSQL.Append("   to_char(Q.MIN_TEMP_UOM) AS MIN_TEMP_UOM,");
                        strSQL.Append("      to_char( Q.MAX_TEMP) As MAX_TEMP,");
                        strSQL.Append("     to_char(Q.MAX_TEMP_UOM) AS MAX_TEMP_UOM,");
                        strSQL.Append("     Q.IMDG_CLASS_CODE AS MDGCODE,");
                        strSQL.Append("     UN_NO AS UNNO,");
                        strSQL.Append("                   PACKTYPE, ");
                        strSQL.Append("    FLASH_PNT_TEMP AS FLASHPOINT ");

                        strSQL.Append("  FROM (SELECT DGS.MIN_TEMP,");
                        strSQL.Append("  DGS.MAX_TEMP, ");
                        strSQL.Append("  DGS.MIN_TEMP_UOM,");
                        strSQL.Append("  DGS.MAX_TEMP_UOM,");
                        strSQL.Append("   DGS.Un_No, ");
                        strSQL.Append("  DGS.Imdg_Class_Code, ");
                        strSQL.Append("        (SELECT PACK.PACK_TYPE_DESC");
                        strSQL.Append("          FROM PACK_TYPE_MST_TBL PACK");
                        strSQL.Append("         WHERE PACK.PACK_TYPE_MST_PK = DGS.OUTER_PACK_TYPE_MST_FK) PACKTYPE,");
                        strSQL.Append("  DGS.Flash_Pnt_Temp, ");

                        strSQL.Append("         CASE  ");
                        strSQL.Append("       WHEN JOB.CARGO_TYPE=4 THEN ");
                        strSQL.Append("         CMT.COMMODITY_NAME ");
                        strSQL.Append("    Else ");
                        strSQL.Append("      (SELECT REPLACE(ROWTOCOL('SELECT DISTINCT CMD.COMMODITY_NAME || '';'' FROM JOB_TRN_CONT JOB_CONT, ");
                        strSQL.Append("         JOB_TRN_COMMODITY JCD,COMMODITY_MST_TBL CMD WHERE JOB_CONT.JOB_TRN_CONT_PK = JCD.JOB_TRN_CONT_FK ");
                        strSQL.Append("            AND JCD.COMMODITY_MST_FK = CMD.COMMODITY_MST_PK AND JOB_CONT.JOB_TRN_CONT_PK = ' ||");
                        strSQL.Append("                    jc.JOB_TRN_CONT_PK),");
                        strSQL.Append("        ';,',");
                        strSQL.Append("     ';')");
                        strSQL.Append("     FROM DUAL)");
                        strSQL.Append("       END CONTAINER_TYPE_MST_ID ,");
                        strSQL.Append("  JOB.JOB_CARD_TRN_PK AS HBL_EXP_TBL_PK");
                        strSQL.Append("      FROM JOB_TRN_SPL_REQ DGS,");
                        strSQL.Append("   JOB_CARD_TRN JOB, ");
                        strSQL.Append("        JOB_TRN_CONT jc, ");
                        strSQL.Append("   COMMODITY_MST_TBL CMT ");
                        strSQL.Append("      WHERE JOB.JOB_CARD_TRN_PK IN (" + jobpk + ")");
                        strSQL.Append("     AND JOB.Job_Card_Trn_Pk = JC.JOB_CARD_TRN_FK(+)");
                        strSQL.Append("   AND jc.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                        strSQL.Append("    and jc.job_trn_cont_pk = DGS.job_trn_cont_fk ) Q");
                    }

                    return objWF.GetDataSet(strSQL.ToString());
                }
                else
                {
                    DataSet MainDS = new DataSet();
                    objWF.OpenConnection();
                    objWF.MyDataAdapter = new OracleDataAdapter();
                    var _with3 = objWF.MyDataAdapter;
                    _with3.SelectCommand = new OracleCommand();
                    _with3.SelectCommand.Connection = objWF.MyConnection;
                    _with3.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_SEA_IMPORT_HAZ";
                    _with3.SelectCommand.CommandType = CommandType.StoredProcedure;
                    _with3.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = jobpk;
                    _with3.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = Biztype;
                    _with3.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                    _with3.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with3.Fill(MainDS);
                    return MainDS;
                }
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

        /// <summary>
        /// Fetches the reefer details.
        /// </summary>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="jobpk">The jobpk.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchReeferDetails(Int32 Biztype, string jobpk, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            try
            {
                if (Process == "EXPORT")
                {
                    //strSQL.Append(" select Q.min_temp as MinTemp,Q.max_temp as MaxTemp, Q. ventilation from ( ")
                    //strSQL.Append(" select reffer.min_temp ,reffer.max_temp ,decode(reffer.ventilation,0,'Air Cooled System',2,'Water Cooled System') ventilation ")
                    //strSQL.Append(" from booking_trn_spl_req reffer,job_card_trn job,booking_mst_tbl bkg,")
                    //strSQL.Append(" booking_trn lclfcl ")
                    //strSQL.Append("  where job.job_card_trn_pk in ( " & jobpk & ") And job.booking_mst_fk = bkg.booking_mst_pk ")

                    //strSQL.Append("  and bkg.booking_mst_pk=lclfcl.booking_mst_fk and reffer.booking_trn_fk=lclfcl.booking_trn_pk ")
                    //strSQL.Append("  group by min_temp,max_temp,reffer.ventilation ) Q ")
                    //If Biztype = 1 Then
                    //    strSQL.Replace("sea", "air")
                    //    strSQL.Replace("booking_trn_air_fcl_lcl", "booking_trn_air")
                    //    strSQL.Replace("booking_trn_air_fk", "booking_air_fk")
                    //End If

                    if (Biztype == 2)
                    {
                        strSQL.Append("             SELECT Q.HBL_EXP_TBL_PK,");
                        strSQL.Append("  Q.CONTAINER_TYPE_MST_ID,");
                        strSQL.Append("   Q.MIN_TEMP              AS MINTEMP,");
                        strSQL.Append("  to_char( Q.MIN_TEMP_UOM) as MIN_TEMP_UOM ,");
                        strSQL.Append("   Q.MAX_TEMP              AS MAXTEMP,");
                        strSQL.Append("  to_char( Q.MAX_TEMP_UOM) as MAX_TEMP_UOM ,");
                        strSQL.Append("  Q.VENTILATION");

                        strSQL.Append("  FROM (SELECT REFFER.MIN_TEMP,");
                        strSQL.Append("      REFFER.MAX_TEMP,");
                        strSQL.Append("    REFFER.MIN_TEMP_UOM,");
                        strSQL.Append("   REFFER.MAX_TEMP_UOM,");
                        strSQL.Append("   DECODE(REFFER.VENTILATION,");
                        strSQL.Append("  0,");
                        strSQL.Append("    'AIR COOLED SYSTEM',");
                        strSQL.Append(" 1,");
                        strSQL.Append("           'WATER COOLED SYSTEM') VENTILATION,");
                        strSQL.Append("   JOB.JOB_CARD_TRN_PK AS HBL_EXP_TBL_PK,");

                        strSQL.Append("         CASE WHEN JOB.CARGO_TYPE=1 THEN ");
                        strSQL.Append("      CTMT.CONTAINER_TYPE_MST_ID ");
                        strSQL.Append("       WHEN JOB.CARGO_TYPE=4 THEN ");
                        strSQL.Append("         CMT.COMMODITY_NAME ");
                        strSQL.Append("    Else ");
                        strSQL.Append("      (SELECT REPLACE(ROWTOCOL('SELECT DISTINCT CMD.COMMODITY_NAME || '';'' FROM JOB_TRN_CONT JOB_CONT, ");
                        strSQL.Append("         JOB_TRN_COMMODITY JCD,COMMODITY_MST_TBL CMD WHERE JOB_CONT.JOB_TRN_CONT_PK = JCD.JOB_TRN_CONT_FK ");
                        strSQL.Append("            AND JCD.COMMODITY_MST_FK = CMD.COMMODITY_MST_PK AND JOB_CONT.JOB_TRN_CONT_PK = ' ||");
                        strSQL.Append("                    JTC.JOB_TRN_CONT_PK),");
                        strSQL.Append("        ';,',");
                        strSQL.Append("     ';')");
                        strSQL.Append("     FROM DUAL)");
                        strSQL.Append("       END CONTAINER_TYPE_MST_ID");

                        // strSQL.Append("  CTMT.CONTAINER_TYPE_MST_ID")
                        strSQL.Append("  FROM JOB_TRN_SPL_REQ    REFFER,");
                        strSQL.Append("    JOB_CARD_TRN           JOB,");

                        strSQL.Append("  CONTAINER_TYPE_MST_TBL CTMT,");
                        strSQL.Append("  HBL_EXP_TBL            HBL,");
                        strSQL.Append("  JOB_TRN_CONT  JTC ,");
                        strSQL.Append("  COMMODITY_MST_TBL CMT ");
                        strSQL.Append("  WHERE JOB.JOB_CARD_TRN_PK IN (" + jobpk + ")");
                        strSQL.Append("  AND REFFER.JOB_TRN_CONT_FK=JTC.JOB_TRN_CONT_PK ");
                        strSQL.Append("  AND JTC.JOB_CARD_TRN_FK=JOB.JOB_CARD_TRN_PK ");
                        //  strSQL.Append("  AND REFFER.BOOKING_TRN_FK = LCLFCL.BOOKING_TRN_PK ")
                        strSQL.Append("    AND JTC.CONTAINER_TYPE_MST_FK = CTMT.CONTAINER_TYPE_MST_PK(+) ");
                        strSQL.Append("  AND HBL.JOB_CARD_SEA_EXP_FK = JOB.JOB_CARD_TRN_PK(+)");
                        strSQL.Append(" AND  JTC.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+) ");
                        strSQL.Append(" ORDER BY  CTMT.PREFERENCES) Q ");
                    }
                    else
                    {
                        strSQL.Append("   SELECT Q.HBL_EXP_TBL_PK,");
                        strSQL.Append("     CONTAINER_TYPE_MST_ID,");
                        strSQL.Append("    Q.MIN_TEMP AS MINTEMP,");
                        strSQL.Append("    to_char(Q.MIN_TEMP_UOM) as MIN_TEMP_UOM,");
                        strSQL.Append("    Q.MAX_TEMP AS MAXTEMP,");
                        strSQL.Append("     to_char(Q.MAX_TEMP_UOM) as MAX_TEMP_UOM,");
                        strSQL.Append("                  Q.VENTILATION");
                        strSQL.Append("    FROM (SELECT REFFER.MIN_TEMP,");
                        strSQL.Append("    REFFER.MAX_TEMP, ");
                        strSQL.Append("    REFFER.MIN_TEMP_UOM, ");
                        strSQL.Append("    REFFER.MAX_TEMP_UOM, ");
                        strSQL.Append("  DECODE(REFFER.VENTILATION, ");
                        strSQL.Append("   0,");
                        strSQL.Append("  'AIR COOLED SYSTEM',");
                        strSQL.Append(" 1,");
                        strSQL.Append("  'WATER COOLED SYSTEM') VENTILATION,");

                        strSQL.Append("         CASE  ");
                        strSQL.Append("       WHEN JOB.CARGO_TYPE=4 THEN ");
                        strSQL.Append("         CMT.COMMODITY_NAME ");
                        strSQL.Append("    Else ");
                        strSQL.Append("      (SELECT REPLACE(ROWTOCOL('SELECT DISTINCT CMD.COMMODITY_NAME || '';'' FROM JOB_TRN_CONT JOB_CONT, ");
                        strSQL.Append("         JOB_TRN_COMMODITY JCD,COMMODITY_MST_TBL CMD WHERE JOB_CONT.JOB_TRN_CONT_PK = JCD.JOB_TRN_CONT_FK ");
                        strSQL.Append("            AND JCD.COMMODITY_MST_FK = CMD.COMMODITY_MST_PK AND JOB_CONT.JOB_TRN_CONT_PK = ' ||");
                        strSQL.Append("                    jc.JOB_TRN_CONT_PK),");
                        strSQL.Append("        ';,',");
                        strSQL.Append("     ';')");
                        strSQL.Append("     FROM DUAL)");
                        strSQL.Append("       END CONTAINER_TYPE_MST_ID ,");

                        strSQL.Append("   JOB.JOB_CARD_TRN_PK AS HBL_EXP_TBL_PK ");
                        strSQL.Append("  FROM JOB_TRN_SPL_REQ    REFFER,");
                        strSQL.Append("          JOB_CARD_TRN JOB, ");
                        strSQL.Append("  JOB_TRN_CONT jc,");
                        strSQL.Append("   COMMODITY_MST_TBL CMT ");
                        strSQL.Append("  WHERE JOB.JOB_CARD_TRN_PK IN (" + jobpk + ") ");
                        strSQL.Append("  AND JOB.Job_Card_Trn_Pk = JC.JOB_CARD_TRN_FK(+)");
                        strSQL.Append("   AND jc.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK(+)");
                        strSQL.Append("   and jc.job_trn_cont_pk=reffer.job_trn_cont_fk )Q");
                    }

                    return objWF.GetDataSet(strSQL.ToString());
                }
                else
                {
                    DataSet MainDS = new DataSet();
                    objWF.OpenConnection();
                    objWF.MyDataAdapter = new OracleDataAdapter();
                    var _with4 = objWF.MyDataAdapter;
                    _with4.SelectCommand = new OracleCommand();
                    _with4.SelectCommand.Connection = objWF.MyConnection;
                    _with4.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_SEA_IMPORT_REFEER";
                    _with4.SelectCommand.CommandType = CommandType.StoredProcedure;
                    _with4.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = jobpk;
                    _with4.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = Biztype;
                    _with4.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                    _with4.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with4.Fill(MainDS);
                    return MainDS;
                }
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

        /// <summary>
        /// Fetches the odc details.
        /// </summary>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="JobPK">The job pk.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchODCDetails(Int32 Biztype, string JobPK, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();

            try
            {
                if (Process == "EXPORT")
                {
                    strSQL.Append("SELECT LENGTH, DLE.DIMENTION_ID LENGTH_UOM, HEIGHT, DHE.DIMENTION_ID HEIGHT_UOM, ");
                    strSQL.Append("WIDTH, DWI.DIMENTION_ID WIDTH_UOM, WEIGHT, DWE.DIMENTION_ID WEIGHT_UOM, ");
                    strSQL.Append("VOLUME, DVO.DIMENTION_ID VOLUME_UOM, SLOT_LOSS, LOSS_QUANTITY, APPR_REQ ");
                    strSQL.Append("FROM BOOKING_TRN_SPL_REQ SR ");
                    strSQL.Append("INNER JOIN BOOKING_TRN BT ON BT.BOOKING_TRN_PK = SR.BOOKING_TRN_FK  ");
                    strSQL.Append("INNER JOIN BOOKING_MST_TBL BM ON BM.BOOKING_MST_PK = BT.BOOKING_MST_FK ");
                    strSQL.Append("INNER JOIN JOB_CARD_TRN JOB ON JOB.BOOKING_MST_FK = BM.BOOKING_MST_PK ");
                    strSQL.Append("INNER JOIN DIMENTION_UNIT_MST_TBL DLE ON SR.LENGTH_UOM_MST_FK = DLE.DIMENTION_UNIT_MST_PK ");
                    strSQL.Append("INNER JOIN DIMENTION_UNIT_MST_TBL DHE ON SR.HEIGHT_UOM_MST_FK = DHE.DIMENTION_UNIT_MST_PK ");
                    strSQL.Append("INNER JOIN DIMENTION_UNIT_MST_TBL DWI ON SR.WIDTH_UOM_MST_FK = DWI.DIMENTION_UNIT_MST_PK ");
                    strSQL.Append("INNER JOIN DIMENTION_UNIT_MST_TBL DWE ON SR.WEIGHT_UOM_MST_FK = DWE.DIMENTION_UNIT_MST_PK ");
                    strSQL.Append("INNER JOIN DIMENTION_UNIT_MST_TBL DVO ON SR.VOLUME_UOM_MST_FK = DVO.DIMENTION_UNIT_MST_PK ");
                    strSQL.Append("WHERE JOB.JOB_CARD_TRN_PK IN (" + JobPK + ")");
                    strSQL.Append(" AND JOB.BUSINESS_TYPE = 1 ");
                    return objWF.GetDataSet(strSQL.ToString());
                }
                else
                {
                    DataSet MainDS = new DataSet();
                    objWF.OpenConnection();
                    objWF.MyDataAdapter = new OracleDataAdapter();
                    var _with5 = objWF.MyDataAdapter;
                    _with5.SelectCommand = new OracleCommand();
                    _with5.SelectCommand.Connection = objWF.MyConnection;
                    _with5.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_SEA_IMPORT_ODC";
                    _with5.SelectCommand.CommandType = CommandType.StoredProcedure;
                    _with5.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = JobPK;
                    _with5.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = Biztype;
                    _with5.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                    _with5.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                    _with5.Fill(MainDS);
                    return MainDS;
                }
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

        /// <summary>
        /// Fetches the seacargo report details MJC.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchSeacargoReportDetailsMJC(string MBLPK = "0", string JOBPK = "0", long CommodityType = 0, int Biztype = 1, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet MainDS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with6 = objWF.MyDataAdapter;
                _with6.SelectCommand = new OracleCommand();
                _with6.SelectCommand.Connection = objWF.MyConnection;
                _with6.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_SEA_MJC_RPT_CARGO";
                _with6.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with6.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = JOBPK;
                _with6.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = Biztype;
                _with6.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                _with6.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with6.Fill(MainDS);
                return MainDS;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        /// <summary>
        /// Fetches the seacargo report details.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="Biztype">The biztype.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchSeacargoReportDetails(string MBLPK = "0", string JOBPK = "0", long CommodityType = 0, int Biztype = 1, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet MainDS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with7 = objWF.MyDataAdapter;
                _with7.SelectCommand = new OracleCommand();
                _with7.SelectCommand.Connection = objWF.MyConnection;
                _with7.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_SEA_IMPORT_RPT_CARGO";
                _with7.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with7.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = JOBPK;
                _with7.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = Biztype;
                _with7.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                _with7.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with7.Fill(MainDS);
                return MainDS;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }

            //Dim objWF As New WorkFlow
            //Dim strSQL As String
            //strSQL = "SELECT jse.mbl_exp_tbl_fk MBLPK," & _
            //          vbCrLf & "VVTBL.VESSEL_NAME VESSEL," & _
            //          vbCrLf & " VVTRN.VOYAGE      VOYAGE,"
            //strSQL &= vbCrLf & "JSE.JOB_CARD_SEA_EXP_PK JOBPK,"
            //strSQL &= vbCrLf & "JSE.jobcard_ref_no JOBREFNO,"
            //strSQL &= vbCrLf & "POLMST.PORT_NAME  POL,"
            //strSQL &= vbCrLf & "PODMST.PORT_NAME  POD,"
            //strSQL &= vbCrLf & "jse.hbl_exp_tbl_fk HBLPK,"
            //strSQL &= vbCrLf & "HBL.HBL_REF_NO HBLREFNO,"
            //strSQL &= vbCrLf & "HBL.HBL_DATE HBLDATE,"
            //strSQL &= vbCrLf & "SHIPPER.CUSTOMER_NAME SHIPPERNAME,"
            //strSQL &= vbCrLf & " SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADD1,"
            //strSQL &= vbCrLf & "SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,"
            //strSQL &= vbCrLf & "SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,"
            //strSQL &= vbCrLf & "SHIPPERDTLS.ADM_CITY SHIPPERCITY,"
            //strSQL &= vbCrLf & "SHIPPERDTLS.ADM_ZIP_CODE SHIPPERZIP,"
            //strSQL &= vbCrLf & "SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,"
            //strSQL &= vbCrLf & "SHIPPERDTLS.ADM_FAX_NO SHIPPERFAX,"
            //strSQL &= vbCrLf & "SHIPPERDTLS.ADM_EMAIL_ID SHIPPEREMAIL,"
            //strSQL &= vbCrLf & "SHIPPERCNT.COUNTRY_NAME SHIPPERCOUNTRY,"

            //strSQL &= vbCrLf & "CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEEZIP,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEEFAX,"
            //strSQL &= vbCrLf & "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEEEMAIL,"
            //strSQL &= vbCrLf & "CONSIGNEECNT.COUNTRY_NAME CONSIGNEECOUNTRY,"

            //strSQL &= vbCrLf & "NOTIFY1.CUSTOMER_NAME NOTIFY1NAME,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_ADDRESS_1 NOTIFY1ADD1,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_ADDRESS_2 NOTIFY1ADD2,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_ADDRESS_3 NOTIFY1ADD3,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_CITY NOTIFY1CITY,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_ZIP_CODE NOTIFY1ZIP,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_PHONE_NO_1 NOTIFY1PHONE,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_FAX_NO NOTIFY1FAX,"
            //strSQL &= vbCrLf & "NOTIFY1DTLS.ADM_EMAIL_ID NOTIFY1MAIL,"
            //strSQL &= vbCrLf & "NOTIFY1CNT.COUNTRY_NAME NOTIFY1COUNTRY,"

            //strSQL &= vbCrLf & "NOTIFY2.CUSTOMER_NAME NOTIFY2NAME,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_ADDRESS_1 NOTIFY2ADD1,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_ADDRESS_2 NOTIFY2ADD2,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_ADDRESS_3 NOTIFY2ADD3,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_CITY NOTIFY2CITY,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_ZIP_CODE NOTIFY2ZIP,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_PHONE_NO_1 NOTIFY2PHONE,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_FAX_NO NOTIFY2FAX,"
            //strSQL &= vbCrLf & "NOTIFY2DTLS.ADM_EMAIL_ID NOTIFY2MAIL,"
            //strSQL &= vbCrLf & "NOTIFY2CNT.COUNTRY_NAME NOTIFY2COUNTRY,"

            //strSQL &= vbCrLf & "jse.marks_numbers MARKS,"
            //strSQL &= vbCrLf & "jse.goods_description GOODS,"
            //strSQL &= vbCrLf & "(select sum(j.gross_weight) from job_trn_sea_exp_cont j where j.job_card_sea_exp_fk = jse.job_card_sea_exp_pk) GROSSWT ,"
            //strSQL &= vbCrLf & "(select sum(j.pack_count) from job_trn_sea_exp_cont j where j.job_card_sea_exp_fk = jse.job_card_sea_exp_pk) TOTAL_PACK_COUNT,"
            //strSQL &= vbCrLf & "(select sum(j.volume_in_cbm) from job_trn_sea_exp_cont j where j.job_card_sea_exp_fk = jse.job_card_sea_exp_pk) VOLUME,"

            //strSQL &= vbCrLf & "COLPMST.PLACE_NAME COLPLACE,"
            //strSQL &= vbCrLf & "DELPMST.PLACE_NAME DELPLACE,"
            //strSQL &= vbCrLf & "STMST.INCO_CODE TERMS,"
            //strSQL &= vbCrLf & "JSE.DEPARTURE_DATE SAILDATE,"
            //strSQL &= vbCrLf & "JTSEC.CONTAINER_NUMBER CONTAINERS,"
            //strSQL &= vbCrLf & "JTSEC.SEAL_NUMBER SEALNUMBER,"
            //strSQL &= vbCrLf & "CTMST.CONTAINER_TYPE_MST_ID CONTAINERTYPE"
            //strSQL &= vbCrLf & "FROM MBL_EXP_TBL       MBL,"
            //strSQL &= vbCrLf & "VESSEL_VOYAGE_TRN VVTRN,"
            //strSQL &= vbCrLf & "VESSEL_VOYAGE_TBL VVTBL,"
            //strSQL &= vbCrLf & "PORT_MST_TBL      POLMST,"
            //strSQL &= vbCrLf & "PORT_MST_TBL      PODMST,"
            //strSQL &= vbCrLf & "HBL_EXP_TBL HBL,"
            //strSQL &= vbCrLf & "JOB_CARD_SEA_EXP_TBL JSE,"
            //strSQL &= vbCrLf & "BOOKING_SEA_TBL BST,"
            //strSQL &= vbCrLf & "PLACE_MST_TBL COLPMST,"
            //strSQL &= vbCrLf & "PLACE_MST_TBL DELPMST,"
            //strSQL &= vbCrLf & "SHIPPING_TERMS_MST_TBL STMST,"
            //strSQL &= vbCrLf & "JOB_TRN_SEA_EXP_CONT JTSEC,"
            //strSQL &= vbCrLf & "CONTAINER_TYPE_MST_TBL CTMST,"
            //strSQL &= vbCrLf & "CUSTOMER_MST_TBL SHIPPER,"
            //strSQL &= vbCrLf & "CUSTOMER_CONTACT_DTLS SHIPPERDTLS,"
            //strSQL &= vbCrLf & "COUNTRY_MST_TBL SHIPPERCNT,"

            //strSQL &= vbCrLf & "CUSTOMER_MST_TBL CONSIGNEE,"
            //strSQL &= vbCrLf & "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,"
            //strSQL &= vbCrLf & "COUNTRY_MST_TBL CONSIGNEECNT,"

            //strSQL &= vbCrLf & "CUSTOMER_MST_TBL NOTIFY1,"
            //strSQL &= vbCrLf & "CUSTOMER_CONTACT_DTLS NOTIFY1DTLS,"
            //strSQL &= vbCrLf & "COUNTRY_MST_TBL NOTIFY1CNT,"

            //strSQL &= vbCrLf & "CUSTOMER_MST_TBL NOTIFY2,"
            //strSQL &= vbCrLf & "CUSTOMER_CONTACT_DTLS NOTIFY2DTLS,"
            //strSQL &= vbCrLf & "COUNTRY_MST_TBL NOTIFY2CNT"

            //strSQL &= vbCrLf & "WHERE JSE.BOOKING_SEA_FK = BST.BOOKING_SEA_PK"
            //strSQL &= vbCrLf & "AND JSE.JOB_CARD_SEA_EXP_PK = JTSEC.JOB_CARD_SEA_EXP_FK(+)"
            //strSQL &= vbCrLf & "AND JSE.MBL_EXP_TBL_FK = MBL.MBL_EXP_TBL_PK(+)"
            //strSQL &= vbCrLf & "AND JSE.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)"
            //strSQL &= vbCrLf & "AND VVTBL.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK"
            //strSQL &= vbCrLf & "AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)"
            //strSQL &= vbCrLf & "AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)"
            //strSQL &= vbCrLf & "AND JSE.HBL_EXP_TBL_FK = HBL.HBL_EXP_TBL_PK(+)"
            //strSQL &= vbCrLf & "AND BST.COL_PLACE_MST_FK = COLPMST.PLACE_PK(+)"
            //strSQL &= vbCrLf & "AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+)"
            //strSQL &= vbCrLf & "AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)"
            //strSQL &= vbCrLf & "AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)"
            //strSQL &= vbCrLf & "AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)"
            //strSQL &= vbCrLf & "AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)"
            //strSQL &= vbCrLf & "AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)"

            //strSQL &= vbCrLf & "AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)"
            //strSQL &= vbCrLf & "AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)"
            //strSQL &= vbCrLf & "AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)"

            //strSQL &= vbCrLf & "AND JSE.NOTIFY1_CUST_MST_FK = NOTIFY1.CUSTOMER_MST_PK(+)"
            //strSQL &= vbCrLf & "AND NOTIFY1.CUSTOMER_MST_PK = NOTIFY1DTLS.CUSTOMER_MST_FK(+)"
            //strSQL &= vbCrLf & "AND NOTIFY1DTLS.ADM_COUNTRY_MST_FK = NOTIFY1CNT.COUNTRY_MST_PK(+)"

            //strSQL &= vbCrLf & "AND JSE.NOTIFY2_CUST_MST_FK = NOTIFY2.CUSTOMER_MST_PK(+)"
            //strSQL &= vbCrLf & "AND NOTIFY2.CUSTOMER_MST_PK = NOTIFY2DTLS.CUSTOMER_MST_FK(+)"
            //strSQL &= vbCrLf & "AND NOTIFY2DTLS.ADM_COUNTRY_MST_FK = NOTIFY2CNT.COUNTRY_MST_PK(+)"
            //strSQL &= vbCrLf & "AND  JSE.JOB_CARD_SEA_EXP_PK IN (" & JOBPK & ")"

            //Try
            //    Return objWF.GetDataSet(strSQL)
            //Catch oraExp As Exception
            //    ErrorMessage = oraExp.Message
            //    Throw oraExp
            //Catch ex As Exception
            //    Throw ex
            //End Try
        }

        /// <summary>
        /// Fetches the container details.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetails(string MBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT MBL.MBL_EXP_TBL_PK MBLPK,";
            strSQL += " JTSEC.CONTAINER_NUMBER CONTAINER,";
            strSQL += " JTSEC.SEAL_NUMBER SEALNUMBER";
            strSQL += " FROM MBL_EXP_TBL  MBL,";
            strSQL += " JOB_CARD_TRN JSE,";
            strSQL += " JOB_TRN_CONT JTSEC";
            strSQL += " WHERE JSE.MBL_MAWB_FK(+) = MBL.MBL_EXP_TBL_PK";
            strSQL += " AND JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK(+)";
            strSQL += " AND MBL.MBL_EXP_TBL_PK IN (" + MBLPK + ")";
            strSQL += " AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ";
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the freight details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchFreightDetails(string JOBPK, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet MainDS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with8 = objWF.MyDataAdapter;
                _with8.SelectCommand = new OracleCommand();
                _with8.SelectCommand.Connection = objWF.MyConnection;
                _with8.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_SEA_IMPORT_FREIGHT";
                _with8.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with8.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = JOBPK;
                _with8.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                _with8.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with8.Fill(MainDS);
                return MainDS;
            }
            catch (OracleException ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        /// <summary>
        /// Fetches the commodity details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchCommodityDetails(string JOBPK, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet MainDS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with9 = objWF.MyDataAdapter;
                _with9.SelectCommand = new OracleCommand();
                _with9.SelectCommand.Connection = objWF.MyConnection;
                _with9.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_SEA_COMMODITY";
                _with9.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with9.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = JOBPK;
                _with9.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                _with9.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with9.Fill(MainDS);
                return MainDS;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        /// <summary>
        /// Fetches the aircargo report details.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchAircargoReportDetails(string MBLPK = "0", string JOBPK = "0", long CommodityType = 0, string Process = "EXPORT")
        {
            //Dim objWF As New WorkFlow
            //Dim strSQL As String
            //strSQL = "SELECT MAWB.MAWB_EXP_TBL_PK MBLPK,"
            //strSQL &= vbCrLf & "MAWB.MAWB_REF_NO MBLREFNO, "
            //strSQL &= vbCrLf & "HAWB.HAWB_EXP_TBL_PK HBLPK,"
            //strSQL &= vbCrLf & "JOB.JOB_CARD_AIR_EXP_PK JOBPK,"
            //strSQL &= vbCrLf & "JOB.jobcard_ref_no JOBREFNO,"
            //strSQL &= vbCrLf & "HAWB.HAWB_REF_NO HBLREFNO,"
            //strSQL &= vbCrLf & "OMST.OPERATOR_NAME OPERATOR,"
            //strSQL &= vbCrLf & "HAWB.FLIGHT_NO VESSEL,"
            //strSQL &= vbCrLf & "'' VOYAGE,"
            //strSQL &= vbCrLf & "POL.PORT_NAME POLNAME,"
            //strSQL &= vbCrLf & "POD.PORT_NAME PODNAME,"
            //strSQL &= vbCrLf & "MAWB.MAWB_DATE MBLDATE,"
            //strSQL &= vbCrLf & "MAWB.TOTAL_PACK_COUNT MBLPIECES,"
            //strSQL &= vbCrLf & "HAWB.TOTAL_PACK_COUNT HBLPIECES,"
            //strSQL &= vbCrLf & "MAWB.TOTAL_GROSS_WEIGHT MBLGROSSWT,"
            //strSQL &= vbCrLf & "HAWB.TOTAL_GROSS_WEIGHT HBLGROSSWT,"
            //strSQL &= vbCrLf & "MAWB.SHIPPER_NAME MBLSHIPNAME,"
            //strSQL &= vbCrLf & "MAWB.SHIPPER_ADDRESS MBLSHIPADDRESS,"
            //strSQL &= vbCrLf & "MAWB.CONSIGNEE_NAME MBLCONSIGNEENAME,"
            //strSQL &= vbCrLf & "MAWB.CONSIGNEE_ADDRESS MBLCONSIGADDRESS,"
            //strSQL &= vbCrLf & "SHIPPER.CUSTOMER_NAME HBLSHIPPER,"
            //strSQL &= vbCrLf & "SDTLS.ADM_ADDRESS_1 SHIPADD1,"
            //strSQL &= vbCrLf & "SDTLS.ADM_ADDRESS_2 SHIPADD2,"
            //strSQL &= vbCrLf & "SDTLS.ADM_ADDRESS_3 SHIPADD3,"
            //strSQL &= vbCrLf & "SDTLS.ADM_CITY SHIPCITY,"
            //strSQL &= vbCrLf & "SDTLS.ADM_ZIP_CODE SHIPZIP,"
            //strSQL &= vbCrLf & "SDTLS.ADM_PHONE_NO_1 SHIPPHONE,"
            //strSQL &= vbCrLf & "CONSIGNEE.CUSTOMER_NAME HBLCONSIGNEE,"
            //strSQL &= vbCrLf & "CDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,"
            //strSQL &= vbCrLf & "CDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,"
            //strSQL &= vbCrLf & "CDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,"
            //strSQL &= vbCrLf & "CDTLS.ADM_CITY CONSIGNEECITY,"
            //strSQL &= vbCrLf & "CDTLS.ADM_ZIP_CODE CONSIGNEEZIP,"
            //strSQL &= vbCrLf & "CDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,"
            //strSQL &= vbCrLf & "MAWB.AGENT_NAME MBLAGENT,"
            //strSQL &= vbCrLf & "MAWB.AGENT_ADDRESS MBLAGENTADDRESS,"
            //strSQL &= vbCrLf & "MAWB.GOODS_DESCRIPTION MBLGOODS,"
            //strSQL &= vbCrLf & "HAWB.GOODS_DESCRIPTION HBLGOODS, CGT.COMMODITY_GROUP_PK"
            //strSQL &= vbCrLf & "FROM HAWB_EXP_TBL HAWB,"
            //strSQL &= vbCrLf & "JOB_CARD_AIR_EXP_TBL JOB,"
            //strSQL &= vbCrLf & "BOOKING_AIR_TBL BKG,"
            //strSQL &= vbCrLf & "MAWB_EXP_TBL MAWB,"
            //strSQL &= vbCrLf & "OPERATOR_MST_TBL OMST,"
            //strSQL &= vbCrLf & "PORT_MST_TBL POL,"
            //strSQL &= vbCrLf & "PORT_MST_TBL POD,"
            //strSQL &= vbCrLf & "CUSTOMER_MST_TBL SHIPPER,"
            //strSQL &= vbCrLf & "CUSTOMER_MST_TBL CONSIGNEE,"
            //strSQL &= vbCrLf & "CUSTOMER_CONTACT_DTLS SDTLS,"
            //strSQL &= vbCrLf & "CUSTOMER_CONTACT_DTLS CDTLS, COMMODITY_GROUP_MST_TBL CGT"
            //strSQL &= vbCrLf & "WHERE JOB.BOOKING_AIR_FK = BKG.BOOKING_AIR_PK"
            //strSQL &= vbCrLf & "AND JOB.HAWB_EXP_TBL_FK = HAWB.HAWB_EXP_TBL_PK(+)"
            //strSQL &= vbCrLf & "AND JOB.MAWB_EXP_TBL_FK = MAWB.MAWB_EXP_TBL_PK(+)"
            //strSQL &= vbCrLf & "AND BKG.AIRLINE_MST_FK = OMST.OPERATOR_MST_PK(+)"
            //strSQL &= vbCrLf & "AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK(+)"
            //strSQL &= vbCrLf & "AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK(+)"
            //strSQL &= vbCrLf & "AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)"
            //strSQL &= vbCrLf & "AND JOB.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)"
            //strSQL &= vbCrLf & "AND SDTLS.CUSTOMER_MST_FK(+)=SHIPPER.CUSTOMER_MST_PK"
            //strSQL &= vbCrLf & "AND CDTLS.CUSTOMER_MST_FK(+)=CONSIGNEE.CUSTOMER_MST_PK "
            //strSQL &= vbCrLf & "AND CGT.COMMODITY_GROUP_PK = BKG.COMMODITY_GROUP_FK "
            //strSQL &= vbCrLf & " AND JOB.JOB_CARD_AIR_EXP_PK IN (" & JOBPK & ")"

            //Try
            //    Return objWF.GetDataSet(strSQL)
            //Catch oraExp As Exception
            //    ErrorMessage = oraExp.Message
            //    Throw oraExp
            //Catch ex As Exception
            //    Throw ex
            //End Try
            WorkFlow objWF = new WorkFlow();
            DataSet MainDS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with10 = objWF.MyDataAdapter;
                _with10.SelectCommand = new OracleCommand();
                _with10.SelectCommand.Connection = objWF.MyConnection;
                _with10.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_AIR_IMPORT_RPT_CARGO";
                _with10.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with10.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = JOBPK;
                _with10.SelectCommand.Parameters.Add("BIZ_TYPE_IN", OracleDbType.Int32).Value = 1;
                _with10.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                _with10.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with10.Fill(MainDS);
                return MainDS;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        /// <summary>
        /// Fetches the air freight details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public DataSet FetchAirFreightDetails(string JOBPK, string Process = "EXPORT")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet MainDS = new DataSet();
            try
            {
                objWF.OpenConnection();
                objWF.MyDataAdapter = new OracleDataAdapter();
                var _with11 = objWF.MyDataAdapter;
                _with11.SelectCommand = new OracleCommand();
                _with11.SelectCommand.Connection = objWF.MyConnection;
                _with11.SelectCommand.CommandText = objWF.MyUserName + ".IMPORT_CARGO_MANIFEST_PKG.FETCH_AIR_IMPORT_FREIGHT";
                _with11.SelectCommand.CommandType = CommandType.StoredProcedure;
                _with11.SelectCommand.Parameters.Add("JOB_PK_IN", OracleDbType.Varchar2).Value = JOBPK;
                _with11.SelectCommand.Parameters.Add("PROCESS_TYPE_IN", OracleDbType.Varchar2).Value = Process;
                _with11.SelectCommand.Parameters.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                _with11.Fill(MainDS);
                return MainDS;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        /// <summary>
        /// Fetches the haz seacargo report details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="Loc">The loc.</param>
        /// <returns></returns>
        public DataSet FetchHazSeacargoReportDetails(string JOBPK = "0", long Loc = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT jse.mbl_mawb_fk MBLPK," + "mbl.mbl_ref_no MBLNO," + "VVTBL.VESSEL_NAME VESSEL," + " VVTRN.VOYAGE      VOYAGE,";
            strSQL += "JSE.JOB_CARD_TRN_PK JOBPK,";
            strSQL += "JSE.jobcard_ref_no JOBREFNO,";
            strSQL += "POLMST.PORT_NAME  POL,";
            strSQL += "PODMST.PORT_NAME  POD,";
            strSQL += "jse.hbl_hawb_fk HBLPK,";
            strSQL += "HBL.HBL_REF_NO HBLNO,";
            strSQL += "HBL.HBL_DATE HBLDATE,";
            strSQL += "SHIPPER.CUSTOMER_NAME SHIPPERNAME,";
            strSQL += " SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
            strSQL += "SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            strSQL += "SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            strSQL += "SHIPPERDTLS.ADM_CITY SHIPPERCITY,";
            strSQL += "SHIPPERDTLS.ADM_ZIP_CODE SHIPPERZIP,";
            strSQL += "SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            strSQL += "SHIPPERDTLS.ADM_FAX_NO SHIPPERFAX,";
            strSQL += "SHIPPERDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
            strSQL += "SHIPPERCNT.COUNTRY_NAME SHIPPERCOUNTRY,";

            strSQL += "CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
            strSQL += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,";
            strSQL += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,";
            strSQL += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,";
            strSQL += "CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,";
            strSQL += "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEEZIP,";
            strSQL += "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,";
            strSQL += "CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEEFAX,";
            strSQL += "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEEEMAIL,";
            strSQL += "CONSIGNEECNT.COUNTRY_NAME CONSIGNEECOUNTRY,";

            strSQL += "NOTIFY1.CUSTOMER_NAME NOTIFY1NAME,";
            strSQL += "NOTIFY1DTLS.ADM_ADDRESS_1 NOTIFY1ADD1,";
            strSQL += "NOTIFY1DTLS.ADM_ADDRESS_2 NOTIFY1ADD2,";
            strSQL += "NOTIFY1DTLS.ADM_ADDRESS_3 NOTIFY1ADD3,";
            strSQL += "NOTIFY1DTLS.ADM_CITY NOTIFY1CITY,";
            strSQL += "NOTIFY1DTLS.ADM_ZIP_CODE NOTIFY1ZIP,";
            strSQL += "NOTIFY1DTLS.ADM_PHONE_NO_1 NOTIFY1PHONE,";
            strSQL += "NOTIFY1DTLS.ADM_FAX_NO NOTIFY1FAX,";
            strSQL += "NOTIFY1DTLS.ADM_EMAIL_ID NOTIFY1MAIL,";
            strSQL += "NOTIFY1CNT.COUNTRY_NAME NOTIFY1COUNTRY,";

            strSQL += "NOTIFY2.CUSTOMER_NAME NOTIFY2NAME,";
            strSQL += "NOTIFY2DTLS.ADM_ADDRESS_1 NOTIFY2ADD1,";
            strSQL += "NOTIFY2DTLS.ADM_ADDRESS_2 NOTIFY2ADD2,";
            strSQL += "NOTIFY2DTLS.ADM_ADDRESS_3 NOTIFY2ADD3,";
            strSQL += "NOTIFY2DTLS.ADM_CITY NOTIFY2CITY,";
            strSQL += "NOTIFY2DTLS.ADM_ZIP_CODE NOTIFY2ZIP,";
            strSQL += "NOTIFY2DTLS.ADM_PHONE_NO_1 NOTIFY2PHONE,";
            strSQL += "NOTIFY2DTLS.ADM_FAX_NO NOTIFY2FAX,";
            strSQL += "NOTIFY2DTLS.ADM_EMAIL_ID NOTIFY2MAIL,";
            strSQL += "NOTIFY2CNT.COUNTRY_NAME NOTIFY2COUNTRY,";

            strSQL += "jse.marks_numbers MARKS,";
            strSQL += "jse.goods_description GOODS,";
            strSQL += "(select sum(j.gross_weight) from job_trn_cont j where j.job_card_trn_fk = jse.job_card_trn_pk) GROSSWTKGS ,";
            strSQL += "(select sum(j.volume_in_cbm) from job_trn_cont j where j.job_card_trn_fk = jse.job_card_trn_pk) MEASUREMENT,";

            strSQL += "decode(JSE.PYMT_TYPE,1,'PrePaid',2,'Collect') CHARGETYPE,";
            strSQL += "(SELECT SUM(JFD.FREIGHT_AMT*JFD.EXCHANGE_RATE) FROM JOB_TRN_FD JFD WHERE JFD.JOB_CARD_TRN_FK=JSE.JOB_CARD_TRN_PK AND JFD.FREIGHT_TYPE=1) PREPAIDAMOUNT,";
            strSQL += "(SELECT SUM(JFD.FREIGHT_AMT*JFD.EXCHANGE_RATE) FROM JOB_TRN_FD JFD WHERE JFD.JOB_CARD_TRN_FK=JSE.JOB_CARD_TRN_PK AND JFD.FREIGHT_TYPE=2) COLLECTAMOUNT,";
            strSQL += "CUMT.CURRENCY_ID CURRENCY,";

            strSQL += "COLPMST.PLACE_NAME COLPLACE,";
            strSQL += "DELPMST.PLACE_NAME DELPLACE,";
            strSQL += "STMST.INCO_CODE TERMS,";
            strSQL += "JSE.ARRIVAL_DATE ARRIVALDATE,";
            strSQL += "(select sysdate from dual) reptdatetime,";
            strSQL += "l.location_name Location,";
            strSQL += "loc.location_type_desc locationtype,";
            strSQL += "comm.commodity_group_code CommodityType,";
            strSQL += "ploc.location_name pol_location,";
            strSQL += "Dloc.location_name pod_location";

            strSQL += "FROM MBL_EXP_TBL       MBL,";
            strSQL += "VESSEL_VOYAGE_TRN VVTRN,";
            strSQL += "VESSEL_VOYAGE_TBL VVTBL,";
            strSQL += "PORT_MST_TBL      POLMST,";
            strSQL += "PORT_MST_TBL      PODMST,";
            strSQL += "HBL_EXP_TBL HBL,";
            strSQL += "JOB_CARD_TRN JSE,";
            strSQL += "BOOKING_MST_TBL BST,";
            strSQL += "PLACE_MST_TBL COLPMST,";
            strSQL += "PLACE_MST_TBL DELPMST,";
            strSQL += "SHIPPING_TERMS_MST_TBL STMST,";
            strSQL += "CUSTOMER_MST_TBL SHIPPER,";
            strSQL += "CUSTOMER_CONTACT_DTLS SHIPPERDTLS,";
            strSQL += "COUNTRY_MST_TBL SHIPPERCNT,";

            strSQL += "CUSTOMER_MST_TBL CONSIGNEE,";
            strSQL += "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            strSQL += "COUNTRY_MST_TBL CONSIGNEECNT,";

            strSQL += "CUSTOMER_MST_TBL NOTIFY1,";
            strSQL += "CUSTOMER_CONTACT_DTLS NOTIFY1DTLS,";
            strSQL += "COUNTRY_MST_TBL NOTIFY1CNT,";

            strSQL += "CUSTOMER_MST_TBL NOTIFY2,";
            strSQL += "CUSTOMER_CONTACT_DTLS NOTIFY2DTLS,";
            strSQL += "COUNTRY_MST_TBL NOTIFY2CNT,";

            strSQL += "LOCATION_MST_TBL L,";
            strSQL += "CORPORATE_MST_TBL CMT,";
            strSQL += "CURRENCY_TYPE_MST_TBL CUMT,";
            strSQL += "COMMODITY_GROUP_MST_TBL COMM,";
            strSQL += "location_type_mst_tbl loc,";
            strSQL += "LOCATION_MST_TBL Ploc,";
            strSQL += "LOCATION_MST_TBL Dloc";

            strSQL += "WHERE JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK";
            strSQL += "AND JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)";
            strSQL += "AND JSE.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)";
            strSQL += "AND VVTBL.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK";
            strSQL += "AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)";
            strSQL += "AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)";
            strSQL += "AND JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)";
            strSQL += "AND BST.COL_PLACE_MST_FK = COLPMST.PLACE_PK(+)";
            strSQL += "AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+)";
            strSQL += "AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)";
            strSQL += "AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
            strSQL += "AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)";

            strSQL += "AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
            strSQL += "AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)";

            strSQL += "AND JSE.NOTIFY1_CUST_MST_FK = NOTIFY1.CUSTOMER_MST_PK(+)";
            strSQL += "AND NOTIFY1.CUSTOMER_MST_PK = NOTIFY1DTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND NOTIFY1DTLS.ADM_COUNTRY_MST_FK = NOTIFY1CNT.COUNTRY_MST_PK(+)";

            strSQL += "AND JSE.NOTIFY2_CUST_MST_FK = NOTIFY2.CUSTOMER_MST_PK(+)";
            strSQL += "AND NOTIFY2.CUSTOMER_MST_PK = NOTIFY2DTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND NOTIFY2DTLS.ADM_COUNTRY_MST_FK = NOTIFY2CNT.COUNTRY_MST_PK(+)";
            strSQL += "and CMT.CURRENCY_MST_FK = CUMT.CURRENCY_MST_PK";
            strSQL += "AND JSE.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            strSQL += "and l.location_type_fk=loc.location_type_mst_pk";
            strSQL += "and polmst.location_mst_fk=ploc.location_mst_pk";
            strSQL += "and podmst.location_mst_fk=Dloc.location_mst_pk";
            strSQL += " and JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ";
            strSQL += "and L.LOCATION_MST_PK = " + Loc;
            strSQL += "AND  JSE.JOB_CARD_TRN_PK IN (" + JOBPK + ")";

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraExp)
            {
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the haz container details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchHazContainerDetails(string JOBPK, long CommodityType)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT job.job_card_trn_pk JOBPK,JTSEC.CONTAINER_NUMBER CONTAINERNO,";
            strSQL += "CTMST.CONTAINER_TYPE_MST_ID CONTAINERTYPE,";
            strSQL += "JTSEC.SEAL_NUMBER SEALNUMBER,";
            strSQL += "JTSEC.gross_weight  WTKGS,";
            strSQL += "CTMST.CONTAINER_TAREWEIGHT_TONE TAREWT,";
            strSQL += "comm.commodity_group_code COMMODITYTYPE,";
            strSQL += "jsreq.imdg_class_code F1,";
            strSQL += "jsreq.un_no F2,";
            strSQL += "jsreq.flash_pnt_temp+jsreq.flash_pnt_temp_uom F3";
            strSQL += "from job_trn_cont jtsec,";
            strSQL += "CONTAINER_TYPE_MST_TBL CTMST,";
            strSQL += "job_trn_spl_req jsreq,";
            strSQL += "COMMODITY_GROUP_MST_TBL COMM,";
            strSQL += "job_card_trn job";
            strSQL += "where jtsec.container_type_mst_fk=ctmst.container_type_mst_pk";
            strSQL += "and jsreq.job_trn_cont_fk(+)=jtsec.job_trn_cont_pk";
            strSQL += "AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            strSQL += "and jtsec.job_card_trn_fk = job.job_card_trn_pk";
            strSQL += " and job.BUSINESS_TYPE = 2 AND job.PROCESS_TYPE = 1 ";
            strSQL += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            strSQL += "and jtsec.job_card_trn_fk IN (" + JOBPK + ")";

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the reefer container details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchReeferContainerDetails(string JOBPK, long CommodityType)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT jtsec.job_card_trn_fk jobpk ,JTSEC.CONTAINER_NUMBER CONTAINERNO,";
            strSQL += "CTMST.CONTAINER_TYPE_NAME CONTAINERTYPE,";
            strSQL += "JTSEC.SEAL_NUMBER SEALNUMBER,";
            strSQL += "JTSEC.gross_weight  WTKGS,";
            strSQL += "CTMST.CONTAINER_TAREWEIGHT_TONE TAREWT,";
            strSQL += "comm.commodity_group_code COMMODITYTYPE,";
            strSQL += "jsreq.min_temp_uom F1,";
            strSQL += "jsreq.max_temp_uom F2,";
            strSQL += "'' as F3";
            strSQL += "from job_trn_cont jtsec,";
            strSQL += "CONTAINER_TYPE_MST_TBL CTMST,";
            strSQL += "job_trn_spl_req jsreq,";
            strSQL += "COMMODITY_GROUP_MST_TBL COMM,";
            strSQL += "job_card_trn job";
            strSQL += "where jtsec.container_type_mst_fk=ctmst.container_type_mst_pk";
            strSQL += "and jsreq.job_trn_cont_fk=jtsec.job_trn_cont_pk";
            strSQL += "AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            strSQL += "and jtsec.job_card_trn_fk = job.job_card_trn_pk";
            strSQL += " and job.BUSINESS_TYPE = 2 AND job.PROCESS_TYPE = 1 ";
            strSQL += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            strSQL += "and jtsec.job_card_trn_fk IN (" + JOBPK + ")";

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the odc container details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchODCContainerDetails(string JOBPK, long CommodityType)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT jtsec.job_card_trn_fk jobpk ,JTSEC.CONTAINER_NUMBER CONTAINERNO,";
            strSQL += "CTMST.CONTAINER_TYPE_NAME CONTAINERTYPE,";
            strSQL += "JTSEC.SEAL_NUMBER SEALNUMBER,";
            strSQL += "JTSEC.gross_weight  WTKGS,";
            strSQL += "CTMST.CONTAINER_TAREWEIGHT_TONE TAREWT,";
            strSQL += "comm.commodity_group_code COMMODITYTYPE,";
            strSQL += "jsreq.height F1,";
            strSQL += "jsreq.length F2,";
            strSQL += "jsreq.width F3";
            strSQL += "from job_trn_cont jtsec,";
            strSQL += "CONTAINER_TYPE_MST_TBL CTMST,";
            strSQL += "job_trn_spl_req jsreq,";
            strSQL += "COMMODITY_GROUP_MST_TBL COMM,";
            strSQL += "job_card_trn job";
            strSQL += "where jtsec.container_type_mst_fk=ctmst.container_type_mst_pk";
            strSQL += "and jsreq.job_trn_cont_fk=jtsec.job_trn_cont_pk";
            strSQL += "AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            strSQL += "and jtsec.job_card_trn_fk = job.job_card_trn_pk";
            strSQL += " and job.BUSINESS_TYPE = 2 AND job.PROCESS_TYPE = 1 ";
            strSQL += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            strSQL += "and jtsec.job_card_trn_fk IN (" + JOBPK + ")";
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the noofhawb.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchNOOFHAWB(string MBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT HBL.MBL_EXP_TBL_FK MBLPK ,COUNT(*) COUNT";
            strSQL += "FROM HAWB_EXP_TBL HAWB";
            strSQL += "WHERE HAWB.MAWB_EXP_TBL_FK IN (" + MBLPK + ")";
            strSQL += "GROUP BY HBL.MBL_EXP_TBL_FK";
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the noofhbls.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchNOOFHBLS(string MBLPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT HBL.MBL_EXP_TBL_FK MBLPK ,COUNT(*) COUNT";
            strSQL += "FROM HBL_EXP_TBL HBL";
            strSQL += "WHERE HBL.MBL_EXP_TBL_FK IN(" + MBLPK + ")";
            strSQL += "GROUP BY HBL.MBL_EXP_TBL_FK";
            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion " Report Details"

        #region " Print function for Break bulk"

        /// <summary>
        /// Fetches the bb seacargo report details.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchBBSeacargoReportDetails(string MBLPK = "0", string JOBPK = "0", long CommodityType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;

            strSQL = "SELECT distinct jse.mbl_mawb_fk MBLPK," + "VVTBL.VESSEL_NAME VESSEL," + " VVTRN.VOYAGE      VOYAGE,";
            strSQL += "JSE.JOB_CARD_TRN_PK JOBPK,";
            strSQL += "JSE.jobcard_ref_no JOBREFNO,";
            strSQL += "POLMST.PORT_NAME  POL,";
            strSQL += "PODMST.PORT_NAME  POD,";
            strSQL += "jse.hbl_hawb_fk HBLPK,";
            strSQL += "HBL.HBL_REF_NO HBLREFNO,";
            strSQL += "HBL.HBL_DATE HBLDATE,";
            strSQL += "SHIPPER.CUSTOMER_NAME SHIPPERNAME,";
            strSQL += " SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADD1,";
            strSQL += "SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,";
            strSQL += "SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,";
            strSQL += "SHIPPERDTLS.ADM_CITY SHIPPERCITY,";
            strSQL += "SHIPPERDTLS.ADM_ZIP_CODE SHIPPERZIP,";
            strSQL += "SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,";
            strSQL += "SHIPPERDTLS.ADM_FAX_NO SHIPPERFAX,";
            strSQL += "SHIPPERDTLS.ADM_EMAIL_ID SHIPPEREMAIL,";
            strSQL += "SHIPPERCNT.COUNTRY_NAME SHIPPERCOUNTRY,";

            strSQL += "CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,";
            strSQL += "CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,";
            strSQL += "CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,";
            strSQL += "CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,";
            strSQL += "CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,";
            strSQL += "CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEEZIP,";
            strSQL += "CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,";
            strSQL += "CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEEFAX,";
            strSQL += "CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEEEMAIL,";
            strSQL += "CONSIGNEECNT.COUNTRY_NAME CONSIGNEECOUNTRY,";

            strSQL += "NOTIFY1.CUSTOMER_NAME NOTIFY1NAME,";
            strSQL += "NOTIFY1DTLS.ADM_ADDRESS_1 NOTIFY1ADD1,";
            strSQL += "NOTIFY1DTLS.ADM_ADDRESS_2 NOTIFY1ADD2,";
            strSQL += "NOTIFY1DTLS.ADM_ADDRESS_3 NOTIFY1ADD3,";
            strSQL += "NOTIFY1DTLS.ADM_CITY NOTIFY1CITY,";
            strSQL += "NOTIFY1DTLS.ADM_ZIP_CODE NOTIFY1ZIP,";
            strSQL += "NOTIFY1DTLS.ADM_PHONE_NO_1 NOTIFY1PHONE,";
            strSQL += "NOTIFY1DTLS.ADM_FAX_NO NOTIFY1FAX,";
            strSQL += "NOTIFY1DTLS.ADM_EMAIL_ID NOTIFY1MAIL,";
            strSQL += "NOTIFY1CNT.COUNTRY_NAME NOTIFY1COUNTRY,";

            strSQL += "NOTIFY2.CUSTOMER_NAME NOTIFY2NAME,";
            strSQL += "NOTIFY2DTLS.ADM_ADDRESS_1 NOTIFY2ADD1,";
            strSQL += "NOTIFY2DTLS.ADM_ADDRESS_2 NOTIFY2ADD2,";
            strSQL += "NOTIFY2DTLS.ADM_ADDRESS_3 NOTIFY2ADD3,";
            strSQL += "NOTIFY2DTLS.ADM_CITY NOTIFY2CITY,";
            strSQL += "NOTIFY2DTLS.ADM_ZIP_CODE NOTIFY2ZIP,";
            strSQL += "NOTIFY2DTLS.ADM_PHONE_NO_1 NOTIFY2PHONE,";
            strSQL += "NOTIFY2DTLS.ADM_FAX_NO NOTIFY2FAX,";
            strSQL += "NOTIFY2DTLS.ADM_EMAIL_ID NOTIFY2MAIL,";
            strSQL += "NOTIFY2CNT.COUNTRY_NAME NOTIFY2COUNTRY,";

            strSQL += "jse.marks_numbers MARKS,";
            strSQL += "jse.goods_description GOODS,";
            strSQL += "(select sum(j.chargeable_weight) from job_trn_cont j where j.job_card_trn_fk = jse.job_card_trn_pk) GROSSWT ,";
            strSQL += "(select sum(j.pack_count) from job_trn_cont j where j.job_card_trn_fk = jse.job_card_trn_pk) TOTAL_PACK_COUNT,";
            strSQL += "(select sum(j.volume_in_cbm) from job_trn_cont j where j.job_card_trn_fk = jse.job_card_trn_pk) VOLUME,";

            strSQL += "COLPMST.PLACE_NAME COLPLACE,";
            strSQL += "DELPMST.PLACE_NAME DELPLACE,";
            strSQL += "STMST.INCO_CODE TERMS,";
            strSQL += "JSE.DEPARTURE_DATE SAILDATE,";
            strSQL += "JTSEC.CONTAINER_NUMBER CONTAINERS,";
            strSQL += "JTSEC.SEAL_NUMBER SEALNUMBER,";
            strSQL += "CTMST.CONTAINER_TYPE_MST_ID CONTAINERTYPE";
            strSQL += "FROM MBL_EXP_TBL       MBL,";
            strSQL += "VESSEL_VOYAGE_TRN VVTRN,";
            strSQL += "VESSEL_VOYAGE_TBL VVTBL,";
            strSQL += "PORT_MST_TBL      POLMST,";
            strSQL += "PORT_MST_TBL      PODMST,";
            strSQL += "HBL_EXP_TBL HBL,";
            strSQL += "JOB_CARD_TRN JSE,";
            strSQL += "BOOKING_MST_TBL BST,";
            strSQL += "PLACE_MST_TBL COLPMST,";
            strSQL += "PLACE_MST_TBL DELPMST,";
            strSQL += "SHIPPING_TERMS_MST_TBL STMST,";
            strSQL += "JOB_TRN_CONT JTSEC,";
            strSQL += "CONTAINER_TYPE_MST_TBL CTMST,";
            strSQL += "CUSTOMER_MST_TBL SHIPPER,";
            strSQL += "CUSTOMER_CONTACT_DTLS SHIPPERDTLS,";
            strSQL += "COUNTRY_MST_TBL SHIPPERCNT,";

            strSQL += "CUSTOMER_MST_TBL CONSIGNEE,";
            strSQL += "CUSTOMER_CONTACT_DTLS CONSIGNEEDTLS,";
            strSQL += "COUNTRY_MST_TBL CONSIGNEECNT,";

            strSQL += "CUSTOMER_MST_TBL NOTIFY1,";
            strSQL += "CUSTOMER_CONTACT_DTLS NOTIFY1DTLS,";
            strSQL += "COUNTRY_MST_TBL NOTIFY1CNT,";

            strSQL += "CUSTOMER_MST_TBL NOTIFY2,";
            strSQL += "CUSTOMER_CONTACT_DTLS NOTIFY2DTLS,";
            strSQL += "COUNTRY_MST_TBL NOTIFY2CNT";

            strSQL += "WHERE JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK";
            strSQL += "AND JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK(+)";
            strSQL += "AND JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)";
            strSQL += "AND JSE.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)";
            strSQL += "AND VVTBL.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK";
            strSQL += "AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)";
            strSQL += "AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)";
            strSQL += "AND JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)";
            strSQL += "AND BST.COL_PLACE_MST_FK = COLPMST.PLACE_PK(+)";
            strSQL += "AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+)";
            strSQL += "AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)";
            strSQL += "AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)";
            strSQL += "AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)";
            strSQL += "AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)";

            strSQL += "AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)";
            strSQL += "AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)";

            strSQL += "AND JSE.NOTIFY1_CUST_MST_FK = NOTIFY1.CUSTOMER_MST_PK(+)";
            strSQL += "AND NOTIFY1.CUSTOMER_MST_PK = NOTIFY1DTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND NOTIFY1DTLS.ADM_COUNTRY_MST_FK = NOTIFY1CNT.COUNTRY_MST_PK(+)";

            strSQL += "AND JSE.NOTIFY2_CUST_MST_FK = NOTIFY2.CUSTOMER_MST_PK(+)";
            strSQL += "AND NOTIFY2.CUSTOMER_MST_PK = NOTIFY2DTLS.CUSTOMER_MST_FK(+)";
            strSQL += "AND NOTIFY2DTLS.ADM_COUNTRY_MST_FK = NOTIFY2CNT.COUNTRY_MST_PK(+)";
            strSQL += " and JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ";
            strSQL += "AND  JSE.JOB_CARD_TRN_PK IN (" + JOBPK + ")";

            try
            {
                return objWF.GetDataSet(strSQL);
            }
            catch (Exception oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
        }

        #endregion " Print function for Break bulk"

        #region " Fetch Grid Data"

        /// <summary>
        /// Fetches the sea cargo manifest data.
        /// </summary>
        /// <param name="VesPK">The ves pk.</param>
        /// <param name="Ves_Flight">The ves_ flight.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="MBLPk">The MBL pk.</param>
        /// <param name="HBLPk">The HBL pk.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="nLocationFk">The n location fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchSeaCargoManifestData(long VesPK = 0, string Ves_Flight = "", string Voyage = "", string POL = "", long MBLPk = 0, long HBLPk = 0, string POD = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long CommodityType = 0,
        int nLocationFk = 0, Int32 flag = 0, string Customer = "", string Consignee = "", string DPAgent = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (Ves_Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND VVTBL.VESSEL_NAME = '" + Ves_Flight + "'";
            }
            if (Voyage.Trim().Length > 0)
            {
                strCondition = strCondition + " AND VVTRN.VOYAGE = '" + Voyage + "'";
            }
            if (Customer.Trim().Length > 0)
            {
                //strCondition = strCondition & " AND Upper(CUSTSHIP.CUSTOMER_NAME) LIKE Upper('%" & Customer.ToUpper() & "%')" & vbCrLf
                strCondition = strCondition + " AND Upper(MBL.SHIPPER_NAME) LIKE Upper('%" + Customer.ToUpper() + "%')";
            }
            if (Consignee.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MBL.CONSIGNEE_NAME) LIKE Upper('%" + Consignee + "%')";
            }
            if (DPAgent.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(AMT.AGENT_NAME) LIKE Upper('%" + DPAgent + "%')";
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (MBLPk > 0)
            {
                strCondition = strCondition + " AND MBL.MBL_EXP_TBL_PK = " + MBLPk;
            }

            if (HBLPk > 0)
            {
                strCondition = strCondition + " AND HBL.HBL_EXP_TBL_PK = " + HBLPk;
            }

            if (POL.Trim().Length > 0)
            {
                strCondition = strCondition + "AND POL.PORT_ID = '" + POL + "'";
            }

            if (POD.Trim().Length > 0)
            {
                strCondition = strCondition + "AND POD.PORT_ID = '" + POD + "'";
            }

            Strsql += " SELECT DISTINCT ";
            Strsql += " MBL.MBL_EXP_TBL_PK MBLPK, ";
            Strsql += " MBL.MBL_REF_NO MBLREFNO,  MBL.MBL_DATE, HBL.HBL_REF_NO HBLREFNO, HBL.HBL_DATE, ";
            Strsql += " DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') AS CARGOTYPE,";
            Strsql += " COMM.COMMODITY_GROUP_CODE,";
            Strsql += " POL.PORT_NAME POLNAME,";
            Strsql += " POD.PORT_NAME PODNAME,";
            //Strsql &= vbCrLf & " CUSTSHIP.CUSTOMER_NAME SHIPNAME,"
            Strsql += " MBL.SHIPPER_NAME SHIPNAME,";
            //Strsql &= vbCrLf & " CUSTCONS.CUSTOMER_NAME CONSIGNAME,"
            Strsql += " MBL.CONSIGNEE_NAME CONSIGNAME,";
            Strsql += " AMT.AGENT_NAME,";
            Strsql += " '' OPERATOR_NAME,";
            Strsql += " (CASE WHEN VVTRN.VOYAGE IS NOT NULL THEN";
            Strsql += " VVTBL.VESSEL_NAME ||'-' || VVTRN.VOYAGE ";
            Strsql += " ELSE";
            Strsql += " VVTBL.VESSEL_NAME END) VES_FLIGHT, ";
            Strsql += " DECODE(JOB.CARGO_MANIFEST, 0, 'Open', 1,'Provisional',2, 'Close') STATUS,";
            Strsql += " ''SEL,";
            Strsql += "  JOB.JOB_CARD_TRN_PK JOBPK, ''Flag ";
            Strsql += " FROM JOB_CARD_TRN JOB,";
            Strsql += " BOOKING_MST_TBL BKG,";
            Strsql += " MBL_EXP_TBL MBL,";
            Strsql += " HBL_exp_tbl HBL,";
            Strsql += " PORT_MST_TBL POL,";
            Strsql += " PORT_MST_TBL POD,";
            Strsql += " VESSEL_VOYAGE_TRN VVTRN,";
            Strsql += " VESSEL_VOYAGE_TBL VVTBL,";
            Strsql += " CUSTOMER_MST_TBL CUSTSHIP,";
            Strsql += " CUSTOMER_MST_TBL CUSTCONS,";
            Strsql += " COMMODITY_GROUP_MST_TBL COMM, USER_MST_TBL USR, AGENT_MST_TBL  AMT";
            Strsql += " WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK ";
            Strsql += " AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            Strsql += " AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            Strsql += " AND JOB.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)";
            Strsql += " AND JOB.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK";
            Strsql += " AND JOB.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)";
            Strsql += " AND JOB.DP_AGENT_MST_FK=AMT.AGENT_MST_PK(+) ";
            Strsql += " AND VVTBL.VESSEL_VOYAGE_TBL_PK(+)=VVTRN.VESSEL_VOYAGE_TBL_FK";
            Strsql += " AND JOB.SHIPPER_CUST_MST_FK = CUSTSHIP.CUSTOMER_MST_PK(+)";
            Strsql += " AND JOB.CONSIGNEE_CUST_MST_FK = CUSTCONS.CUSTOMER_MST_PK(+)";
            Strsql += " AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            Strsql += " AND JOB.BUSINESS_TYPE = 2 AND JOB.PROCESS_TYPE = 1";
            if (CommodityType > 0)
            {
                Strsql += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            }

            Strsql += " AND MBL.CREATED_BY_FK = USR.USER_MST_PK ";
            Strsql += " AND MBL.MBL_REF_NO IS NOT NULL";
            Strsql += " AND USR.DEFAULT_LOCATION_FK = " + nLocationFk;
            Strsql += " AND VVTRN.VOYAGE_TRN_PK = " + VesPK;
            Strsql += strCondition;
            DataTable tbl = new DataTable();
            tbl = objWF.GetDataTable(Strsql.ToString());
            TotalRecords = (Int32)tbl.Rows.Count;
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
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT Q.* FROM (SELECT ROWNUM AS \"SLNO\", QRY.* FROM ");
            sqlstr.Append("  (" + Strsql.ToString() + " ");
            sqlstr.Append("   ORDER BY JOB_CARD_TRN_PK DESC) QRY )Q ");
            sqlstr.Append("  WHERE Q.SLNO  BETWEEN " + start + " AND " + last + "");

            try
            {
                return Objwk.GetDataSet(sqlstr.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the sea cargo manifest data new.
        /// </summary>
        /// <param name="VesPK">The ves pk.</param>
        /// <param name="Ves_Flight">The ves_ flight.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="MBLPk">The MBL pk.</param>
        /// <param name="HBLPk">The HBL pk.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="Status">The status.</param>
        /// <param name="nLocationFk">The n location fk.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchSeaCargoManifestDataNew(long VesPK = 0, string Ves_Flight = "", string Voyage = "", string POL = "", long MBLPk = 0, long HBLPk = 0, string POD = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string CargoType = "",
        long CommodityType = 0, string Status = "", int nLocationFk = 0, Int32 flag = 0, string Customer = "", string Consignee = "", string DPAgent = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);

            if (Ves_Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND UPPER(VVTBL.VESSEL_NAME) = '" + Ves_Flight.ToUpper() + "'";
            }
            if (Voyage.Trim().Length > 0)
            {
                strCondition = strCondition + " AND VVTRN.VOYAGE = '" + Voyage + "'";
            }
            if (Customer.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MBL.SHIPPER_NAME) LIKE Upper('%" + Customer + "%')";
            }
            //****
            if (Consignee.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MBL.CONSIGNEE_NAME) LIKE Upper('%" + Consignee + "%')";
            }
            if (DPAgent.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(AMT.AGENT_NAME) LIKE Upper('%" + DPAgent + "%')";
            }
            //****
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (MBLPk > 0)
            {
                strCondition = strCondition + " AND MBL.MBL_EXP_TBL_PK = " + MBLPk;
            }

            if (HBLPk > 0)
            {
                strCondition = strCondition + " AND JSE.HBL_HAWB_FK = " + HBLPk;
            }

            if (POL.Trim().Length > 0)
            {
                strCondition = strCondition + "AND POL.PORT_ID = '" + POL + "'";
            }

            if (POD.Trim().Length > 0)
            {
                strCondition = strCondition + "AND POD.PORT_ID = '" + POD + "'";
            }
            if (CargoType != "0")
            {
                strCondition = strCondition + " AND MBL.CARGO_TYPE=" + CargoType;
            }
            if (Status != "-1")
            {
                strCondition = strCondition + " AND JSE.CARGO_MANIFEST=" + Status;
            }
            Strsql = "SELECT COUNT(*)";
            Strsql += " FROM MBL_EXP_TBL             MBL,";
            Strsql += "       USER_MST_TBL            USR,";
            Strsql += "       COMMODITY_GROUP_MST_TBL CGP,";
            Strsql += "       PORT_MST_TBL            POL,";
            Strsql += "       PORT_MST_TBL            POD,";
            Strsql += "       OPERATOR_MST_TBL        OPR,";
            Strsql += "       VESSEL_VOYAGE_TRN       VVTRN,";
            Strsql += "       VESSEL_VOYAGE_TBL       VVTBL,";
            Strsql += "       JOB_CARD_TRN    JSE, AGENT_MST_TBL  AMT";
            Strsql += " WHERE USR.USER_MST_PK = MBL.CREATED_BY_FK";
            Strsql += "   AND CGP.COMMODITY_GROUP_PK = MBL.COMMODITY_GROUP_FK";
            Strsql += "   AND POL.PORT_MST_PK = MBL.PORT_MST_POL_FK";
            Strsql += "   AND POD.PORT_MST_PK = MBL.PORT_MST_POD_FK";
            Strsql += "   AND OPR.OPERATOR_MST_PK = MBL.OPERATOR_MST_FK";
            Strsql += "   AND MBL.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)";
            Strsql += "   AND VVTBL.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK";
            Strsql += "   AND JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK ";
            Strsql += "   AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1";
            Strsql += "   AND AMT.AGENT_MST_PK(+)=JSE.DP_AGENT_MST_FK  ";
            Strsql += "   AND USR.DEFAULT_LOCATION_FK = " + nLocationFk;
            Strsql += "   AND MBL.VOYAGE_TRN_FK=" + VesPK;

            if (CommodityType > 0)
            {
                Strsql += "AND CGP.COMMODITY_GROUP_PK=" + CommodityType;
            }

            Strsql += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Strsql));
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
            Strsql = " SELECT *  FROM ";
            Strsql += "(SELECT ROWNUM AS SLNO, Q.*";
            Strsql += "   FROM (SELECT DISTINCT MBL.MBL_EXP_TBL_PK,";
            Strsql += "              MBL.MBL_REF_NO MBLREFNO,";
            Strsql += "    TO_DATE(MBL.MBL_DATE, 'dd/MM/yyyy') AS MBL_DATE,";
            Strsql += "  JSE.HBL_HAWB_REF_NO HBLREFNO,TO_DATE(JSE.HBL_HAWB_DATE, 'dd/MM/yyyy') HBL_DATE ,";
            //Strsql &= vbCrLf & "     DECODE(MBL.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGOTYPE,"
            Strsql += "     DECODE(BKG.CARGO_TYPE,1,'FCL',2,'LCL',4,'BBC') CARGOTYPE,";
            Strsql += "     CGP.COMMODITY_GROUP_CODE,";
            Strsql += "     POL.PORT_ID POLNAME,";
            Strsql += "      POD.PORT_ID PODNAME,";
            Strsql += "      MBL.SHIPPER_NAME SHIPNAME,";
            Strsql += "       MBL.CONSIGNEE_NAME CONSIGNAME,  AMT.AGENT_NAME,";
            Strsql += "       OPR.OPERATOR_NAME,";
            Strsql += "       (CASE";
            Strsql += "         WHEN VVTRN.VOYAGE IS NOT NULL THEN";
            Strsql += "        VVTBL.VESSEL_NAME || '/' || VVTRN.VOYAGE";
            Strsql += "       ELSE";
            Strsql += "        VVTBL.VESSEL_NAME";
            Strsql += "       END) VES_FLIGHT,";
            Strsql += "      DECODE(JSE.CARGO_MANIFEST, 0, 'Open', 1, 'Provisional', 2, 'Close') MANI_STATUS,";
            Strsql += "      '' SEL,JSE.JOB_CARD_TRN_PK JOBPK, ''Flag  ";
            Strsql += "   FROM MBL_EXP_TBL             MBL,";
            Strsql += "        USER_MST_TBL            USR,";
            Strsql += "       COMMODITY_GROUP_MST_TBL CGP,";
            Strsql += "        PORT_MST_TBL            POL,";
            Strsql += "        PORT_MST_TBL            POD,";
            Strsql += "         OPERATOR_MST_TBL        OPR,";
            Strsql += "         VESSEL_VOYAGE_TRN       VVTRN,";
            Strsql += "         VESSEL_VOYAGE_TBL       VVTBL,";
            Strsql += "         JOB_CARD_TRN    JSE,  AGENT_MST_TBL AMT, BOOKING_MST_TBL BKG";
            Strsql += "   WHERE USR.USER_MST_PK = MBL.CREATED_BY_FK";
            Strsql += "     AND CGP.COMMODITY_GROUP_PK = MBL.COMMODITY_GROUP_FK";
            Strsql += "     AND POL.PORT_MST_PK = MBL.PORT_MST_POL_FK";
            Strsql += "     AND POD.PORT_MST_PK = MBL.PORT_MST_POD_FK";
            Strsql += "    AND OPR.OPERATOR_MST_PK = MBL.OPERATOR_MST_FK";
            Strsql += "     AND MBL.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)";
            Strsql += "     AND BKG.BOOKING_MST_PK=JSE.BOOKING_MST_FK";
            Strsql += "      AND VVTBL.VESSEL_VOYAGE_TBL_PK(+) =";
            Strsql += "         VVTRN.VESSEL_VOYAGE_TBL_FK";
            Strsql += "     AND AMT.AGENT_MST_PK(+)=JSE.DP_AGENT_MST_FK  ";
            Strsql += "    AND JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK ";
            Strsql += "   AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ";
            Strsql += " AND USR.DEFAULT_LOCATION_FK = " + nLocationFk;
            Strsql += "   AND MBL.VOYAGE_TRN_FK=" + VesPK;

            if (CommodityType > 0)
            {
                Strsql += "AND CGP.COMMODITY_GROUP_PK=" + CommodityType;
            }

            Strsql += strCondition;
            Strsql += "   ORDER BY MBL_DATE DESC, MBLREFNO DESC";
            Strsql += " )q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the air cargo manifest data1.
        /// </summary>
        /// <param name="VesPK">The ves pk.</param>
        /// <param name="Flight">The flight.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="MBLPk">The MBL pk.</param>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchAirCargoManifestData1(string VesPK = "", string Flight = "", string POL = "", long MBLPk = 0, long HAWBPk = 0, string POD = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long CommodityType = 0, Int32 flag = 0,
        string Customer = "", string Consignee = "", string DPAgent = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }

            if (Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JOB.VOYAGE_FLIGHT_NO = '" + Flight + "'";
            }
            if (Customer.Trim().Length > 0)
            {
                //strCondition = strCondition & " AND Upper(CUSTSHIP.CUSTOMER_NAME) LIKE Upper('%" & Customer & "%')" & vbCrLf
                strCondition = strCondition + " AND Upper(MAWB.SHIPPER_NAME) LIKE Upper('%" + Customer + "%')";
            }
            //****
            if (Consignee.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MAWB.CONSIGNEE_NAME) LIKE Upper('%" + Consignee + "%')";
            }
            if (DPAgent.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(AMT.AGENT_NAME) LIKE Upper('%" + DPAgent + "%')";
            }
            //****
            if (MBLPk > 0)
            {
                strCondition = strCondition + " AND MAWB.MAWB_EXP_TBL_PK = " + MBLPk;
            }

            if (HAWBPk > 0)
            {
                strCondition = strCondition + " AND HAWB.HAWB_EXP_TBL_PK = " + HAWBPk;
            }

            if (POL.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POL.PORT_ID = '" + POL + "'";
            }

            if (POD.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POD.PORT_ID = '" + POD + "'";
            }

            Strsql = "SELECT COUNT(DISTINCT(JOB.JOB_CARD_TRN_PK))";
            Strsql += "FROM JOB_CARD_TRN JOB,";
            Strsql += "BOOKING_MST_TBL BKG,";
            Strsql += "MAWB_EXP_TBL MAWB,";
            Strsql += "hawb_exp_tbl HAWB,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += "CUSTOMER_MST_TBL CUSTSHIP,";
            Strsql += "CUSTOMER_MST_TBL CUSTCONS,";
            Strsql += "COMMODITY_GROUP_MST_TBL COMM";
            Strsql += "WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK";
            Strsql += "AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            Strsql += "AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            Strsql += "AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)";
            Strsql += "AND JOB.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK(+)";

            //WHY REQUIRED??????
            //Strsql &= vbCrLf & " AND (select count(*) from job_trn_cont cont where cont.job_card_trn_fk=JOB.JOB_CARD_TRN_PK and cont.load_date  is not null)>0"
            Strsql += "AND JOB.SHIPPER_CUST_MST_FK = CUSTSHIP.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.CONSIGNEE_CUST_MST_FK = CUSTCONS.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            Strsql += " AND JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1";
            Strsql += " AND MAWB.AIRLINE_MST_FK= '" + VesPK + "'";

            if (CommodityType > 0)
            {
                Strsql += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            }
            Strsql += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Strsql));
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

            Strsql = " select  * from ";
            Strsql += "(SELECT ROWNUM AS SLNO,Q.*  ";

            Strsql += "  FROM (SELECT COUNT(HBLREFNO) TOTALBL, ";
            Strsql += "           COUNT(CASE ";
            Strsql += "                   WHEN HAWB_STATUS = 1 THEN";
            Strsql += " HAWB_EXP_TBL_PK";
            Strsql += "                   ELSE";
            Strsql += "  0 ";
            Strsql += " END) DRAFTBL, ";
            Strsql += "           0 GEN20,";
            Strsql += "           0 GEN40,";
            Strsql += "           0 GENHC,";
            Strsql += "           0 REF20,";
            Strsql += "           0 REF40,";
            Strsql += "           0 REFHC,";
            Strsql += "           0 ODC20,";
            Strsql += "           0 ODC40,";
            Strsql += "           0 ODCHC,";
            Strsql += "           0 EMT20,";
            Strsql += "           0 EMT40,";
            Strsql += "           0 TOTTEU ";
            Strsql += "  from (SELECT DISTINCT ";

            Strsql += "MAWB.MAWB_EXP_TBL_PK,";
            Strsql += "MAWB.MAWB_REF_NO MBLREFNO,";
            Strsql += "MAWB.MAWB_DATE, HAWB.HAWB_REF_NO HBLREFNO, HAWB.HAWB_DATE, ";
            Strsql += " '' AS CARGOTYPE, ";
            Strsql += " COMM.COMMODITY_GROUP_CODE,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_NAME PODNAME,";
            //Strsql &= vbCrLf & "CUSTSHIP.CUSTOMER_NAME SHIPNAME,"
            Strsql += " MAWB.SHIPPER_NAME SHIPNAME,";
            //Strsql &= vbCrLf & "CUSTCONS.CUSTOMER_NAME CONSIGNAME,"
            Strsql += " MAWB.CONSIGNEE_NAME CONSIGNAME,";
            Strsql += " AMT.AGENT_NAME,";
            Strsql += " '' OPERATOR_NAME,";
            Strsql += "JOB.VOYAGE_FLIGHT_NO VES_FLIGHT,DECODE(JOB.CARGO_MANIFEST, 0, 'Open', 1, 'Provisional' , 2, 'Close') STATUS,";
            Strsql += " '' SEL ,JOB.JOB_CARD_TRN_PK JOBPK,HAWB.HAWB_EXP_TBL_PK,HAWB.HAWB_STATUS,''Flag ";
            Strsql += "FROM JOB_CARD_TRN JOB,";
            Strsql += "BOOKING_MST_TBL BKG,";
            Strsql += "MAWB_EXP_TBL MAWB,";
            Strsql += "hawb_exp_tbl HAWB,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += "CUSTOMER_MST_TBL CUSTSHIP,";
            Strsql += "CUSTOMER_MST_TBL CUSTCONS,";
            Strsql += "COMMODITY_GROUP_MST_TBL COMM, AGENT_MST_TBL AMT";
            Strsql += " WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK";
            Strsql += "AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            Strsql += "AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            Strsql += "AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)";
            Strsql += "AND JOB.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK(+)";
            Strsql += " AND JOB.DP_AGENT_MST_FK=AMT.AGENT_MST_PK(+) ";
            //WHY REQUIRED??????
            //Strsql &= vbCrLf & " AND (select count(*) from job_trn_cont cont where cont.job_card_trn_fk=JOB.JOB_CARD_TRN_PK and cont.load_date  is not null)>0"
            Strsql += "AND JOB.SHIPPER_CUST_MST_FK = CUSTSHIP.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.CONSIGNEE_CUST_MST_FK = CUSTCONS.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            Strsql += " AND JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1";
            Strsql += " AND MAWB.AIRLINE_MST_FK= '" + VesPK + "'";
            Strsql += "AND MAWB.MAWB_REF_NO IS NOT NULL";

            if (CommodityType > 0)
            {
                Strsql += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            }
            Strsql += strCondition;
            Strsql += " ORDER BY JOBPK DESC ";
            Strsql += " ))q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the air cargo manifest data.
        /// </summary>
        /// <param name="VesPK">The ves pk.</param>
        /// <param name="Flight">The flight.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="MBLPk">The MBL pk.</param>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchAirCargoManifestData(string VesPK = "", string Flight = "", string POL = "", long MBLPk = 0, long HAWBPk = 0, string POD = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, long CommodityType = 0, Int32 flag = 0,
        string Customer = "", string Consignee = "", string DPAgent = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }

            if (Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND JOB.VOYAGE_FLIGHT_NO = '" + Flight + "'";
            }
            if (Customer.Trim().Length > 0)
            {
                //strCondition = strCondition & " AND Upper(CUSTSHIP.CUSTOMER_NAME) LIKE Upper('%" & Customer & "%')" & vbCrLf
                strCondition = strCondition + " AND Upper(MAWB.SHIPPER_NAME) LIKE Upper('%" + Customer + "%')";
            }
            //****
            if (Consignee.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MAWB.CONSIGNEE_NAME) LIKE Upper('%" + Consignee + "%')";
            }
            if (DPAgent.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(AMT.AGENT_NAME) LIKE Upper('%" + DPAgent + "%')";
            }
            //****
            if (MBLPk > 0)
            {
                strCondition = strCondition + " AND MAWB.MAWB_EXP_TBL_PK = " + MBLPk;
            }

            if (HAWBPk > 0)
            {
                strCondition = strCondition + " AND HAWB.HAWB_EXP_TBL_PK = " + HAWBPk;
            }

            if (POL.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POL.PORT_ID = '" + POL + "'";
            }

            if (POD.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POD.PORT_ID = '" + POD + "'";
            }

            Strsql = "SELECT COUNT(DISTINCT(JOB.JOB_CARD_TRN_PK))";
            Strsql += "FROM JOB_CARD_TRN JOB,";
            Strsql += "BOOKING_MST_TBL BKG,";
            Strsql += "MAWB_EXP_TBL MAWB,";
            Strsql += "hawb_exp_tbl HAWB,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += "CUSTOMER_MST_TBL CUSTSHIP,";
            Strsql += "CUSTOMER_MST_TBL CUSTCONS,";
            Strsql += "COMMODITY_GROUP_MST_TBL COMM";
            Strsql += "WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK";
            Strsql += "AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            Strsql += "AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            Strsql += "AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)";
            Strsql += "AND JOB.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK(+)";

            //WHY REQUIRED??????
            //Strsql &= vbCrLf & " AND (select count(*) from job_trn_cont cont where cont.job_card_trn_fk=JOB.JOB_CARD_TRN_PK and cont.load_date  is not null)>0"
            Strsql += "AND JOB.SHIPPER_CUST_MST_FK = CUSTSHIP.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.CONSIGNEE_CUST_MST_FK = CUSTCONS.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            Strsql += " AND JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1";
            Strsql += " AND MAWB.AIRLINE_MST_FK= '" + VesPK + "'";

            if (CommodityType > 0)
            {
                Strsql += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            }
            Strsql += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Strsql));
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

            Strsql = " select  * from ";
            Strsql += "(SELECT ROWNUM AS SLNO,Q.* FROM ";
            Strsql += "(SELECT DISTINCT ";
            Strsql += "MAWB.MAWB_EXP_TBL_PK,";
            Strsql += "MAWB.MAWB_REF_NO MBLREFNO,";
            Strsql += "MAWB.MAWB_DATE, HAWB.HAWB_REF_NO HBLREFNO, HAWB.HAWB_DATE, ";
            Strsql += " '' AS CARGOTYPE, ";
            Strsql += " COMM.COMMODITY_GROUP_CODE,";
            Strsql += "POL.PORT_NAME POLNAME,";
            Strsql += "POD.PORT_NAME PODNAME,";
            //Strsql &= vbCrLf & "CUSTSHIP.CUSTOMER_NAME SHIPNAME,"
            Strsql += " MAWB.SHIPPER_NAME SHIPNAME,";
            //Strsql &= vbCrLf & "CUSTCONS.CUSTOMER_NAME CONSIGNAME,"
            Strsql += " MAWB.CONSIGNEE_NAME CONSIGNAME,";
            Strsql += " AMT.AGENT_NAME,";
            Strsql += " '' OPERATOR_NAME,";
            Strsql += "JOB.VOYAGE_FLIGHT_NO VES_FLIGHT,DECODE(JOB.CARGO_MANIFEST, 0, 'Open', 1, 'Provisional' , 2, 'Close') STATUS,";
            Strsql += " '' SEL ,JOB.JOB_CARD_TRN_PK JOBPK , ''Flag ";
            Strsql += "FROM JOB_CARD_TRN JOB,";
            Strsql += "BOOKING_MST_TBL BKG,";
            Strsql += "MAWB_EXP_TBL MAWB,";
            Strsql += "hawb_exp_tbl HAWB,";
            Strsql += "PORT_MST_TBL POL,";
            Strsql += "PORT_MST_TBL POD,";
            Strsql += "CUSTOMER_MST_TBL CUSTSHIP,";
            Strsql += "CUSTOMER_MST_TBL CUSTCONS,";
            Strsql += "COMMODITY_GROUP_MST_TBL COMM, AGENT_MST_TBL AMT";
            Strsql += " WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK";
            Strsql += "AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK(+)";
            Strsql += "AND BKG.PORT_MST_POD_FK = POD.PORT_MST_PK(+)";
            Strsql += "AND JOB.HBL_HAWB_FK = HAWB.HAWB_EXP_TBL_PK(+)";
            Strsql += "AND JOB.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK(+)";
            Strsql += " AND JOB.DP_AGENT_MST_FK=AMT.AGENT_MST_PK(+) ";
            //WHY REQUIRED??????
            //Strsql &= vbCrLf & " AND (select count(*) from job_trn_cont cont where cont.job_card_trn_fk=JOB.JOB_CARD_TRN_PK and cont.load_date  is not null)>0"
            Strsql += "AND JOB.SHIPPER_CUST_MST_FK = CUSTSHIP.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.CONSIGNEE_CUST_MST_FK = CUSTCONS.CUSTOMER_MST_PK(+)";
            Strsql += "AND JOB.COMMODITY_GROUP_FK= COMM.COMMODITY_GROUP_PK";
            Strsql += " AND JOB.BUSINESS_TYPE = 1 AND JOB.PROCESS_TYPE = 1";
            Strsql += " AND MAWB.AIRLINE_MST_FK= '" + VesPK + "'";
            Strsql += "AND MAWB.MAWB_REF_NO IS NOT NULL";

            if (CommodityType > 0)
            {
                Strsql += "AND COMM.COMMODITY_GROUP_PK=" + CommodityType;
            }
            Strsql += strCondition;
            Strsql += " ORDER BY JOBPK DESC ";
            Strsql += " )q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the air cargo manifest data new1.
        /// </summary>
        /// <param name="VesPK">The ves pk.</param>
        /// <param name="Ves_Flight">The ves_ flight.</param>
        /// <param name="Flight">The flight.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="MBLPk">The MBL pk.</param>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="Status">The status.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchAirCargoManifestDataNew1(long VesPK = 0, string Ves_Flight = "", string Flight = "", string POL = "", long MBLPk = 0, long HAWBPk = 0, string POD = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string Cargotype = "",
        long CommodityType = 0, string Status = "", Int32 flag = 0, string Customer = "", string Consignee = "", string DPAgent = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND (JAE.VOYAGE_FLIGHT_NO) = '" + Flight + "'";
            }
            if (Ves_Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND OPR.AIRLINE_NAME = '" + Ves_Flight + "'";
            }
            if (Customer.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MBL.SHIPPER_NAME) LIKE Upper('%" + Customer + "%')";
            }
            //****
            if (Consignee.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MBL.CONSIGNEE_NAME) LIKE Upper('%" + Consignee + "%')";
            }
            if (DPAgent.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(AMT.AGENT_NAME) LIKE Upper('%" + DPAgent + "%')";
            }
            //****
            if (MBLPk > 0)
            {
                strCondition = strCondition + " AND MBL.MAWB_EXP_TBL_PK = " + MBLPk;
            }

            if (HAWBPk > 0)
            {
                strCondition = strCondition + " AND JAE.HBL_HAWB_FK = " + HAWBPk;
            }

            if (POL.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POL.PORT_ID = '" + POL + "'";
            }

            if (POD.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POD.PORT_ID = '" + POD + "'";
            }
            if (Status != "-1")
            {
                strCondition = strCondition + " AND JAE.CARGO_MANIFEST=" + Status;
            }

            strCondition = strCondition + " ) ";

            Strsql = "   SELECT COUNT(HBLREFNO) TOTALBL,";
            Strsql += " COUNT(CASE WHEN HAWB_STATUS = 1 THEN";
            Strsql += "   MBL_EXP_TBL_PK";
            Strsql += "    END) DRAFTBL,";
            Strsql += " COUNT(CASE WHEN HAWB_STATUS = 2 THEN MBL_EXP_TBL_PK END) CONFIRMBL";
            Strsql += " FROM (SELECT MBL.MAWB_EXP_TBL_PK MBL_EXP_TBL_PK,MBL.MAWB_REF_NO MBLREFNO,JAE.HBL_HAWB_REF_NO HBLREFNO, HBL.HAWB_EXP_TBL_PK,HBL.HAWB_STATUS";
            Strsql += " FROM MAWB_EXP_TBL MBL, HAWB_EXP_TBL HBL,  USER_MST_TBL USR, COMMODITY_GROUP_MST_TBL CGP,PORT_MST_TBL POL,PORT_MST_TBL POD,AIRLINE_MST_TBL OPR,JOB_CARD_TRN  JAE";
            Strsql += " WHERE USR.USER_MST_PK = MBL.CREATED_BY_FK AND CGP.COMMODITY_GROUP_PK = MBL.COMMODITY_GROUP_FK ";
            Strsql += " AND POL.PORT_MST_PK = MBL.PORT_MST_POL_FK";
            Strsql += " AND POD.PORT_MST_PK = MBL.PORT_MST_POD_FK";
            Strsql += " AND OPR.AIRLINE_MST_PK = MBL.AIRLINE_MST_FK";
            Strsql += "  AND JAE.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK";
            Strsql += "  AND JAE.BUSINESS_TYPE = 1 AND JAE.PROCESS_TYPE = 1 AND HBL.HAWB_EXP_TBL_PK = JAE.HBL_HAWB_FK";
            Strsql += "  AND USR.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            Strsql += "  AND OPR.AIRLINE_MST_PK =" + VesPK;

            if (CommodityType > 0)
            {
                Strsql += "AND CGP.COMMODITY_GROUP_PK=" + CommodityType;
            }

            Strsql += strCondition;

            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the air cargo manifest data new.
        /// </summary>
        /// <param name="VesPK">The ves pk.</param>
        /// <param name="Ves_Flight">The ves_ flight.</param>
        /// <param name="Flight">The flight.</param>
        /// <param name="POL">The pol.</param>
        /// <param name="MBLPk">The MBL pk.</param>
        /// <param name="HAWBPk">The hawb pk.</param>
        /// <param name="POD">The pod.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Cargotype">The cargotype.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <param name="Status">The status.</param>
        /// <param name="flag">The flag.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchAirCargoManifestDataNew(long VesPK = 0, string Ves_Flight = "", string Flight = "", string POL = "", long MBLPk = 0, long HAWBPk = 0, string POD = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string Cargotype = "",
        long CommodityType = 0, string Status = "", Int32 flag = 0, string Customer = "", string Consignee = "", string DPAgent = "")
        {
            string Strsql = null;
            WorkFlow Objwk = new WorkFlow();
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            if (flag == 0)
            {
                strCondition += " AND 1=2";
            }
            if (Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND (JAE.VOYAGE_FLIGHT_NO) = '" + Flight + "'";
            }
            if (Ves_Flight.Trim().Length > 0)
            {
                strCondition = strCondition + " AND OPR.AIRLINE_NAME = '" + Ves_Flight + "'";
            }
            if (Customer.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MBL.SHIPPER_NAME) LIKE Upper('%" + Customer + "%')";
            }
            //****
            if (Consignee.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(MBL.CONSIGNEE_NAME) LIKE Upper('%" + Consignee + "%')";
            }
            if (DPAgent.Trim().Length > 0)
            {
                strCondition = strCondition + " AND Upper(AMT.AGENT_NAME) LIKE Upper('%" + DPAgent + "%')";
            }
            //****
            if (MBLPk > 0)
            {
                strCondition = strCondition + " AND MBL.MAWB_EXP_TBL_PK = " + MBLPk;
            }

            if (HAWBPk > 0)
            {
                strCondition = strCondition + " AND JAE.HBL_HAWB_FK = " + HAWBPk;
            }

            if (POL.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POL.PORT_ID = '" + POL + "'";
            }

            if (POD.Trim().Length > 0)
            {
                strCondition = strCondition + " AND POD.PORT_ID = '" + POD + "'";
            }
            if (Status != "-1")
            {
                strCondition = strCondition + " AND JAE.CARGO_MANIFEST=" + Status;
            }
            Strsql = "SELECT COUNT(*)";
            Strsql += "FROM  MAWB_EXP_TBL            MBL,";
            Strsql += "  USER_MST_TBL            USR,";
            Strsql += " COMMODITY_GROUP_MST_TBL CGP,";
            Strsql += " PORT_MST_TBL            POL,";
            Strsql += "  PORT_MST_TBL            POD,";
            Strsql += " AIRLINE_MST_TBL         OPR,";
            Strsql += " JOB_CARD_TRN    JAE, ";
            Strsql += " AGENT_MST_TBL           AMT";
            Strsql += " WHERE USR.USER_MST_PK = MBL.CREATED_BY_FK";
            Strsql += " AND CGP.COMMODITY_GROUP_PK = MBL.COMMODITY_GROUP_FK";
            Strsql += " AND POL.PORT_MST_PK = MBL.PORT_MST_POL_FK";
            Strsql += " AND POD.PORT_MST_PK = MBL.PORT_MST_POD_FK";
            Strsql += "  AND OPR.AIRLINE_MST_PK = MBL.AIRLINE_MST_FK";
            Strsql += "  AND JAE.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK";
            Strsql += "  AND JAE.BUSINESS_TYPE = 1 AND JAE.PROCESS_TYPE = 1";
            Strsql += "  AND AMT.AGENT_MST_PK(+)=JAE.DP_AGENT_MST_FK ";
            Strsql += "  AND USR.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            Strsql += "  AND OPR.AIRLINE_MST_PK =" + VesPK;

            if (CommodityType > 0)
            {
                Strsql += "AND CGP.COMMODITY_GROUP_PK=" + CommodityType;
            }

            Strsql += strCondition;
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(Strsql));
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

            Strsql = " SELECT * FROM (SELECT ROWNUM AS SLNO, Q.* ";
            Strsql += " FROM (SELECT MBL.MAWB_EXP_TBL_PK MBL_EXP_TBL_PK,";
            Strsql += " MBL.MAWB_REF_NO MBLREFNO,";
            Strsql += " TO_DATE(MBL.MAWB_DATE, 'dd/MM/yyyy') AS MBL_DATE,";
            Strsql += " JAE.HBL_HAWB_REF_NO HBLREFNO,";
            Strsql += " TO_DATE(JAE.HBL_HAWB_DATE, 'dd/MM/yyyy') HBL_DATE ,";
            Strsql += " '' CARGOTYPE,";
            Strsql += " CGP.COMMODITY_GROUP_CODE,";
            Strsql += " POL.PORT_ID POLNAME,";
            Strsql += " POD.PORT_ID PODNAME,";
            Strsql += " MBL.SHIPPER_NAME SHIPNAME,";
            Strsql += " MBL.CONSIGNEE_NAME CONSIGNAME,  AMT.AGENT_NAME,";
            Strsql += " OPR.AIRLINE_NAME OPERATOR_NAME,";
            Strsql += " (SELECT DISTINCT J.VOYAGE_FLIGHT_NO";
            Strsql += "   FROM JOB_CARD_TRN J";
            Strsql += " WHERE J.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK";
            Strsql += "   AND ROWNUM = 1) VES_FLIGHT,";
            Strsql += "  DECODE(JAE.CARGO_MANIFEST, 0, 'Open', 1, 'Provisional' , 2, 'Close') MANI_STATUS,";
            Strsql += " '' SEL,JAE.JOB_CARD_TRN_PK JOBPK, ''Flag ";
            Strsql += " FROM MAWB_EXP_TBL            MBL,";
            Strsql += " USER_MST_TBL            USR,";
            Strsql += " COMMODITY_GROUP_MST_TBL CGP,";
            Strsql += " PORT_MST_TBL            POL,";
            Strsql += " PORT_MST_TBL            POD,";
            Strsql += " AIRLINE_MST_TBL         OPR,";
            Strsql += " JOB_CARD_TRN           JAE,  ";
            Strsql += "  AGENT_MST_TBL           AMT  ";
            Strsql += " WHERE USR.USER_MST_PK = MBL.CREATED_BY_FK";
            Strsql += "  AND CGP.COMMODITY_GROUP_PK = MBL.COMMODITY_GROUP_FK";
            Strsql += "  AND POL.PORT_MST_PK = MBL.PORT_MST_POL_FK";
            Strsql += "  AND POD.PORT_MST_PK = MBL.PORT_MST_POD_FK";
            Strsql += "  AND OPR.AIRLINE_MST_PK = MBL.AIRLINE_MST_FK";
            Strsql += "  AND JAE.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK";
            Strsql += " AND JAE.BUSINESS_TYPE = 1 AND JAE.PROCESS_TYPE = 1 ";
            Strsql += "  AND AMT.AGENT_MST_PK(+)=JAE.DP_AGENT_MST_FK  ";
            Strsql += "  AND USR.DEFAULT_LOCATION_FK =" + HttpContext.Current.Session["LOGED_IN_LOC_FK"];
            Strsql += "  AND OPR.AIRLINE_MST_PK =" + VesPK;

            if (CommodityType > 0)
            {
                Strsql += "AND CGP.COMMODITY_GROUP_PK=" + CommodityType;
            }

            Strsql += strCondition;
            Strsql += " )Q) WHERE SLNO  Between " + start + " and " + last;
            try
            {
                return Objwk.GetDataSet(Strsql);
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #endregion " Fetch Grid Data"

        #region " Track And Trace Report "

        /// <summary>
        /// Fetches the sea track and trace details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSeaTrackAndTraceDetails(Int64 JOBPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            strSQL.Append(" SELECT TNT.JOB_CARD_FK      JOBPK,");
            strSQL.Append(" JSE.JOBCARD_REF_NO   JOBREFNO,");
            strSQL.Append(" SHPMST.CUSTOMER_NAME SHPNAME,");
            strSQL.Append(" POLMST.PORT_NAME POL,");
            strSQL.Append(" PODMST.PORT_NAME POD,");
            strSQL.Append(" TNT.CREATED_ON       JDATE,");
            strSQL.Append(" TNT.STATUS           STATUS,");
            strSQL.Append(" LMST.LOCATION_ID     LOCATIONID,");
            strSQL.Append(" LMST.LOCATION_NAME LOCATIONNAME");

            strSQL.Append(" FROM TRACK_N_TRACE_TBL    TNT,");
            strSQL.Append(" JOB_CARD_TRN          JSE,");
            strSQL.Append(" LOCATION_MST_TBL     LMST,");
            strSQL.Append(" CUSTOMER_MST_TBL     SHPMST,");
            strSQL.Append(" BOOKING_MST_TBL BST,");
            strSQL.Append(" PORT_MST_TBL POLMST,");
            strSQL.Append(" PORT_MST_TBL PODMST,");
            strSQL.Append("  MBL_EXP_TBL  MBL ");

            strSQL.Append(" WHERE LMST.LOCATION_MST_PK(+) = TNT.LOCATION_FK");
            strSQL.Append(" AND (JSE.JOB_CARD_TRN_PK = TNT.JOB_CARD_FK");
            strSQL.Append(" OR MBL.MBL_EXP_TBL_PK = TNT.JOB_CARD_FK) ");
            strSQL.Append(" AND JSE.Mbl_MAWB_Fk = MBL.MBL_EXP_TBL_PK(+)     ");
            strSQL.Append(" AND SHPMST.CUSTOMER_MST_PK(+) = JSE.SHIPPER_CUST_MST_FK");
            strSQL.Append(" AND BST.BOOKING_MST_PK=JSE.BOOKING_MST_FK");
            strSQL.Append(" AND BST.PORT_MST_POL_FK=POLMST.PORT_MST_PK(+)");
            strSQL.Append(" AND BST.PORT_MST_POD_FK=PODMST.PORT_MST_PK(+)");
            strSQL.Append(" AND TNT.BIZ_TYPE = 2 ");
            strSQL.Append(" AND TNT.PROCESS = 1 ");
            strSQL.Append(" AND JSE.JOB_CARD_TRN_PK = " + JOBPK + "");
            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the air import track and trace details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchAirImportTrackAndTraceDetails(Int64 JOBPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            strSQL.Append(" SELECT TNT.JOB_CARD_FK      JOBPK,");
            strSQL.Append(" JAI.JOBCARD_REF_NO   JOBREFNO,");
            strSQL.Append(" SHPMST.CUSTOMER_NAME SHPNAME,");
            strSQL.Append(" POLMST.PORT_NAME POL,");
            strSQL.Append(" PODMST.PORT_NAME POD,");
            strSQL.Append(" TNT.CREATED_ON       JDATE,");
            strSQL.Append(" TNT.STATUS           STATUS,");
            strSQL.Append(" LMST.LOCATION_ID     LOCATIONID,");
            strSQL.Append(" LMST.LOCATION_NAME LOCATIONNAME");
            strSQL.Append(" FROM TRACK_N_TRACE_TBL    TNT,");
            strSQL.Append(" JOB_CARD_TRN JAI,");
            strSQL.Append(" LOCATION_MST_TBL     LMST,");
            strSQL.Append(" CUSTOMER_MST_TBL     SHPMST,");
            strSQL.Append(" PORT_MST_TBL POLMST,");
            strSQL.Append(" PORT_MST_TBL PODMST");
            strSQL.Append(" WHERE LMST.LOCATION_MST_PK(+) = TNT.LOCATION_FK");
            strSQL.Append(" AND JAI.JOB_CARD_TRN_PK = TNT.JOB_CARD_FK");
            strSQL.Append(" AND SHPMST.CUSTOMER_MST_PK(+) = JAI.SHIPPER_CUST_MST_FK");
            strSQL.Append(" AND JAI.PORT_MST_POL_FK=POLMST.PORT_MST_PK(+)");
            strSQL.Append(" AND JAI.PORT_MST_POD_FK=PODMST.PORT_MST_PK(+)");
            strSQL.Append(" AND TNT.BIZ_TYPE = 1 ");
            strSQL.Append(" AND TNT.PROCESS = 2 ");
            strSQL.Append(" AND JAI.JOB_CARD_TRN_PK = " + JOBPK + "");
            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the sea import track and trace details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchSeaImportTrackAndTraceDetails(Int64 JOBPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            strSQL.Append(" SELECT TNT.JOB_CARD_FK      JOBPK,");
            strSQL.Append(" JSI.JOBCARD_REF_NO   JOBREFNO,");
            strSQL.Append(" SHPMST.CUSTOMER_NAME SHPNAME,");
            strSQL.Append(" POLMST.PORT_NAME POL,");
            strSQL.Append(" PODMST.PORT_NAME POD,");
            strSQL.Append(" TNT.CREATED_ON       JDATE,");
            strSQL.Append(" TNT.STATUS           STATUS,");
            strSQL.Append(" LMST.LOCATION_ID     LOCATIONID,");
            strSQL.Append(" LMST.LOCATION_NAME LOCATIONNAME");
            strSQL.Append(" FROM TRACK_N_TRACE_TBL    TNT,");
            strSQL.Append(" JOB_CARD_TRN JSI,");
            strSQL.Append(" LOCATION_MST_TBL     LMST,");
            strSQL.Append(" CUSTOMER_MST_TBL     SHPMST,");
            strSQL.Append(" PORT_MST_TBL POLMST,");
            strSQL.Append(" PORT_MST_TBL PODMST");
            strSQL.Append(" WHERE LMST.LOCATION_MST_PK(+) = TNT.LOCATION_FK");
            strSQL.Append(" AND JSI.JOB_CARD_TRN_PK = TNT.JOB_CARD_FK");
            strSQL.Append(" AND SHPMST.CUSTOMER_MST_PK(+) = JSI.SHIPPER_CUST_MST_FK");
            strSQL.Append(" AND JSI.PORT_MST_POL_FK=POLMST.PORT_MST_PK(+)");
            strSQL.Append(" AND JSI.PORT_MST_POD_FK=PODMST.PORT_MST_PK(+)");
            strSQL.Append(" AND TNT.BIZ_TYPE = 2 ");
            strSQL.Append(" AND TNT.PROCESS = 2 ");
            strSQL.Append(" AND JSI.JOB_CARD_TRN_PK = " + JOBPK + "");
            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the air track and trace details.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchAirTrackAndTraceDetails(Int64 JOBPK)
        {
            WorkFlow objWF = new WorkFlow();

            StringBuilder strSQL = new StringBuilder();

            strSQL.Append(" SELECT TNT.JOB_CARD_FK   JOBPK,");
            strSQL.Append(" JAE.JOBCARD_REF_NO       JOBREFNO,");
            strSQL.Append(" SHPMST.CUSTOMER_NAME     SHPNAME,");
            strSQL.Append(" POLMST.PORT_NAME         POL,");
            strSQL.Append(" PODMST.PORT_NAME         POD,");
            strSQL.Append(" TNT.CREATED_ON           JDATE,");
            strSQL.Append(" TNT.STATUS               STATUS,");
            strSQL.Append(" LMST.LOCATION_ID         LOCATIONID,");
            strSQL.Append(" LMST.LOCATION_NAME       LOCATIONNAME");

            strSQL.Append(" FROM TRACK_N_TRACE_TBL   TNT,");
            strSQL.Append(" JOB_CARD_TRN     JAE,");
            strSQL.Append(" LOCATION_MST_TBL         LMST,");
            strSQL.Append(" CUSTOMER_MST_TBL         SHPMST,");
            strSQL.Append(" BOOKING_MST_TBL          BAT,");
            strSQL.Append(" PORT_MST_TBL             POLMST,");
            strSQL.Append(" PORT_MST_TBL             PODMST,");
            strSQL.Append("  MAWB_EXP_TBL            MAWB ");

            strSQL.Append(" WHERE LMST.LOCATION_MST_PK(+) = TNT.LOCATION_FK");

            strSQL.Append(" AND (JAE.JOB_CARD_TRN_PK = TNT.JOB_CARD_FK");
            strSQL.Append(" OR MAWB.MAWB_EXP_TBL_PK = TNT.JOB_CARD_FK) ");
            strSQL.Append(" AND JAE.MAWB_MAWB_Fk = MAWB.MAWB_EXP_TBL_PK(+)     ");

            strSQL.Append(" AND SHPMST.CUSTOMER_MST_PK(+) = JAE.SHIPPER_CUST_MST_FK");
            strSQL.Append(" AND BAT.BOOKING_MST_PK=JAE.BOOKING_MST_FK");
            strSQL.Append(" AND BAT.PORT_MST_POL_FK=POLMST.PORT_MST_PK(+)");
            strSQL.Append(" AND BAT.PORT_MST_POD_FK=PODMST.PORT_MST_PK(+)");
            strSQL.Append(" AND TNT.BIZ_TYPE = 1 ");
            strSQL.Append(" AND TNT.PROCESS = 1 ");
            strSQL.Append(" AND JAE.JOB_CARD_TRN_PK = " + JOBPK + " ");
            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the enquire date.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchEnquireDate(Int64 JOBPK)
        {
            WorkFlow objWF = new WorkFlow();
            StringBuilder strSQL = new StringBuilder();
            strSQL.Append(" SELECT E.ENQUIRY_REF_NO, TO_CHAR(E.ENQUIRY_DATE,'dd mon yyyy') ENQUIRYDATE");
            strSQL.Append(" FROM ENQUIRY_BKG_SEA_TBL E");
            strSQL.Append(" WHERE E.ENQUIRY_REF_NO =");
            strSQL.Append(" (SELECT DISTINCT QFL.TRANS_REF_NO");
            strSQL.Append(" FROM QUOTATION_DTL_TBL QFL, QUOTATION_MST_TBL QST");
            strSQL.Append(" WHERE(QFL.QUOTATION_MST_FK = QST.QUOTATION_MST_PK)");
            strSQL.Append(" AND QST.QUOTATION_REF_NO =");
            strSQL.Append(" (SELECT DISTINCT BFL.TRANS_REF_NO");
            strSQL.Append(" FROM BOOKING_MST_TBL BST, BOOKING_TRN BFL");
            strSQL.Append(" WHERE(BST.BOOKING_MST_PK = BFL.BOOKING_MST_FK)");
            strSQL.Append("  AND BFL.BOOKING_MST_FK =");
            strSQL.Append(" (SELECT DISTINCT JSE.BOOKING_MST_FK");
            strSQL.Append(" FROM JOB_CARD_TRN JSE, BOOKING_MST_TBL BST,TRACK_N_TRACE_TBL TNT");
            strSQL.Append(" WHERE(JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK)");
            strSQL.Append(" AND JSE.JOB_CARD_TRN_PK = TNT.JOB_CARD_FK");
            strSQL.Append(" AND JSE.JOB_CARD_TRN_PK=" + JOBPK + " AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 )))");

            try
            {
                return (objWF.GetDataSet(strSQL.ToString()));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        /// <summary>
        /// Fetches the air enquire date.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchAirEnquireDate(Int64 JOBPK)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            strSQL = "SELECT EBA.ENQUIRY_REF_NO,TO_CHAR(EBA.ENQUIRY_DATE,'DD MON YYYY') ENQUIRYDATE" + "FROM ENQUIRY_BKG_AIR_TBL EBA" + "WHERE EBA.ENQUIRY_REF_NO=" + "(SELECT DISTINCT QTA.TRANS_REF_NO FROM QUOTATION_DTL_TBL QTA,QUOTATION_MST_TBL QAT" + "WHERE(QTA.QUOTATION_MST_FK = QAT.QUOTATION_MST_PK)" + "AND QAT.QUOTATION_REF_NO=" + "(SELECT DISTINCT BTA.TRANS_REF_NO FROM BOOKING_MST_TBL BAT,BOOKING_TRN  BTA" + "WHERE(BAT.BOOKING_MST_PK = BTA.BOOKING_MST_FK)" + "AND BTA.BOOKING_MST_FK =" + "(SELECT DISTINCT JAE.BOOKING_MST_FK FROM JOB_CARD_TRN JAE,BOOKING_MST_TBL BAT,TRACK_N_TRACE_TBL TTT" + "WHERE(JAE.BOOKING_MST_FK = BAT.BOOKING_MST_PK)" + "AND JAE.JOB_CARD_TRN_PK=TTT.JOB_CARD_FK" + "AND JAE.JOB_CARD_TRN_PK=" + JOBPK + " AND JAE.BUSINESS_TYPE = 1 AND JAE.PROCESS_TYPE = 1 )))";
            try
            {
                return (objWF.GetDataSet(strSQL));
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
            }
            return new DataSet();
        }

        #endregion " Track And Trace Report "

        #region "Constructor"

        /// <summary>
        /// Initializes a new instance of the <see cref="Cls_SeaCargoManifest"/> class.
        /// </summary>
        /// <param name="SelectAll">if set to <c>true</c> [select all].</param>
        public Cls_SeaCargoManifest(bool SelectAll = false)
        {
            string Sql = null;
            string strSelect = "Select";
            if (SelectAll)
            {
                strSelect = "ALL";
            }
            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1 ";
            Sql += " AND CG.COMMODITY_GROUP_PK=" + GENERAL;
            Sql += " OR CG.COMMODITY_GROUP_PK=" + HAZARDOUS;
            Sql += " OR CG.COMMODITY_GROUP_PK=" + REEFER;
            Sql += " OR CG.COMMODITY_GROUP_PK=" + ODC;
            Sql += " ORDER BY COMMODITY_GROUP_CODE ";

            try
            {
                M_DataSet = (new WorkFlow()).GetDataSet(Sql);
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

        #endregion "Constructor"

        #region "FetchCommGrp"

        /// <summary>
        /// Fetches the comm GRP.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BIZType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public string FetchCommGrp(string JOBPK, string BIZType, int Process)
        {
            WorkFlow objWF = new WorkFlow();
            string strSQL = null;
            if (Convert.ToInt32(BIZType) == 2)
            {
                if (Process == 1)
                {
                    strSQL = " select upper(cgm.commodity_group_code) cgmgrp ";
                    strSQL += " from job_card_trn job, commodity_group_mst_tbl cgm ";
                    strSQL += " where job.commodity_group_fk = cgm.commodity_group_pk ";
                    strSQL += " and job.job_card_trn_pk =" + JOBPK + "";
                }
                else
                {
                    strSQL = " select upper(cgm.commodity_group_code) cgmgrp ";
                    strSQL += " from job_card_trn job, commodity_group_mst_tbl cgm ";
                    strSQL += " where job.commodity_group_fk = cgm.commodity_group_pk ";
                    strSQL += " and job.job_card_trn_pk =" + JOBPK + "";
                }
            }
            try
            {
                return objWF.ExecuteScaler(strSQL);
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchCommGrp"

        #region "Cargo Manifrst for MJC"

        /// <summary>
        /// Fetches the main seacargo report.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchMainSeacargoReport(string MBLPK = "0", string JOBPK = "0", long CommodityType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT MJ.MASTER_JC_SEA_EXP_PK,");
            sb.Append("       MJ.MASTER_JC_REF_NO,");
            sb.Append("       OP.OPERATOR_MST_PK,");
            sb.Append("       OP.OPERATOR_NAME,");
            sb.Append("       VM.VESSEL_NAME,");
            sb.Append("       VT.VOYAGE,");
            sb.Append("       MBL.MBL_EXP_TBL_PK,");
            sb.Append("       MBL.MBL_REF_NO,");
            sb.Append("       POL.PORT_MST_PK         POLPK,");
            sb.Append("       POL.PORT_NAME           POL,");
            sb.Append("       POD.PORT_MST_PK         PODPK,");
            sb.Append("       POD.PORT_NAME           POD,");
            sb.Append("       MBL.MBL_DATE,MJ.MASTER_JC_DATE,MJ.CARGO_TYPE ,");
            sb.Append("       CMMT.CARGO_MOVE_CODE,");
            sb.Append("       CASE");
            sb.Append("       WHEN MBL.PYMT_TYPE = 1 THEN");
            sb.Append("        'Preapaid'");
            sb.Append("        ELSE");
            sb.Append("        'Collect'");
            sb.Append("       END PYMT_TYPE");
            sb.Append("  FROM MASTER_JC_SEA_EXP_TBL MJ,");
            sb.Append("       MBL_EXP_TBL           MBL,");
            sb.Append("       OPERATOR_MST_TBL      OP,");
            sb.Append("       VESSEL_VOYAGE_TRN     VT,");
            sb.Append("       VESSEL_VOYAGE_TBL     VM,");
            sb.Append("       PORT_MST_TBL          POL,");
            sb.Append("       PORT_MST_TBL          POD,");
            sb.Append("       CARGO_MOVE_MST_TBL    CMMT");
            sb.Append(" WHERE MJ.MBL_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND MJ.OPERATOR_MST_FK = OP.OPERATOR_MST_PK(+)");
            sb.Append("   AND VT.VOYAGE_TRN_PK(+)= MJ.VOYAGE_TRN_FK");
            sb.Append("   AND VM.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND MJ.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND MJ.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK = MBL.CARGO_MOVE_FK");
            sb.Append("   AND   MJ.MASTER_JC_SEA_EXP_PK IN (" + JOBPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the main seacargo report new.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchMainSeacargoReportNew(string MBLPK = "0", string JOBPK = "0", long CommodityType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT MJ.MASTER_JC_SEA_EXP_PK,");
            sb.Append("       MJ.MASTER_JC_REF_NO,");
            sb.Append("       OP.OPERATOR_MST_PK,");
            sb.Append("       OP.OPERATOR_NAME,");
            sb.Append("       VM.VESSEL_NAME,");
            sb.Append("       VT.VOYAGE,");
            sb.Append("       MBL.MBL_EXP_TBL_PK,");
            sb.Append("       MBL.MBL_REF_NO,");
            sb.Append("       POL.PORT_MST_PK         POLPK,");
            sb.Append("       POL.PORT_NAME           POL,");
            sb.Append("       POD.PORT_MST_PK         PODPK,");
            sb.Append("       POD.PORT_NAME           POD,");
            sb.Append("       MBL.MBL_DATE,");
            sb.Append("       MJ.MASTER_JC_DATE,");
            sb.Append("       MBL.CARGO_TYPE,");
            sb.Append("       CMMT.CARGO_MOVE_CODE,");
            sb.Append("      CASE");
            sb.Append("       WHEN MBL.PYMT_TYPE = 1 THEN");
            sb.Append("        'Prepaid'");
            sb.Append("        ELSE");
            sb.Append("        'Collect'");
            sb.Append("       END PYMT_TYPE");
            sb.Append("  FROM MASTER_JC_SEA_EXP_TBL MJ,");
            sb.Append("       MBL_EXP_TBL           MBL,");
            sb.Append("       OPERATOR_MST_TBL      OP,");
            sb.Append("       VESSEL_VOYAGE_TRN     VT,");
            sb.Append("       VESSEL_VOYAGE_TBL     VM,");
            sb.Append("       PORT_MST_TBL          POL,");
            sb.Append("       PORT_MST_TBL          POD,");
            sb.Append("       CARGO_MOVE_MST_TBL    CMMT");
            sb.Append(" WHERE MJ.MBL_FK(+) = MBL.MBL_EXP_TBL_PK");
            sb.Append("   AND MBL.OPERATOR_MST_FK = OP.OPERATOR_MST_PK(+)");
            sb.Append("   AND VT.VOYAGE_TRN_PK(+) = MBL.VOYAGE_TRN_FK");
            sb.Append("   AND VM.VESSEL_VOYAGE_TBL_PK(+) = VT.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND MBL.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND MBL.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND CMMT.CARGO_MOVE_PK(+) = MBL.CARGO_MOVE_FK");
            sb.Append("   AND   MBL.MBL_EXP_TBL_PK IN (" + MBLPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the main aircargo report new.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="CommodityType">Type of the commodity.</param>
        /// <returns></returns>
        public DataSet FetchMainAircargoReportNew(string MBLPK = "0", string JOBPK = "0", long CommodityType = 0)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT A.MASTER_JC_AIR_EXP_PK,");
            sb.Append("                A.MASTER_JC_REF_NO,");
            sb.Append("                OP.AIRLINE_MST_PK,");
            sb.Append("                OP.AIRLINE_NAME OPERATOR_NAME,");
            sb.Append("                (SELECT DISTINCT JJ.VOYAGE_FLIGHT_NO");
            sb.Append("                   FROM JOB_CARD_TRN JJ");
            sb.Append("                  WHERE JJ.MBL_MAWB_FK = MBL.MAWB_EXP_TBL_PK) FLIGHT_NR,");
            sb.Append("                MBL.MAWB_EXP_TBL_PK MBL_EXP_TBL_PK,");
            sb.Append("                MBL.MAWB_REF_NO,");
            sb.Append("                POL.PORT_MST_PK POLPK,");
            sb.Append("                POL.PORT_NAME POL,");
            sb.Append("                POD.PORT_MST_PK PODPK,");
            sb.Append("                POD.PORT_NAME POD,");
            sb.Append("                MBL.MAWB_DATE MBL_DATE,");
            sb.Append("                A.MASTER_JC_DATE");
            sb.Append("  FROM MAWB_EXP_TBL MBL,");
            sb.Append("       AIRLINE_MST_TBL OP,");
            sb.Append("       PORT_MST_TBL POL,");
            sb.Append("       PORT_MST_TBL POD,");
            sb.Append("       (SELECT *");
            sb.Append("          FROM JOB_CARD_TRN J, MASTER_JC_AIR_EXP_TBL MJ");
            sb.Append("         WHERE MJ.MASTER_JC_AIR_EXP_PK(+) = J.MASTER_JC_FK AND J.BUSINESS_TYPE = 1 AND J.PROCESS_TYPE = 1) A");
            sb.Append(" WHERE MBL.AIRLINE_MST_FK = OP.AIRLINE_MST_PK(+)");
            sb.Append("   AND MBL.PORT_MST_POL_FK = POL.PORT_MST_PK");
            sb.Append("   AND MBL.PORT_MST_POD_FK = POD.PORT_MST_PK");
            sb.Append("   AND MBL.MAWB_EXP_TBL_PK = A.MBL_MAWB_FK");
            sb.Append("   AND   MBL.MAWB_EXP_TBL_PK IN (" + MBLPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (Exception oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
        }

        /// <summary>
        /// Fetches the container detaails.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetaails(string JOBPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("  SELECT DISTINCT JTSEC.CONTAINER_NUMBER   CONTAINERS,");
            sb.Append("  JTSEC.SEAL_NUMBER   SEALNUMBER");
            sb.Append("  FROM JOB_TRN_CONT JTSEC, JOB_CARD_TRN JSE");
            sb.Append("  WHERE JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK");
            sb.Append("  AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ");
            sb.Append("   AND JSE.MASTER_JC_FK IN (" + JOBPK + ")");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the container detaails new.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchContainerDetaailsNew(string MBLPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JTSEC.CONTAINER_NUMBER CONTAINERS,");
            sb.Append("                JTSEC.SEAL_NUMBER      SEALNUMBER");
            sb.Append("  FROM JOB_TRN_CONT JTSEC, JOB_CARD_TRN JSE, MBL_EXP_TBL M");
            sb.Append(" WHERE JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK");
            sb.Append("   AND M.MBL_EXP_TBL_PK = JSE.MBL_MAWB_FK");
            sb.Append("  AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ");
            sb.Append("   AND M.MBL_EXP_TBL_PK IN (" + MBLPK + ")");
            sb.Append("  ORDER BY CONTAINERS ASC");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the seal number.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchSealNumber(string MBLPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JTSEC.SEAL_NUMBER  SEALNUMBER");
            sb.Append("  FROM JOB_TRN_CONT JTSEC, JOB_CARD_TRN JSE, MBL_EXP_TBL M");
            sb.Append(" WHERE JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK");
            sb.Append("   AND M.MBL_EXP_TBL_PK = JSE.MBL_MAWB_FK");
            sb.Append("  AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ");
            sb.Append("   AND M.MBL_EXP_TBL_PK IN (" + MBLPK + ")");
            sb.Append("  ORDER BY SEALNUMBER ASC");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the detail seacargo report.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchDetailSeacargoReport(string JOBPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JSE.HBL_HAWB_FK HBLPK,");
            sb.Append("       HBL.HBL_REF_NO HBLREFNO,");
            sb.Append("       SHIPPER.CUSTOMER_NAME SHIPPERNAME,");
            sb.Append("       SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADD1,");
            sb.Append("       SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,");
            sb.Append("       SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,");
            sb.Append("       SHIPPERDTLS.ADM_CITY SHIPPERCITY,");
            sb.Append("       SHIPPERDTLS.ADM_ZIP_CODE SHIPPERZIP,");
            sb.Append("       SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,");
            sb.Append("       SHIPPERDTLS.ADM_FAX_NO SHIPPERFAX,");
            sb.Append("       SHIPPERDTLS.ADM_EMAIL_ID SHIPPEREMAIL,");
            sb.Append("       SHIPPERCNT.COUNTRY_NAME SHIPPERCOUNTRY,");
            sb.Append("       CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,");
            sb.Append("       CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,");
            sb.Append("       CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,");
            sb.Append("       CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,");
            sb.Append("       CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,");
            sb.Append("       CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEEZIP,");
            sb.Append("       CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,");
            sb.Append("       CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEEFAX,");
            sb.Append("       CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEEEMAIL,");
            sb.Append("       CONSIGNEECNT.COUNTRY_NAME CONSIGNEECOUNTRY,");
            sb.Append("       JSE.MARKS_NUMBERS MARKS,");
            sb.Append("       JSE.GOODS_DESCRIPTION GOODS,");
            sb.Append("         JTSEC.GROSS_WEIGHT GROSSWT,");
            sb.Append("       PY.PACK_TYPE_DESC,");
            sb.Append("        JTSEC.PACK_COUNT TOTAL_PACK_COUNT,");
            sb.Append("          JTSEC.VOLUME_IN_CBM VOLUME,");
            sb.Append("       COLPMST.PLACE_NAME COLPLACE,");
            sb.Append("       DELPMST.PLACE_NAME DELPLACE,");
            sb.Append("       PFD.PORT_NAME PFD,");
            sb.Append("       STMST.INCO_CODE TERMS,");
            sb.Append("       JSE.DEPARTURE_DATE SAILDATE,");
            sb.Append("       NULL CONTAINERS,");
            sb.Append("       NULL SEALNUMBER,");
            sb.Append("       NULL CONTAINERTYPE,");
            sb.Append("       DECODE(JSE.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PYMT_TYPE,");
            sb.Append("       MBL.MBL_REF_NO MBL_REF_NO");
            sb.Append("  FROM MBL_EXP_TBL            MBL,");
            sb.Append("       VESSEL_VOYAGE_TRN      VVTRN,");
            sb.Append("       VESSEL_VOYAGE_TBL      VVTBL,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_TRN   JSE,");
            sb.Append("       BOOKING_MST_TBL        BST,");
            sb.Append("       PLACE_MST_TBL          COLPMST,");
            sb.Append("       PLACE_MST_TBL          DELPMST,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMST,");
            sb.Append("       JOB_TRN_CONT   JTSEC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       CUSTOMER_MST_TBL       NOTIFY1,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  NOTIFY1DTLS,");
            sb.Append("       COUNTRY_MST_TBL        NOTIFY1CNT,");
            sb.Append("       CUSTOMER_MST_TBL       NOTIFY2,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  NOTIFY2DTLS,");
            sb.Append("       COUNTRY_MST_TBL        NOTIFY2CNT,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       PACK_TYPE_MST_TBL      PY");
            sb.Append(" WHERE JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK");
            sb.Append("   AND JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)");
            sb.Append("   AND VVTBL.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.COL_PLACE_MST_FK = COLPMST.PLACE_PK(+)");
            sb.Append("   AND BST.PFD_FK = PFD.PORT_MST_PK(+)");
            sb.Append("   AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+)");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.NOTIFY1_CUST_MST_FK = NOTIFY1.CUSTOMER_MST_PK(+)");
            sb.Append("   AND NOTIFY1.CUSTOMER_MST_PK = NOTIFY1DTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOTIFY1DTLS.ADM_COUNTRY_MST_FK = NOTIFY1CNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.NOTIFY2_CUST_MST_FK = NOTIFY2.CUSTOMER_MST_PK(+)");
            sb.Append("   AND NOTIFY2.CUSTOMER_MST_PK = NOTIFY2DTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOTIFY2DTLS.ADM_COUNTRY_MST_FK = NOTIFY2CNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.MASTER_JC_FK IN (" + JOBPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the detail seacargo report new.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchDetailSeacargoReportNew(string MBLPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT JSE.HBL_HAWB_FK HBLPK,");
            sb.Append("                HBL.HBL_REF_NO HBLREFNO,");
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPERNAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADD1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPERCITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPERZIP,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPERFAX,");
            sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPEREMAIL,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPERCOUNTRY,");
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEEZIP,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEEFAX,");
            sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEEEMAIL,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEECOUNTRY,");
            sb.Append("                JSE.MARKS_NUMBERS MARKS,");
            sb.Append("                JSE.GOODS_DESCRIPTION GOODS,");
            sb.Append("                JTSEC.GROSS_WEIGHT GROSSWT,");
            sb.Append("                PY.PACK_TYPE_DESC,");
            sb.Append("                JTSEC.PACK_COUNT TOTAL_PACK_COUNT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM VOLUME,");
            sb.Append("                COLPMST.PLACE_NAME COLPLACE,");
            sb.Append("                DELPMST.PLACE_NAME DELPLACE,");
            sb.Append("                PFD.PORT_NAME PFD,");
            sb.Append("                STMST.INCO_CODE TERMS,");
            sb.Append("                JSE.DEPARTURE_DATE SAILDATE,");
            sb.Append("                NULL CONTAINERS,");
            sb.Append("                NULL SEALNUMBER,");
            sb.Append("                NULL CONTAINERTYPE,");
            sb.Append("                DECODE(JSE.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PYMT_TYPE,");
            sb.Append("                MBL.MBL_REF_NO MBL_REF_NO");
            sb.Append("  FROM MBL_EXP_TBL            MBL,");
            sb.Append("       VESSEL_VOYAGE_TRN      VVTRN,");
            sb.Append("       VESSEL_VOYAGE_TBL      VVTBL,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HBL_EXP_TBL            HBL,");
            sb.Append("       JOB_CARD_TRN   JSE,");
            sb.Append("       BOOKING_MST_TBL        BST,");
            sb.Append("       PLACE_MST_TBL          COLPMST,");
            sb.Append("       PLACE_MST_TBL          DELPMST,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMST,");
            sb.Append("       JOB_TRN_CONT   JTSEC,");
            sb.Append("       CONTAINER_TYPE_MST_TBL CTMST,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       CUSTOMER_MST_TBL       NOTIFY1,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  NOTIFY1DTLS,");
            sb.Append("       COUNTRY_MST_TBL        NOTIFY1CNT,");
            sb.Append("       CUSTOMER_MST_TBL       NOTIFY2,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  NOTIFY2DTLS,");
            sb.Append("       COUNTRY_MST_TBL        NOTIFY2CNT,");
            sb.Append("       PORT_MST_TBL           PFD,");
            sb.Append("       PACK_TYPE_MST_TBL      PY");
            sb.Append(" WHERE JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK");
            sb.Append("   AND JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND JSE.MBL_MAWB_FK = MBL.MBL_EXP_TBL_PK(+)");
            sb.Append("   AND JSE.VOYAGE_TRN_FK = VVTRN.VOYAGE_TRN_PK(+)");
            sb.Append("   AND VVTBL.VESSEL_VOYAGE_TBL_PK(+) = VVTRN.VESSEL_VOYAGE_TBL_FK");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HBL_HAWB_FK = HBL.HBL_EXP_TBL_PK(+)");
            sb.Append("   AND BST.COL_PLACE_MST_FK = COLPMST.PLACE_PK(+)");
            sb.Append("   AND BST.PFD_FK = PFD.PORT_MST_PK(+)");
            sb.Append("   AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+)");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)");
            sb.Append("   AND JTSEC.CONTAINER_TYPE_MST_FK = CTMST.CONTAINER_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.NOTIFY1_CUST_MST_FK = NOTIFY1.CUSTOMER_MST_PK(+)");
            sb.Append("   AND NOTIFY1.CUSTOMER_MST_PK = NOTIFY1DTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOTIFY1DTLS.ADM_COUNTRY_MST_FK = NOTIFY1CNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.NOTIFY2_CUST_MST_FK = NOTIFY2.CUSTOMER_MST_PK(+)");
            sb.Append("   AND NOTIFY2.CUSTOMER_MST_PK = NOTIFY2DTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOTIFY2DTLS.ADM_COUNTRY_MST_FK = NOTIFY2CNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND MBL.MBL_EXP_TBL_PK  IN (" + MBLPK + ") ");
            sb.Append("  AND JSE.BUSINESS_TYPE = 2 AND JSE.PROCESS_TYPE = 1 ");
            sb.Append("  ORDER BY HBL.HBL_REF_NO  ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the detail aircargo report new.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchDetailAircargoReportNew(string MBLPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT DISTINCT MAWB.MAWB_REF_NO,");
            sb.Append("                JSE.HBL_HAWB_FK HBLPK,");
            sb.Append("                HBL.HAWB_REF_NO HBLREFNO,");
            sb.Append("                SHIPPER.CUSTOMER_NAME SHIPPERNAME,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_1 SHIPPERADD1,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_2 SHIPPERADD2,");
            sb.Append("                SHIPPERDTLS.ADM_ADDRESS_3 SHIPPERADD3,");
            sb.Append("                SHIPPERDTLS.ADM_CITY SHIPPERCITY,");
            sb.Append("                SHIPPERDTLS.ADM_ZIP_CODE SHIPPERZIP,");
            sb.Append("                SHIPPERDTLS.ADM_PHONE_NO_1 SHIPPERPHONE,");
            sb.Append("                SHIPPERDTLS.ADM_FAX_NO SHIPPERFAX,");
            sb.Append("                SHIPPERDTLS.ADM_EMAIL_ID SHIPPEREMAIL,");
            sb.Append("                SHIPPERCNT.COUNTRY_NAME SHIPPERCOUNTRY,");
            sb.Append("                CONSIGNEE.CUSTOMER_NAME CONSIGNEENAME,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_1 CONSIGNEEADD1,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_2 CONSIGNEEADD2,");
            sb.Append("                CONSIGNEEDTLS.ADM_ADDRESS_3 CONSIGNEEADD3,");
            sb.Append("                CONSIGNEEDTLS.ADM_CITY CONSIGNEECITY,");
            sb.Append("                CONSIGNEEDTLS.ADM_ZIP_CODE CONSIGNEEZIP,");
            sb.Append("                CONSIGNEEDTLS.ADM_PHONE_NO_1 CONSIGNEEPHONE,");
            sb.Append("                CONSIGNEEDTLS.ADM_FAX_NO CONSIGNEEFAX,");
            sb.Append("                CONSIGNEEDTLS.ADM_EMAIL_ID CONSIGNEEEMAIL,");
            sb.Append("                CONSIGNEECNT.COUNTRY_NAME CONSIGNEECOUNTRY,");
            sb.Append("                JSE.MARKS_NUMBERS MARKS,");
            sb.Append("                JSE.GOODS_DESCRIPTION GOODS,");
            sb.Append("                JTSEC.GROSS_WEIGHT GROSSWT,");
            sb.Append("                PY.PACK_TYPE_DESC,");
            sb.Append("                JTSEC.PACK_COUNT TOTAL_PACK_COUNT,");
            sb.Append("                JTSEC.VOLUME_IN_CBM VOLUME,");
            sb.Append("                COLPMST.PLACE_NAME COLPLACE,");
            sb.Append("                DELPMST.PLACE_NAME DELPLACE,");
            sb.Append("                STMST.INCO_CODE TERMS,");
            sb.Append("                JSE.DEPARTURE_DATE SAILDATE,");
            sb.Append("                NULL CONTAINERS,");
            sb.Append("                NULL AIRLNUMBER,");
            sb.Append("                NULL CONTAINERTYPE,");
            sb.Append("                DECODE(JSE.PYMT_TYPE, 1, 'Prepaid', 2, 'Collect') PYMT_TYPE");
            sb.Append("  FROM MAWB_EXP_TBL           MAWB,");
            sb.Append("       PORT_MST_TBL           POLMST,");
            sb.Append("       PORT_MST_TBL           PODMST,");
            sb.Append("       HAWB_EXP_TBL           HBL,");
            sb.Append("       JOB_CARD_TRN   JSE,");
            sb.Append("       BOOKING_MST_TBL        BST,");
            sb.Append("       PLACE_MST_TBL          COLPMST,");
            sb.Append("       PLACE_MST_TBL          DELPMST,");
            sb.Append("       SHIPPING_TERMS_MST_TBL STMST,");
            sb.Append("       JOB_TRN_CONT   JTSEC,");
            sb.Append("       CUSTOMER_MST_TBL       SHIPPER,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  SHIPPERDTLS,");
            sb.Append("       COUNTRY_MST_TBL        SHIPPERCNT,");
            sb.Append("       CUSTOMER_MST_TBL       CONSIGNEE,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  CONSIGNEEDTLS,");
            sb.Append("       COUNTRY_MST_TBL        CONSIGNEECNT,");
            sb.Append("       CUSTOMER_MST_TBL       NOTIFY1,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  NOTIFY1DTLS,");
            sb.Append("       COUNTRY_MST_TBL        NOTIFY1CNT,");
            sb.Append("       CUSTOMER_MST_TBL       NOTIFY2,");
            sb.Append("       CUSTOMER_CONTACT_DTLS  NOTIFY2DTLS,");
            sb.Append("       COUNTRY_MST_TBL        NOTIFY2CNT,");
            sb.Append("       PACK_TYPE_MST_TBL      PY");
            sb.Append(" WHERE JSE.BOOKING_MST_FK = BST.BOOKING_MST_PK");
            sb.Append("   AND JSE.JOB_CARD_TRN_PK = JTSEC.JOB_CARD_TRN_FK(+)");
            sb.Append("   AND JSE.MBL_MAWB_FK = MAWB.MAWB_EXP_TBL_PK");
            sb.Append("   AND BST.PORT_MST_POL_FK = POLMST.PORT_MST_PK(+)");
            sb.Append("   AND BST.PORT_MST_POD_FK = PODMST.PORT_MST_PK(+)");
            sb.Append("   AND JSE.HBL_HAWB_FK = HBL.HAWB_EXP_TBL_PK");
            sb.Append("   AND BST.COL_PLACE_MST_FK = COLPMST.PLACE_PK(+)");
            sb.Append("   AND BST.DEL_PLACE_MST_FK = DELPMST.PLACE_PK(+)");
            sb.Append("   AND JTSEC.PACK_TYPE_MST_FK = PY.PACK_TYPE_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPING_TERMS_MST_FK = STMST.SHIPPING_TERMS_MST_PK(+)");
            sb.Append("   AND JSE.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK(+)");
            sb.Append("   AND SHIPPER.CUSTOMER_MST_PK = SHIPPERDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND SHIPPERDTLS.ADM_COUNTRY_MST_FK = SHIPPERCNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK(+)");
            sb.Append("   AND CONSIGNEE.CUSTOMER_MST_PK = CONSIGNEEDTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND CONSIGNEEDTLS.ADM_COUNTRY_MST_FK = CONSIGNEECNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.NOTIFY1_CUST_MST_FK = NOTIFY1.CUSTOMER_MST_PK(+)");
            sb.Append("   AND NOTIFY1.CUSTOMER_MST_PK = NOTIFY1DTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOTIFY1DTLS.ADM_COUNTRY_MST_FK = NOTIFY1CNT.COUNTRY_MST_PK(+)");
            sb.Append("   AND JSE.NOTIFY2_CUST_MST_FK = NOTIFY2.CUSTOMER_MST_PK(+)");
            sb.Append("   AND NOTIFY2.CUSTOMER_MST_PK = NOTIFY2DTLS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOTIFY2DTLS.ADM_COUNTRY_MST_FK = NOTIFY2CNT.COUNTRY_MST_PK(+)");
            sb.Append("  AND JSE.BUSINESS_TYPE = 1 AND JSE.PROCESS_TYPE = 1 ");
            sb.Append("   AND MAWB.MAWB_EXP_TBL_PK  IN (" + MBLPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agnt DTLS.
        /// </summary>
        /// <param name="JOBPK">The jobpk.</param>
        /// <returns></returns>
        public DataSet FetchAgntDtls(string JOBPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT AG.AGENT_MST_PK,");
            sb.Append("       AG.AGENT_NAME,");
            sb.Append("       AGC.ADM_ADDRESS_1 AGNT_ADD1,");
            sb.Append("       AGC.ADM_ADDRESS_2 AGNT_ADD2,");
            sb.Append("       AGC.ADM_ADDRESS_3 AGNT_ADD3,");
            sb.Append("       AGC.ADM_CITY AGNT_CITY,");
            sb.Append("       AGC.ADM_ZIP_CODE AGNT_ZIP,");
            sb.Append("       AGC.ADM_PHONE_NO_1 AGNT_PHONE,");
            sb.Append("       AGC.ADM_FAX_NO AGNT_FAX,");
            sb.Append("       AGC.ADM_EMAIL_ID AGNT_EMAIL,");
            sb.Append("       CM.COUNTRY_NAME AGNT_CNTRY,");
            sb.Append("       MBL.MBL_REF_NO");
            sb.Append("  FROM MASTER_JC_SEA_EXP_TBL MJ,");
            sb.Append("       AGENT_MST_TBL         AG,");
            sb.Append("       MBL_EXP_TBL           MBL,");
            sb.Append("       AGENT_CONTACT_DTLS    AGC,");
            sb.Append("       COUNTRY_MST_TBL       CM");
            sb.Append(" WHERE MJ.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)");
            sb.Append("   AND AGC.ADM_COUNTRY_MST_FK = CM.COUNTRY_MST_PK");
            sb.Append("   AND AGC.AGENT_MST_FK(+) = AG.AGENT_MST_PK");
            sb.Append("   AND MBL.MBL_EXP_TBL_PK (+)= MJ.MBL_FK");
            sb.Append("   AND MJ.MASTER_JC_SEA_EXP_PK IN (" + JOBPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the agnt DTLS new.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchAgntDtlsNew(string MBLPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT AG.AGENT_MST_PK,");
            sb.Append("       AG.AGENT_NAME,");
            sb.Append("       AGC.ADM_ADDRESS_1  AGNT_ADD1,");
            sb.Append("       AGC.ADM_ADDRESS_2  AGNT_ADD2,");
            sb.Append("       AGC.ADM_ADDRESS_3  AGNT_ADD3,");
            sb.Append("       AGC.ADM_CITY       AGNT_CITY,");
            sb.Append("       AGC.ADM_ZIP_CODE   AGNT_ZIP,");
            sb.Append("       AGC.ADM_PHONE_NO_1 AGNT_PHONE,");
            sb.Append("       AGC.ADM_FAX_NO     AGNT_FAX,");
            sb.Append("       AGC.ADM_EMAIL_ID   AGNT_EMAIL,");
            sb.Append("       CM.COUNTRY_NAME    AGNT_CNTRY,");
            sb.Append("       MBL.MBL_REF_NO");
            sb.Append("  FROM MASTER_JC_SEA_EXP_TBL MJ,");
            sb.Append("       MBL_EXP_TBL           MBL,");
            sb.Append("       AGENT_MST_TBL         AG,");
            sb.Append("       AGENT_CONTACT_DTLS    AGC,");
            sb.Append("       COUNTRY_MST_TBL       CM");
            sb.Append(" WHERE MJ.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)");
            sb.Append("   AND AGC.ADM_COUNTRY_MST_FK = CM.COUNTRY_MST_PK");
            sb.Append("   AND AGC.AGENT_MST_FK(+) = AG.AGENT_MST_PK");
            sb.Append("   AND MBL.MBL_EXP_TBL_PK = MJ.MBL_FK");
            sb.Append("   AND MBL.MBL_EXP_TBL_PK IN (" + MBLPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Fetches the air agnt DTLS new.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <returns></returns>
        public DataSet FetchAirAgntDtlsNew(string MBLPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT AG.AGENT_MST_PK,");
            sb.Append("       AG.AGENT_NAME,");
            sb.Append("       AGC.ADM_ADDRESS_1  AGNT_ADD1,");
            sb.Append("       AGC.ADM_ADDRESS_2  AGNT_ADD2,");
            sb.Append("       AGC.ADM_ADDRESS_3  AGNT_ADD3,");
            sb.Append("       AGC.ADM_CITY       AGNT_CITY,");
            sb.Append("       AGC.ADM_ZIP_CODE   AGNT_ZIP,");
            sb.Append("       AGC.ADM_PHONE_NO_1 AGNT_PHONE,");
            sb.Append("       AGC.ADM_FAX_NO     AGNT_FAX,");
            sb.Append("       AGC.ADM_EMAIL_ID   AGNT_EMAIL,");
            sb.Append("       CM.COUNTRY_NAME    AGNT_CNTRY,");
            sb.Append("       MBL.MAWB_REF_NO");
            sb.Append("  FROM MAWB_EXP_TBL MBL,");
            sb.Append("       AGENT_MST_TBL AG,");
            sb.Append("       AGENT_CONTACT_DTLS AGC,");
            sb.Append("       COUNTRY_MST_TBL CM,");
            sb.Append("       (SELECT DISTINCT J.MASTER_JC_FK,");
            sb.Append("                        MJ.DP_AGENT_MST_FK,");
            sb.Append("                        J.MBL_MAWB_FK");
            sb.Append("          FROM JOB_CARD_TRN J, MASTER_JC_AIR_EXP_TBL MJ");
            sb.Append("         WHERE MJ.MASTER_JC_AIR_EXP_PK(+) = J.MASTER_JC_FK) A");
            sb.Append(" WHERE A.DP_AGENT_MST_FK = AG.AGENT_MST_PK(+)");
            sb.Append("   AND AGC.ADM_COUNTRY_MST_FK = CM.COUNTRY_MST_PK");
            sb.Append("   AND AGC.AGENT_MST_FK(+) = AG.AGENT_MST_PK");
            sb.Append("   AND MBL.MAWB_EXP_TBL_PK = A.MBL_MAWB_FK");
            sb.Append("   AND MBL.MAWB_EXP_TBL_PK IN (" + MBLPK + ") ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Cargo Manifrst for MJC"

        #region "Update File Name"

        /// <summary>
        /// Updates the manifest FLG.
        /// </summary>
        /// <param name="MBLPK">The MBLPK.</param>
        /// <param name="JOBPK">The jobpk.</param>
        /// <param name="BIZType">Type of the biz.</param>
        /// <param name="Process">The process.</param>
        /// <returns></returns>
        public bool UpdateManifestFlg(string MBLPK, string JOBPK, int BIZType, string Process)
        {
            string RemQuery = null;
            WorkFlow objwk = new WorkFlow();
            if (Process == "Export")
            {
                if (BIZType == 1)
                {
                    RemQuery = " UPDATE JOB_CARD_TRN J SET J.CARGO_MANIFEST = 1 WHERE J.MBL_MAWB_FK IN (" + MBLPK + ") AND  J.BUSINESS_TYPE = 1 AND J.PROCESS_TYPE = 1 ";
                }
                else
                {
                    RemQuery = " UPDATE JOB_CARD_TRN J SET J.CARGO_MANIFEST = 1 WHERE J.MBL_MAWB_FK IN (" + MBLPK + ") AND  J.BUSINESS_TYPE = 2 AND J.PROCESS_TYPE = 1 ";
                }
            }
            else
            {
                if (BIZType == 2)
                {
                    RemQuery = " UPDATE JOB_CARD_TRN J SET J.CARGO_MANIFEST = 1 WHERE J.JOB_CARD_TRN_PK IN (" + JOBPK + ") AND  J.BUSINESS_TYPE = 2 AND J.PROCESS_TYPE = 2 ";
                }
                else
                {
                    RemQuery = " UPDATE JOB_CARD_TRN J SET J.CARGO_MANIFEST = 1 WHERE J.JOB_CARD_TRN_PK IN (" + JOBPK + ") AND  J.BUSINESS_TYPE = 1 AND J.PROCESS_TYPE = 2 ";
                }
            }

            try
            {
                objwk.OpenConnection();
                objwk.ExecuteCommands(RemQuery);
                return true;
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
                return false;
            }
            finally
            {
                objwk.MyCommand.Connection.Close();
            }
        }

        #endregion "Update File Name"

        #region " Load Commodity Group "

        /// <summary>
        /// Loads the commodity GRP.
        /// </summary>
        /// <returns></returns>
        public DataSet LoadCommodityGrp()
        {
            string Sql = null;
            Sql += " select 0 COMMODITY_GROUP_PK,";
            Sql += " 'All' COMMODITY_GROUP_CODE, ";
            Sql += " 'All' COMMODITY_GROUP_DESC, ";
            Sql += " 0 VERSION_NO from dual UNION ";
            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1 ";
            Sql += " ORDER BY COMMODITY_GROUP_PK ";

            try
            {
                return (new WorkFlow()).GetDataSet(Sql);
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

        #endregion " Load Commodity Group "

        #region "Fetch Status"

        /// <summary>
        /// Fetches the drop down values.
        /// </summary>
        /// <param name="Flag">The flag.</param>
        /// <param name="ConfigID">The configuration identifier.</param>
        /// <returns></returns>
        public DataSet FetchDropDownValues(string Flag, string ConfigID)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT T.DD_VALUE, T.DD_ID");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL T");
            sb.Append(" WHERE T.DD_FLAG = '" + Flag + "'");
            sb.Append(" AND T.CONFIG_ID  = '" + ConfigID + "'");
            sb.Append(" ORDER BY T.DD_VALUE ");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception exp)
            {
                ErrorMessage = exp.Message;
                throw exp;
            }
        }

        #endregion "Fetch Status"

        #region "Fetch Import Caro Manifest"

        /// <summary>
        /// Fetches the imp cargo manifest.
        /// </summary>
        /// <param name="VesselVoyPk">The vessel voy pk.</param>
        /// <param name="VesselName">Name of the vessel.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="Polfk">The polfk.</param>
        /// <param name="Podfk">The podfk.</param>
        /// <param name="MblRefNo">The MBL reference no.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Commodity_Grp_fk">The commodity_ GRP_FK.</param>
        /// <param name="Status">The status.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Log_Loc_fk">The log_ loc_fk.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="HBL_Pk">The hb l_ pk.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchImpCargoManifest(long VesselVoyPk = 0, string VesselName = "", string Voyage = "", int Polfk = 0, int Podfk = 0, string MblRefNo = "", int BizType = 0, int CargoType = 0, int Commodity_Grp_fk = 0, int Status = 0,
        Int32 Flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, int Log_Loc_fk = 0, string Customer = "", int HBL_Pk = 0, string Consignee = "", string DPAgent = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                var _with12 = objWF.MyCommand.Parameters;
                _with12.Add("VESSEL_VOY_FK_IN", VesselVoyPk).Direction = ParameterDirection.Input;
                _with12.Add("VESSEL_FLIGHT_IN", getDefault(VesselName, "")).Direction = ParameterDirection.Input;
                _with12.Add("VOY_FLIGHT_IN", getDefault(Voyage, "")).Direction = ParameterDirection.Input;
                _with12.Add("POL_FK_IN", Polfk).Direction = ParameterDirection.Input;
                _with12.Add("POD_FK_IN", Podfk).Direction = ParameterDirection.Input;
                _with12.Add("MBL_REF_NO_IN", getDefault(MblRefNo, "")).Direction = ParameterDirection.Input;
                _with12.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with12.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with12.Add("COMMODITY_GRP_FK_IN", Commodity_Grp_fk).Direction = ParameterDirection.Input;
                _with12.Add("LOCTION_FK_IN", Log_Loc_fk).Direction = ParameterDirection.Input;
                _with12.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                _with12.Add("LD_FLAG_IN", Flag).Direction = ParameterDirection.Input;
                _with12.Add("CUST_IN", getDefault(Customer, "")).Direction = ParameterDirection.Input;

                _with12.Add("HBL_PK_IN", getDefault(HBL_Pk, "")).Direction = ParameterDirection.Input;
                _with12.Add("CONSIGNEE_IN", getDefault(Consignee, "")).Direction = ParameterDirection.Input;
                _with12.Add("DPAGENT_IN", getDefault(DPAgent, "")).Direction = ParameterDirection.Input;

                _with12.Add("MASTER_PAGE_SIZE_IN", MasterPageSize).Direction = ParameterDirection.Input;
                _with12.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with12.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.Output;
                _with12.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("IMPORT_CARGO_MANIFEST_PKG", "FETCH_IMPORT_CARGO");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                }
                return ds;
            }
            catch (Exception oraExp)
            {
                ErrorMessage = oraExp.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        #endregion "Fetch Import Caro Manifest"

        #region "Fetch Import Caro Manifest"

        /// <summary>
        /// Fetches the imp FRT manifest.
        /// </summary>
        /// <param name="VesselVoyPk">The vessel voy pk.</param>
        /// <param name="VesselName">Name of the vessel.</param>
        /// <param name="Voyage">The voyage.</param>
        /// <param name="Polfk">The polfk.</param>
        /// <param name="Podfk">The podfk.</param>
        /// <param name="MblRefNo">The MBL reference no.</param>
        /// <param name="BizType">Type of the biz.</param>
        /// <param name="CargoType">Type of the cargo.</param>
        /// <param name="Commodity_Grp_fk">The commodity_ GRP_FK.</param>
        /// <param name="Status">The status.</param>
        /// <param name="Flag">The flag.</param>
        /// <param name="CurrentPage">The current page.</param>
        /// <param name="TotalPage">The total page.</param>
        /// <param name="Log_Loc_fk">The log_ loc_fk.</param>
        /// <param name="Customer">The customer.</param>
        /// <param name="HBL_Pk">The hb l_ pk.</param>
        /// <param name="Consignee">The consignee.</param>
        /// <param name="DPAgent">The dp agent.</param>
        /// <returns></returns>
        public DataSet FetchImpFrtManifest(long VesselVoyPk = 0, string VesselName = "", string Voyage = "", int Polfk = 0, int Podfk = 0, string MblRefNo = "", int BizType = 0, int CargoType = 0, int Commodity_Grp_fk = 0, int Status = 0,
        Int32 Flag = 0, Int32 CurrentPage = 0, Int32 TotalPage = 0, int Log_Loc_fk = 0, string Customer = "", int HBL_Pk = 0, string Consignee = "", string DPAgent = "")
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                var _with13 = objWF.MyCommand.Parameters;
                _with13.Add("VESSEL_VOY_FK_IN", VesselVoyPk).Direction = ParameterDirection.Input;
                _with13.Add("VESSEL_FLIGHT_IN", getDefault(VesselName, "")).Direction = ParameterDirection.Input;
                _with13.Add("VOY_FLIGHT_IN", getDefault(Voyage, "")).Direction = ParameterDirection.Input;
                _with13.Add("POL_FK_IN", Polfk).Direction = ParameterDirection.Input;
                _with13.Add("POD_FK_IN", Podfk).Direction = ParameterDirection.Input;
                _with13.Add("MBL_REF_NO_IN", getDefault(MblRefNo, "")).Direction = ParameterDirection.Input;
                _with13.Add("BIZ_TYPE_IN", BizType).Direction = ParameterDirection.Input;
                _with13.Add("CARGO_TYPE_IN", CargoType).Direction = ParameterDirection.Input;
                _with13.Add("COMMODITY_GRP_FK_IN", Commodity_Grp_fk).Direction = ParameterDirection.Input;
                _with13.Add("LOCTION_FK_IN", Log_Loc_fk).Direction = ParameterDirection.Input;
                _with13.Add("STATUS_IN", Status).Direction = ParameterDirection.Input;
                _with13.Add("LD_FLAG_IN", Flag).Direction = ParameterDirection.Input;
                _with13.Add("CUST_IN", getDefault(Customer, "")).Direction = ParameterDirection.Input;

                _with13.Add("HBL_PK_IN", getDefault(HBL_Pk, "")).Direction = ParameterDirection.Input;
                _with13.Add("CONSIGNEE_IN", getDefault(Consignee, "")).Direction = ParameterDirection.Input;
                _with13.Add("DPAGENT_IN", getDefault(DPAgent, "")).Direction = ParameterDirection.Input;

                _with13.Add("MASTER_PAGE_SIZE_IN", MasterPageSize).Direction = ParameterDirection.Input;
                _with13.Add("CURRENT_PAGE_IN", CurrentPage).Direction = ParameterDirection.InputOutput;
                _with13.Add("TOTAL_PAGE_IN", TotalPage).Direction = ParameterDirection.Output;
                _with13.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("IMPORT_CARGO_MANIFEST_PKG", "FETCH_IMPORT_FREIGHT");
                TotalPage = Convert.ToInt32(objWF.MyCommand.Parameters["TOTAL_PAGE_IN"].Value);
                CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                if (TotalPage == 0)
                {
                    CurrentPage = 0;
                }
                else
                {
                    CurrentPage = Convert.ToInt32(objWF.MyCommand.Parameters["CURRENT_PAGE_IN"].Value);
                }
                return ds;
            }
            catch (Exception oraExp)
            {
                ErrorMessage = oraExp.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        #endregion "Fetch Import Caro Manifest"

        #region "FetchPartys"

        /// <summary>
        /// Fetches the partyold.
        /// </summary>
        /// <param name="MBL_PK">The mb l_ pk.</param>
        /// <param name="BIZType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchPartyold(string MBL_PK, string BIZType)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT J.CONSIGNEE_CUST_MST_FK CONSIGNEE_PK,");
            sb.Append("       CONS.CUSTOMER_ID CONSIGNEE_ID,");
            sb.Append("       CONS.CUSTOMER_NAME CONSIGNEE_NAME,");
            sb.Append("       CCONS.ADM_EMAIL_ID || ',' ||");
            sb.Append("       (SELECT ROWTOCOL('SELECT CCT.EMAIL FROM CUSTOMER_CONTACT_TRN CCT WHERE CCT.CUSTOMER_MST_FK=' ||");
            sb.Append("                        J.CONSIGNEE_CUST_MST_FK) EMAIL");
            sb.Append("          FROM DUAL) CONSIGNEE_MAIL,");
            sb.Append("       CASE");
            sb.Append("         WHEN J.NOTIFY1_CUST_MST_FK IS NOT NULL THEN");
            sb.Append("          CNOT1.ADM_EMAIL_ID || ',' ||");
            sb.Append("          (SELECT ROWTOCOL('SELECT CCT.EMAIL FROM CUSTOMER_CONTACT_TRN CCT WHERE CCT.CUSTOMER_MST_FK=' ||");
            sb.Append("                           J.NOTIFY1_CUST_MST_FK) EMAIL");
            sb.Append("             FROM DUAL)");
            sb.Append("         ELSE");
            sb.Append("          CNOT1.ADM_EMAIL_ID || ',' ||");
            sb.Append("          (SELECT ROWTOCOL('SELECT CCT.EMAIL FROM CUSTOMER_CONTACT_TRN CCT WHERE CCT.CUSTOMER_MST_FK=' ||");
            sb.Append("                           J.NOTIFY2_CUST_MST_FK) EMAIL");
            sb.Append("             FROM DUAL)");
            sb.Append("       END NOTIFY_PARTY_EMAIL,");
            sb.Append("       CASE");
            sb.Append("         WHEN J.NOTIFY1_CUST_MST_FK IS NOT NULL THEN");
            sb.Append("          J.NOTIFY1_CUST_MST_FK");
            sb.Append("         ELSE");
            sb.Append("          J.NOTIFY2_CUST_MST_FK");
            sb.Append("       END NOTIFY_PARTY_PK,");
            sb.Append("       CASE");
            sb.Append("         WHEN J.NOTIFY1_CUST_MST_FK IS NOT NULL THEN");
            sb.Append("          NOT1.CUSTOMER_ID");
            sb.Append("         ELSE");
            sb.Append("          NOT2.CUSTOMER_ID");
            sb.Append("       END NOTIFY_PARTY_ID,");
            sb.Append("       CASE");
            sb.Append("         WHEN J.NOTIFY1_CUST_MST_FK IS NOT NULL THEN");
            sb.Append("          NOT1.CUSTOMER_NAME");
            sb.Append("         ELSE");
            sb.Append("          NOT2.CUSTOMER_NAME");
            sb.Append("       END NOTIFY_PARTY_NAME,");
            sb.Append("       J.DP_AGENT_MST_FK,");
            sb.Append("       DPA.AGENT_ID,");
            sb.Append("       DPA.AGENT_NAME,");
            sb.Append("       ACD.ADM_EMAIL_ID AGENT_MAIL");
            sb.Append("  FROM JOB_CARD_TRN          J,");
            sb.Append("       CUSTOMER_MST_TBL      CONS,");
            sb.Append("       CUSTOMER_MST_TBL      NOT1,");
            sb.Append("       CUSTOMER_MST_TBL      NOT2,");
            sb.Append("       AGENT_MST_TBL         DPA,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CCONS,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CNOT1,");
            sb.Append("       CUSTOMER_CONTACT_DTLS CNOT2,");
            sb.Append("       AGENT_CONTACT_DTLS    ACD");
            sb.Append(" WHERE J.MBL_MAWB_FK = " + MBL_PK + "");
            sb.Append("   AND J.BUSINESS_TYPE = " + BIZType + "");
            sb.Append("   AND J.CONSIGNEE_CUST_MST_FK = CONS.CUSTOMER_MST_PK(+)");
            sb.Append("   AND J.NOTIFY1_CUST_MST_FK = NOT1.CUSTOMER_MST_PK(+)");
            sb.Append("   AND J.NOTIFY2_CUST_MST_FK = NOT2.CUSTOMER_MST_PK(+)");
            sb.Append("   AND J.DP_AGENT_MST_FK = DPA.AGENT_MST_PK(+)");
            sb.Append("   AND CONS.CUSTOMER_MST_PK = CCONS.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOT1.CUSTOMER_MST_PK = CNOT1.CUSTOMER_MST_FK(+)");
            sb.Append("   AND NOT2.CUSTOMER_MST_PK = CNOT2.CUSTOMER_MST_FK(+)");
            sb.Append("   AND DPA.AGENT_MST_PK = ACD.AGENT_MST_FK(+)");
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraExp)
            {
                ErrorMessage = oraExp.Message;
                throw oraExp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "FetchPartys"

        #region "Fetch Import Caro Manifest"

        /// <summary>
        /// Fetches the party.
        /// </summary>
        /// <param name="MBL_PK">The mb l_ pk.</param>
        /// <param name="BIZType">Type of the biz.</param>
        /// <returns></returns>
        public DataSet FetchParty(string MBL_PK, string BIZType)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();
            try
            {
                var _with14 = objWF.MyCommand.Parameters;
                _with14.Add("MBL_PK_IN", MBL_PK).Direction = ParameterDirection.Input;
                _with14.Add("BIZ_TYPE_IN", BIZType).Direction = ParameterDirection.Input;
                _with14.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;
                ds = objWF.GetDataSet("IMPORT_CARGO_MANIFEST_PKG", "FETCH_PARTY_DETAILS");
                return ds;
            }
            catch (Exception oraExp)
            {
                ErrorMessage = oraExp.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        #endregion "Fetch Import Caro Manifest"

        #region "Fetch Temperature"

        /// <summary>
        /// Fetches the temperatur.
        /// </summary>
        /// <param name="HBLPk">The HBL pk.</param>
        /// <returns></returns>
        public DataSet FetchTemperatur(string HBLPk)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(1000);
            DataSet CommDs = new DataSet();
            WorkFlow objWF = new WorkFlow();
            sb.Append("  SELECT CTMT.CONTAINER_TYPE_MST_ID,");
            sb.Append(" C.VENTILATION,");
            sb.Append(" C.MIN_TEMP,");
            sb.Append(" C.MIN_TEMP_UOM,");
            sb.Append(" C.MAX_TEMP,");
            sb.Append(" C.MAX_TEMP_UOM");
            sb.Append(" FROM BOOKING_TRN_SPL_REQ C, BOOKING_TRN T, CONTAINER_TYPE_MST_TBL CTMT ");
            sb.Append(" WHERE(C.BOOKING_TRN_FK = T.BOOKING_TRN_PK) ");
            sb.Append(" AND CTMT.CONTAINER_TYPE_MST_PK(+) = T.CONTAINER_TYPE_MST_FK ");
            sb.Append(" AND T.BOOKING_MST_FK =");
            sb.Append("(SELECT JCT.BOOKING_MST_FK ");
            sb.Append(" FROM JOB_CARD_TRN JCT");
            sb.Append(" WHERE JCT.JOB_CARD_TRN_PK IN");
            sb.Append(" (SELECT H.JOB_CARD_SEA_EXP_FK");
            sb.Append(" FROM HBL_EXP_TBL H ");
            sb.Append("  WHERE H.HBL_REF_NO IN");
            sb.Append(" (SELECT HBLPK.HBL_REF_NO");
            sb.Append("  FROM HBL_EXP_TBL HBLPK");
            sb.Append(" WHERE HBLPK.HBL_EXP_TBL_PK IN (" + HBLPk + "))))");
            CommDs = objWF.GetDataSet(sb.ToString());
            return CommDs;
        }

        #endregion "Fetch Temperature"

        #region "Fetch Import Caro Manifest"

        /// <summary>
        /// Fetches the cont summary details.
        /// </summary>
        /// <param name="BIZ_TYPE_IN">The bi z_ typ e_ in.</param>
        /// <param name="CARGO_TYPE_IN">The carg o_ typ e_ in.</param>
        /// <param name="STATUS_TYPE_IN">The statu s_ typ e_ in.</param>
        /// <param name="MBL_REF_NO_IN">The mb l_ re f_ n o_ in.</param>
        /// <param name="HBL_REF_NO_IN">The hb l_ re f_ n o_ in.</param>
        /// <param name="VESSEL_NAME_IN">The vesse l_ nam e_ in.</param>
        /// <param name="VOYAGE_IN">The voyag e_ in.</param>
        /// <param name="SHIPPER_NAME_IN">The shippe r_ nam e_ in.</param>
        /// <param name="CONSIGNEE_NAME_IN">The consigne e_ nam e_ in.</param>
        /// <param name="DPAGENT_NAME_IN">The dpagen t_ nam e_ in.</param>
        /// <param name="POL_ID_IN">The po l_ i d_ in.</param>
        /// <param name="POD_ID_IN">The po d_ i d_ in.</param>
        /// <param name="COMMODITY_GRP_FK_IN">The commodit y_ gr p_ f k_ in.</param>
        /// <param name="PROCESS_TYPE_IN">The proces s_ typ e_ in.</param>
        /// <returns></returns>
        public DataSet FetchContSummaryDetails(Int32 BIZ_TYPE_IN = 0, Int32 CARGO_TYPE_IN = 0, Int32 STATUS_TYPE_IN = 0, string MBL_REF_NO_IN = "", string HBL_REF_NO_IN = "", string VESSEL_NAME_IN = "", string VOYAGE_IN = "", string SHIPPER_NAME_IN = "", string CONSIGNEE_NAME_IN = "", string DPAGENT_NAME_IN = "",
        string POL_ID_IN = "", string POD_ID_IN = "", int COMMODITY_GRP_FK_IN = 0, int PROCESS_TYPE_IN = 0)
        {
            WorkFlow objWF = new WorkFlow();
            DataSet ds = new DataSet();

            try
            {
                var _with15 = objWF.MyCommand.Parameters;
                _with15.Add("BIZ_TYPE_IN", BIZ_TYPE_IN).Direction = ParameterDirection.Input;
                _with15.Add("CARGO_TYPE_IN", CARGO_TYPE_IN).Direction = ParameterDirection.Input;
                _with15.Add("STATUS_TYPE_IN", STATUS_TYPE_IN).Direction = ParameterDirection.Input;
                _with15.Add("MBL_REF_NO_IN", getDefault(MBL_REF_NO_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("HBL_REF_NO_IN", getDefault(HBL_REF_NO_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("VESSEL_NAME_IN", getDefault(VESSEL_NAME_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("VOYAGE_IN", VOYAGE_IN).Direction = ParameterDirection.Input;
                _with15.Add("SHIPPER_NAME_IN", getDefault(SHIPPER_NAME_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("CONSIGNEE_NAME_IN", getDefault(CONSIGNEE_NAME_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("DPAGENT_NAME_IN", getDefault(DPAGENT_NAME_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("POL_ID_IN", getDefault(POL_ID_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("POD_ID_IN", getDefault(POD_ID_IN, "")).Direction = ParameterDirection.Input;
                _with15.Add("COMMODITY_GRP_FK_IN", COMMODITY_GRP_FK_IN).Direction = ParameterDirection.Input;
                _with15.Add("PROCESS_TYPE_IN", PROCESS_TYPE_IN).Direction = ParameterDirection.Input;
                _with15.Add("IMP_CARGO_CUR", OracleDbType.RefCursor).Direction = ParameterDirection.Output;

                ds = objWF.GetDataSet("IMPORT_CARGO_MANIFEST_PKG", "FETCH_CONTAINER_SUM");
                return ds;
            }
            catch (Exception oraExp)
            {
                ErrorMessage = oraExp.Message;
            }
            finally
            {
                objWF.MyConnection.Close();
            }
            return new DataSet();
        }

        #endregion "Fetch Import Caro Manifest"
    }
}