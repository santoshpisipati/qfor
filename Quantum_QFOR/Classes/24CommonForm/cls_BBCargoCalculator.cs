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

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_BBCargoCalculator : CommonFeatures
    {
        /// <summary>
        /// Gets the detail.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="check">The check.</param>
        /// <param name="ShipConPK">The ship con pk.</param>
        /// <param name="Sno">The sno.</param>
        /// <param name="Base">The base.</param>
        /// <returns></returns>
        public DataSet GetDetail(Int64 pk, string check, int ShipConPK, int Sno = 1, string Base = "")
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            int baseFlag = 0;
            baseFlag = 0;
            if ((Base.ToLower() != check.ToLower()))
            {
                baseFlag = 1;
            }

            if (check == "PBOOKING")
            {
                if (baseFlag == 0)
                {
                    sb.Append("SELECT  ROWNUM SNO, Q.LENGTH as Length, Q.WIDTH as Width, Q.HEIGHT as Height,  Q.WTUNIT as WTUNIT,Q.NO_OF_PIECES as NOP,  Q.WEIGHT as Uom, 0 LTON ,Q.CBM as Cube,0 CBFT ,'" + Sno + "'QuotRef, q.PROV_CARGO_CALL_PK CARGO_CALL_PK, 0 Measurement,  ''REFERENCE_FK ,  ''DELFLAG");
                }
                else
                {
                    sb.Append("SELECT  ROWNUM SNO, Q.LENGTH as Length, Q.WIDTH as Width, Q.HEIGHT as Height,  Q.WTUNIT as WTUNIT,Q.NO_OF_PIECES as NOP,  Q.WEIGHT as Uom, 0 LTON ,Q.CBM as Cube,0 CBFT ,'" + Sno + "'QuotRef, 0 CARGO_CALL_PK,0 Measurement,  ''REFERENCE_FK , ''DELFLAG");
                }

                sb.Append("  FROM PROV_BOOKING_CARGO_CALL Q");
                sb.Append(" WHERE Q.PROV_BOOKING_FK =" + pk);
            }
            else if (check == "QUOTATION")
            {
                if (baseFlag == 0)
                {
                    sb.Append("SELECT  ROWNUM SNO, Q.LENGTH as Length, Q.WIDTH as Width, Q.HEIGHT as Height,  Q.WTUNIT as WTUNIT,Q.NO_OF_PIECES as NOP,  Q.WEIGHT as Uom, 0 LTON ,Q.CBM as Cube,0 CBFT ,'" + Sno + "'QuotRef, q.QUOTATION_CARGO_CALL_PK CARGO_CALL_PK,0 Measurement,  ''REFERENCE_FK , ''DELFLAG");
                }
                else
                {
                    sb.Append("SELECT  ROWNUM SNO, Q.LENGTH as Length, Q.WIDTH as Width, Q.HEIGHT as Height,  Q.WTUNIT as WTUNIT,Q.NO_OF_PIECES as NOP,  Q.WEIGHT as Uom, 0 LTON ,Q.CBM as Cube,0 CBFT ,'" + Sno + "'QuotRef, 0 CARGO_CALL_PK,0 Measurement,  ''REFERENCE_FK , ''DELFLAG");
                }
                sb.Append("  FROM QUOTATION_CARGO_CALL_TBL Q");

                sb.Append(" WHERE Q.QUOTATION_MST_FK =" + pk);
            }
            else if (check == "BBQUOTATION")
            {
                sb.Append("SELECT ROWNUM SNO,");
                sb.Append("       Q.CARGO_LENGTH AS LENGTH,");
                sb.Append("       Q.CARGO_WIDTH AS WIDTH,");
                sb.Append("       Q.CARGO_HEIGHT AS HEIGHT,");
                sb.Append("       Q.CARGO_DIVISION_FACT AS WTUNIT,");
                sb.Append("       Q.CARGO_NOP AS NOP,");
                sb.Append("       Q.CARGO_ACTUAL_WT AS UOM,");
                sb.Append("       0 LTON,");
                sb.Append("       Q.CARGO_CUBE AS CUBE,");
                sb.Append("       0 CBFT,");
                //sb.Append("       '1' QUOTREF,")
                sb.Append("       '" + Sno + "' QUOTREF,");
                sb.Append("       Q.QUOTATION_CARGO_CALC_PK CARGO_CALL_PK,");
                sb.Append("       Q.CARGO_MEASUREMENT Measurement,");
                sb.Append("       '' REFERENCE_FK,");
                sb.Append("       '' DELFLAG");
                sb.Append("  FROM QUOTATION_CARGO_CALC Q");
                sb.Append(" WHERE Q.QUOTATION_DTL_FK =" + pk);
            }
            else if (check == "BOOKING")
            {
                sb.Append("SELECT ROWNUM SNO,");
                sb.Append("       Q.CARGO_LENGTH AS LENGTH,");
                sb.Append("       Q.CARGO_WIDTH AS WIDTH,");
                sb.Append("       Q.CARGO_HEIGHT AS HEIGHT,");
                sb.Append("       Q.CARGO_DIVISION_FACT AS WTUNIT,");
                sb.Append("       Q.CARGO_NOP AS NOP,");
                sb.Append("       Q.CARGO_ACTUAL_WT AS UOM,");
                sb.Append("       0 LTON,");
                sb.Append("       Q.CARGO_CUBE AS CUBE,");
                sb.Append("       0 CBFT,");
                //sb.Append("       '1' QUOTREF,")
                sb.Append("       '" + Sno + "' QUOTREF,");
                sb.Append("       Q.BOOKING_CARGO_CALC_PK CARGO_CALL_PK,");
                sb.Append("       Q.CARGO_MEASUREMENT Measurement,");
                sb.Append("       '' REFERENCE_FK,");
                sb.Append("       '' DELFLAG");
                sb.Append("  FROM BOOKING_CARGO_CALC Q");
                sb.Append(" WHERE Q.BOOKING_MST_FK =" + pk);
            }
            else if (check == "FROMQUOTATION")
            {
                sb.Append("SELECT ROWNUM SNO,");
                sb.Append("       Q.CARGO_LENGTH AS LENGTH,");
                sb.Append("       Q.CARGO_WIDTH AS WIDTH,");
                sb.Append("       Q.CARGO_HEIGHT AS HEIGHT,");
                sb.Append("       Q.CARGO_DIVISION_FACT AS WTUNIT,");
                sb.Append("       Q.CARGO_NOP AS NOP,");
                sb.Append("       Q.CARGO_ACTUAL_WT AS UOM,");
                sb.Append("       0 LTON,");
                sb.Append("       Q.CARGO_CUBE AS CUBE,");
                sb.Append("       0 CBFT,");
                //sb.Append("       '1' QUOTREF,")
                sb.Append("       '" + Sno + "' QUOTREF,");
                sb.Append("       0 CARGO_CALL_PK,");
                sb.Append("       Q.CARGO_MEASUREMENT Measurement,");
                sb.Append("       '' REFERENCE_FK,");
                sb.Append("       '' DELFLAG");
                sb.Append("  FROM QUOTATION_CARGO_CALC Q");
                sb.Append(" WHERE Q.QUOTATION_MST_FK =" + pk);
            }

            try
            {
                return ObjWk.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Gets the detail new.
        /// </summary>
        /// <param name="RefPkNew">The reference pk new.</param>
        /// <returns></returns>
        public DataSet GetDetailNew(int RefPkNew)
        {
            WorkFlow ObjWk = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT ROWNUM SNO, Q.LENGTH as Length, Q.WIDTH as Width, Q.HEIGHT as Height,  Q.WTUNIT as WTUNIT,Q.NO_OF_PIECES as NOP,  Q.WEIGHT as Uom,Q.CBM as Cube,''DELFLAG");

            sb.Append("  FROM ENQURY_CARGO_CALL Q");
            sb.Append(" WHERE Q.Enquiry_Mst_Fk =" + RefPkNew);
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        /// <summary>
        /// Texts the detail.
        /// </summary>
        /// <param name="pk">The pk.</param>
        /// <param name="check">The check.</param>
        /// <returns></returns>
        public DataSet TextDetail(Int64 pk, string check)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            WorkFlow ObjWk = new WorkFlow();
            sb.Append("SELECT QQ.QUANTITY,QQ.WEIGHT,QQ.VOLUME FROM   ");
            if (check == "PBOOKING")
            {
                sb.Append("  PROV_BOOKING_CARGO  QQ  WHERE QQ.PROV_CARGO_PK=" + pk);
            }
            else if (check == "QUOTATION")
            {
                sb.Append("  QUOTATION_DTL_TBL  QQ  WHERE QQ.QUOTATION_DTL_PK=" + pk);
            }
            else if (check == "BOOKING")
            {
                sb.Append("   BOOKING_CARGO_TRN  QQ  WHERE QQ.BOOKING_CARGO_PK=" + pk);
            }
            else if (check == "ENQURY")
            {
                sb.Append("   ENQUIRY_BOOKING_DTL  QQ  WHERE QQ.ENQUIRY_BOOKING_PK=" + pk);
            }
            try
            {
                return ObjWk.GetDataSet(sb.ToString());
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
                //'Exception Handling Added by Gangadhar on 17/09/2011, PTS ID: SEP-01
            }
            catch (Exception ex)
            {
                ErrorMessage = ex.Message;
                throw ex;
            }
        }

        #region "Fetch Grid for Excel"

        /// <summary>
        /// Fetches the grid.
        /// </summary>
        /// <returns></returns>
        public DataSet FetchGrid()
        {
            WorkFlow objWK = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("    SELECT '' SNO,");
            sb.Append("    '' ITEMN0,");
            sb.Append("    '' LENGTH,");
            sb.Append("    '' WIDTH,");
            sb.Append("    '' HEIGHT,");
            sb.Append("    '' WTUNIT,");
            sb.Append("    '' NOP,");
            sb.Append("    '' UOM,");
            sb.Append("    '' CUBE,");
            sb.Append("    '' QUOTREF,");
            sb.Append("    '' CARGO_CALL_PK,");
            sb.Append("    '' REFERENCE_FK");
            sb.Append("  FROM DUAL");

            try
            {
                objWK.OpenConnection();
                return objWK.GetDataSet(sb.ToString());
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

        #endregion "Fetch Grid for Excel"
    }
}