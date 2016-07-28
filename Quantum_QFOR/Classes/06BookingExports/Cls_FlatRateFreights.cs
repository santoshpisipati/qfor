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
    public class Cls_FlatRateFreights : CommonFeatures
    {
        /// <summary>
        /// Gets the ot hstring.
        /// </summary>
        /// <param name="DT">The dt.</param>
        /// <param name="FreightCol">The freight col.</param>
        /// <param name="CurrencyCol">The currency col.</param>
        /// <param name="BasisCol">The basis col.</param>
        /// <param name="RateCol">The rate col.</param>
        /// <param name="PkCol">The pk col.</param>
        /// <param name="PkVal">The pk value.</param>
        /// <param name="ChargeableWt">The chargeable wt.</param>
        /// <param name="Amount">The amount.</param>
        /// <param name="ExchTable">The exch table.</param>
        /// <param name="pymtCol">The pymt col.</param>
        /// <param name="Session_Currency">The session_ currency.</param>
        /// <returns></returns>
        public static string GetOTHstring(DataTable DT, Int16 FreightCol, Int16 CurrencyCol, Int16 BasisCol, Int16 RateCol, Int16 PkCol, string PkVal, decimal ChargeableWt, decimal Amount = 0, DataTable ExchTable = null,
 Int16 pymtCol = 5, long Session_Currency = 0)
        {
            string functionReturnValue = null;

            try
            {
                Int16 RowCnt = default(Int16);
                DataRow DR = null;

                if (ExchTable == null)
                {
                    System.Text.StringBuilder str_Qry = new System.Text.StringBuilder(1000);
                    str_Qry.Append(" Select CURRENCY_MST_BASE_FK, ");
                    str_Qry.Append(" CURRENCY_MST_FK CURRENCY_MST_PK, ");
                    str_Qry.Append(" EXCHANGE_RATE ");
                    str_Qry.Append(" from V_EXCHANGE_RATE where ");
                    str_Qry.Append(" sysdate between FROM_DATE and TO_DATE and exch_rate_type_fk = 1 ");
                    str_Qry.Append(" AND currency_mst_base_fk=" + Session_Currency + "");

                    ExchTable = (new WorkFlow()).GetDataTable(str_Qry.ToString());
                }
                Amount = 0;
                for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                {
                    if (DT.Rows[RowCnt][PkCol] == PkVal)
                    {
                        foreach (DataRow DR_loopVariable in ExchTable.Rows)
                        {
                            DR = DR_loopVariable;
                            if (DR["CURRENCY_MST_PK"] == DT.Rows[RowCnt][CurrencyCol])
                            {
                                if (string.IsNullOrEmpty(Convert.ToString(DT.Rows[RowCnt][BasisCol])))
                                {
                                    //Amount += getDefault(DT.Rows[RowCnt][RateCol], 0) * DR["EXCHANGE_RATE"] * ChargeableWt;
                                }
                                else
                                {
                                    //if (DT.Rows[RowCnt][BasisCol] == 2)
                                    //{
                                    //    Amount += getDefault(DT.Rows[RowCnt][RateCol], 0) * DR["EXCHANGE_RATE"];
                                    //}
                                    //else
                                    //{
                                    //    Amount += getDefault(DT.Rows[RowCnt][RateCol], 0) * DR["EXCHANGE_RATE"] * ChargeableWt;
                                    //}
                                }
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                        functionReturnValue += DT.Rows[RowCnt][FreightCol] + "~" + DT.Rows[RowCnt][CurrencyCol] + "~" + DT.Rows[RowCnt][BasisCol] + "~" + DT.Rows[RowCnt][RateCol] + "~" + DT.Rows[RowCnt][pymtCol] + "^";
                    }
                }
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return functionReturnValue;
        }

        #region " SHARED: Update Other Freight Table with supplied string "
        public static double UpdateOTHFreights(DataTable DT, string strRows, Int16 FreightCol, Int16 CurrencyCol, Int16 BasisCol, Int16 RateCol, Int16 PkCol, string PKValue, decimal ChargeableWeight, DataTable ExchTable = null,
        Int16 pymtCol = 8)
        {
            WorkFlow objWF = new WorkFlow();
            try
            {
                strRows = strRows.TrimEnd('^');
                bool ToCreate = false;
                if (DT == null)
                {
                    DT = objWF.GetDataTable(" Select FREIGHT_ELEMENT_MST_FK, " + " CURRENCY_MST_FK, CHARGE_BASIS, " + " 0 RATE, " + PKValue + " QUOTATION_MST_FK,1 PYMT_TYPE " + " from QUOTATION_OTHER_FREIGHT_TRN where 1 = 2 ");
                    ToCreate = true;
                }
                if (DT.Columns.Count == 0)
                {
                    DT = objWF.GetDataTable(" Select FREIGHT_ELEMENT_MST_FK, " + " CURRENCY_MST_FK, CHARGE_BASIS, " + " 0 RATE, " + PKValue + " QUOTATION_MST_FK,1 PYMT_TYPE " + " from QUOTATION_OTHER_FREIGHT_TRN where 1 = 2 ");
                    ToCreate = true;
                }

                if (ExchTable == null)
                {
                    ExchTable = objWF.GetDataTable(" Select CURRENCY_MST_BASE_FK, " + " CURRENCY_MST_FK CURRENCY_MST_PK, " + " EXCHANGE_RATE " + " from V_EXCHANGE_RATE where " + " sysdate between FROM_DATE and TO_DATE and exch_rate_type_fk = 1 ");
                }

                double sum = 0;
                Array arr = null;
                arr = strRows.Split('^');
                Int16 i = default(Int16);
                Int16 exRowCnt = default(Int16);
                Int16 RowCnt = default(Int16);
                Int16 ColCnt = default(Int16);
                Int16 sumCol = default(Int16);
                sumCol = RateCol;
                bool Flag = true;
                DataRow DR = null;
                for (i = 0; i <= arr.Length - 1; i++)
                {
                    Array innerArr = null;
                    innerArr = Convert.ToString(arr.GetValue(i)).Split('~');
                    if (ToCreate)
                    {
                        DR = DT.NewRow();
                        DR[FreightCol] = Convert.ToInt32(innerArr.GetValue(0));
                        DR[CurrencyCol] = Convert.ToInt32(innerArr.GetValue(1));
                        DR[BasisCol] = Convert.ToInt32(innerArr.GetValue(2));
                        DR[RateCol] = Convert.ToInt32(innerArr.GetValue(3));
                        DR[PkCol] = PKValue;
                        DR[pymtCol] = Convert.ToInt32(innerArr.GetValue(4));
                        DT.Rows.Add(DR);
                        for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                        {
                            if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_PK"] == Convert.ToString(innerArr.GetValue(1)))
                            {
                                if (Convert.ToInt32(innerArr.GetValue(4)) == 2)
                                {
                                    sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(3));
                                }
                                else
                                {
                                    sum += Convert.ToDouble(Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * (Convert.ToDecimal(innerArr.GetValue(3)) * Convert.ToDecimal(ChargeableWeight)));
                                }
                                break; // TODO: might not be correct. Was : Exit For
                            }
                        }
                    }
                    else
                    {
                        Flag = false;
                        for (RowCnt = 0; RowCnt <= DT.Rows.Count - 1; RowCnt++)
                        {
                            if (getDefault(DT.Rows[RowCnt][PkCol], "-1") == PKValue)
                            {
                                if (DT.Rows[RowCnt][FreightCol] == Convert.ToString(innerArr.GetValue(0)))
                                {
                                    DT.Rows[RowCnt][CurrencyCol] = Convert.ToInt32(innerArr.GetValue(1));
                                    DT.Rows[RowCnt][BasisCol] = Convert.ToInt32(innerArr.GetValue(2));
                                    DT.Rows[RowCnt][RateCol] = Convert.ToInt32(innerArr.GetValue(3));
                                    DT.Rows[RowCnt][pymtCol] = Convert.ToInt32(innerArr.GetValue(4));
                                    Flag = true;
                                    for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                                    {
                                        if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_PK"].ToString() == Convert.ToString(innerArr.GetValue(1)))
                                        {
                                            if (Convert.ToInt32(innerArr.GetValue(2)) == 2)
                                            {
                                                sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(3));
                                            }
                                            else
                                            {
                                                sum += Convert.ToDouble(Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * (Convert.ToDecimal(innerArr.GetValue(3)) * Convert.ToDecimal(ChargeableWeight)));
                                            }
                                            break; // TODO: might not be correct. Was : Exit For
                                        }
                                    }
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                        }
                        if (Flag == false)
                        {
                            DR = DT.NewRow();
                            if (DT.Rows.Count > 0)
                            {
                                for (ColCnt = 0; ColCnt <= DT.Columns.Count - 1; ColCnt++)
                                {
                                    DR[ColCnt] = DT.Rows[0][ColCnt];
                                }
                            }

                            DR[FreightCol] = Convert.ToInt32(innerArr.GetValue(0));
                            DR[CurrencyCol] = Convert.ToInt32(innerArr.GetValue(1));
                            DR[BasisCol] = Convert.ToInt32(innerArr.GetValue(2));
                            DR[RateCol] = Convert.ToInt32(innerArr.GetValue(3));
                            DR[PkCol] = PKValue;
                            DR[pymtCol] = Convert.ToInt32(innerArr.GetValue(4));
                            DT.Rows.Add(DR);
                            for (exRowCnt = 0; exRowCnt <= ExchTable.Rows.Count - 1; exRowCnt++)
                            {
                                if (ExchTable.Rows[exRowCnt]["CURRENCY_MST_PK"].ToString() == Convert.ToString(innerArr.GetValue(1)))
                                {
                                    if (Convert.ToInt32(innerArr.GetValue(2)) == 2)
                                    {
                                        sum += Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * Convert.ToInt32(innerArr.GetValue(3));
                                    }
                                    else
                                    {
                                        sum += Convert.ToDouble(Convert.ToInt32(ExchTable.Rows[exRowCnt]["EXCHANGE_RATE"]) * (Convert.ToDecimal(innerArr.GetValue(3)) * Convert.ToDecimal(ChargeableWeight)));
                                    }
                                    break; // TODO: might not be correct. Was : Exit For
                                }
                            }
                        }
                    }
                }
                return sum;
            }
            catch (OracleException Oraexp)
            {
                throw Oraexp;
            }
            catch (Exception ex)
            {
                return -1.0;
            }
        }
        #endregion
    }
}