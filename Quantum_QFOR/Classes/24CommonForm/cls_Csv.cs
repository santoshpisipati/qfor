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
//'*  Modified DateTime(DD-MON-YYYY)              Modified By                             Remarks (Bugs Related)
//'*
//'*
//'***************************************************************************************************************

#endregion "Comments"

using System;
using System.Data;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class cls_Csv 
    {
        public string Export(DataSet ds, string strHeader = "")
        {
            //Dim body As System.Text.StringBuilder
            System.Text.StringBuilder record = new System.Text.StringBuilder();
            try
            {
                if (!string.IsNullOrEmpty(strHeader))
                {
                    record.Append(strHeader);
                    record.Append("");
                }
                foreach (DataRow row in ds.Tables[0].Rows)
                {
                    object[] arr = row.ItemArray;
                    for (int i = 0; i <= arr.Length - 1; i++)
                    {
                        if (arr[i].ToString().IndexOf(",") > 0)
                        {
                            if (!string.IsNullOrEmpty(strHeader))
                            {
                                if (arr[i].ToString().Trim() != Convert.ToString(9999999999999L))
                                {
                                    record.Append(Environment.NewLine + arr[i].ToString() + Environment.NewLine + ",");
                                }
                            }
                            else
                            {
                                record.Append(Environment.NewLine + arr[i].ToString() + Environment.NewLine + ",");
                            }
                        }
                        else
                        {
                            if (!string.IsNullOrEmpty(strHeader))
                            {
                                if (arr[i].ToString().Trim() != Convert.ToString(9999999999999L))
                                {
                                    record.Append(arr[i].ToString() + ",");
                                }
                            }
                            else
                            {
                                record.Append(arr[i].ToString() + ",");
                            }
                        }
                    }
                    record.Remove(record.Length - 1, 1);
                    record.Append(false);
                }
                return record.ToString();
                //Manjunath  PTS ID:Sep-02  12/09/2011
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
    }
}