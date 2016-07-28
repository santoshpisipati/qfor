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
using System.Configuration;
using System.Data;
using System.Text;

namespace Quantum_QFOR
{
    public class clsMultipleEnhanceSearch : CommonFeatures
    {
        #region "FetchPortsGroupPort"

        public DataSet FetchPortsGroupPort(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string PolID = "", string PolName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT          PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                //sb.Append("  AND PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                if (Convert.ToInt32(ConditionPK) == 2)
                {
                    sb.Append("   AND PMT.PORT_TYPE  = 2 ");
                }
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + selectedPKs + ")");
                }
                sb.Append("  UNION ");
                sb.Append("  SELECT          PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                //sb.Append("  AND PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                if (Convert.ToInt32(ConditionPK) == 2)
                {
                    sb.Append("   AND PMT.PORT_TYPE  = 2 ");
                }
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN (" + selectedPKs + ")");
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                //sb.Append("    AND PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                if (Convert.ToInt32(ConditionPK) == 2)
                {
                    sb.Append("   AND PMT.PORT_TYPE  = 2 ");
                }
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PMT.PORT_MST_PK <> 0");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + selectedPKs + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "FetchPortsGroupPort"

        #region "FetchPortsGroupPort"

        public DataSet FetchPortsGroupPod(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string PolID = "", string PolName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string ConditionPK1 = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                if (LoginPK != 0)
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN");
                    sb.Append("       (SELECT LWPT.PORT_MST_FK");
                    sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT");
                    sb.Append("         WHERE LWPT.LOCATION_MST_FK = " + LoginPK + ")");
                }
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                //sb.Append("   AND PMT.PORT_TYPE  = 2 ")

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + selectedPKs + ")");
                }
                sb.Append("  UNION ");
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                if (LoginPK != 0)
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN");
                    sb.Append("       (SELECT LWPT.PORT_MST_FK");
                    sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT");
                    sb.Append("         WHERE LWPT.LOCATION_MST_FK = " + LoginPK + ")");
                }
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                //sb.Append("   AND PMT.PORT_TYPE  = 2 ")

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN (" + selectedPKs + ")");
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                if (LoginPK != 0)
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN");
                    sb.Append("       (SELECT LWPT.PORT_MST_FK");
                    sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT");
                    sb.Append("         WHERE LWPT.LOCATION_MST_FK = " + LoginPK + ")");
                }
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                //sb.Append("   AND PMT.PORT_TYPE = 2 ")
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PMT.PORT_MST_PK <> 0");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "FetchPortsGroupPort"

        #region "POL"

        public DataSet FetchAllPol(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string ConditionPK4 = "", string ConditionPK5 = "",
        string ConditionPK6 = "", string TypedData = "", string PolID = "", string PolName = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       PGM.PORT_GRP_CODE,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PGM.PORT_GRP_MST_PK(+) = PMT.PORT_GRP_MST_FK");
                sb.Append("   AND PMT.PORT_TYPE IN (0, 2)");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PolPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK3))
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.TO_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                }

                if (!string.IsNullOrEmpty(ConditionPK4))
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.TO_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                }

                if (!string.IsNullOrEmpty(ConditionPK5))
                {
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.POLFK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 3));
                    }
                }

                sb.Append(" UNION ");
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       PGM.PORT_GRP_CODE,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PGM.PORT_GRP_MST_PK(+) = PMT.PORT_GRP_MST_FK");
                sb.Append("   AND PMT.PORT_TYPE IN (0, 2)");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + PolPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK3))
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.TO_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                }

                if (!string.IsNullOrEmpty(ConditionPK4))
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.TO_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                }

                if (!string.IsNullOrEmpty(ConditionPK5))
                {
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                }
                //'
                if (!string.IsNullOrEmpty(ConditionPK6))
                {
                    sb.Append("    AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                }
                //'
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (arrAnnSearchPks.GetValue(0).ToString() == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.POLFK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 3));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       PGM.PORT_GRP_CODE,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PGM.PORT_GRP_MST_PK(+) = PMT.PORT_GRP_MST_FK");
                sb.Append("   AND PMT.PORT_TYPE IN (0, 2)");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PolPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK3))
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.TO_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                }

                if (!string.IsNullOrEmpty(ConditionPK4))
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.TO_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                }

                if (!string.IsNullOrEmpty(ConditionPK5))
                {
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK6))
                {
                    sb.Append("    AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.POLFK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 3));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        public DataSet FetchFrtCrtPOL(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TypedData = "", string PolID = "", string PolName = "", string selectedPKs = "", long LoginPK = 0,
        bool IsAdmin = false, string AdditionalPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            string BizType = null;
            string LocationFk = null;
            string FrtHdrStatus = null;
            string HBLNo = null;
            string HBLDate = null;
            string BKGNo = null;
            string VslAirlineFk = null;
            string VoyFlightNo = null;
            string CargoType = null;
            string POLPks = null;
            string PODPks = null;
            string STR_ED_FRT_CORR = "";
            BizType = "";
            LocationFk = "";
            FrtHdrStatus = "";
            HBLNo = "";
            HBLDate = "";
            BKGNo = "";
            VslAirlineFk = "";
            VoyFlightNo = "";
            CargoType = "";
            POLPks = "";
            PODPks = "";

            string[] sRet = AdditionalPks.Split('$');

            if (sRet.Length > 1)
                BizType = sRet[1];
            if (sRet.Length > 2)
                LocationFk = sRet[2];
            if (sRet.Length > 3)
                FrtHdrStatus = sRet[3];
            if (sRet.Length > 4)
                HBLNo = sRet[4];
            if (sRet.Length > 5)
                HBLDate = sRet[5];
            if (sRet.Length > 6)
                BKGNo = sRet[6];
            if (sRet.Length > 7)
                VslAirlineFk = sRet[7];
            if (sRet.Length > 8)
                VoyFlightNo = sRet[8];
            if (sRet.Length > 9)
                CargoType = sRet[9];
            if (sRet.Length > 10)
                POLPks = sRet[10];
            if (sRet.Length > 11)
                PODPks = sRet[11];

            if (BizType == "1" | BizType == "2")
            {
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND VHBL.BIZ_TYPE = " + BizType;
            }

            if (Convert.ToInt32(FrtHdrStatus) > 0)
            {
                //CORRECTOR STATUS
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND FCH.STATUS=" + FrtHdrStatus;
            }
            if (!string.IsNullOrEmpty(HBLNo.Trim()))
            {
                //HBL NUMBER
                STR_ED_FRT_CORR += "AND UPPER(VHBL.HBL_REF_NO) LIKE '%" + HBLNo.ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(HBLDate.Trim()))
            {
                //HBL DATE
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND TO_DATE(VHBL.HBL_DATE,DATEFORMAT) = TO_DATE('" + HBLDate + "',DATEFORMAT) ";
            }
            if (!string.IsNullOrEmpty(BKGNo.Trim()))
            {
                //BOOKING NUMBER
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND UPPER(VHBL.BOOKING_REF_NO) LIKE '%" + BKGNo + "%'";
            }
            if (!string.IsNullOrEmpty(VoyFlightNo.Trim()))
            {
                //VOYAGE FLIGHT NO
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND UPPER(VHBL.VOYAGE_FLIGHT_NO) LIKE '%" + VoyFlightNo.ToUpper() + "%'";
            }
            if (Convert.ToInt32(VslAirlineFk) > 0)
            {
                //VOYAGE FLIGHT NO
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND VHBL.VOYAGE_TRN_FK = " + VslAirlineFk;
            }
            if (Convert.ToInt32(CargoType) > 0)
            {
                //CARGO TYPE
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND VHBL.CARGO_TYPE = " + CargoType;
            }
            //If Not IsNothing(POLPks) Then
            //    'POL FKS
            //    STR_ED_FRT_CORR = STR_ED_FRT_CORR & " AND POL_FK IN (" & POLPks & ")"
            //End If
            //If Not IsNothing(PODPks) Then
            //    'POD FKS
            //    STR_ED_FRT_CORR = STR_ED_FRT_CORR & " AND POD_FK IN (" & PODPks & ")"
            //End If

            sb.Append("SELECT DISTINCT VHBL.POL_FK PORT_MST_PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                VHBL.POL PORT_ID,");
            sb.Append("                VHBL.POL_NAME,");
            sb.Append("                '' EXTRA5,");
            sb.Append("                '' EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                '1' ACTIVE ");
            sb.Append("  FROM FREIGHT_CORRECTOR_HDR FCH, VIEW_EN_HBL_HAWB VHBL ");
            sb.Append(" WHERE ((VHBL.HBLPK = FCH.HBL_FK AND VHBL.BIZ_TYPE = 2) ");
            sb.Append("    OR (VHBL.HBLPK = FCH.HAWB_FK AND VHBL.BIZ_TYPE = 1)) ");
            if (!string.IsNullOrEmpty(POLPks.Trim()))
            {
                sb.Append(" AND POL_FK IN (" + POLPks + ")");
            }
            else
            {
                sb.Append(" AND 1=2 ");
            }
            if (!string.IsNullOrEmpty(PODPks.Trim()))
            {
                sb.Append(" AND POD_FK IN (" + PODPks + ")");
            }
            sb.Append(STR_ED_FRT_CORR);

            sb.Append(" UNION ");
            sb.Append("SELECT DISTINCT VHBL.POL_FK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                VHBL.POL PORT_ID,");
            sb.Append("                VHBL.POL_NAME,");
            sb.Append("                '' EXTRA5,");
            sb.Append("                '' EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                '0' ACTIVE ");
            sb.Append("  FROM FREIGHT_CORRECTOR_HDR FCH, VIEW_EN_HBL_HAWB VHBL ");
            sb.Append(" WHERE ((VHBL.HBLPK = FCH.HBL_FK AND VHBL.BIZ_TYPE = 2) ");
            sb.Append("    OR (VHBL.HBLPK = FCH.HAWB_FK AND VHBL.BIZ_TYPE = 1)) ");
            if (!string.IsNullOrEmpty(POLPks.Trim()))
            {
                sb.Append(" AND POL_FK NOT IN (" + POLPks + ")");
            }
            if (!string.IsNullOrEmpty(PODPks.Trim()))
            {
                sb.Append(" AND POD_FK IN (" + PODPks + ")");
            }
            sb.Append(STR_ED_FRT_CORR);

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        public DataSet FetchFrtCrtPOD(string PodPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TypedData = "", string PodID = "", string PodName = "", string selectedPKs = "", long LoginPK = 0,
        bool IsAdmin = false, string AdditionalPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PodPK = PodPK.TrimEnd(',');
            PodPK = PodPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            string BizType = null;
            string LocationFk = null;
            string FrtHdrStatus = null;
            string HBLNo = null;
            string HBLDate = null;
            string BKGNo = null;
            string VslAirlineFk = null;
            string VoyFlightNo = null;
            string CargoType = null;
            string POLPks = null;
            string PODPks = null;
            string STR_ED_FRT_CORR = "";
            BizType = "";
            LocationFk = "";
            FrtHdrStatus = "";
            HBLNo = "";
            HBLDate = "";
            BKGNo = "";
            VslAirlineFk = "";
            VoyFlightNo = "";
            CargoType = "";
            POLPks = "";
            PODPks = "";

            string[] sRet = AdditionalPks.Split('$');

            if (sRet.Length > 1)
                BizType = sRet[1];
            if (sRet.Length > 2)
                LocationFk = sRet[2];
            if (sRet.Length > 3)
                FrtHdrStatus = sRet[3];
            if (sRet.Length > 4)
                HBLNo = sRet[4];
            if (sRet.Length > 5)
                HBLDate = sRet[5];
            if (sRet.Length > 6)
                BKGNo = sRet[6];
            if (sRet.Length > 7)
                VslAirlineFk = sRet[7];
            if (sRet.Length > 8)
                VoyFlightNo = sRet[8];
            if (sRet.Length > 9)
                CargoType = sRet[9];
            if (sRet.Length > 10)
                POLPks = sRet[10];
            if (sRet.Length > 11)
                PODPks = sRet[11];

            if (BizType == "1" | BizType == "2")
            {
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND VHBL.BIZ_TYPE = " + BizType;
            }

            if (Convert.ToInt32(FrtHdrStatus) > 0)
            {
                //CORRECTOR STATUS
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND FCH.STATUS=" + FrtHdrStatus;
            }
            if (!string.IsNullOrEmpty(HBLNo.Trim()))
            {
                //HBL NUMBER
                STR_ED_FRT_CORR += "AND UPPER(VHBL.HBL_REF_NO) LIKE '%" + HBLNo.ToUpper() + "%'";
            }

            if (!string.IsNullOrEmpty(HBLDate.Trim()))
            {
                //HBL DATE
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND TO_DATE(VHBL.HBL_DATE,DATEFORMAT) = TO_DATE('" + HBLDate + "',DATEFORMAT) ";
            }
            if (!string.IsNullOrEmpty(BKGNo.Trim()))
            {
                //BOOKING NUMBER
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND UPPER(VHBL.BOOKING_REF_NO) LIKE '%" + BKGNo + "%'";
            }
            if (!string.IsNullOrEmpty(VoyFlightNo.Trim()))
            {
                //VOYAGE FLIGHT NO
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND UPPER(VHBL.VOYAGE_FLIGHT_NO) LIKE '%" + VoyFlightNo.ToUpper() + "%'";
            }
            if (Convert.ToInt32(VslAirlineFk) > 0)
            {
                //VOYAGE FLIGHT NO
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND VHBL.VOYAGE_TRN_FK = " + VslAirlineFk;
            }
            if (Convert.ToInt32(CargoType) > 0)
            {
                //CARGO TYPE
                STR_ED_FRT_CORR = STR_ED_FRT_CORR + " AND VHBL.CARGO_TYPE = " + CargoType;
            }
            //If Not IsNothing(POLPks) Then
            //    'POL FKS
            //    STR_ED_FRT_CORR = STR_ED_FRT_CORR & " AND POL_FK IN (" & POLPks & ")"
            //End If
            //If Not IsNothing(PODPks) Then
            //    'POD FKS
            //    STR_ED_FRT_CORR = STR_ED_FRT_CORR & " AND POD_FK IN (" & PODPks & ")"
            //End If

            sb.Append("SELECT DISTINCT VHBL.POD_FK PORT_MST_PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                VHBL.POD PORT_ID,");
            sb.Append("                VHBL.POD_NAME,");
            sb.Append("                '' EXTRA5,");
            sb.Append("                '' EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                '1' ACTIVE ");
            sb.Append("  FROM FREIGHT_CORRECTOR_HDR FCH, VIEW_EN_HBL_HAWB VHBL ");
            sb.Append(" WHERE ((VHBL.HBLPK = FCH.HBL_FK AND VHBL.BIZ_TYPE = 2) ");
            sb.Append("    OR (VHBL.HBLPK = FCH.HAWB_FK AND VHBL.BIZ_TYPE = 1)) ");
            if (!string.IsNullOrEmpty(POLPks.Trim()))
            {
                sb.Append(" AND POL_FK IN (" + POLPks + ")");
            }
            if (!string.IsNullOrEmpty(PODPks.Trim()))
            {
                sb.Append(" AND POD_FK IN (" + PODPks + ")");
            }
            else
            {
                sb.Append(" AND 1=2 ");
            }
            sb.Append(STR_ED_FRT_CORR);

            sb.Append(" UNION ");
            sb.Append("SELECT DISTINCT VHBL.POD_FK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                VHBL.POD PORT_ID,");
            sb.Append("                VHBL.POD_NAME,");
            sb.Append("                '' EXTRA5,");
            sb.Append("                '' EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                '0' ACTIVE ");
            sb.Append("  FROM FREIGHT_CORRECTOR_HDR FCH, VIEW_EN_HBL_HAWB VHBL ");
            sb.Append(" WHERE ((VHBL.HBLPK = FCH.HBL_FK AND VHBL.BIZ_TYPE = 2) ");
            sb.Append("    OR (VHBL.HBLPK = FCH.HAWB_FK AND VHBL.BIZ_TYPE = 1)) ");
            if (!string.IsNullOrEmpty(POLPks.Trim()))
            {
                sb.Append(" AND POL_FK IN (" + POLPks + ")");
            }
            if (!string.IsNullOrEmpty(PODPks.Trim()))
            {
                sb.Append(" AND POD_FK NOT IN (" + PODPks + ")");
            }
            sb.Append(STR_ED_FRT_CORR);

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        public DataSet FetchAllPolNEW(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string PolID = "", string PolName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string EnCondition = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = EnCondition.Split('$');
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                //strSQL.Append("  SELECT DISTINCT PMT.PORT_MST_PK,")
                //strSQL.Append("                '' EMPTY,")
                //strSQL.Append("                PMT.PORT_ID,")
                //strSQL.Append("                INITCAP(PMT.PORT_NAME),")
                //strSQL.Append("                '' EXTRA5,")
                //strSQL.Append("                '' EXTRA6,")
                //strSQL.Append("                '' EXTRA7,")
                //strSQL.Append("                '' EXTRA8,")
                //strSQL.Append("                '' EXTRA9,")
                //strSQL.Append("                '1' ACTIVE")
                //strSQL.Append("  FROM PORT_MST_TBL               PMT,")
                //strSQL.Append("       LOCATION_WORKING_PORTS_TRN LWPT,")
                //strSQL.Append("       QMPV_TRN_T_SERVICE         STRN,")
                //strSQL.Append("       QMPV_TRN_T_SERVICE_INFO    SINF")
                //strSQL.Append(" WHERE PMT.ACTIVE_FLAG = 1")
                //strSQL.Append("   AND PMT.PORT_MST_PK = LWPT.PORT_MST_FK")
                //strSQL.Append("   AND STRN.SERVICE_MST_PK = SINF.SERVICE_MST_FK")
                //strSQL.Append("   AND PMT.PORT_MST_PK = SINF.SERVICE_PORT_FK")
                //strSQL.Append("   AND PMT.PORT_TYPE = 'SEA'")
                sb.Append("SELECT      DISTINCT    PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GROUP_MST_TBL PGM");
                //sb.Append(" WHERE PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                //sb.Append("   AND PMT.BUSINESS_TYPE = 2")
                sb.Append("   WHERE PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PMT.PORT_MST_PK <> 0");

                if (Convert.ToString(arrAnnSearchPks.GetValue(7)) != "2")
                {
                    if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) != 1)
                    {
                        //  sb.Append("   AND PGM.PORT_GRP_MST_PK(+)=PMT.PORT_GRP_MST_FK")
                        sb.Append("   AND PMT.PORT_TYPE IN (0,2)");
                    }
                }
                // sb.Append(" ORDER BY PMT.PORT_ID  ")
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND PMT.BUSINESS_TYPE = " + ConditionPK);
                }
                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(EnCondition))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.polfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(EnCondition));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POL", EnCondition) + ")");
                    }
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.POLFK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.POLFK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.POLFK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }
                //If IsAdmin = True Then
                //    strSQL.Append("   AND LWPT.LOCATION_MST_FK IN")
                //    strSQL.Append("       (SELECT L.LOCATION_MST_PK")
                //    strSQL.Append("          FROM LOCATION_MST_TBL L")
                //    strSQL.Append("         START WITH L.LOCATION_MST_PK= " & LoginPK & "")
                //    strSQL.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)")
                //Else
                //    strSQL.Append("   AND LWPT.LOCATION_MST_FK = " & LoginPK & "")
                //End If

                //If ConditionPK <> "" Then
                //    strSQL.Append("    AND STRN.SERVICE_MST_PK IN ('" & ConditionPK.ToUpper.Replace(",", "','") & "')")
                //End If

                //If TradeID <> "" Then
                //    strSQL.Append("    AND STRN.SERVICE_ID IN ('" & TradeID.ToUpper.Replace(",", "','") & "')")
                //End If

                //If PolPK <> "" Then
                //    strSQL.Append("   AND PMT.PORT_MST_PK IN ('" & PolPK.ToUpper.Replace(",", "','") & "')")
                //End If

                sb.Append(" UNION ");
                sb.Append("SELECT    DISTINCT      PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GROUP_MST_TBL PGM");
                //sb.Append(" WHERE PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                //sb.Append("   AND PMT.BUSINESS_TYPE = 2")
                sb.Append("   WHERE PMT.ACTIVE_FLAG = 1");
                if (Convert.ToString(arrAnnSearchPks.GetValue(7)) != "2")
                {
                    if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) != 1)
                    {
                        //  sb.Append("   AND PGM.PORT_GRP_MST_PK(+)=PMT.PORT_GRP_MST_FK")
                        sb.Append("   AND PMT.PORT_TYPE IN (0,2)");
                    }
                }
                sb.Append("   AND PMT.PORT_MST_PK <> 0");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND PMT.BUSINESS_TYPE = " + ConditionPK);
                }
                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PortGroup))
                {
                    sb.Append(" AND UPPER(PGM.PORT_GRP_CODE) LIKE '%" + PortGroup.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(EnCondition))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.polfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(EnCondition));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POL", EnCondition) + ")");
                    }
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.POLFK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.POLFK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.POLFK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT    DISTINCT      PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GROUP_MST_TBL PGM");
                //sb.Append(" WHERE PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                //sb.Append("   AND PMT.BUSINESS_TYPE = 2")
                sb.Append("   WHERE 1 = 1 ");
                if (Convert.ToString(arrAnnSearchPks.GetValue(7)) != "2")
                {
                    if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) != 1)
                    {
                        //sb.Append("   AND PGM.PORT_GRP_MST_PK(+)=PMT.PORT_GRP_MST_FK")
                        sb.Append("   AND PMT.PORT_TYPE IN (0,2)");
                    }
                }
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PMT.PORT_MST_PK <> 0");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    if ((Convert.ToInt32(ConditionPK) == 3))
                    {
                        sb.Append("   AND PMT.BUSINESS_TYPE IN (1,2) ");
                    }
                    else
                    {
                        sb.Append("   AND PMT.BUSINESS_TYPE = " + ConditionPK);
                    }
                }
                //If IsAdmin = True Then
                //    strSQL.Append("   AND LWPT.LOCATION_MST_FK IN")
                //    strSQL.Append("       (SELECT L.LOCATION_MST_PK")
                //    strSQL.Append("          FROM LOCATION_MST_TBL L")
                //    strSQL.Append("         START WITH L.LOCATION_MST_PK= " & LoginPK & "")
                //    strSQL.Append("        CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)")
                //Else
                //    strSQL.Append("   AND LWPT.LOCATION_MST_FK = " & LoginPK & "")
                //End If

                //If ConditionPK <> "" Then
                //    strSQL.Append("    AND STRN.SERVICE_MST_PK IN ('" & ConditionPK.ToUpper.Replace(",", "','") & "')")
                //End If

                //If TradeID <> "" Then
                //    strSQL.Append("    AND STRN.SERVICE_ID IN ('" & TradeID.ToUpper.Replace(",", "','") & "')")
                //End If

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PortGroup))
                {
                    sb.Append(" AND UPPER(PGM.PORT_GRP_CODE) LIKE '%" + PortGroup.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(EnCondition))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.polfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(EnCondition));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POL", EnCondition) + ")");
                    }
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.POLFK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.POLFK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.POLFK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "POL"

        #region "POD"

        public DataSet FetchAllPod(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string ConditionPK4 = "", string ConditionPK5 = "",
        string ConditionPK6 = "", string TypedData = "", string PolID = "", string PolName = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       PGM.PORT_GRP_CODE,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PGM.PORT_GRP_MST_PK(+) = PMT.PORT_GRP_MST_FK");
                sb.Append("   AND PMT.PORT_TYPE IN (0, 2)");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PolPK + ")");
                }

                if (ConditionPK6.Split('$')[0] == "TARIFF")
                {
                }
                else
                {
                    if (ConditionPK6.Split('$')[0] != "AGENTSOA")
                    {
                        if (!string.IsNullOrEmpty(ConditionPK))
                        {
                            sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK1))
                        {
                            sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK2))
                        {
                            sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK3))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK4))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK5))
                        {
                            sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                        }
                        if (!string.IsNullOrEmpty(ConditionPK6))
                        {
                            sb.Append("   AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                        }
                    }
                    else
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POD", ConditionPK6) + ")");
                    }
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 7));
                    }
                }

                sb.Append(" UNION ");
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       PGM.PORT_GRP_CODE,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PGM.PORT_GRP_MST_PK(+) = PMT.PORT_GRP_MST_FK");
                sb.Append("   AND PMT.PORT_TYPE IN (0, 2)");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + PolPK + ")");
                }

                if (ConditionPK6.Split('$')[0] == "TARIFF")
                {
                }
                else
                {
                    if (ConditionPK6.Split('$')[0] != "AGENTSOA")
                    {
                        if (!string.IsNullOrEmpty(ConditionPK))
                        {
                            sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK1))
                        {
                            sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK2))
                        {
                            sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK3))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK4))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK5))
                        {
                            sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                        }
                        if (!string.IsNullOrEmpty(ConditionPK6))
                        {
                            sb.Append("   AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                        }
                    }
                    else
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POD", ConditionPK6) + ")");
                    }
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 7));
                    }
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       PGM.PORT_GRP_CODE,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PGM.PORT_GRP_MST_PK(+) = PMT.PORT_GRP_MST_FK");
                sb.Append("   AND PMT.PORT_TYPE IN (0, 2)");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PolPK + ")");
                }

                if (ConditionPK6.Split('$')[0] == "TARIFF")
                {
                }
                else
                {
                    if (ConditionPK6.Split('$')[0] != "AGENTSOA")
                    {
                        if (!string.IsNullOrEmpty(ConditionPK))
                        {
                            sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK1))
                        {
                            sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK2))
                        {
                            sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK3))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK4))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK5))
                        {
                            sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK6))
                        {
                            sb.Append("   AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                        }
                    }
                    else
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POD", ConditionPK6) + ")");
                    }
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 7));
                    }
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        public DataSet FetchAllPodTARIFF(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string ConditionPK4 = "", string ConditionPK5 = "",
        string ConditionPK6 = "", string TypedData = "", string PolID = "", string PolName = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(ConditionPK6))
            {
                arrAnnSearchPks = ConditionPK6.Split('$');
            }
            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                if (Convert.ToString(arrAnnSearchPks.GetValue(7)) != "2")
                {
                    if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) != 1)
                    {
                        // sb.Append("   AND PGM.PORT_GRP_MST_PK(+)=PMT.PORT_GRP_MST_FK")
                        sb.Append("   AND PMT.PORT_TYPE IN (0,2)");
                    }
                }

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PolPK + ")");
                }

                if (ConditionPK6.Split('$')[0] != "TARIFF")
                {
                    if (ConditionPK6.Split('$')[0] != "AGENTSOA")
                    {
                        if (!string.IsNullOrEmpty(ConditionPK))
                        {
                            sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK1))
                        {
                            sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK2))
                        {
                            sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK3))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK4))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK5))
                        {
                            sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                        }
                        if (!string.IsNullOrEmpty(ConditionPK6))
                        {
                            sb.Append("   AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                        }
                    }
                    else
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POD", ConditionPK6) + ")");
                    }
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 7));
                    }
                }

                if (ConditionPK6.Split('$')[0] == "TARIFF")
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.PODFK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.PODFK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(7)) == 1)
                        {
                            sb.Append("   select t.PODFK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.PODFK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(ConditionPK6));
                }

                //If ConditionPK6.Split('$')(0) = "TARIFF" Then
                //    sb.Append("   AND PMT.PORT_MST_PK IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.PODFK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.PODFK from VIEW_SEA_RATES_FREIGHT T")
                //    End If
                //    sb.Append(GetTariff(ConditionPK6))
                //End If

                sb.Append(" UNION ");
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                if (Convert.ToString(arrAnnSearchPks.GetValue(7)) != "2")
                {
                    if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) != 1)
                    {
                        //    sb.Append("   AND PGM.PORT_GRP_MST_PK(+)=PMT.PORT_GRP_MST_FK")
                        sb.Append("   AND PMT.PORT_TYPE IN (0,2)");
                    }
                }

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + PolPK + ")");
                }

                if (ConditionPK6.Split('$')[0] != "TARIFF")
                {
                    if (ConditionPK6.Split('$')[0] != "AGENTSOA")
                    {
                        if (!string.IsNullOrEmpty(ConditionPK))
                        {
                            sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK1))
                        {
                            sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK2))
                        {
                            sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK3))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK4))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK5))
                        {
                            sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                        }
                        if (!string.IsNullOrEmpty(ConditionPK6))
                        {
                            sb.Append("   AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                        }
                    }
                    else
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POD", ConditionPK6) + ")");
                    }
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 7));
                    }
                }
                //If ConditionPK6.Split('$')(0) = "TARIFF" Then
                //    sb.Append("   AND PMT.PORT_MST_PK IN (")
                //        If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //            sb.Append("   select t.PODFK from VIEW_AIR_RATES_FREIGHT T")
                //        Else
                //            sb.Append("   select t.PODFK from VIEW_SEA_RATES_FREIGHT T")
                //        End If
                //    sb.Append(GetTariff(ConditionPK6))
                //End If

                if (ConditionPK6.Split('$')[0] == "TARIFF")
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.PODFK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.PODFK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.PODFK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.PODFK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(ConditionPK6));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PMT.PORT_ID,");
                sb.Append("       PMT.PORT_NAME,");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL       PMT,");
                sb.Append("       PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PMT.ACTIVE_FLAG = 1");
                if (Convert.ToString(arrAnnSearchPks.GetValue(7)) != "2")
                {
                    if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) != 1)
                    {
                        //    sb.Append("   AND PGM.PORT_GRP_MST_PK(+)=PMT.PORT_GRP_MST_FK")
                        sb.Append("   AND PMT.PORT_TYPE IN (0,2)");
                    }
                }

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (" + PolPK + ")");
                }

                if (ConditionPK6.Split('$')[0] != "TARIFF")
                {
                    if (ConditionPK6.Split('$')[0] != "AGENTSOA")
                    {
                        if (!string.IsNullOrEmpty(ConditionPK))
                        {
                            sb.Append("   AND PMT.BUSINESS_TYPE IN (" + ConditionPK + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK1))
                        {
                            sb.Append("    AND PGM.PORT_GRP_MST_PK IN (" + ConditionPK1 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK2))
                        {
                            sb.Append("    AND PMT.COUNTRY_MST_FK IN (" + ConditionPK2 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK3))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S WHERE S.SECTOR_MST_PK IN (" + ConditionPK3 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK4))
                        {
                            sb.Append("    AND PMT.PORT_MST_PK IN (SELECT DISTINCT S.FROM_PORT_FK FROM SECTOR_MST_TBL S, TRADE_MST_TBL T WHERE S.TRADE_MST_FK = T.TRADE_MST_PK AND T.TRADE_MST_PK IN  (" + ConditionPK4 + "))");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK5))
                        {
                            sb.Append("   AND PMT.PORT_MST_PK NOT IN (" + ConditionPK5 + ")");
                        }

                        if (!string.IsNullOrEmpty(ConditionPK6))
                        {
                            sb.Append("   AND PMT.Location_Mst_Fk IN (" + ConditionPK6 + ")");
                        }
                    }
                    else
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (" + GetExtendedQueryAgentSOA("POD", ConditionPK6) + ")");
                    }
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN (");
                        sb.Append("   select t.podfk from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 7));
                    }
                }
                //If ConditionPK6.Split('$')(0) = "TARIFF" Then
                //    sb.Append("   AND PMT.PORT_MST_PK IN (")
                //        If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //            sb.Append("   select t.PODFK from VIEW_AIR_RATES_FREIGHT T")
                //        Else
                //            sb.Append("   select t.PODFK from VIEW_SEA_RATES_FREIGHT T")
                //        End If
                //    sb.Append(GetTariff(ConditionPK6))
                //End If
                if (ConditionPK6.Split('$')[0] == "TARIFF")
                {
                    sb.Append("   AND PMT.PORT_MST_PK IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.PODFK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.PODFK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.PODFK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.PODFK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(ConditionPK6));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "POD"

        #region "Fetch Container Types"

        public DataSet FetchContainers(string ContainerPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ContainerID = "", string ContainerName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            ContainerPK = ContainerPK.TrimEnd(',');
            ContainerPK = ContainerPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(ContainerPK))
            {
                sb.Append("SELECT          CMT.CONTAINER_TYPE_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CONTAINER_TYPE_MST_ID ,");
                sb.Append("                CMT.CONTAINER_TYPE_NAME ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM CONTAINER_TYPE_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(ContainerPK))
                {
                    sb.Append("   AND CMT.CONTAINER_TYPE_MST_PK IN ('" + ContainerPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND CMT.CONTAINER_TYPE_MST_PK IN (");
                        sb.Append("   select T.CONTAINER_TYPE_MST_PK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT          CMT.CONTAINER_TYPE_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CONTAINER_TYPE_MST_ID ,");
                sb.Append("                CMT.CONTAINER_TYPE_NAME ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CONTAINER_TYPE_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(ContainerPK))
                {
                    sb.Append("   AND CMT.CONTAINER_TYPE_MST_PK  NOT IN ('" + ContainerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ContainerID))
                {
                    sb.Append(" AND UPPER(CMT.CONTAINER_TYPE_MST_ID) LIKE '%" + ContainerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ContainerName))
                {
                    sb.Append(" AND UPPER(CMT.CONTAINER_TYPE_NAME) LIKE '%" + ContainerName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND CMT.CONTAINER_TYPE_MST_PK IN (");
                        sb.Append("   select T.CONTAINER_TYPE_MST_PK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          CMT.CONTAINER_TYPE_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CONTAINER_TYPE_MST_ID ,");
                sb.Append("                CMT.CONTAINER_TYPE_NAME ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CONTAINER_TYPE_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.CONTAINER_TYPE_MST_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ContainerID))
                {
                    sb.Append(" AND UPPER(CMT.CONTAINER_TYPE_MST_ID) LIKE '%" + ContainerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ContainerName))
                {
                    sb.Append(" AND UPPER(CMT.CONTAINER_TYPE_NAME) LIKE '%" + ContainerName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND CMT.CONTAINER_TYPE_MST_PK IN (");
                        sb.Append("   select T.CONTAINER_TYPE_MST_PK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Container Types"

        #region " Fetch Basis "

        public DataSet FetchBasis(string BasisPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string BasisID = "", string BasisName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            BasisPK = BasisPK.TrimEnd(',');
            BasisPK = BasisPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(BasisPK))
            {
                sb.Append("SELECT          UOM.DIMENTION_UNIT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                UOM.DIMENTION_ID ,");
                sb.Append("                UOM.DIMENTION_ID as Name ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM DIMENTION_UNIT_MST_TBL UOM ");
                sb.Append("   WHERE UOM.ACTIVE = 1");

                if (!string.IsNullOrEmpty(BasisPK))
                {
                    sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK IN ('" + BasisPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK IN (");
                        sb.Append("   select T.DIMENTION_UNIT_MST_PK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" UNION ");
                sb.Append("SELECT          UOM.DIMENTION_UNIT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                UOM.DIMENTION_ID ,");
                sb.Append("                UOM.DIMENTION_ID as Name ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM DIMENTION_UNIT_MST_TBL UOM ");
                sb.Append("   WHERE UOM.ACTIVE = 1");

                if (!string.IsNullOrEmpty(BasisPK))
                {
                    sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK  NOT IN ('" + BasisPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(BasisID))
                {
                    sb.Append(" AND UPPER(UOM.DIMENTION_ID) LIKE '%" + BasisID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK IN (");
                        sb.Append("   select T.DIMENTION_UNIT_MST_PK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          UOM.DIMENTION_UNIT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                UOM.DIMENTION_ID ,");
                sb.Append("                UOM.DIMENTION_ID as Name ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM DIMENTION_UNIT_MST_TBL UOM ");
                sb.Append("   WHERE UOM.ACTIVE = 1");
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(UOM.DIMENTION_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(BasisID))
                {
                    sb.Append(" AND UPPER(UOM.DIMENTION_ID) LIKE '%" + BasisID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND UOM.DIMENTION_UNIT_MST_PK IN (");
                        sb.Append("   select T.DIMENTION_UNIT_MST_PK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion " Fetch Basis "

        #region " Fetch Slab "

        public DataSet FetchSlab(string SlabPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string SlabID = "", string SlabName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }

            SlabPK = SlabPK.TrimEnd(',');
            SlabPK = SlabPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(SlabPK))
            {
                sb.Append("SELECT          A.AIRFREIGHT_SLABS_TBL_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                A.BREAKPOINT_ID ,");
                sb.Append("                A.BREAKPOINT_DESC ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM AIRFREIGHT_SLABS_TBL A ");
                sb.Append("   WHERE A.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(SlabPK))
                {
                    sb.Append("   AND A.AIRFREIGHT_SLABS_TBL_PK IN ('" + SlabPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND A.AIRFREIGHT_SLABS_TBL_PK IN (");
                        sb.Append("   select T.SLABPK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          A.AIRFREIGHT_SLABS_TBL_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                A.BREAKPOINT_ID ,");
                sb.Append("                A.BREAKPOINT_DESC ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM AIRFREIGHT_SLABS_TBL A ");
                sb.Append("   WHERE A.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(SlabPK))
                {
                    sb.Append("   AND A.AIRFREIGHT_SLABS_TBL_PK  NOT IN ('" + SlabPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(SlabID))
                {
                    sb.Append(" AND UPPER(A.BREAKPOINT_ID) LIKE '%" + SlabID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SlabName))
                {
                    sb.Append(" AND UPPER(A.BREAKPOINT_DESC) LIKE '%" + SlabName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND A.AIRFREIGHT_SLABS_TBL_PK IN (");
                        sb.Append("   select T.SLABPK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          A.AIRFREIGHT_SLABS_TBL_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                A.BREAKPOINT_ID ,");
                sb.Append("                A.BREAKPOINT_DESC ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM AIRFREIGHT_SLABS_TBL A  ");
                sb.Append("   WHERE A.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(A.AIRFREIGHT_SLABS_TBL_PK) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SlabID))
                {
                    sb.Append(" AND UPPER(A.BREAKPOINT_ID) LIKE '%" + SlabID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SlabName))
                {
                    sb.Append(" AND UPPER(A.BREAKPOINT_DESC) LIKE '%" + SlabName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "QTNRPT")
                    {
                        sb.Append("   AND A.AIRFREIGHT_SLABS_TBL_PK IN (");
                        sb.Append("   select T.SLABPK from VIEW_QTNEXPIRY T");
                        sb.Append(GetQTNRpt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion " Fetch Slab "

        #region "Fetch Location"

        public DataSet FetchLocation(string LocationPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            LocationPK = LocationPK.TrimEnd(',');
            LocationPK = LocationPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(LocationPK))
            {
                sb.Append("SELECT          LMT.LOCATION_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                LMT.LOCATION_ID,");
                sb.Append("                LMT.LOCATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  LOCATION_MST_TBL LMT ");
                sb.Append("   WHERE LMT.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(LocationPK))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN ('" + LocationPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.LOCATION_MST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.LOCATION_MST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.ADM_LOCATION_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (" + GetExtendedQueryVendorSOA("LOCATION", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (" + GetExtendedQueryAgentSOA("LOCATION", AnnSearchPks) + ")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 0));
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT          LMT.LOCATION_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                LMT.LOCATION_ID,");
                sb.Append("                LMT.LOCATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM LOCATION_MST_TBL LMT ");
                sb.Append("   WHERE LMT.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(LocationPK))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK  NOT IN ('" + LocationPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND LMT.COUNTRY_MST_FK IN(" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.LOCATION_MST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.LOCATION_MST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.ADM_LOCATION_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (" + GetExtendedQueryVendorSOA("LOCATION", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (" + GetExtendedQueryAgentSOA("LOCATION", AnnSearchPks) + ")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 0));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          LMT.LOCATION_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                LMT.LOCATION_ID,");
                sb.Append("                 LMT.LOCATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM LOCATION_MST_TBL LMT ");
                sb.Append("   WHERE LMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK) & ConditionPK != "0")
                {
                    sb.Append("  AND LMT.COUNTRY_MST_FK IN(" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.LOCATION_MST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.LOCATION_MST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (");
                        sb.Append("   select t.ADM_LOCATION_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (" + GetExtendedQueryVendorSOA("LOCATION", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND LMT.LOCATION_MST_PK IN (" + GetExtendedQueryAgentSOA("LOCATION", AnnSearchPks) + ")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 0));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Location"

        #region "FetchJMFLocation"

        public DataSet FetchJMFLocation(string LocationPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            LocationPK = LocationPK.TrimEnd(',');
            LocationPK = LocationPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(LocationPK))
            {
                sb.Append("SELECT     DISTINCT  LMT.LOCATION_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                LMT.LOCATION_ID,");
                sb.Append("                LMT.LOCATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM LOCATION_MST_TBL LMT, ");
                sb.Append("  USER_MST_TBL       U, ");
                sb.Append("  DEPARTMENT_MST_TBL D, ");
                sb.Append("  EMPLOYEE_MST_TBL   E ");
                sb.Append("  WHERE LMT.ACTIVE_FLAG =1");
                sb.Append("  AND U.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK ");
                sb.Append("  AND U.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK ");
                sb.Append("  AND E.DEPARTMENT_MST_FK = D.DEPARTMENT_MST_PK ");

                if (!string.IsNullOrEmpty(LocationPK))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN ('" + LocationPK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT    DISTINCT LMT.LOCATION_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                LMT.LOCATION_ID,");
                sb.Append("                LMT.LOCATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM LOCATION_MST_TBL LMT, ");
                sb.Append("  USER_MST_TBL       U, ");
                sb.Append("  DEPARTMENT_MST_TBL D, ");
                sb.Append("  EMPLOYEE_MST_TBL   E ");
                sb.Append("  WHERE LMT.ACTIVE_FLAG =1");
                sb.Append("  AND U.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK ");
                sb.Append("  AND U.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK ");
                sb.Append("  AND E.DEPARTMENT_MST_FK = D.DEPARTMENT_MST_PK ");

                if (!string.IsNullOrEmpty(LocationPK))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK  NOT IN ('" + LocationPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND LMT.COUNTRY_MST_FK IN(" + ConditionPK + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT     DISTINCT    LMT.LOCATION_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                LMT.LOCATION_ID,");
                sb.Append("                 LMT.LOCATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM LOCATION_MST_TBL LMT, ");
                sb.Append("  USER_MST_TBL       U, ");
                sb.Append("  DEPARTMENT_MST_TBL D, ");
                sb.Append("  EMPLOYEE_MST_TBL   E ");
                sb.Append("  WHERE LMT.ACTIVE_FLAG =1");
                sb.Append("  AND U.DEFAULT_LOCATION_FK = LMT.LOCATION_MST_PK ");
                sb.Append("  AND U.EMPLOYEE_MST_FK = E.EMPLOYEE_MST_PK ");
                sb.Append("  AND E.DEPARTMENT_MST_FK = D.DEPARTMENT_MST_PK ");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(LMT.LOCATION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK) & ConditionPK != "0")
                {
                    sb.Append("  AND LMT.COUNTRY_MST_FK IN(" + ConditionPK + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "FetchJMFLocation"

        #region "Fetch Customer"

        public DataSet FetchCustomer(string CustomerPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CustomerID = "", string CustomerName = "", string selectedPKs = "",
        string LoginPK = "", bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            CustomerPK = CustomerPK.TrimEnd(',');
            CustomerPK = CustomerPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(CustomerPK))
            {
                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  CUSTOMER_MST_TBL CMT ");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append("  , CUSTOMER_CONTACT_DTLS CCD ");
                }

                sb.Append(" WHERE CMT.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
                    sb.Append(" AND CCD.ADM_LOCATION_MST_FK IN (" + LoginPK + ") ");
                }

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUSTOMER_MST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUSTOMER_MST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUST_CUSTOMER_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.CUSTOMER_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.CUSTOMER_MST_FK from VIEW_CUST_CONTRACT_SEA  T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 9));
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL CMT");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append("  , CUSTOMER_CONTACT_DTLS CCD ");
                }

                sb.Append("   WHERE CMT.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
                    sb.Append(" AND CCD.ADM_LOCATION_MST_FK IN (" + LoginPK + ") ");
                }

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK  NOT IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUSTOMER_MST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUSTOMER_MST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUST_CUSTOMER_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.CUSTOMER_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.CUSTOMER_MST_FK from VIEW_CUST_CONTRACT_SEA  T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 9));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL CMT ");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append("  , CUSTOMER_CONTACT_DTLS CCD ");
                }

                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
                    sb.Append(" AND CCD.ADM_LOCATION_MST_FK IN (" + LoginPK + ") ");
                }

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUSTOMER_MST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUSTOMER_MST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.CUST_CUSTOMER_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.CUSTOMER_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.CUSTOMER_MST_FK from VIEW_CUST_CONTRACT_SEA  T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 9));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Customer"

        #region "Fetch Agent"

        public DataSet FetchAgent(string AgentPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string AgentID = "", string AgentName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            AgentPK = AgentPK.TrimEnd(',');
            AgentPK = AgentPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(AgentPK))
            {
                sb.Append("SELECT          AMT.AGENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                AMT.AGENT_ID,");
                sb.Append("                AMT.AGENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  AGENT_MST_TBL AMT");
                sb.Append("   WHERE AMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(AgentPK))
                {
                    sb.Append("   AND AMT.AGENT_MST_PK IN ('" + AgentPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (AnnSearchPks.Split('$')[0] == "AGENTSOA")
                    {
                        sb.Append("   AND AMT.AGENT_MST_PK IN (" + GetExtendedQueryAgentSOA("AGENT", AnnSearchPks) + ")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 8));
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT          AMT.AGENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                AMT.AGENT_ID,");
                sb.Append("                AMT.AGENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("   FROM AGENT_MST_TBL AMT");
                sb.Append("   WHERE AMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(AgentPK))
                {
                    sb.Append("   AND AMT.AGENT_MST_PK NOT IN ('" + AgentPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AgentID))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_ID) LIKE '%" + AgentID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AgentName))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_NAME) LIKE '%" + AgentName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (AnnSearchPks.Split('$')[0] == "AGENTSOA")
                    {
                        sb.Append("   AND AMT.AGENT_MST_PK IN (" + GetExtendedQueryAgentSOA("AGENT", AnnSearchPks) + ")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 8));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          AMT.AGENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                AMT.AGENT_ID,");
                sb.Append("                AMT.AGENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("   FROM AGENT_MST_TBL AMT ");
                sb.Append("   WHERE AMT.ACTIVE_FLAG= 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AgentID))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_ID) LIKE '%" + AgentID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AgentName))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_NAME) LIKE '%" + AgentName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (AnnSearchPks.Split('$')[0] == "AGENTSOA")
                    {
                        sb.Append("   AND AMT.AGENT_MST_PK IN (" + GetExtendedQueryAgentSOA("AGENT", AnnSearchPks) + ")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 8));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Agent"

        #region "Fetch CustomerGroup"

        public DataSet FetchCustomerGroup(string CustGroupPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CustGroupID = "", string CustGroupName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = AnnSearchPks.Split('$');

            CustGroupPK = CustGroupPK.TrimEnd(',');
            CustGroupPK = CustGroupPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(CustGroupPK))
            {
                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  CUSTOMER_MST_TBL CMT");
                sb.Append("   WHERE CMT.ACTIVE_FLAG =1");
                sb.Append("   AND CMT.GROUP_HEADER = 1");

                if (!string.IsNullOrEmpty(CustGroupPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK IN ('" + CustGroupPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.REF_GROUP_CUST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.REF_GROUP_CUST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL CMT");
                sb.Append("   WHERE CMT.ACTIVE_FLAG =1");
                sb.Append("   AND CMT.GROUP_HEADER = 1");

                if (!string.IsNullOrEmpty(CustGroupPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK NOT IN ('" + CustGroupPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(CustGroupID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustGroupID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustGroupName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustGroupName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.REF_GROUP_CUST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.REF_GROUP_CUST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");
                sb.Append("   AND CMT.GROUP_HEADER = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustGroupID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustGroupID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustGroupName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustGroupName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.REF_GROUP_CUST_PK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN (");
                        sb.Append("   select t.REF_GROUP_CUST_PK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch CustomerGroup"

        #region "Fetch Carrier"

        public DataSet FetchCarrier(string CarrierPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CarrierID = "", string CarrierName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string EnCondition = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CarrierPK = CarrierPK.TrimEnd(',');
            CarrierPK = CarrierPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            Array arrAnnSearchPks = EnCondition.Split('$');

            if (!string.IsNullOrEmpty(CarrierPK))
            {
                sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                OPR.OPERATOR_ID,");
                sb.Append("                OPR.OPERATOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  OPERATOR_MST_TBL OPR");
                sb.Append("   WHERE OPR.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(CarrierPK))
                {
                    sb.Append("   AND OPR.OPERATOR_MST_PK IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                }

                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("   AND OPR.OPERATOR_MST_PK IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.OPERATOR_MST_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.OPERATOR_MST_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    'sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T")
                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND OPR.OPERATOR_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                OPR.OPERATOR_ID,");
                sb.Append("                OPR.OPERATOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM OPERATOR_MST_TBL OPR");
                sb.Append("   WHERE OPR.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(CarrierPK))
                {
                    sb.Append("   AND OPR.OPERATOR_MST_PK NOT IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(CarrierID))
                {
                    sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CarrierName))
                {
                    sb.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                }

                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("   AND OPR.OPERATOR_MST_PK IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.OPERATOR_MST_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.OPERATOR_MST_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    'sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T")
                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND OPR.OPERATOR_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                OPR.OPERATOR_ID,");
                sb.Append("                OPR.OPERATOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM OPERATOR_MST_TBL OPR ");
                sb.Append("   WHERE OPR.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CarrierID))
                {
                    sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CarrierName))
                {
                    sb.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                }

                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("   AND OPR.OPERATOR_MST_PK IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.OPERATOR_MST_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.OPERATOR_MST_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    'sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T")
                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND OPR.OPERATOR_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.OPERATOR_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Carrier"

        #region "Fetch AirLine"

        public DataSet FetchAirLine(string AirLinePK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string AirLinerID = "", string AirLineName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string EnCondition = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            AirLinePK = AirLinePK.TrimEnd(',');
            AirLinePK = AirLinePK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            Array arrAnnSearchPks = EnCondition.Split('$');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(AirLinePK))
            {
                sb.Append("SELECT          AMT.AIRLINE_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                AMT.AIRLINE_ID,");
                sb.Append("                AMT.AIRLINE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  AIRLINE_MST_TBL AMT");
                sb.Append("   WHERE AMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(AirLinePK))
                {
                    sb.Append("   AND AMT.AIRLINE_MST_PK IN ('" + AirLinePK.ToUpper().Replace(",", "','") + "')");
                }
                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("     AND AMT.AIRLINE_MST_PK IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.AIRLINE_MST_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.AIRLINE_MST_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    'sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T")
                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND AMT.AIRLINE_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          AMT.AIRLINE_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                AMT.AIRLINE_ID,");
                sb.Append("                AMT.AIRLINE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM AIRLINE_MST_TBL AMT");
                sb.Append("   WHERE AMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(AirLinePK))
                {
                    sb.Append("   AND AMT.AIRLINE_MST_PK NOT IN ('" + AirLinePK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AirLinerID))
                {
                    sb.Append(" AND UPPER(AMT.AIRLINE_ID) LIKE '%" + AirLinerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AirLineName))
                {
                    sb.Append(" AND UPPER(AMT.AIRLINE_NAME) LIKE '%" + AirLineName.ToUpper().Replace("'", "''") + "%'");
                }
                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("     AND AMT.AIRLINE_MST_PK IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.AIRLINE_MST_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.AIRLINE_MST_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    'sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T")
                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND AMT.AIRLINE_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          AMT.AIRLINE_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                AMT.AIRLINE_ID,");
                sb.Append("                AMT.AIRLINE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM AIRLINE_MST_TBL AMT");
                sb.Append("   WHERE AMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(AMT.AIRLINE_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AirLinerID))
                {
                    sb.Append(" AND UPPER(AMT.AIRLINE_ID) LIKE '%" + AirLinerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AirLineName))
                {
                    sb.Append(" AND UPPER(AMT.AIRLINE_NAME) LIKE '%" + AirLineName.ToUpper().Replace("'", "''") + "%'");
                }

                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("     AND AMT.AIRLINE_MST_PK IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.AIRLINE_MST_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.AIRLINE_MST_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND AMT.AIRLINE_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.AIRLINE_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch AirLine"

        #region "Fetch Vessel/Voyage"

        public DataSet FetchVesselVoy(string VslVoyPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string VesselID = "", string VoyageNo = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            VslVoyPK = VslVoyPK.TrimEnd(',');
            VslVoyPK = VslVoyPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(VslVoyPK))
            {
                sb.Append("SELECT          VVT.VOYAGE_TRN_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.VESSEL_NAME,");
                sb.Append("                VVT.VOYAGE,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  VESSEL_VOYAGE_TBL V,VESSEL_VOYAGE_TRN VVT");
                sb.Append("   WHERE VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK");

                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    sb.Append("   AND  VVT.VOYAGE_TRN_PK IN ('" + VslVoyPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND VVT.VOYAGE_TRN_PK IN (");
                        sb.Append("   select t.VOYAGE_TRN_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT           VVT.VOYAGE_TRN_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.VESSEL_NAME,");
                sb.Append("                 VVT.VOYAGE,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VESSEL_VOYAGE_TBL V,VESSEL_VOYAGE_TRN VVT");
                sb.Append("   WHERE VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK");

                if (!string.IsNullOrEmpty(VslVoyPK))
                {
                    sb.Append("   AND VVT.VOYAGE_TRN_PK NOT IN ('" + VslVoyPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(VesselID))
                {
                    sb.Append(" AND UPPER(V.VESSEL_NAME) LIKE '%" + VesselID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VoyageNo))
                {
                    sb.Append(" AND UPPER(VVT.VOYAGE) LIKE '%" + VoyageNo.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND VVT.VOYAGE_TRN_PK IN (");
                        sb.Append("   select t.VOYAGE_TRN_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT         VVT.VOYAGE_TRN_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("               V.VESSEL_NAME,");
                sb.Append("                VVT.VOYAGE,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VESSEL_VOYAGE_TBL V,VESSEL_VOYAGE_TRN VVT");
                sb.Append("   WHERE VVT.VESSEL_VOYAGE_TBL_FK = V.VESSEL_VOYAGE_TBL_PK");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(V.VESSEL_NAME) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VesselID))
                {
                    sb.Append(" AND UPPER(V.VESSEL_NAME) LIKE '%" + VesselID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VoyageNo))
                {
                    sb.Append(" AND UPPER(VVT.VOYAGE) LIKE '%" + VoyageNo.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND VVT.VOYAGE_TRN_PK IN (");
                        sb.Append("   select t.VOYAGE_TRN_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Vessel/Voyage"

        #region "Fetch FlightNo"

        public DataSet FetchFlightNo(string FlightPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string FlightNo = "", string FlightName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = AnnSearchPks.Split('$');

            FlightPK = FlightPK.TrimEnd(',');
            FlightPK = FlightPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(TypedData))
            {
                sb.Append("SELECT  DISTINCT  ' ' ,");
                sb.Append("                '' EMPTY,");
                sb.Append("               JOB.VOYAGE_FLIGHT_NO,");
                sb.Append("                '' EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  JOB_CARD_TRN JOB WHERE JOB.BUSINESS_TYPE=1 AND JOB.VOYAGE_FLIGHT_NO IS NOT NULL");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN (");
                        sb.Append("   select t.VOYAGE_FLIGHT_NO from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT   DISTINCT  ' ',");
                sb.Append("                '' EMPTY,");
                sb.Append("                JOB.VOYAGE_FLIGHT_NO,");
                sb.Append("                '' EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  JOB_CARD_TRN JOB WHERE JOB.BUSINESS_TYPE=1 AND JOB.VOYAGE_FLIGHT_NO IS NOT NULL");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(JOB.VOYAGE_FLIGHT_NO) NOT LIKE '%" + FlightNo.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN (");
                        sb.Append("   select t.VOYAGE_FLIGHT_NO from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT    DISTINCT ' ',");
                sb.Append("                '' EMPTY,");
                sb.Append("               JOB.VOYAGE_FLIGHT_NO,");
                sb.Append("                '' EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  JOB_CARD_TRN JOB WHERE JOB.BUSINESS_TYPE=1 AND JOB.VOYAGE_FLIGHT_NO IS NOT NULL");
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND JOB.VOYAGE_FLIGHT_NO IN (");
                        sb.Append("   select t.VOYAGE_FLIGHT_NO from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch FlightNo"

        #region "Fetch Vendor"

        public DataSet FetchVendor(string VendorPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string Businesstype = "", string TypedData = "", string VendorID = "", string VendorName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AdditionalPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            VendorPK = VendorPK.TrimEnd(',');
            VendorPK = VendorPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(VendorPK))
            {
                sb.Append("SELECT          VMT.VENDOR_MST_PK,");

                sb.Append("                '' EMPTY,");
                sb.Append("                VMT.VENDOR_ID,");
                sb.Append("                VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  VENDOR_MST_TBL VMT,");
                sb.Append("        VENDOR_CONTACT_DTLS VCD,");
                sb.Append("        VENDOR_TYPE_MST_TBL VT,");
                sb.Append("        VENDOR_SERVICES_TRN VS");
                sb.Append("   WHERE VMT.ACTIVE = 1");
                sb.Append("   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                //sb.Append("   AND UPPER(VT.VENDOR_TYPE_ID) = 'SHIPPINGLINE'")
                if (Businesstype == "1")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 1)");
                }
                else if (Businesstype == "2")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 2)");
                }

                if (!string.IsNullOrEmpty(VendorPK))
                {
                    sb.Append("   AND  VMT.VENDOR_MST_PK IN ('" + VendorPK.ToUpper().Replace(",", "','") + "')");
                }
                sb.Append("   AND   VMT.VENDOR_MST_PK IN (" + GetExtendedQueryVendorSOA("VENDOR", AdditionalPks) + ")");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND VT.VENDOR_TYPE_PK IN(" + ConditionPK + ")");
                }
                if (LoginPK > 0)
                {
                    sb.Append("   AND VCD.ADM_LOCATION_MST_FK IN (SELECT DISTINCT LMT.LOCATION_MST_PK FROM LOCATION_MST_TBL LMT");
                    sb.Append("                                START WITH LMT.LOCATION_MST_PK=" + LoginPK);
                    sb.Append("                                CONNECT BY PRIOR LMT.LOCATION_MST_PK=LMT.REPORTING_TO_FK )");
                }
                sb.Append(" UNION ");

                sb.Append("SELECT          VMT.VENDOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                VMT.VENDOR_ID,");
                sb.Append("                VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  VENDOR_MST_TBL VMT,");
                sb.Append("        VENDOR_CONTACT_DTLS VCD,");
                sb.Append("        VENDOR_TYPE_MST_TBL VT,");
                sb.Append("        VENDOR_SERVICES_TRN VS");
                sb.Append("   WHERE VMT.ACTIVE = 1");
                sb.Append("   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                //sb.Append("   AND UPPER(VT.VENDOR_TYPE_ID) = 'SHIPPINGLINE'")
                if (Businesstype == "1")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 1)");
                }
                else if (Businesstype == "2")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 2)");
                }
                if (!string.IsNullOrEmpty(VendorPK))
                {
                    sb.Append("   AND VMT.VENDOR_MST_PK NOT IN ('" + VendorPK.ToUpper().Replace(",", "','") + "')");
                }
                sb.Append("   AND   VMT.VENDOR_MST_PK IN (" + GetExtendedQueryVendorSOA("VENDOR", AdditionalPks) + ")");
                if (!string.IsNullOrEmpty(VendorID))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + VendorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + VendorName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK) & ConditionPK != "0")
                {
                    sb.Append("  AND VT.VENDOR_TYPE_PK IN(" + ConditionPK + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT     DISTINCT     VMT.VENDOR_MST_PK,");

                sb.Append("                '' EMPTY,");
                sb.Append("                VMT.VENDOR_ID,");
                sb.Append("                VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  VENDOR_MST_TBL VMT,");
                sb.Append("        VENDOR_CONTACT_DTLS VCD,");
                sb.Append("        VENDOR_TYPE_MST_TBL VT,");
                sb.Append("        VENDOR_SERVICES_TRN VS");
                sb.Append("   WHERE VMT.ACTIVE = 1");
                sb.Append("   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                // sb.Append("   AND UPPER(VT.VENDOR_TYPE_ID) = 'SHIPPINGLINE'")
                if (Businesstype == "1")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 1)");
                }
                else if (Businesstype == "2")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 2)");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorID))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + VendorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorName))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_NAME) LIKE '%" + VendorName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND VT.VENDOR_TYPE_PK IN(" + ConditionPK + ")");
                }
                sb.Append("   AND   VMT.VENDOR_MST_PK IN (" + GetExtendedQueryVendorSOA("VENDOR", AdditionalPks) + ")");
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Vendor"

        #region "Fetch Jobcard Vendor"

        public DataSet FetchJobVendor(string VendorPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string Businesstype = "", string ProcessType = "", string TypedData = "", string VendorID = "",
        string VendorName = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            VendorPK = VendorPK.TrimEnd(',');
            VendorPK = VendorPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(VendorPK))
            {
                sb.Append("SELECT          VMT.VENDOR_MST_PK,");

                sb.Append("                '' EMPTY,");
                sb.Append("                VMT.VENDOR_ID,");
                sb.Append("                VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  VENDOR_MST_TBL VMT,");
                sb.Append("        VENDOR_CONTACT_DTLS VCD,");
                sb.Append("        VENDOR_TYPE_MST_TBL VT,");
                sb.Append("        VENDOR_SERVICES_TRN VS,");
                sb.Append("        JOB_TRN_COST JOBCOST");
                sb.Append("   WHERE VMT.ACTIVE = 1");
                sb.Append("   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                if (IsAdmin == false)
                {
                    sb.Append("   AND JOBCOST.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                }
                if (Businesstype == "1")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 1)");
                }
                else if (Businesstype == "2")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 2)");
                }

                if (!string.IsNullOrEmpty(VendorPK))
                {
                    sb.Append("   AND  VMT.VENDOR_MST_PK IN ('" + VendorPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ConditionPK1) & ConditionPK1 != "0" & Convert.ToInt32(ConditionPK1) != -1)
                {
                    sb.Append("  AND VT.VENDOR_TYPE_PK IN(" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "SUPSALES")
                    {
                        sb.Append("   AND VMT.VENDOR_MST_PK IN (");
                        sb.Append("   select t.VENDOR_MST_PK from VIEW_SUP_SALESRPT T");
                        sb.Append(GetSupSales(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          VMT.VENDOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                VMT.VENDOR_ID,");
                sb.Append("                VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  VENDOR_MST_TBL VMT,");
                sb.Append("        VENDOR_CONTACT_DTLS VCD,");
                sb.Append("        VENDOR_TYPE_MST_TBL VT,");
                sb.Append("        VENDOR_SERVICES_TRN VS,");
                sb.Append("        JOB_TRN_COST JOBCOST");
                sb.Append("   WHERE VMT.ACTIVE = 1");
                sb.Append("   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                if (IsAdmin == false)
                {
                    sb.Append("   AND JOBCOST.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                }
                if (Businesstype == "1")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 1)");
                }
                else if (Businesstype == "2")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 2)");
                }
                if (!string.IsNullOrEmpty(VendorPK))
                {
                    sb.Append("   AND VMT.VENDOR_MST_PK NOT IN ('" + VendorPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(VendorID))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + VendorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + VendorName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1) & ConditionPK1 != "0" & Convert.ToInt32(ConditionPK1) != -1)
                {
                    sb.Append("  AND VT.VENDOR_TYPE_PK IN(" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "SUPSALES")
                    {
                        sb.Append("   AND VMT.VENDOR_MST_PK IN (");
                        sb.Append("   select t.VENDOR_MST_PK from VIEW_SUP_SALESRPT T");
                        sb.Append(GetSupSales(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
                if (Businesstype == "1")
                {
                    sb.Replace("SEA", "AIR");
                }
                if (ProcessType == "2")
                {
                    sb.Replace("EXP", "IMP");
                }
            }
            else
            {
                sb.Append("SELECT     DISTINCT     VMT.VENDOR_MST_PK,");

                sb.Append("                '' EMPTY,");
                sb.Append("                VMT.VENDOR_ID,");
                sb.Append("                VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  VENDOR_MST_TBL VMT,");
                sb.Append("        VENDOR_CONTACT_DTLS VCD,");
                sb.Append("        VENDOR_TYPE_MST_TBL VT,");
                sb.Append("        VENDOR_SERVICES_TRN VS,");
                sb.Append("        JOB_TRN_COST JOBCOST");
                sb.Append("   WHERE VMT.ACTIVE = 1");
                sb.Append("   AND VMT.VENDOR_MST_PK = VCD.VENDOR_MST_FK");
                sb.Append("   AND VMT.VENDOR_MST_PK = VS.VENDOR_MST_FK(+)");
                sb.Append("   AND VS.VENDOR_TYPE_FK = VT.VENDOR_TYPE_PK");
                if (IsAdmin == false)
                {
                    sb.Append("   AND JOBCOST.VENDOR_MST_FK = VMT.VENDOR_MST_PK");
                }
                if (Businesstype == "1")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 1)");
                }
                else if (Businesstype == "2")
                {
                    sb.Append("   AND VMT.BUSINESS_TYPE IN (3, 2)");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorID))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + VendorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorName))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_NAME) LIKE '%" + VendorName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1) & ConditionPK1 != "0" & Convert.ToInt32(ConditionPK1) != -1)
                {
                    sb.Append("  AND VT.VENDOR_TYPE_PK IN(" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "SUPSALES")
                    {
                        sb.Append("   AND VMT.VENDOR_MST_PK IN (");
                        sb.Append("   select t.VENDOR_MST_PK from VIEW_SUP_SALESRPT T");
                        sb.Append(GetSupSales(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
                if (Businesstype == "1")
                {
                    sb.Replace("SEA", "AIR");
                }
                if (ProcessType == "2")
                {
                    sb.Replace("EXP", "IMP");
                }
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Jobcard Vendor"

        #region "Fetch VendorType"

        public DataSet FetchVendorType(string VendorTypePK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string VendorTypeID = "", string VendorTypeName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AdditionalPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            VendorTypePK = VendorTypePK.TrimEnd(',');
            VendorTypePK = VendorTypePK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(VendorTypePK))
            {
                sb.Append("SELECT          VTMT.VENDOR_TYPE_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                VTMT.VENDOR_TYPE_ID,");
                sb.Append("                VTMT.VENDOR_TYPE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM VENDOR_TYPE_MST_TBL VTMT");
                sb.Append("   WHERE VTMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(VendorTypePK))
                {
                    sb.Append("   AND   VTMT.VENDOR_TYPE_PK IN ('" + VendorTypePK.ToUpper().Replace(",", "','") + "')");
                }
                sb.Append("   AND   VTMT.VENDOR_TYPE_PK IN (" + GetExtendedQueryVendorSOA("VENDOR_TYPE", AdditionalPks) + ")");

                sb.Append(" UNION ");

                sb.Append("SELECT          VTMT.VENDOR_TYPE_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                VTMT.VENDOR_TYPE_ID,");
                sb.Append("                VTMT.VENDOR_TYPE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VENDOR_TYPE_MST_TBL VTMT");
                sb.Append("    WHERE VTMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(VendorTypePK))
                {
                    sb.Append("   AND VTMT.VENDOR_TYPE_PK NOT IN ('" + VendorTypePK.ToUpper().Replace(",", "','") + "')");
                }
                sb.Append("   AND   VTMT.VENDOR_TYPE_PK IN (" + GetExtendedQueryVendorSOA("VENDOR_TYPE", AdditionalPks) + ")");
                if (!string.IsNullOrEmpty(VendorTypeID))
                {
                    sb.Append(" AND UPPER(VTMT.VENDOR_TYPE_ID) LIKE '%" + VendorTypeID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorTypeName))
                {
                    sb.Append(" AND UPPER(VTMT.VENDOR_TYPE_NAME) LIKE '%" + VendorTypeName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          VTMT.VENDOR_TYPE_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                VTMT.VENDOR_TYPE_ID,");
                sb.Append("                VTMT.VENDOR_TYPE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VENDOR_TYPE_MST_TBL VTMT ");
                sb.Append("   WHERE VTMT.ACTIVE_FLAG = 1");

                sb.Append("   AND   VTMT.VENDOR_TYPE_PK IN (" + GetExtendedQueryVendorSOA("VENDOR_TYPE", AdditionalPks) + ")");
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER( VTMT.VENDOR_TYPE_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorTypeID))
                {
                    sb.Append(" AND UPPER( VTMT.VENDOR_TYPE_ID) LIKE '%" + VendorTypeID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(VendorTypeName))
                {
                    sb.Append(" AND UPPER(VTMT.VENDOR_TYPE_NAME) LIKE '%" + VendorTypeName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        public string GetExtendedQueryVendorSOA(string Flag, string AdditionalPks)
        {
            StringBuilder sbNew = new StringBuilder();
            sbNew.Append("SELECT DISTINCT VVS.COUNTRY_FK,");
            sbNew.Append("                VVS.LOC_FK,");
            sbNew.Append("                VVS.VENDOR_FK,");
            sbNew.Append("                VVS.VENDOR_TYPE_FK,");
            sbNew.Append("                VVS.CURRENCY_FK");
            sbNew.Append("  FROM VIEW_VENDOR_SOA VVS WHERE 1=1 ");

            try
            {
                string _paramVal = "";
                int _paramIndex = 1;
                while (AdditionalPks.Split('$').Length > _paramIndex)
                {
                    _paramVal = AdditionalPks.Split('$')[_paramIndex];

                    if (_paramIndex == 1 & Convert.ToInt32(_paramVal) > 0 & Flag.ToUpper() != "COUNTRY")
                    {
                        //'COUNTRY FK
                        sbNew.Append(" AND VVS.COUNTRY_FK=" + Convert.ToInt32(_paramVal));
                    }
                    else if (_paramIndex == 2 & Convert.ToInt32(_paramVal) > 0 & Flag.ToUpper() != "LOCATION")
                    {
                        //LOCATION FK
                        sbNew.Append(" AND VVS.LOC_FK=" + Convert.ToInt32(_paramVal));
                    }
                    else if (_paramIndex == 3 & Convert.ToInt32(_paramVal) > 0 & Flag.ToUpper() != "VENDOR_TYPE")
                    {
                        //VENDOR TYPE
                        sbNew.Append(" AND VVS.VENDOR_TYPE_FK=" + Convert.ToInt32(_paramVal));
                    }
                    else if (_paramIndex == 4 & Convert.ToInt32(_paramVal) > 0 & Flag.ToUpper() != "VENDOR")
                    {
                        //VENDOR FK
                        sbNew.Append(" AND VVS.VENDOR_FK=" + Convert.ToInt32(_paramVal));
                    }
                    else if (_paramIndex == 5 & _paramVal.Length > 0 & Flag.ToUpper() != "CURRENCY")
                    {
                        //CURRENCY FK
                        sbNew.Append(" AND VVS.CURRENCY_FK=" + Convert.ToInt32(_paramVal));
                    }
                    else if (_paramIndex == 6 & _paramVal.Length > 0)
                    {
                        //INVOICE FROM DATE
                        sbNew.Append(" AND (TO_DATE(VVS.VOUCHER_DATE,DATEFORMAT) >= TO_DATE('" + _paramVal.ToUpper() + "',DATEFORMAT) ");
                        sbNew.Append(" OR TO_DATE(VVS.PAYMENT_DATE,DATEFORMAT) >= TO_DATE('" + _paramVal.ToUpper() + "',DATEFORMAT)) ");
                    }
                    else if (_paramIndex == 7 & Convert.ToInt32(_paramVal) > 0)
                    {
                        //INVOICE TO DATE
                        sbNew.Append(" AND (TO_DATE(VVS.VOUCHER_DATE,DATEFORMAT) <= TO_DATE('" + _paramVal.ToUpper() + "',DATEFORMAT) ");
                        sbNew.Append(" OR TO_DATE(VVS.PAYMENT_DATE,DATEFORMAT) <= TO_DATE('" + _paramVal.ToUpper() + "',DATEFORMAT)) ");
                    }
                    else if (_paramIndex == 8 & Convert.ToInt32(_paramVal) != 3)
                    {
                        //bIZtYPE
                        sbNew.Append(" AND VVS.BIZ_TYPE=" + _paramVal);
                    }
                    else if (_paramIndex == 9 & Convert.ToInt32(_paramVal) != 0)
                    {
                        //PROCESS
                        sbNew.Append(" AND VVS.PROCESS_TYPE=" + _paramVal);
                    }

                    _paramIndex += 1;
                }
                if (Flag.ToUpper() == "COUNTRY")
                {
                    return "SELECT DISTINCT COUNTRY_FK FROM (" + sbNew.ToString() + ") ";
                }
                else if (Flag.ToUpper() == "LOCATION")
                {
                    return "SELECT DISTINCT LOC_FK FROM (" + sbNew.ToString() + ") ";
                }
                else if (Flag.ToUpper() == "VENDOR_TYPE")
                {
                    return "SELECT DISTINCT VENDOR_TYPE_FK FROM (" + sbNew.ToString() + ") ";
                }
                else if (Flag.ToUpper() == "VENDOR")
                {
                    return "SELECT DISTINCT VENDOR_FK FROM (" + sbNew.ToString() + ") ";
                }
                else if (Flag.ToUpper() == "CURRENCY")
                {
                    return "SELECT DISTINCT CURRENCY_FK FROM (" + sbNew.ToString() + ") ";
                }
            }
            catch (Exception ex)
            {
                return "0";
            }
            return "";
        }

        #endregion "Fetch VendorType"

        #region "Fetch Freight Elements"

        public DataSet FetchFrtElements(string FrtElementPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string BizType = "", string TypedData = "", string FrtElementID = "", string FrtElementName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string ChargePK = "", string EnCondition = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            FrtElementPK = FrtElementPK.TrimEnd(',');
            FrtElementPK = FrtElementPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            Array arrAnnSearchPks = EnCondition.Split('$');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (FrtElementPK == "undefined")
            {
                FrtElementPK = "";
            }
            if (FrtElementID == "undefined")
            {
                FrtElementID = "";
            }
            if (FrtElementName == "undefined")
            {
                FrtElementName = "";
            }
            if (TypedData == "UNDEFINED" | TypedData == "undefined")
            {
                TypedData = "";
            }
            if (!string.IsNullOrEmpty(FrtElementPK))
            {
                sb.Append("SELECT          FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                FM.FREIGHT_ELEMENT_ID,");
                sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                FM.Preference EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  FREIGHT_ELEMENT_MST_TBL FM ");
                // sb.Append("   WHERE FM.ACTIVE_FLAG =1 AND FM.CHARGE_BASIS <> 1 ")
                sb.Append("   WHERE FM.ACTIVE_FLAG = 1   ");

                if (!string.IsNullOrEmpty(FrtElementPK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK IN ('" + FrtElementPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND FM.FREIGHT_ELEMENT_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                FM.FREIGHT_ELEMENT_ID,");
                sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                FM.Preference EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FM ");
                //    sb.Append("   WHERE FM.ACTIVE_FLAG =1 AND FM.CHARGE_BASIS <> 1 ")
                sb.Append("   WHERE FM.ACTIVE_FLAG = 1   ");
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(FrtElementPK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + FrtElementPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(FrtElementID))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + FrtElementID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementName))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_NAME) LIKE '%" + FrtElementName.ToUpper().Replace("'", "''") + "%'");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND FM.FREIGHT_ELEMENT_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY EXTRA9");
            }
            else
            {
                sb.Append("SELECT          FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                FM.FREIGHT_ELEMENT_ID,");
                sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                FM.Preference EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FM ");
                // sb.Append("   WHERE FM.ACTIVE_FLAG = 1 AND FM.CHARGE_BASIS <> 1  ")
                sb.Append("   WHERE FM.ACTIVE_FLAG = 1   ");
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementID))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + FrtElementID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementName))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_NAME) LIKE '%" + FrtElementName.ToUpper().Replace("'", "''") + "%'");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append(" AND FM.FREIGHT_ELEMENT_MST_PK IN ( ");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_CUST_CONTRACT_SEA T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("   select t.FREIGHT_ELEMENT_MST_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY EXTRA9");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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
            return new DataSet();
        }

        #endregion "Fetch Freight Elements"

        #region "Fetch Freight Elements Local"

        public DataSet FetchFrtElementsLocal(string FrtElementPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string BizType = "", string TypedData = "", string FrtElementID = "", string FrtElementName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string ChargePK = "", Int32 CargoType = 0)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            FrtElementPK = FrtElementPK.TrimEnd(',');
            FrtElementPK = FrtElementPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (FrtElementPK == "undefined")
            {
                FrtElementPK = "";
            }
            if (FrtElementID == "undefined")
            {
                FrtElementID = "";
            }
            if (FrtElementName == "undefined")
            {
                FrtElementName = "";
            }
            if (TypedData == "UNDEFINED" | TypedData == "undefined")
            {
                TypedData = "";
            }
            if (!string.IsNullOrEmpty(FrtElementPK))
            {
                sb.Append(" SELECT * FROM (");
                sb.Append("SELECT DISTINCT FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("       FM.FREIGHT_ELEMENT_ID,");
                sb.Append("       FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("       DECODE(FM.BUSINESS_TYPE, 1, 'Air', 2, 'Sea', 3, 'Both') BUSINESS_TYPE,");
                sb.Append("       FM.BUSINESS_TYPE BIZ,");
                sb.Append("       D.DDID CHARGE_BASIS,");
                sb.Append("       FM.CHARGE_BASIS BASIS,");
                sb.Append("       FM.PREFERENCE EXTRA8,");
                //sb.Append("       FT.CHARGE_TYPE EXTRA9,")
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM (SELECT TO_NUMBER(DD.DD_VALUE) DDVALUE, DD.DD_ID DDID");
                sb.Append("          FROM QFOR_DROP_DOWN_TBL DD");
                sb.Append("         WHERE DD.DD_FLAG = 'BASIS'");
                sb.Append("           AND DD.CONFIG_ID = 'QFOR4458') D,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FM,");
                sb.Append("       FREIGHT_CONFIG_TRN_TBL FT ");
                sb.Append(" WHERE FM.FREIGHT_ELEMENT_MST_PK=FT.FREIGHT_ELEMENT_FK ");
                sb.Append("   AND FM.CHARGE_BASIS = D.DDVALUE(+) ");

                if (Convert.ToInt32(BizType) == 2 | Convert.ToInt32(BizType) == 1)
                {
                    if (CargoType == 2 | CargoType == 4)
                    {
                        sb.Append("   AND FM.CHARGE_BASIS = 2");
                    }
                }

                sb.Append("   AND FM.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(FrtElementPK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK IN ('" + FrtElementPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    //If BizType <> "3" And BizType <> "0" And BizType.ToUpper() <> "BOTH" Then
                    //    sb.Append("   AND FM.BUSINESS_TYPE IN ('" & BizType.ToUpper.Replace(",", "','") & "','3')")
                    //End If
                    if (BizType == "3" | BizType == "4")
                    {
                        sb.Append("   AND FM.BUSINESS_TYPE IN ('1','2','3') ");
                    }
                    else
                    {
                        sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                    }
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("       FM.FREIGHT_ELEMENT_ID,");
                sb.Append("       FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("       DECODE(FM.BUSINESS_TYPE, 1, 'Air', 2, 'Sea', 3, 'Both') BUSINESS_TYPE,");
                sb.Append("       FM.BUSINESS_TYPE BIZ,");
                sb.Append("       D.DDID CHARGE_BASIS,");
                sb.Append("       FM.CHARGE_BASIS BASIS,");
                sb.Append("       FM.PREFERENCE EXTRA8,");
                //sb.Append("       FT.CHARGE_TYPE EXTRA9,")
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM (SELECT TO_NUMBER(DD.DD_VALUE) DDVALUE, DD.DD_ID DDID");
                sb.Append("          FROM QFOR_DROP_DOWN_TBL DD");
                sb.Append("         WHERE DD.DD_FLAG = 'BASIS'");
                sb.Append("           AND DD.CONFIG_ID = 'QFOR4458') D,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FM,");
                sb.Append("       FREIGHT_CONFIG_TRN_TBL FT ");
                sb.Append(" WHERE FM.FREIGHT_ELEMENT_MST_PK=FT.FREIGHT_ELEMENT_FK ");
                sb.Append("   AND FM.CHARGE_BASIS = D.DDVALUE(+) ");

                // sb.Append("   AND FM.CHARGE_TYPE = 3") 'Local Charges
                if (Convert.ToInt32(BizType) == 2 | Convert.ToInt32(BizType) == 1)
                {
                    if (CargoType == 2 | CargoType == 4)
                    {
                        sb.Append("   AND FM.CHARGE_BASIS  = 2");
                    }
                }

                sb.Append("   AND FM.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(BizType))
                {
                    //If BizType <> "3" And BizType <> "0" And BizType.ToUpper() <> "BOTH" Then
                    //    sb.Append("   AND FM.BUSINESS_TYPE IN ('" & BizType.ToUpper.Replace(",", "','") & "','3')")
                    //End If
                    if (BizType == "3" | BizType == "4")
                    {
                        sb.Append("   AND FM.BUSINESS_TYPE IN ('1','2','3') ");
                    }
                    else
                    {
                        sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                    }
                }
                if (!string.IsNullOrEmpty(FrtElementPK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + FrtElementPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(FrtElementID))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + FrtElementID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementName))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_NAME) LIKE '%" + FrtElementName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ) Q ");
                sb.Append(" ORDER BY Q.EXTRA8,2,3");
            }
            else
            {
                sb.Append("SELECT DISTINCT FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("       FM.FREIGHT_ELEMENT_ID,");
                sb.Append("       FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("       DECODE(FM.BUSINESS_TYPE, 1, 'Air', 2, 'Sea', 3, 'Both') BUSINESS_TYPE,");
                sb.Append("       FM.BUSINESS_TYPE BIZ,");
                sb.Append("       D.DDID CHARGE_BASIS,");
                sb.Append("       FM.CHARGE_BASIS BASIS,");
                sb.Append("       FM.PREFERENCE EXTRA8,");
                //sb.Append("       FT.CHARGE_TYPE EXTRA9,")
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM (SELECT TO_NUMBER(DD.DD_VALUE) DDVALUE, DD.DD_ID DDID");
                sb.Append("          FROM QFOR_DROP_DOWN_TBL DD");
                sb.Append("         WHERE DD.DD_FLAG = 'BASIS'");
                sb.Append("           AND DD.CONFIG_ID = 'QFOR4458') D,");
                sb.Append("       FREIGHT_ELEMENT_MST_TBL FM,");
                sb.Append("       FREIGHT_CONFIG_TRN_TBL FT ");
                sb.Append(" WHERE FM.FREIGHT_ELEMENT_MST_PK=FT.FREIGHT_ELEMENT_FK ");
                sb.Append("   AND FM.CHARGE_BASIS = D.DDVALUE(+) ");

                // sb.Append("   AND FM.CHARGE_TYPE = 3") 'Local Charges
                if (Convert.ToInt32(BizType) == 2 | Convert.ToInt32(BizType) == 1)
                {
                    if (CargoType == 2 | CargoType == 3)
                    {
                        sb.Append("   AND FM.CHARGE_BASIS  = 2");
                    }
                }

                sb.Append("   AND FM.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(BizType))
                {
                    //If BizType <> "3" And BizType <> "0" And BizType.ToUpper() <> "BOTH" Then
                    //    sb.Append("   AND FM.BUSINESS_TYPE IN ('" & BizType.ToUpper.Replace(",", "','") & "','3')")
                    //End If
                    if (BizType == "3" | BizType == "4")
                    {
                        sb.Append("   AND FM.BUSINESS_TYPE IN ('1','2','3') ");
                    }
                    else
                    {
                        sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                    }
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementID))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + FrtElementID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementName))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_NAME) LIKE '%" + FrtElementName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY FM.PREFERENCE,2,3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Freight Elements Local"

        #region "Fetch Freight Elements"

        public DataSet FetchFrtElementsNew(string FrtElementPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string BizType = "", string TypedData = "", string FrtElementID = "", string FrtElementName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string ChargePK = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            FrtElementPK = FrtElementPK.TrimEnd(',');
            FrtElementPK = FrtElementPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (FrtElementPK == "undefined")
            {
                FrtElementPK = "";
            }
            if (FrtElementID == "undefined")
            {
                FrtElementID = "";
            }
            if (FrtElementName == "undefined")
            {
                FrtElementName = "";
            }
            if (TypedData == "UNDEFINED" | TypedData == "undefined")
            {
                TypedData = "";
            }
            if (!string.IsNullOrEmpty(FrtElementPK))
            {
                sb.Append("SELECT          FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                FM.FREIGHT_ELEMENT_ID,");
                sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  FREIGHT_ELEMENT_MST_TBL FM ");
                sb.Append("   WHERE FM.ACTIVE_FLAG =1 AND FM.CHARGE_TYPE <> 3 ");

                if (!string.IsNullOrEmpty(FrtElementPK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK IN ('" + FrtElementPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                sb.Append(" UNION ");

                sb.Append("SELECT          FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                FM.FREIGHT_ELEMENT_ID,");
                sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FM ");
                sb.Append("   WHERE FM.ACTIVE_FLAG =1 AND FM.CHARGE_TYPE <> 3 ");
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(FrtElementPK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + FrtElementPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(FrtElementID))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + FrtElementID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementName))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_NAME) LIKE '%" + FrtElementName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          FM.FREIGHT_ELEMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                FM.FREIGHT_ELEMENT_ID,");
                sb.Append("                FM.FREIGHT_ELEMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM FREIGHT_ELEMENT_MST_TBL FM ");
                sb.Append("   WHERE FM.ACTIVE_FLAG = 1 AND FM.CHARGE_TYPE <> 3  ");
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND FM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ChargePK))
                {
                    sb.Append("   AND FM.FREIGHT_ELEMENT_MST_PK NOT IN ('" + ChargePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementID))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_ID) LIKE '%" + FrtElementID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(FrtElementName))
                {
                    sb.Append(" AND UPPER(FM.FREIGHT_ELEMENT_NAME) LIKE '%" + FrtElementName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Freight Elements"

        #region "Fetch Sector"

        public DataSet FetchSector(string SectorPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradePK = "", string TypedData = "", string SectorID = "", string SectorName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string BizType = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            SectorPK = SectorPK.TrimEnd(',');
            SectorPK = SectorPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (SectorPK == "undefined")
            {
                SectorPK = "";
            }
            if (SectorID == "undefined")
            {
                SectorID = "";
            }
            if (SectorName == "undefined")
            {
                SectorName = "";
            }
            if (TypedData == "UNDEFINED" | TypedData == "undefined")
            {
                TypedData = "";
            }
            if (!string.IsNullOrEmpty(SectorPK))
            {
                sb.Append("SELECT          SM.SECTOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                SM.SECTOR_ID,");
                sb.Append("                (POL.PORT_ID || '-' || POD.PORT_ID) SECTOR_DESC,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM SECTOR_MST_TBL SM,TRADE_MST_TBL  TD,PORT_MST_TBL   POL,PORT_MST_TBL   POD ");
                sb.Append("   WHERE TD.TRADE_MST_PK = SM.TRADE_MST_FK ");
                sb.Append("   AND POL.PORT_MST_PK = SM.FROM_PORT_FK ");
                sb.Append("   AND POD.PORT_MST_PK = SM.TO_PORT_FK ");
                sb.Append("   AND SM.ACTIVE = 1 ");

                if (!string.IsNullOrEmpty(SectorPK))
                {
                    sb.Append("   AND SM.SECTOR_MST_PK IN ('" + SectorPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND SM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TradePK))
                {
                    sb.Append("   AND SM.TRADE_MST_FK IN ('" + TradePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(SectorID))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_ID) LIKE '%" + SectorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SectorName))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_DESC) LIKE '%" + SectorName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" UNION ");

                sb.Append("SELECT          SM.SECTOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                SM.SECTOR_ID,");
                sb.Append("                (POL.PORT_ID || '-' || POD.PORT_ID) SECTOR_DESC,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM SECTOR_MST_TBL SM,TRADE_MST_TBL  TD,PORT_MST_TBL   POL,PORT_MST_TBL   POD ");
                sb.Append("   WHERE TD.TRADE_MST_PK = SM.TRADE_MST_FK ");
                sb.Append("   AND POL.PORT_MST_PK = SM.FROM_PORT_FK ");
                sb.Append("   AND POD.PORT_MST_PK = SM.TO_PORT_FK ");
                sb.Append("   AND SM.ACTIVE = 1 ");
                if (!string.IsNullOrEmpty(TradePK))
                {
                    sb.Append("   AND SM.TRADE_MST_FK IN ('" + TradePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND SM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(SectorPK))
                {
                    sb.Append("   AND SM.SECTOR_MST_PK  NOT IN ('" + SectorPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(SectorID))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_ID) LIKE '%" + SectorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SectorName))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_DESC) LIKE '%" + SectorName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          SM.SECTOR_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                SM.SECTOR_ID,");
                sb.Append("                (POL.PORT_ID || '-' || POD.PORT_ID) SECTOR_DESC,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM SECTOR_MST_TBL SM,TRADE_MST_TBL  TD,PORT_MST_TBL   POL,PORT_MST_TBL   POD ");
                sb.Append("   WHERE TD.TRADE_MST_PK = SM.TRADE_MST_FK ");
                sb.Append("   AND POL.PORT_MST_PK = SM.FROM_PORT_FK ");
                sb.Append("   AND POD.PORT_MST_PK = SM.TO_PORT_FK ");
                sb.Append("   AND SM.ACTIVE = 1 ");
                if (!string.IsNullOrEmpty(TradePK))
                {
                    sb.Append("   AND SM.TRADE_MST_FK IN ('" + TradePK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND SM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SectorID))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_ID) LIKE '%" + SectorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SectorName))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_DESC) LIKE '%" + SectorName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Sector"

        #region "Fetch InvToAgentFlag"

        public DataSet FetchInvToAgentFlag(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AdditionalPks = "")
        {
            StringBuilder sb = new StringBuilder();
            System.Text.StringBuilder sbNew = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            //------------------------------
            sbNew.Append(" SELECT DISTINCT (CASE WHEN VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='CB' THEN 1 ");
            sbNew.Append("                WHEN VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='DP' THEN 2");
            sbNew.Append("                WHEN VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='CB' THEN 3");
            sbNew.Append("                WHEN VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='LA' THEN 4");
            sbNew.Append("                WHEN VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='CB' THEN 5");
            sbNew.Append("                WHEN VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='DP' THEN 6");
            sbNew.Append("                WHEN VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='CB' THEN 7");
            sbNew.Append("                WHEN VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='LA' THEN 8 END) ");
            sbNew.Append(" FROM VIEW_INVOICE_TO_AGENT_LST VW WHERE 1=1 ");

            string _paramVal = "";
            int _paramIndex = 1;
            while (AdditionalPks.Split('$').Length > _paramIndex)
            {
                _paramVal = AdditionalPks.Split('$')[_paramIndex];

                if (_paramIndex == 1 & Convert.ToInt32(_paramVal) > 0)
                {
                    //'AGENT TYPE
                    //Select Case Val(_paramVal)
                    //    Case 1 'CB AGENT SEA EXP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='CB'")
                    //    Case 2 'DP AGENT SEA EXP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='DP'")
                    //    Case 3 'CB AGENT SEA IMP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='CB'")
                    //    Case 4 'LOAD AGENT SEA IMP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=2 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='LA'")
                    //    Case 5 'CB AGENT AIR EXP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='CB'")
                    //    Case 6 'DP AGENT AIR EXP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=1 AND VW.AGENT_TYPE='DP'")
                    //    Case 7 'CB AGENT AIR IMP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='CB'")
                    //    Case 8 'LOAD AGENT AIR IMP
                    //        sbNew.Append(" AND VW.BUSINESS_TYPE=1 AND VW.PROCESS_TYPE=2 AND VW.AGENT_TYPE='LA'")
                    //End Select
                }
                else if (_paramIndex == 2 & Convert.ToInt32(_paramVal) > 0)
                {
                    //LOCATION FK
                    sbNew.Append(" AND VW.DEFAULT_LOCATION_FK=" + Convert.ToInt32(_paramVal));
                }
                else if (_paramIndex == 3 & Convert.ToInt32(_paramVal) > 0)
                {
                    //INVOICE FK
                    sbNew.Append(" AND VW.INV_PK=" + Convert.ToInt32(_paramVal));
                }
                else if (_paramIndex == 4 & Convert.ToInt32(_paramVal) > 0)
                {
                    //JC FK
                    sbNew.Append(" AND VW.JC_PK=" + Convert.ToInt32(_paramVal));
                }
                else if (_paramIndex == 5 & _paramVal.Length > 0)
                {
                    //HBL REF
                    sbNew.Append(" AND UPPER(VW.HBL_REF_NO) LIKE '%" + _paramVal.ToUpper() + "%'");
                }
                else if (_paramIndex == 6 & _paramVal.Length > 0)
                {
                    //MBL REF
                    sbNew.Append(" AND UPPER(VW.MBL_REF_NO) LIKE '%" + _paramVal.ToUpper() + "%'");
                }
                else if (_paramIndex == 7 & Convert.ToInt32(_paramVal) > 0)
                {
                    //VOY TRN FK
                    sbNew.Append(" AND VW.VOYAGE_TRN_FK=" + Convert.ToInt32(_paramVal));
                }
                else if (_paramIndex == 8 & _paramVal.Length > 0)
                {
                    //VOY-FLIGHT NR
                    sbNew.Append(" AND UPPER(VW.VOY_FLIGHT_NO) LIKE '%" + _paramVal.Trim().ToUpper() + "%'");
                }
                else if (_paramIndex == 9 & Convert.ToInt32(_paramVal) > 0)
                {
                    //AGENT FK
                    sbNew.Append(" AND VW.AGENT_PK=" + Convert.ToInt32(_paramVal));
                }
                else if (_paramIndex == 10 & !string.IsNullOrEmpty(_paramVal))
                {
                    //STATUS
                    sbNew.Append(" AND (SELECT COUNT(*) FROM INV_AGENT_TBL INV WHERE INV.INV_AGENT_PK=VW.INV_PK  AND INV.CHK_INVOICE=" + Convert.ToInt32(_paramVal) + ")>0 ");
                }

                _paramIndex += 1;
            }
            //------------------------------
            sb.Append(" SELECT DISTINCT Q.DD_VALUE AGENT_TYPE_PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                Q.DD_ID AGENT_TYPE_ID,");
            sb.Append("                Q.DESCRIPTION AGENT_TYPE,");
            sb.Append("                '' EXTRA5,");
            sb.Append("                '' EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                '1' ACTIVE");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL       Q ");
            sb.Append("  WHERE Q.CONFIG_ID = 'QFOR4444' AND Q.DD_FLAG = 'INV_TO_AGENT'");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("   AND   Q.DD_VALUE IN ('" + PK.ToUpper().Replace(",", "','") + "')");
            }
            else
            {
                sb.Append("   AND   Q.DD_VALUE IS NULL ");
            }
            sb.Append(" AND Q.DD_VALUE IN (" + sbNew.ToString() + ") ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT Q.DD_VALUE AGENT_TYPE_PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                Q.DD_ID AGENT_TYPE_ID,");
            sb.Append("                Q.DESCRIPTION AGENT_TYPE,");
            sb.Append("                '' EXTRA5,");
            sb.Append("                '' EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                '0' ACTIVE");
            sb.Append("  FROM QFOR_DROP_DOWN_TBL       Q ");
            sb.Append("  WHERE Q.CONFIG_ID = 'QFOR4444' AND Q.DD_FLAG = 'INV_TO_AGENT'");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("   AND  Q.DD_VALUE NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
            }
            if (!string.IsNullOrEmpty(ID))
            {
                sb.Append(" AND UPPER(Q.DD_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
            }
            if (!string.IsNullOrEmpty(NAME))
            {
                sb.Append(" AND UPPER(Q.DESCRIPTION) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
            }
            sb.Append(" AND Q.DD_VALUE IN (" + sbNew.ToString() + ") ");

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch InvToAgentFlag"

        ///FOR Announcement Internal

        #region "Fetch Department"

        public DataSet FetchDepartment(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append(" SELECT DISTINCT DMT.DEPARTMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                DMT.DEPARTMENT_ID,");
                sb.Append("                DMT.DEPARTMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM DEPARTMENT_MST_TBL       DMT,");
                sb.Append("       LOCATION_DEPARTMENTS_TRN LDT,");
                sb.Append("       DESIGNATION_MST_TBL      DSMT,");
                sb.Append("       DEPT_DESIG_TRN           DDT");
                sb.Append(" WHERE DMT.ACTIVE_FLAG = 1");
                sb.Append("   AND DMT.DEPARTMENT_MST_PK = LDT.DEPARTMENT_MST_FK");
                sb.Append("   AND DMT.DEPARTMENT_MST_PK = DDT.DEPART_MST_FK");
                sb.Append("   AND DSMT.DESIGNATION_MST_PK = DDT.DESIGNATION_MST_FK");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("     AND LDT.LOCATION_MST_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("     AND DSMT.DESIGNATION_MST_PK IN(" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  DMT.DEPARTMENT_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 1));
                }

                sb.Append(" UNION ");

                sb.Append(" SELECT DISTINCT DMT.DEPARTMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                DMT.DEPARTMENT_ID,");
                sb.Append("                DMT.DEPARTMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM DEPARTMENT_MST_TBL       DMT,");
                sb.Append("       LOCATION_DEPARTMENTS_TRN LDT,");
                sb.Append("       DESIGNATION_MST_TBL      DSMT,");
                sb.Append("       DEPT_DESIG_TRN           DDT");
                sb.Append(" WHERE DMT.ACTIVE_FLAG = 1");
                sb.Append("   AND DMT.DEPARTMENT_MST_PK = LDT.DEPARTMENT_MST_FK");
                sb.Append("   AND DMT.DEPARTMENT_MST_PK = DDT.DEPART_MST_FK");
                sb.Append("   AND DSMT.DESIGNATION_MST_PK = DDT.DESIGNATION_MST_FK");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("     AND LDT.LOCATION_MST_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("     AND DSMT.DESIGNATION_MST_PK  IN(" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  DMT.DEPARTMENT_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(DMT.DEPARTMENT_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(DMT.DEPARTMENT_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 1));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append(" SELECT DISTINCT DMT.DEPARTMENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                DMT.DEPARTMENT_ID,");
                sb.Append("                DMT.DEPARTMENT_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM DEPARTMENT_MST_TBL       DMT,");
                sb.Append("       LOCATION_DEPARTMENTS_TRN LDT,");
                sb.Append("       DESIGNATION_MST_TBL      DSMT,");
                sb.Append("       DEPT_DESIG_TRN           DDT");
                sb.Append(" WHERE DMT.ACTIVE_FLAG = 1");
                sb.Append("   AND DMT.DEPARTMENT_MST_PK = LDT.DEPARTMENT_MST_FK");
                sb.Append("   AND DMT.DEPARTMENT_MST_PK = DDT.DEPART_MST_FK");
                sb.Append("   AND DSMT.DESIGNATION_MST_PK = DDT.DESIGNATION_MST_FK");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("     AND LDT.LOCATION_MST_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("     AND DSMT.DESIGNATION_MST_PK IN(" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(DMT.DEPARTMENT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(DMT.DEPARTMENT_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(DMT.DEPARTMENT_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 1));
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Department"

        #region "Fetch Designation"

        public DataSet FetchDesignation(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append(" SELECT DISTINCT DSMT.DESIGNATION_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       DSMT.DESIGNATION_ID,");
                sb.Append("       DSMT.DESIGNATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append("  FROM DEPARTMENT_MST_TBL DMT, DESIGNATION_MST_TBL DSMT, DEPT_DESIG_TRN DDT");
                sb.Append(" WHERE DMT.DEPARTMENT_MST_PK = DDT.DEPART_MST_FK");
                sb.Append("   AND DSMT.ACTIVE_FLAG=1");
                sb.Append("   AND DSMT.DESIGNATION_MST_PK = DDT.DESIGNATION_MST_FK");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND DMT.DEPARTMENT_MST_PK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  DSMT.DESIGNATION_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 2));
                }

                sb.Append(" UNION ");

                sb.Append(" SELECT DISTINCT DSMT.DESIGNATION_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       DSMT.DESIGNATION_ID,");
                sb.Append("       DSMT.DESIGNATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("  FROM DEPARTMENT_MST_TBL DMT, DESIGNATION_MST_TBL DSMT, DEPT_DESIG_TRN DDT");
                sb.Append(" WHERE DMT.DEPARTMENT_MST_PK = DDT.DEPART_MST_FK");
                sb.Append("   AND DSMT.ACTIVE_FLAG= 1 ");
                sb.Append("   AND DSMT.DESIGNATION_MST_PK = DDT.DESIGNATION_MST_FK");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND DMT.DEPARTMENT_MST_PK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND DSMT.DESIGNATION_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(DSMT.DESIGNATION_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(DSMT.DESIGNATION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 2));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append(" SELECT DISTINCT DSMT.DESIGNATION_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       DSMT.DESIGNATION_ID,");
                sb.Append("       DSMT.DESIGNATION_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("  FROM DEPARTMENT_MST_TBL DMT, DESIGNATION_MST_TBL DSMT, DEPT_DESIG_TRN DDT");
                sb.Append(" WHERE DMT.DEPARTMENT_MST_PK = DDT.DEPART_MST_FK");
                sb.Append("   AND DSMT.ACTIVE_FLAG=1");
                sb.Append("   AND DSMT.DESIGNATION_MST_PK = DDT.DESIGNATION_MST_FK");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND DMT.DEPARTMENT_MST_PK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  DSMT.DESIGNATION_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(DSMT.DESIGNATION_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(DSMT.DESIGNATION_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(DSMT.DESIGNATION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 2));
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Designation"

        #region "Fetch User"

        public DataSet FetchUser(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string TypedData = "", string ID = "", string NAME = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT UMT.USER_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       UMT.USER_ID,");
                sb.Append("       UMT.USER_NAME,");
                sb.Append("       LMT.LOCATION_ID,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       EMT.EMPLOYEE_ID,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append(" FROM  USER_MST_TBL UMT,");
                sb.Append("      LOCATION_MST_TBL  LMT,");
                sb.Append("      EMPLOYEE_MST_TBL EMT");
                sb.Append(" WHERE UMT.DEFAULT_LOCATION_FK=LMT.LOCATION_MST_PK");
                sb.Append(" AND   UMT.EMPLOYEE_MST_FK=EMT.EMPLOYEE_MST_PK");
                sb.Append(" AND   UMT.IS_ACTIVATED=1");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN (" + ConditionPK + ") ");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND EMT.DEPARTMENT_MST_FK IN (" + ConditionPK1 + ") ");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND EMT.DESIGNATION_MST_FK IN (" + ConditionPK2 + ") ");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND UMT.USER_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 3));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT UMT.USER_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       UMT.USER_ID,");
                sb.Append("       UMT.USER_NAME,");
                sb.Append("       LMT.LOCATION_ID,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       EMT.EMPLOYEE_ID,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append(" FROM  USER_MST_TBL UMT,");
                sb.Append("      LOCATION_MST_TBL  LMT,");
                sb.Append("      EMPLOYEE_MST_TBL EMT");
                sb.Append(" WHERE UMT.DEFAULT_LOCATION_FK=LMT.LOCATION_MST_PK");
                sb.Append(" AND   UMT.EMPLOYEE_MST_FK=EMT.EMPLOYEE_MST_PK");
                sb.Append(" AND   UMT.IS_ACTIVATED=1");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN (" + ConditionPK + ") ");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND EMT.DEPARTMENT_MST_FK IN (" + ConditionPK1 + ") ");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND EMT.DESIGNATION_MST_FK IN (" + ConditionPK2 + ") ");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND UMT.USER_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(UMT.USER_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(UMT.USER_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 3));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT UMT.USER_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       UMT.USER_ID,");
                sb.Append("       UMT.USER_NAME,");
                sb.Append("       LMT.LOCATION_ID,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       EMT.EMPLOYEE_ID,");
                sb.Append("               '' EXTRA8,");
                sb.Append("               '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append(" FROM  USER_MST_TBL UMT,");
                sb.Append("      LOCATION_MST_TBL  LMT,");
                sb.Append("      EMPLOYEE_MST_TBL EMT");
                sb.Append(" WHERE UMT.DEFAULT_LOCATION_FK=LMT.LOCATION_MST_PK");
                sb.Append(" AND   UMT.EMPLOYEE_MST_FK=EMT.EMPLOYEE_MST_PK");
                sb.Append(" AND   UMT.IS_ACTIVATED=1");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN (" + ConditionPK + ") ");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND EMT.DEPARTMENT_MST_FK IN (" + ConditionPK1 + ") ");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND EMT.DESIGNATION_MST_FK IN (" + ConditionPK2 + ") ");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(UMT.USER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(UMT.USER_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(UMT.USER_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 3));
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch User"

        #region "Fetch Management"

        public DataSet FetchManagement(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string TypedData = "", string ID = "",
        string NAME = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT DISTINCT EMT.EMPLOYEE_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("         EMT.EMPLOYEE_ID,");
                sb.Append("         EMT.EMPLOYEE_NAME,");
                sb.Append("         LMT.LOCATION_ID,");
                sb.Append("         DMT.DEPARTMENT_ID,");
                sb.Append("         DT.DESIGNATION_ID,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append(" FROM EMPLOYEE_MST_TBL EMT,");
                sb.Append(" LOCATION_MST_TBL LMT,");
                sb.Append(" DEPARTMENT_MST_TBL DMT,");
                sb.Append(" DESIGNATION_MST_TBL DT, ");
                sb.Append(" USER_MST_TBL UMT ");
                sb.Append(" WHERE EMT.LOCATION_MST_FK= LMT.LOCATION_MST_PK");
                sb.Append(" AND EMT.DEPARTMENT_MST_FK = DMT.DEPARTMENT_MST_PK");
                sb.Append(" AND EMT.DESIGNATION_MST_FK = DT.DESIGNATION_MST_PK");
                sb.Append("  AND UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK AND EMT.EMP_TYPE=1");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND EMT.LOCATION_MST_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND EMT.DESIGNATION_MST_FK IN(" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND EMT.DEPARTMENT_MST_FK IN(" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK3))
                {
                    sb.Append("   AND UMT.USER_MST_PK IN(" + ConditionPK3 + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  EMT.EMPLOYEE_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 4));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT EMT.EMPLOYEE_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("         EMT.EMPLOYEE_ID,");
                sb.Append("         EMT.EMPLOYEE_NAME,");
                sb.Append("         LMT.LOCATION_ID,");
                sb.Append("         DMT.DEPARTMENT_ID,");
                sb.Append("         DT.DESIGNATION_ID,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append(" FROM EMPLOYEE_MST_TBL EMT,");
                sb.Append(" LOCATION_MST_TBL LMT,");
                sb.Append(" DEPARTMENT_MST_TBL DMT,");
                sb.Append(" DESIGNATION_MST_TBL DT, ");
                sb.Append(" USER_MST_TBL UMT ");
                sb.Append(" WHERE EMT.LOCATION_MST_FK= LMT.LOCATION_MST_PK");
                sb.Append(" AND EMT.DEPARTMENT_MST_FK = DMT.DEPARTMENT_MST_PK");
                sb.Append(" AND EMT.DESIGNATION_MST_FK = DT.DESIGNATION_MST_PK");
                sb.Append(" AND UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK AND EMT.EMP_TYPE=1");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND EMT.LOCATION_MST_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND EMT.DESIGNATION_MST_FK IN(" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND EMT.DEPARTMENT_MST_FK IN(" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK3))
                {
                    sb.Append("   AND UMT.USER_MST_PK IN(" + ConditionPK3 + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND EMT.EMPLOYEE_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(EMT.EMPLOYEE_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(EMT.EMPLOYEE_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 4));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT EMT.EMPLOYEE_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("         EMT.EMPLOYEE_ID,");
                sb.Append("         EMT.EMPLOYEE_NAME,");
                sb.Append("         LMT.LOCATION_ID,");
                sb.Append("         DMT.DEPARTMENT_ID,");
                sb.Append("         DT.DESIGNATION_ID,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append(" FROM EMPLOYEE_MST_TBL EMT,");
                sb.Append(" LOCATION_MST_TBL LMT,");
                sb.Append(" DEPARTMENT_MST_TBL DMT,");
                sb.Append(" DESIGNATION_MST_TBL DT, ");
                sb.Append(" USER_MST_TBL UMT ");
                sb.Append(" WHERE EMT.LOCATION_MST_FK= LMT.LOCATION_MST_PK");
                sb.Append(" AND EMT.DEPARTMENT_MST_FK = DMT.DEPARTMENT_MST_PK");
                sb.Append(" AND EMT.DESIGNATION_MST_FK = DT.DESIGNATION_MST_PK");
                sb.Append("  AND UMT.EMPLOYEE_MST_FK = EMT.EMPLOYEE_MST_PK AND EMT.EMP_TYPE=1");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND EMT.LOCATION_MST_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND EMT.DESIGNATION_MST_FK IN(" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND EMT.DEPARTMENT_MST_FK IN(" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK3))
                {
                    sb.Append("   AND UMT.USER_MST_PK IN(" + ConditionPK3 + ")");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(EMT.EMPLOYEE_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(EMT.EMPLOYEE_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(EMT.EMPLOYEE_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQuery(AnnSearchPks, 4));
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Management"

        ///FOR Announcement External

        #region "Fetch Region"

        public DataSet FetchAllRegion(string RegionPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            RegionPK = RegionPK.TrimEnd(',');
            RegionPK = RegionPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(RegionPK))
            {
                sb.Append("SELECT RMT.REGION_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("         RMT.REGION_CODE, ");
                sb.Append("        RMT.REGION_NAME, ");
                sb.Append("        DECODE(RMT.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BTYPE, ");
                sb.Append("              '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM REGION_MST_TBL RMT");
                sb.Append(" WHERE RMT.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(RegionPK))
                {
                    sb.Append("   AND RMT.REGION_MST_PK IN ('" + RegionPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND RMT.REGION_MST_PK IN (");
                        sb.Append("   select t.REGION_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 1));
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT RMT.REGION_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("        RMT.REGION_CODE, ");
                sb.Append("        RMT.REGION_NAME, ");
                sb.Append("        DECODE(RMT.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BIZTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM REGION_MST_TBL RMT");
                sb.Append(" WHERE RMT.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(RegionPK))
                {
                    sb.Append("   AND RMT.REGION_MST_PK  NOT IN ('" + RegionPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(RMT.REGION_CODE) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(RMT.REGION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK) & ConditionPK != "0")
                {
                    sb.Append("  AND RMT.BUSINESS_TYPE IN(" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND RMT.REGION_MST_PK IN (");
                        sb.Append("   select t.REGION_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 1));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT RMT.REGION_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("       RMT.REGION_CODE, ");
                sb.Append("       RMT.REGION_NAME, ");
                sb.Append("       DECODE(RMT.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BIZTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM REGION_MST_TBL RMT");
                sb.Append(" WHERE RMT.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(RMT.REGION_CODE) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(RMT.REGION_CODE) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(RMT.REGION_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK) & ConditionPK != "0")
                {
                    sb.Append("  AND RMT.BUSINESS_TYPE IN(" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND RMT.REGION_MST_PK IN (");
                        sb.Append("   select t.REGION_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 1));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Region"

        #region "Fetch Trade"

        public DataSet FetchTrade(string TrdPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string BizType = "", string TypedData = "", string TrdID = "", string TrdName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            TrdPK = TrdPK.TrimEnd(',');
            TrdPK = TrdPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (TrdPK == "undefined")
            {
                TrdPK = "";
            }
            if (TrdID == "undefined")
            {
                TrdID = "";
            }
            if (TrdName == "undefined")
            {
                TrdName = "";
            }
            if (TypedData == "UNDEFINED" | TypedData == "undefined")
            {
                TypedData = "";
            }
            if (!string.IsNullOrEmpty(TrdPK))
            {
                sb.Append("SELECT          TD.TRADE_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                TD.TRADE_CODE,");
                sb.Append("                TD.TRADE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  TRADE_MST_TBL TD ");
                sb.Append("   WHERE TD.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(TrdPK))
                {
                    sb.Append("   AND TD.TRADE_MST_PK IN ('" + TrdPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND TD.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TrdID))
                {
                    sb.Append(" AND UPPER(TD.TRADE_CODE) LIKE '%" + TrdID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(TrdName))
                {
                    sb.Append(" AND UPPER(TD.TRADE_NAME) LIKE '%" + TrdName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 2));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          TD.TRADE_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                TD.TRADE_CODE,");
                sb.Append("                TD.TRADE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM TRADE_MST_TBL TD ");
                sb.Append("   WHERE TD.ACTIVE_FLAG =1");
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND TD.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TrdPK))
                {
                    sb.Append("   AND TD.TRADE_MST_PK  NOT IN ('" + TrdPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(TrdID))
                {
                    sb.Append(" AND UPPER(TD.TRADE_CODE) LIKE '%" + TrdID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(TrdName))
                {
                    sb.Append(" AND UPPER(TD.TRADE_NAME) LIKE '%" + TrdName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 2));
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          TD.TRADE_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                TD.TRADE_CODE,");
                sb.Append("                TD.TRADE_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM TRADE_MST_TBL TD ");
                sb.Append("   WHERE TD.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND TD.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(TD.TRADE_CODE) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(TrdID))
                {
                    sb.Append(" AND UPPER(TD.TRADE_CODE) LIKE '%" + TrdID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(TrdName))
                {
                    sb.Append(" AND UPPER(TD.TRADE_NAME) LIKE '%" + TrdName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 2));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Trade"

        #region "Fetch Area"

        public DataSet FetchAllArea(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT  AMT.AREA_MST_PK,");
                sb.Append("        '' EMPTY,");
                sb.Append("        AMT.AREA_ID,");
                sb.Append("        AMT.AREA_NAME,");
                sb.Append("        '' EXTRA5,");
                sb.Append("        '' EXTRA6,");
                sb.Append("        '' EXTRA7,");
                sb.Append("        '' EXTRA8,");
                sb.Append("        '' EXTRA9,");
                sb.Append("        '1' ACTIVE");
                sb.Append("  FROM AREA_MST_TBL AMT");
                sb.Append(" WHERE AMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND AMT.AREA_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND AMT.REGION_MST_FK IN (" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 4));
                }
                sb.Append(" UNION ");

                sb.Append("SELECT  AMT.AREA_MST_PK,");
                sb.Append("        '' EMPTY,");
                sb.Append("        AMT.AREA_ID,");
                sb.Append("        AMT.AREA_NAME,");
                sb.Append("        '' EXTRA5,");
                sb.Append("        '' EXTRA6,");
                sb.Append("        '' EXTRA7,");
                sb.Append("        '' EXTRA8,");
                sb.Append("        '' EXTRA9,");
                sb.Append("        '0' ACTIVE");
                sb.Append("  FROM AREA_MST_TBL AMT");
                sb.Append(" WHERE AMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND AMT.AREA_MST_PK  NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND AMT.REGION_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(AMT.AREA_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(AMT.AREA_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 4));
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append(" SELECT AMT.AREA_MST_PK,");
                sb.Append("        '' EMPTY,");
                sb.Append("        AMT.AREA_ID,");
                sb.Append("        AMT.AREA_NAME,");
                sb.Append("        '' EXTRA5,");
                sb.Append("        '' EXTRA6,");
                sb.Append("        '' EXTRA7,");
                sb.Append("        '' EXTRA8,");
                sb.Append("        '' EXTRA9,");
                sb.Append("        '0' ACTIVE");
                sb.Append("  FROM AREA_MST_TBL AMT");
                sb.Append(" WHERE AMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(AMT.AREA_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("  AND AMT.REGION_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(AMT.AREA_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(AMT.AREA_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 4));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Area"

        #region "Fetch SectorTrade"

        public DataSet FetchSectorTrade(string SectorPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string TypedData = "", string SectorID = "", string SectorName = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string BizType = "", string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            SectorPK = SectorPK.TrimEnd(',');
            SectorPK = SectorPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (SectorPK == "undefined")
            {
                SectorPK = "";
            }
            if (SectorID == "undefined")
            {
                SectorID = "";
            }
            if (SectorName == "undefined")
            {
                SectorName = "";
            }
            if (TypedData == "UNDEFINED" | TypedData == "undefined")
            {
                TypedData = "";
            }
            if (!string.IsNullOrEmpty(SectorPK))
            {
                sb.Append("SELECT SM.SECTOR_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       SM.SECTOR_ID,");
                sb.Append("       POL.PORT_ID AS POLID,");
                sb.Append("       POD.PORT_ID AS PODID,");
                sb.Append("       TD.TRADE_CODE,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM SECTOR_MST_TBL SM,");
                sb.Append("       TRADE_MST_TBL  TD,");
                sb.Append("       PORT_MST_TBL   POL,");
                sb.Append("       PORT_MST_TBL   POD");
                sb.Append(" WHERE SM.TRADE_MST_FK = TD.TRADE_MST_PK");
                sb.Append("   AND SM.FROM_PORT_FK = POL.PORT_MST_PK");
                sb.Append("   AND SM.TO_PORT_FK = POD.PORT_MST_PK");
                sb.Append("   AND SM.ACTIVE = 1");

                if (!string.IsNullOrEmpty(SectorPK))
                {
                    sb.Append("   AND SM.SECTOR_MST_PK IN ('" + SectorPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND SM.TRADE_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND SM.FROM_PORT_FK IN (" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND SM.TO_PORT_FK  IN (" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND SM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 5));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT SM.SECTOR_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       SM.SECTOR_ID,");
                sb.Append("       POL.PORT_ID AS POLID,");
                sb.Append("       POD.PORT_ID AS PODID,");
                sb.Append("       TD.TRADE_CODE,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM SECTOR_MST_TBL SM,");
                sb.Append("       TRADE_MST_TBL  TD,");
                sb.Append("       PORT_MST_TBL   POL,");
                sb.Append("       PORT_MST_TBL   POD");
                sb.Append(" WHERE SM.TRADE_MST_FK = TD.TRADE_MST_PK");
                sb.Append("   AND SM.FROM_PORT_FK = POL.PORT_MST_PK");
                sb.Append("   AND SM.TO_PORT_FK = POD.PORT_MST_PK");
                sb.Append("   AND SM.ACTIVE = 1");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND SM.TRADE_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND SM.FROM_PORT_FK IN (" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND SM.TO_PORT_FK  IN (" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND SM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(SectorPK))
                {
                    sb.Append("   AND SM.SECTOR_MST_PK  NOT IN ('" + SectorPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(SectorID))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_ID) LIKE '%" + SectorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SectorName))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_DESC) LIKE '%" + SectorName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 5));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT SM.SECTOR_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       SM.SECTOR_ID,");
                sb.Append("       POL.PORT_ID AS POLID,");
                sb.Append("       POD.PORT_ID AS PODID,");
                sb.Append("       TD.TRADE_CODE,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM SECTOR_MST_TBL SM,");
                sb.Append("       TRADE_MST_TBL  TD,");
                sb.Append("       PORT_MST_TBL   POL,");
                sb.Append("       PORT_MST_TBL   POD");
                sb.Append(" WHERE SM.TRADE_MST_FK = TD.TRADE_MST_PK");
                sb.Append("   AND SM.FROM_PORT_FK = POL.PORT_MST_PK");
                sb.Append("   AND SM.TO_PORT_FK = POD.PORT_MST_PK");
                sb.Append("   AND SM.ACTIVE = 1");

                if (!string.IsNullOrEmpty(BizType))
                {
                    sb.Append("   AND SM.BUSINESS_TYPE IN ('" + BizType.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND SM.TRADE_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND SM.FROM_PORT_FK IN (" + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    sb.Append("   AND SM.TO_PORT_FK  IN (" + ConditionPK2 + ")");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SectorID))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_ID) LIKE '%" + SectorID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(SectorName))
                {
                    sb.Append(" AND UPPER(SM.SECTOR_DESC) LIKE '%" + SectorName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 5));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch SectorTrade"

        #region "Fetch Country"

        public DataSet FetchCountry(string CountryPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CountryID = "", string CountryName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            CountryPK = CountryPK.TrimEnd(',');
            CountryPK = CountryPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(CountryPK))
            {
                sb.Append("SELECT          CMT.COUNTRY_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.COUNTRY_ID,");
                sb.Append("                CMT.COUNTRY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM COUNTRY_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("   AND CMT.COUNTRY_MST_PK IN ('" + CountryPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND CMT.AREA_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryVendorSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryAgentSOA("COUNTRY", AnnSearchPks) + ")");
                        //ElseIf Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                        //    sb.Append("   AND CMT.COUNTRY_MST_PK IN (")
                        //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                        //        sb.Append("   select t.FROMCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T")
                        //    Else
                        //        sb.Append("   select t.FROMCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T")
                        //    End If
                        //    sb.Append(GetTariff(AnnSearchPks))
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append(" AND CMT.COUNTRY_MST_PK IN ( ");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_CUST_CONTRACT_SEA T");
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T");
                            }
                            else
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 6));
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          CMT.COUNTRY_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.COUNTRY_ID,");
                sb.Append("                CMT.COUNTRY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM COUNTRY_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("   AND CMT.COUNTRY_MST_PK  NOT IN ('" + CountryPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND CMT.AREA_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(CountryID))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CountryName))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryVendorSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryAgentSOA("COUNTRY", AnnSearchPks) + ")");
                        //ElseIf Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                        //    sb.Append("   AND CMT.COUNTRY_MST_PK IN (")
                        //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                        //        sb.Append("   select t.FROMCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T")
                        //    Else
                        //        sb.Append("   select t.FROMCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T")
                        //    End If
                        //    sb.Append(GetTariff(AnnSearchPks))
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append(" AND CMT.COUNTRY_MST_PK IN ( ");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_CUST_CONTRACT_SEA T");
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T");
                            }
                            else
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 6));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          CMT.COUNTRY_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.COUNTRY_ID,");
                sb.Append("                CMT.COUNTRY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM COUNTRY_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND CMT.AREA_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(CountryID))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CountryName))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryVendorSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryAgentSOA("COUNTRY", AnnSearchPks) + ")");
                        //ElseIf Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                        //    sb.Append("   AND CMT.COUNTRY_MST_PK IN (")
                        //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                        //        sb.Append("   select t.FROMCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T")
                        //    Else
                        //        sb.Append("   select t.FROMCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T")
                        //    End If
                        //    sb.Append(GetTariff(AnnSearchPks))
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append(" AND CMT.COUNTRY_MST_PK IN ( ");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_CUST_CONTRACT_SEA T");
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T");
                            }
                            else
                            {
                                sb.Append("select t.FROMCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 6));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Country"

        #region "Fetch Country"

        public DataSet FetchCountryTO(string CountryPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CountryID = "", string CountryName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            CountryPK = CountryPK.TrimEnd(',');
            CountryPK = CountryPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(CountryPK))
            {
                sb.Append("SELECT          CMT.COUNTRY_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.COUNTRY_ID,");
                sb.Append("                CMT.COUNTRY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM COUNTRY_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("   AND CMT.COUNTRY_MST_PK IN ('" + CountryPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND CMT.AREA_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryVendorSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryAgentSOA("COUNTRY", AnnSearchPks) + ")");
                        //ElseIf Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                        //    sb.Append("   AND CMT.COUNTRY_MST_PK IN (")
                        //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                        //        sb.Append("   select t.TOCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T")
                        //    Else
                        //        sb.Append("   select t.TOCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T")
                        //    End If
                        //    sb.Append(GetTariff(AnnSearchPks))
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append(" AND CMT.COUNTRY_MST_PK IN ( ");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_CUST_CONTRACT_SEA  T");
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T");
                            }
                            else
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 6));
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          CMT.COUNTRY_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.COUNTRY_ID,");
                sb.Append("                CMT.COUNTRY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM COUNTRY_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(CountryPK))
                {
                    sb.Append("   AND CMT.COUNTRY_MST_PK  NOT IN ('" + CountryPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND CMT.AREA_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(CountryID))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CountryName))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryVendorSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryAgentSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append(" AND CMT.COUNTRY_MST_PK IN ( ");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_CUST_CONTRACT_SEA  T");
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T");
                            }
                            else
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 6));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          CMT.COUNTRY_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.COUNTRY_ID,");
                sb.Append("                CMT.COUNTRY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM COUNTRY_MST_TBL CMT ");
                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND CMT.AREA_MST_FK IN (" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(CountryID))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_ID) LIKE '%" + CountryID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CountryName))
                {
                    sb.Append(" AND UPPER(CMT.COUNTRY_NAME) LIKE '%" + CountryName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTTRAN")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_CUSTOMER_SOA_TRAN T");
                        sb.Append(GetCustStmtByTrans(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "POTACT")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (");
                        sb.Append("   select t.COUNTRY_MST_FK from VIEW_POT_ACT T");
                        sb.Append(GetPOTACT(AnnSearchPks));
                        sb.Append(")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "VENDORSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryVendorSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "AGENTSOA")
                    {
                        sb.Append("   AND CMT.COUNTRY_MST_PK IN (" + GetExtendedQueryAgentSOA("COUNTRY", AnnSearchPks) + ")");
                    }
                    else if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                    {
                        sb.Append(" AND CMT.COUNTRY_MST_PK IN ( ");
                        if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_CUST_CONTRACT_AIR T");
                            }
                            else
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_CUST_CONTRACT_SEA  T");
                            }
                        }
                        else
                        {
                            if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_AIR_RATES_FREIGHT T");
                            }
                            else
                            {
                                sb.Append("select t.TOCOUNTRYPK from VIEW_SEA_RATES_FREIGHT T");
                            }
                        }
                        sb.Append(GetTariff(AnnSearchPks));
                    }
                    else
                    {
                        sb.Append(GetAnnQueryExt(AnnSearchPks, 6));
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Country"

        #region "Fetch AgentAll"

        public DataSet FetchAgentAll(string AgentPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string AgentID = "", string AgentName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            AgentPK = AgentPK.TrimEnd(',');
            AgentPK = AgentPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(AgentPK))
            {
                sb.Append("SELECT AMT.AGENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("       AMT.AGENT_ID,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       ACD.ADM_CONTACT_PERSON,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append(" FROM AGENT_MST_TBL AMT,");
                sb.Append("      AGENT_CONTACT_DTLS ACD,");
                sb.Append("      LOCATION_MST_TBL LMT, ");
                sb.Append("      COUNTRY_MST_TBL ADMCNR");
                sb.Append("  WHERE ACD.AGENT_MST_FK=AMT.AGENT_MST_PK");
                sb.Append("  AND AMT.LOCATION_MST_FK=LMT.LOCATION_MST_PK");
                sb.Append("  AND ACD.ADM_COUNTRY_MST_FK = ADMCNR.COUNTRY_MST_PK");
                sb.Append("  AND AMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(AgentPK))
                {
                    sb.Append("   AND AMT.AGENT_MST_PK IN ('" + AgentPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 8));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT AMT.AGENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("       AMT.AGENT_ID,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       ACD.ADM_CONTACT_PERSON,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append(" FROM AGENT_MST_TBL AMT,");
                sb.Append("      AGENT_CONTACT_DTLS ACD,");
                sb.Append("      LOCATION_MST_TBL LMT, ");
                sb.Append("      COUNTRY_MST_TBL ADMCNR");
                sb.Append("  WHERE ACD.AGENT_MST_FK=AMT.AGENT_MST_PK");
                sb.Append("  AND AMT.LOCATION_MST_FK=LMT.LOCATION_MST_PK");
                sb.Append("  AND ACD.ADM_COUNTRY_MST_FK = ADMCNR.COUNTRY_MST_PK");
                sb.Append("  AND AMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(AgentPK))
                {
                    sb.Append("   AND AMT.AGENT_MST_PK NOT IN ('" + AgentPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AgentID))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_ID) LIKE '%" + AgentID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AgentName))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_NAME) LIKE '%" + AgentName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 8));
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT AMT.AGENT_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("       AMT.AGENT_ID,");
                sb.Append("       AMT.AGENT_NAME,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       ACD.ADM_CONTACT_PERSON,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append(" FROM AGENT_MST_TBL AMT,");
                sb.Append("      AGENT_CONTACT_DTLS ACD,");
                sb.Append("      LOCATION_MST_TBL LMT, ");
                sb.Append("      COUNTRY_MST_TBL ADMCNR");
                sb.Append("  WHERE ACD.AGENT_MST_FK=AMT.AGENT_MST_PK");
                sb.Append("  AND AMT.LOCATION_MST_FK=LMT.LOCATION_MST_PK");
                sb.Append("  AND ACD.ADM_COUNTRY_MST_FK = ADMCNR.COUNTRY_MST_PK");
                sb.Append("  AND AMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AgentID))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_ID) LIKE '%" + AgentID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AgentName))
                {
                    sb.Append(" AND UPPER(AMT.AGENT_NAME) LIKE '%" + AgentName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 8));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch AgentAll"

        #region "Fetch Commodity"

        public DataSet FetchAllCommodity(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT  CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append("        FROM COMMODITY_MST_TBL CMT  ");
                sb.Append("        WHERE CMT.ACTIVE_FLAG=1 ");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND CMT.COMMODITY_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 11));
                }
                sb.Append(" UNION ");

                sb.Append("SELECT  CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM COMMODITY_MST_TBL CMT ");
                sb.Append("        WHERE CMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND CMT.COMMODITY_MST_PK  NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 11));
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT  CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM COMMODITY_MST_TBL CMT ");
                sb.Append("        WHERE CMT.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 11));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Commodity"

        #region "Fetch Commodity"

        public DataSet FetchAllCommodityProfit(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            Array arrPKS = null;
            Int16 I2 = default(Int16);

            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT DISTINCT CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append("        FROM COMMODITY_MST_TBL CMT , VIEW_PROFIT_PER_CUSTOMER V,customer_mst_tbl cmt1 ");
                sb.Append("        WHERE CMT.ACTIVE_FLAG=1 AND V.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK and v.CUST_CUSTOMER_MST_FK = cmt1.customer_mst_pk");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND CMT.COMMODITY_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3")
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(arrAnnSearchPks.GetValue(1).ToString()))
                    {
                        sb.Append(" AND V.BUSINESS_TYPE IN (" + Convert.ToInt32(arrAnnSearchPks.GetValue(1)) + ") ");
                    }
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
                {
                    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((arrAnnSearchPks.GetValue(8) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }
                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM COMMODITY_MST_TBL CMT , VIEW_PROFIT_PER_CUSTOMER V ");
                sb.Append("        WHERE CMT.ACTIVE_FLAG=1 AND V.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND CMT.COMMODITY_MST_PK  NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3")
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))))
                    {
                        sb.Append(" AND V.BUSINESS_TYPE IN (" + Convert.ToInt32(arrAnnSearchPks.GetValue(1)) + ") ");
                    }
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
                {
                    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM COMMODITY_MST_TBL CMT , VIEW_PROFIT_PER_CUSTOMER V ,customer_mst_tbl cmt1");
                sb.Append("        WHERE CMT.ACTIVE_FLAG=1 AND V.COMMODITY_MST_FK = CMT.COMMODITY_MST_PK and v.CUST_CUSTOMER_MST_FK = cmt1.customer_mst_pk");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3")
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))))
                    {
                        sb.Append(" AND V.BUSINESS_TYPE IN (" + Convert.ToInt32(arrAnnSearchPks.GetValue(1)) + ") ");
                    }
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
                {
                    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }
                //If AnnSearchPks <> "" Then
                //sb.Append(GetAnnQueryExt(AnnSearchPks, 11))
                //End If
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Commodity"

        #region "Fetch Commodity"

        public DataSet FetchAllCommodityCommGrp(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT  CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("CG.COMMODITY_GROUP_CODE,");
                sb.Append("'' EXTRA6,");
                sb.Append("'' EXTRA7,");
                sb.Append(" '' EXTRA8,");
                sb.Append(" '' EXTRA9,");
                sb.Append(" '1' ACTIVE");
                sb.Append("       FROM COMMODITY_MST_TBL CMT ,COMMODITY_GROUP_MST_TBL CG, PARAMETERS_TBL PT   ");
                sb.Append(" WHERE CG.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
                sb.Append(" AND CG.COMMODITY_GROUP_PK = PT.ODC_CARGO_FK");
                sb.Append(" AND CMT.ACTIVE_FLAG=1 ");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND CMT.COMMODITY_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT  CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("CG.COMMODITY_GROUP_CODE,");
                sb.Append("'' EXTRA6,");
                sb.Append("'' EXTRA7,");
                sb.Append(" '' EXTRA8,");
                sb.Append(" '' EXTRA9,");
                sb.Append(" '0' ACTIVE");
                sb.Append("       FROM COMMODITY_MST_TBL CMT ,COMMODITY_GROUP_MST_TBL CG, PARAMETERS_TBL PT   ");
                sb.Append(" WHERE CG.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
                sb.Append(" AND CG.COMMODITY_GROUP_PK = PT.ODC_CARGO_FK");
                sb.Append(" AND CMT.ACTIVE_FLAG=1 ");
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND CMT.COMMODITY_MST_PK  NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT  CMT.COMMODITY_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        CMT.COMMODITY_ID,");
                sb.Append("        CMT.COMMODITY_NAME,");
                sb.Append("CG.COMMODITY_GROUP_CODE,");
                sb.Append("'' EXTRA6,");
                sb.Append("'' EXTRA7,");
                sb.Append(" '' EXTRA8,");
                sb.Append(" '' EXTRA9,");
                sb.Append(" '0' ACTIVE");
                sb.Append("       FROM COMMODITY_MST_TBL CMT ,COMMODITY_GROUP_MST_TBL CG, PARAMETERS_TBL PT ");
                sb.Append(" WHERE CG.COMMODITY_GROUP_PK = CMT.COMMODITY_GROUP_FK");
                sb.Append(" AND CG.COMMODITY_GROUP_PK = PT.ODC_CARGO_FK");
                sb.Append(" AND CMT.ACTIVE_FLAG=1 ");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(CMT.COMMODITY_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY CMT.COMMODITY_NAME");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Commodity"

        #region "Fetch Customerall"

        public DataSet FetchCustomerAll(string CustomerPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CustomerID = "", string CustomerName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CustomerPK = CustomerPK.TrimEnd(',');
            CustomerPK = CustomerPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(CustomerPK))
            {
                sb.Append("SELECT CMT.CUSTOMER_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("       CMT.CUSTOMER_ID,  ");
                sb.Append("       CMT.CUSTOMER_NAME ,");
                sb.Append("       CCT.EMAIL,");
                sb.Append("       DECODE(CMT.BUSINESS_TYPE, 1, 'Air', 2, 'Sea', 3, 'Both')  AS \"BUSINESS TYPE\",");
                sb.Append("       CCT.NAME          AS \"CONTACT PERSON\",");
                sb.Append("       CCT.DIR_PHONE     AS \"PHONE NUMBER\",");
                sb.Append("       CMT.CUST_REG_NO    AS \"CUST. REG. NR.\",");
                //sb.Append("       CG.CUSTOMER_NAME  AS ""CUSTOMER GROUP"",")
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("       CUSTOMER_MST_TBL      CG,");
                sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       CUSTOMER_CONTACT_TRN  CCT");
                sb.Append("     WHERE CMT.REF_GROUP_CUST_PK = CG.CUSTOMER_MST_PK(+)");
                sb.Append("     AND CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
                sb.Append("     AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("     AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
                sb.Append("     AND CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 9));
                }
                sb.Append(" UNION ");

                sb.Append("SELECT CMT.CUSTOMER_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("       CMT.CUSTOMER_ID,  ");
                sb.Append("       CMT.CUSTOMER_NAME ,");
                sb.Append("       CCT.EMAIL,");
                sb.Append("       DECODE(CMT.BUSINESS_TYPE, 1, 'Air', 2, 'Sea', 3, 'Both')  AS \"BUSINESS TYPE\",");
                sb.Append("       CCT.NAME          AS \"CONTACT PERSON\",");
                sb.Append("       CCT.DIR_PHONE     AS \"PHONE NUMBER\",");
                sb.Append("       CMT.CUST_REG_NO    AS \"CUST. REG. NR.\",");
                //sb.Append("       CG.CUSTOMER_NAME  AS ""CUSTOMER GROUP"",")
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("       CUSTOMER_MST_TBL      CG,");
                sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       CUSTOMER_CONTACT_TRN  CCT");
                sb.Append("     WHERE CMT.REF_GROUP_CUST_PK = CG.CUSTOMER_MST_PK(+)");
                sb.Append("     AND CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
                sb.Append("     AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("     AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
                sb.Append("     AND CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK  NOT IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 9));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT CMT.CUSTOMER_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("       CMT.CUSTOMER_ID,  ");
                sb.Append("       CMT.CUSTOMER_NAME ,");
                sb.Append("       CCT.EMAIL,");
                sb.Append("       DECODE(CMT.BUSINESS_TYPE, 1, 'Air', 2, 'Sea', 3, 'Both')  AS \"BUSINESS TYPE\",");
                sb.Append("       CCT.NAME          AS \"CONTACT PERSON\",");
                sb.Append("       CCT.DIR_PHONE     AS \"PHONE NUMBER\",");
                sb.Append("       CMT.CUST_REG_NO    AS \"CUST. REG. NR.\",");
                //sb.Append("       CG.CUSTOMER_NAME  AS ""CUSTOMER GROUP"",")
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("       CUSTOMER_MST_TBL      CG,");
                sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       CUSTOMER_CONTACT_TRN  CCT");
                sb.Append("     WHERE CMT.REF_GROUP_CUST_PK = CG.CUSTOMER_MST_PK(+)");
                sb.Append("     AND CMT.CUSTOMER_MST_PK = CCT.CUSTOMER_MST_FK ");
                sb.Append("     AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK");
                sb.Append("     AND LMT.LOCATION_MST_PK = CCD.ADM_LOCATION_MST_FK");
                sb.Append("     AND CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                if (LoginPK != 0)
                {
                    //sb.Append(" AND CCD.ADM_LOCATION_MST_FK IN (" & LoginPK & ")")
                    sb.Append("                   AND CCD.ADM_LOCATION_MST_FK IN");
                    sb.Append("                       ((SELECT L.LOCATION_MST_PK");
                    sb.Append("                          FROM LOCATION_MST_TBL L");
                    sb.Append("                         WHERE L.LOCATION_MST_PK = " + LoginPK + ") ");
                    sb.Append("                        UNION");
                    sb.Append("                        SELECT L.LOCATION_MST_PK");
                    sb.Append("                          FROM LOCATION_MST_TBL L");
                    sb.Append("                         WHERE L.REPORTING_TO_FK =  " + LoginPK + " )");
                }
                if (TradeID == "2")
                {
                    sb.Append("  AND  CMT.Business_Type IN (2,3)");
                }
                else
                {
                    sb.Append("  AND  CMT.Business_Type IN (1,3)");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 9));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Customerall"

        #region "Fetch Port Group"

        public DataSet FetchPortGroup(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT PGM.PORT_GRP_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("     PGM.PORT_GRP_CODE,");
                sb.Append("     PGM.PORT_GRP_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append(" FROM PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PGM.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND PGM.PORT_GRP_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 10));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT PGM.PORT_GRP_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("     PGM.PORT_GRP_CODE,");
                sb.Append("     PGM.PORT_GRP_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append(" FROM PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PGM.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND PGM.PORT_GRP_MST_PK  NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(PGM.PORT_GRP_CODE) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(PGM.PORT_GRP_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 10));
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT PGM.PORT_GRP_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("     PGM.PORT_GRP_CODE,");
                sb.Append("     PGM.PORT_GRP_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append(" FROM PORT_GROUP_MST_TBL PGM");
                sb.Append(" WHERE PGM.ACTIVE_FLAG=1");

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PGM.PORT_GRP_CODE) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(PGM.PORT_GRP_CODE) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(PGM.PORT_GRP_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    sb.Append(GetAnnQueryExt(AnnSearchPks, 10));
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Port Group"

        #region "Fetch LocationBasedCustomerFetchLocationBasedCustomerPROFIT"

        public DataSet FetchLocationBasedCustomer(string CustomerPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CustomerID = "", string CustomerName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CustomerPK = CustomerPK.TrimEnd(',');
            CustomerPK = CustomerPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(CustomerPK))
            {
                sb.Append("SELECT CMT.CUSTOMER_MST_PK,");
                sb.Append("                       '' EMPTY,");
                sb.Append("                       CMT.CUSTOMER_ID,");
                sb.Append("                       CMT.CUSTOMER_NAME,");
                sb.Append("                       '' EXTRA5,");
                sb.Append("                       '' EXTRA6,");
                sb.Append("                       '' EXTRA7,");
                sb.Append("                       '' EXTRA8,");
                sb.Append("                       '' EXTRA9,");
                sb.Append("                       '1' ACTIVE");
                sb.Append("                  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("                       CUSTOMER_CONTACT_DTLS CMTDTL,");
                sb.Append("                       CUSTOMER_TYPE_MST_TBL CTMT,");
                sb.Append("                       LOCATION_MST_TBL      LMT");
                sb.Append("                 WHERE CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK(+)");
                sb.Append("                   AND CMTDTL.CUSTOMER_MST_FK(+) = CMT.CUSTOMER_MST_PK");
                sb.Append("                   AND LMT.LOCATION_MST_PK = CMTDTL.ADM_LOCATION_MST_FK");
                sb.Append("                   AND CMT.TEMP_PARTY = 0");
                sb.Append("                   AND CMT.ACTIVE_FLAG = 1");
                sb.Append("                   AND CMTDTL.ADM_LOCATION_MST_FK IN");
                sb.Append("                       ((SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.LOCATION_MST_PK = " + LoginPK + ") ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.REPORTING_TO_FK =  " + LoginPK + " ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT LL.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL LL");
                sb.Append("                         WHERE LL.REPORTING_TO_FK IN");
                sb.Append("                               (SELECT L.LOCATION_MST_PK");
                sb.Append("                                  FROM LOCATION_MST_TBL L");
                sb.Append("                                 WHERE L.REPORTING_TO_FK =  " + LoginPK + ")) ");

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT CMT.CUSTOMER_MST_PK,");
                sb.Append("                       '' EMPTY,");
                sb.Append("                       CMT.CUSTOMER_ID,");
                sb.Append("                       CMT.CUSTOMER_NAME,");
                sb.Append("                       '' EXTRA5,");
                sb.Append("                       '' EXTRA6,");
                sb.Append("                       '' EXTRA7,");
                sb.Append("                       '' EXTRA8,");
                sb.Append("                       '' EXTRA9,");
                sb.Append("                       '0' ACTIVE");
                sb.Append("                  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("                       CUSTOMER_CONTACT_DTLS CMTDTL,");
                sb.Append("                       CUSTOMER_TYPE_MST_TBL CTMT,");
                sb.Append("                       LOCATION_MST_TBL      LMT");
                sb.Append("                 WHERE CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK(+)");
                sb.Append("                   AND CMTDTL.CUSTOMER_MST_FK(+) = CMT.CUSTOMER_MST_PK");
                sb.Append("                   AND LMT.LOCATION_MST_PK = CMTDTL.ADM_LOCATION_MST_FK");
                sb.Append("                   AND CMT.TEMP_PARTY = 0");
                sb.Append("                   AND CMT.ACTIVE_FLAG = 1");
                sb.Append("                   AND CMTDTL.ADM_LOCATION_MST_FK IN");
                sb.Append("                       ((SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.LOCATION_MST_PK = " + LoginPK + ") ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.REPORTING_TO_FK =  " + LoginPK + " ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT LL.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL LL");
                sb.Append("                         WHERE LL.REPORTING_TO_FK IN");
                sb.Append("                               (SELECT L.LOCATION_MST_PK");
                sb.Append("                                  FROM LOCATION_MST_TBL L");
                sb.Append("                                 WHERE L.REPORTING_TO_FK =  " + LoginPK + ")) ");

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK  NOT IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT CMT.CUSTOMER_MST_PK,");
                sb.Append("                       '' EMPTY,");
                sb.Append("                       CMT.CUSTOMER_ID,");
                sb.Append("                       CMT.CUSTOMER_NAME,");
                sb.Append("                       '' EXTRA5,");
                sb.Append("                       '' EXTRA6,");
                sb.Append("                       '' EXTRA7,");
                sb.Append("                       '' EXTRA8,");
                sb.Append("                       '' EXTRA9,");
                sb.Append("                       '0' ACTIVE");
                sb.Append("                  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("                       CUSTOMER_CONTACT_DTLS CMTDTL,");
                sb.Append("                       CUSTOMER_TYPE_MST_TBL CTMT,");
                sb.Append("                       LOCATION_MST_TBL      LMT");
                sb.Append("                 WHERE CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK(+)");
                sb.Append("                   AND CMTDTL.CUSTOMER_MST_FK(+) = CMT.CUSTOMER_MST_PK");
                sb.Append("                   AND LMT.LOCATION_MST_PK = CMTDTL.ADM_LOCATION_MST_FK");
                sb.Append("                   AND CMT.TEMP_PARTY = 0");
                sb.Append("                   AND CMT.ACTIVE_FLAG = 1");
                sb.Append("                   AND CMTDTL.ADM_LOCATION_MST_FK IN");
                sb.Append("                       ((SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.LOCATION_MST_PK = " + LoginPK + ") ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.REPORTING_TO_FK =  " + LoginPK + " ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT LL.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL LL");
                sb.Append("                         WHERE LL.REPORTING_TO_FK IN");
                sb.Append("                               (SELECT L.LOCATION_MST_PK");
                sb.Append("                                  FROM LOCATION_MST_TBL L");
                sb.Append("                                 WHERE L.REPORTING_TO_FK =  " + LoginPK + ")) ");
                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch LocationBasedCustomerFetchLocationBasedCustomerPROFIT"

        #region "Fetch LocationBasedCustomerPROFIT"

        public DataSet FetchLocationBasedCustomerProfit(string CustomerPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CustomerID = "", string CustomerName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CustomerPK = CustomerPK.TrimEnd(',');
            CustomerPK = CustomerPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(CustomerPK))
            {
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
                sb.Append("                       '' EMPTY,");
                sb.Append("                       CMT.CUSTOMER_ID,");
                sb.Append("                       CMT.CUSTOMER_NAME,");
                sb.Append("                       '' EXTRA5,");
                sb.Append("                       '' EXTRA6,");
                sb.Append("                       '' EXTRA7,");
                sb.Append("                       '' EXTRA8,");
                sb.Append("                       '' EXTRA9,");
                sb.Append("                       '1' ACTIVE");
                sb.Append("                  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("                       CUSTOMER_CONTACT_DTLS CMTDTL,");
                sb.Append("                       CUSTOMER_TYPE_MST_TBL CTMT,");
                sb.Append("                       LOCATION_MST_TBL      LMT,VIEW_PROFIT_PER_CUSTOMER V,COMMODITY_MST_TBL CMT1");
                sb.Append("                 WHERE CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK(+)");
                sb.Append("                   AND CMTDTL.CUSTOMER_MST_FK(+) = CMT.CUSTOMER_MST_PK");
                sb.Append("                   AND LMT.LOCATION_MST_PK = CMTDTL.ADM_LOCATION_MST_FK");
                sb.Append("                   AND CMT.TEMP_PARTY = 0");
                sb.Append("                   AND CMT.ACTIVE_FLAG = 1 AND V.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK   and cmt1.commodity_mst_pk = v.COMMODITY_MST_FK");
                sb.Append("                   AND CMTDTL.ADM_LOCATION_MST_FK IN");
                sb.Append("                       ((SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.LOCATION_MST_PK = " + LoginPK + ") ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.REPORTING_TO_FK =  " + LoginPK + " ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT LL.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL LL");
                sb.Append("                         WHERE LL.REPORTING_TO_FK IN");
                sb.Append("                               (SELECT L.LOCATION_MST_PK");
                sb.Append("                                  FROM LOCATION_MST_TBL L");
                sb.Append("                                 WHERE L.REPORTING_TO_FK =  " + LoginPK + ")) ");

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
                {
                    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
                {
                    sb.Append(" AND V.COMMODITY_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ") ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }
                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
                sb.Append("                       '' EMPTY,");
                sb.Append("                       CMT.CUSTOMER_ID,");
                sb.Append("                       CMT.CUSTOMER_NAME,");
                sb.Append("                       '' EXTRA5,");
                sb.Append("                       '' EXTRA6,");
                sb.Append("                       '' EXTRA7,");
                sb.Append("                       '' EXTRA8,");
                sb.Append("                       '' EXTRA9,");
                sb.Append("                       '0' ACTIVE");
                sb.Append("                  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("                       CUSTOMER_CONTACT_DTLS CMTDTL,");
                sb.Append("                       CUSTOMER_TYPE_MST_TBL CTMT,");
                sb.Append("                       LOCATION_MST_TBL      LMT,VIEW_PROFIT_PER_CUSTOMER V,COMMODITY_MST_TBL CMT1");
                sb.Append("                 WHERE CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK(+)");
                sb.Append("                   AND CMTDTL.CUSTOMER_MST_FK(+) = CMT.CUSTOMER_MST_PK");
                sb.Append("                   AND LMT.LOCATION_MST_PK = CMTDTL.ADM_LOCATION_MST_FK");
                sb.Append("                   AND CMT.TEMP_PARTY = 0");
                sb.Append("                   AND CMT.ACTIVE_FLAG = 1 AND V.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK and cmt1.commodity_mst_pk = v.COMMODITY_MST_FK");
                sb.Append("                   AND CMTDTL.ADM_LOCATION_MST_FK IN");
                sb.Append("                       ((SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.LOCATION_MST_PK = " + LoginPK + ") ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.REPORTING_TO_FK =  " + LoginPK + " ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT LL.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL LL");
                sb.Append("                         WHERE LL.REPORTING_TO_FK IN");
                sb.Append("                               (SELECT L.LOCATION_MST_PK");
                sb.Append("                                  FROM LOCATION_MST_TBL L");
                sb.Append("                                 WHERE L.REPORTING_TO_FK =  " + LoginPK + ")) ");

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,2,3)");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
                {
                    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
                {
                    sb.Append(" AND V.COMMODITY_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ") ");
                }

                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK  NOT IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT CMT.CUSTOMER_MST_PK,");
                sb.Append("                       '' EMPTY,");
                sb.Append("                       CMT.CUSTOMER_ID,");
                sb.Append("                       CMT.CUSTOMER_NAME,");
                sb.Append("                       '' EXTRA5,");
                sb.Append("                       '' EXTRA6,");
                sb.Append("                       '' EXTRA7,");
                sb.Append("                       '' EXTRA8,");
                sb.Append("                       '' EXTRA9,");
                sb.Append("                       '0' ACTIVE");
                sb.Append("                  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("                       CUSTOMER_CONTACT_DTLS CMTDTL,");
                sb.Append("                       CUSTOMER_TYPE_MST_TBL CTMT,");
                sb.Append("                       LOCATION_MST_TBL      LMT,VIEW_PROFIT_PER_CUSTOMER V,COMMODITY_MST_TBL CMT1");
                sb.Append("                 WHERE CMT.CUSTOMER_TYPE_FK = CTMT.CUSTOMER_TYPE_PK(+)");
                sb.Append("                   AND CMTDTL.CUSTOMER_MST_FK(+) = CMT.CUSTOMER_MST_PK");
                sb.Append("                   AND LMT.LOCATION_MST_PK = CMTDTL.ADM_LOCATION_MST_FK");
                sb.Append("                   AND CMT.TEMP_PARTY = 0");
                sb.Append("                   AND CMT.ACTIVE_FLAG = 1 AND V.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK and cmt1.commodity_mst_pk = v.COMMODITY_MST_FK");
                sb.Append("                   AND CMTDTL.ADM_LOCATION_MST_FK IN");
                sb.Append("                       ((SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.LOCATION_MST_PK = " + LoginPK + ") ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT L.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL L");
                sb.Append("                         WHERE L.REPORTING_TO_FK =  " + LoginPK + " ");
                sb.Append("                        UNION");
                sb.Append("                        SELECT LL.LOCATION_MST_PK");
                sb.Append("                          FROM LOCATION_MST_TBL LL");
                sb.Append("                         WHERE LL.REPORTING_TO_FK IN");
                sb.Append("                               (SELECT L.LOCATION_MST_PK");
                sb.Append("                                  FROM LOCATION_MST_TBL L");
                sb.Append("                                 WHERE L.REPORTING_TO_FK =  " + LoginPK + ")) ");
                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,2,3)");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
                {
                    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
                {
                    sb.Append(" AND V.COMMODITY_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ") ");
                }

                //If Not ((Convert.ToString(arrAnnSearchPks.GetValue(7)) Is Nothing Or Trim(Convert.ToString(arrAnnSearchPks.GetValue(7))) = "") And (Convert.ToString(arrAnnSearchPks.GetValue(8)) Is Nothing Or Trim(Convert.ToString(arrAnnSearchPks.GetValue(8))) = "")) Then
                //    sb.Append(" AND V.BOOKING_DATE BETWEEN TO_DATE('" & Convert.ToString(arrAnnSearchPks.GetValue(7)) & "',dateformat)  AND TO_DATE('" & Convert.ToString(arrAnnSearchPks.GetValue(8)) & "',dateformat)  ")
                //End If
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch LocationBasedCustomerPROFIT"

        #region "Fetch Executive"

        public DataSet FetchAllExecutive(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT  EM.EMPLOYEE_MST_PK ,");
                sb.Append("               '' EMPTY,");
                sb.Append("        EM.EMPLOYEE_ID,");
                sb.Append("        EM.EMPLOYEE_NAME,");
                sb.Append("        DECODE(EM.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append("        FROM EMPLOYEE_MST_TBL EM,LOCATION_MST_TBL LMT ");
                sb.Append("   WHERE EM.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("    AND LOCATION_MST_FK IN");
                sb.Append("   (SELECT L.LOCATION_MST_PK");
                sb.Append("   FROM LOCATION_MST_TBL L");

                sb.Append("   START WITH L.LOCATION_MST_PK = " + LoginPK + " ");
                sb.Append("   CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

                if (ConditionPK == "2")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND EM.EMPLOYEE_MST_PK  IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT  EM.EMPLOYEE_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        EM.EMPLOYEE_ID,");
                sb.Append("        EM.EMPLOYEE_NAME,");
                sb.Append("        DECODE(EM.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM EMPLOYEE_MST_TBL EM,LOCATION_MST_TBL LMT ");
                sb.Append("   WHERE EM.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("    AND LOCATION_MST_FK IN");
                sb.Append("   (SELECT L.LOCATION_MST_PK");
                sb.Append("   FROM LOCATION_MST_TBL L");
                sb.Append("   START WITH L.LOCATION_MST_PK = " + LoginPK + " ");
                sb.Append("   CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

                if (ConditionPK == "2")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND EM.EMPLOYEE_MST_PK  NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER( EM.EMPLOYEE_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(EM.EMPLOYEE_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT  EM.EMPLOYEE_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        EM.EMPLOYEE_ID,");
                sb.Append("        EM.EMPLOYEE_NAME,");
                sb.Append("        DECODE(EM.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM EMPLOYEE_MST_TBL EM,LOCATION_MST_TBL LMT ");
                sb.Append("   WHERE EM.LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("    AND LOCATION_MST_FK IN");
                sb.Append("   (SELECT L.LOCATION_MST_PK");
                sb.Append("   FROM LOCATION_MST_TBL L");
                sb.Append("   START WITH L.LOCATION_MST_PK = " + LoginPK + " ");
                sb.Append("   CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

                if (ConditionPK == "2")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(EM.EMPLOYEE_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER( EM.EMPLOYEE_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(EM.EMPLOYEE_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Executive"

        #region "Fetch Executive"

        public DataSet FetchAllExecutiveProfit(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT DISTINCT EM.EMPLOYEE_MST_PK ,");
                sb.Append("               '' EMPTY,");
                sb.Append("        EM.EMPLOYEE_ID,");
                sb.Append("        EM.EMPLOYEE_NAME,");
                sb.Append("        DECODE(EM.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append("        FROM EMPLOYEE_MST_TBL EM,LOCATION_MST_TBL LMT,VIEW_PROFIT_PER_CUSTOMER V,CUSTOMER_MST_TBL CMT,BOOKING_MST_TBL  BST,port_mst_tbl   pol");
                sb.Append("   WHERE EM.LOCATION_MST_FK = LMT.LOCATION_MST_PK AND V.EXECUTIVE_MST_FK = EM.EMPLOYEE_MST_PK   AND CMT.CUSTOMER_MST_PK(+) = V.CUST_CUSTOMER_MST_FK  AND EM.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
                sb.Append("  and bst.port_mst_pol_fk=pol.port_mst_pk");
                sb.Append("    AND pol.location_mst_fk IN ");
                sb.Append("   (SELECT L.LOCATION_MST_PK");
                sb.Append("   FROM LOCATION_MST_TBL L");

                sb.Append("   START WITH L.LOCATION_MST_PK = " + LoginPK + " ");
                sb.Append("   CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

                if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "2")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (2,3)");
                }
                else if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "1")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND EM.EMPLOYEE_MST_PK  IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                //If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) <> "3" Then
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) <> "0" And Convert.ToInt32(arrAnnSearchPks.GetValue(1)) <> "" Then
                //        sb.Append(" AND V.BUSINESS_TYPE IN (" & Convert.ToInt32(arrAnnSearchPks.GetValue(1)) & ") ")
                //    End If
                //End If
                //If Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "" Then
                //    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(3)) & ") ")
                //End If
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
                {
                    sb.Append(" AND V.COMMODITY_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ") ");
                }

                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT EM.EMPLOYEE_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        EM.EMPLOYEE_ID,");
                sb.Append("        EM.EMPLOYEE_NAME,");
                sb.Append("        DECODE(EM.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM EMPLOYEE_MST_TBL EM,LOCATION_MST_TBL LMT,VIEW_PROFIT_PER_CUSTOMER V,CUSTOMER_MST_TBL CMT,BOOKING_MST_TBL  BST,port_mst_tbl   pol");
                sb.Append("   WHERE EM.LOCATION_MST_FK = LMT.LOCATION_MST_PK AND V.EXECUTIVE_MST_FK = EM.EMPLOYEE_MST_PK AND CMT.CUSTOMER_MST_PK(+) = V.CUST_CUSTOMER_MST_FK  AND  EM.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
                //sb.Append(" EM.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK")
                sb.Append("  and bst.port_mst_pol_fk=pol.port_mst_pk");
                sb.Append("    AND pol.location_mst_fk IN");
                sb.Append("   (SELECT L.LOCATION_MST_PK");
                sb.Append("   FROM LOCATION_MST_TBL L");
                sb.Append("   START WITH L.LOCATION_MST_PK = " + LoginPK + " ");
                sb.Append("   CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

                if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "2")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (2,3)");
                }
                else if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "1")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND EM.EMPLOYEE_MST_PK  NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER( EM.EMPLOYEE_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(EM.EMPLOYEE_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
                {
                    sb.Append(" AND V.COMMODITY_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ") ");
                }
                //If Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "" Then
                //    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(3)) & ") ")
                //End If
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT EM.EMPLOYEE_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("        EM.EMPLOYEE_ID,");
                sb.Append("        EM.EMPLOYEE_NAME,");
                sb.Append("        DECODE(EM.BUSINESS_TYPE, 1, 'AIR', 2, 'SEA', 3, 'BOTH') AS BTYPE, ");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("        FROM EMPLOYEE_MST_TBL EM,LOCATION_MST_TBL LMT,VIEW_PROFIT_PER_CUSTOMER V,CUSTOMER_MST_TBL CMT,  BOOKING_MST_TBL  BST,port_mst_tbl   pol");
                sb.Append("   WHERE EM.LOCATION_MST_FK = LMT.LOCATION_MST_PK AND V.EXECUTIVE_MST_FK = EM.EMPLOYEE_MST_PK AND CMT.CUSTOMER_MST_PK(+) = V.CUST_CUSTOMER_MST_FK  AND  EM.EMPLOYEE_MST_PK(+) = BST.EXECUTIVE_MST_FK");
                //EM.EMPLOYEE_MST_PK(+) = CMT.REP_EMP_MST_FK
                sb.Append("  and bst.port_mst_pol_fk=pol.port_mst_pk");
                sb.Append("    AND pol.location_mst_fk IN");

                sb.Append("   (SELECT L.LOCATION_MST_PK");
                sb.Append("   FROM LOCATION_MST_TBL L");
                sb.Append("   START WITH L.LOCATION_MST_PK = " + LoginPK + " ");
                sb.Append("   CONNECT BY PRIOR L.LOCATION_MST_PK = L.REPORTING_TO_FK)");

                if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "2")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (2,3)");
                }
                else if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "1")
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,3)");
                }
                else
                {
                    sb.Append("   AND EM.BUSINESS_TYPE IN (1,2,3)");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(EM.EMPLOYEE_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER( EM.EMPLOYEE_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(EM.EMPLOYEE_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
                {
                    sb.Append(" AND V.COMMODITY_GROUP_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
                {
                    sb.Append(" AND V.COMMODITY_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ") ");
                }
                //If Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "" Then
                //    sb.Append(" AND V.EXECUTIVE_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(3)) & ") ")
                //End If
                if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
                {
                    sb.Append(" AND V.BOOKING_MST_PK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ") ");
                }
                if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
                {
                    sb.Append(" AND V.CUST_CUSTOMER_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ") ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                }
                if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                {
                    sb.Append(" AND TO_DATE(V.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat) ");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Executive"

        #region "Fetch BookingNr"

        public DataSet FetchAllBookingNr(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string TradeID = "", string TypedData = "",
        string ID = "", string NAME = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (ConditionPK == "3")
            {
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BST.VESSEL_NAME IS NOT NULL");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BAT.VESSEL_NAME IS NOT NULL");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BAT.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BST.VESSEL_NAME IS NOT NULL");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BAT.VESSEL_NAME IS NOT NULL");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND  BAT.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BST.VESSEL_NAME IS NOT NULL AND BST.BUSINESS_TYPE = 2");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    // sb.Append(" AND BAT.VESSEL_NAME IS NOT NULL AND BAT.BUSINESS_TYPE = 1")
                    sb.Append(" AND BAT.BUSINESS_TYPE = 1");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BAT.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY 2, 3");
                }
            }

            if (ConditionPK == "2")
            {
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BST.VESSEL_NAME IS NOT NULL  AND BST.BUSINESS_TYPE = 2");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append("  AND BST.VESSEL_NAME IS NOT NULL  AND BST.BUSINESS_TYPE = 2 ");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BST.VESSEL_NAME IS NOT NULL  AND BST.BUSINESS_TYPE = 2");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BST.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
            }

            if (ConditionPK == "1")
            {
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND  BAT.BUSINESS_TYPE = 1");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
                    {
                        sb.Append(" AND BAT.COMMODITY_MST_FK IN (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ") ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BAT.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append(" AND BAT.BUSINESS_TYPE = 1");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BAT.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");
                    sb.Append("  AND BAT.BUSINESS_TYPE = 1");
                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    //If Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "0" And Convert.ToString(arrAnnSearchPks.GetValue(6)) <> "" Then
                    //    sb.Append(" AND V.COMMODITY_MST_FK IN (" & Convert.ToString(arrAnnSearchPks.GetValue(6)) & ") ")
                    //End If

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(7)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) >= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "',dateformat) ");
                    }

                    if (!((Convert.ToString(arrAnnSearchPks.GetValue(8)) == null | string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))))
                    {
                        sb.Append(" AND TO_DATE(BAT.BOOKING_DATE) <= TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "',dateformat)  ");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BAT.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY 2, 3");
                }
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch BookingNr"

        #region "Fetch BookingNr"

        public DataSet FetchAllBookingNrProfit(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string TradeID = "", string TypedData = "",
        string ID = "", string NAME = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (ConditionPK == "3")
            {
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BAT.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND  BAT.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BAT.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY 2, 3");
                }
            }

            if (ConditionPK == "2")
            {
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BST.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT  DISTINCT BST.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER(BST.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BST.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("                 BST.VESSEL_NAME,");
                    sb.Append("                 TO_CHAR(BST.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM BOOKING_MST_TBL      BST, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          LOC_PORT_MAPPING_TRN LPMT, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL     CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE( BST.STATUS = 2)");
                    sb.Append("  AND BST.CARGO_TYPE <> 4");
                    sb.Append("  AND BST.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("   AND BST.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("  AND BST.PORT_MST_POL_FK = LPMT.PORT_MST_FK(+)");
                    sb.Append("  AND CMT.CUSTOMER_MST_PK = BST.CUST_CUSTOMER_MST_FK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BST.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BST.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BST.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
            }

            if (ConditionPK == "1")
            {
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '1' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BAT.BOOKING_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT ,");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(PK))
                    {
                        sb.Append("   AND BAT.BOOKING_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT  DISTINCT BAT.BOOKING_MST_PK ,");
                    sb.Append("               '' EMPTY,");
                    sb.Append("        UPPER( BAT.BOOKING_REF_NO),");
                    sb.Append("        TO_CHAR(BAT.BOOKING_DATE, DATEFORMAT),");
                    sb.Append("        CMT.CUSTOMER_NAME, ");
                    sb.Append("                POL.PORT_ID POL,");
                    sb.Append("                POD.PORT_ID POD,");
                    sb.Append("          AIR.AIRLINE_NAME, ");
                    sb.Append("                 TO_CHAR(BAT.SHIPMENT_DATE, DATEFORMAT),");
                    sb.Append("         '0' ACTIVE");
                    sb.Append("          FROM  BOOKING_MST_TBL BAT, ");
                    sb.Append("          USER_MST_TBL         UMT, ");
                    sb.Append("          AIRLINE_MST_TBL AIR, ");
                    sb.Append("          PORT_MST_TBL         POL, ");
                    sb.Append("          PORT_MST_TBL         POD, ");
                    sb.Append("          CUSTOMER_MST_TBL  CMT, ");
                    sb.Append("          EMPLOYEE_MST_TBL  EMT, ");
                    sb.Append("          COMMODITY_GROUP_MST_TBL CGMT ");
                    sb.Append("   WHERE(  BAT.STATUS = 2)");
                    sb.Append("  AND BAT.CUST_CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK(+)");
                    sb.Append("   AND  BAT.PORT_MST_POL_FK = POL.PORT_MST_PK(+)");
                    sb.Append("  AND BAT.PORT_MST_POD_FK = POD.PORT_MST_PK(+)");
                    sb.Append("  AND AIR.AIRLINE_MST_PK(+) = BAT.CARRIER_MST_FK");
                    sb.Append("  AND  BAT.CREATED_BY_FK = UMT.USER_MST_PK");
                    sb.Append("  AND EMT.EMPLOYEE_MST_PK(+)=CMT.REP_EMP_MST_FK");
                    sb.Append("   AND BAT.COMMODITY_GROUP_FK=CGMT.COMMODITY_GROUP_PK(+)");

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND CMT.CUSTOMER_MST_PK IN ( " + ConditionPK1 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("   AND EMT.EMPLOYEE_MST_PK IN ( " + ConditionPK2 + " )");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK3))
                    {
                        sb.Append("   AND CGMT.COMMODITY_GROUP_PK IN ( " + ConditionPK3 + " )");
                    }

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(BAT.BOOKING_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(ID))
                    {
                        sb.Append(" AND UPPER( BAT.BOOKING_REF_NO) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY 2, 3");
                }
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch BookingNr"

        #region "Fetch Module"

        public DataSet FetchModule(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');

            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT DISTINCT V.MENU_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.MENU_TEXT,");
                sb.Append("                V.MENU_TEXT EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM VIEW_MODULE_FORMS V, MENU_MST_TBL MA, ATTACH_FILE_DTL_TBL A");
                sb.Append(" WHERE V.MENU_LEVEL=1");
                sb.Append(" AND MA.MENU_MST_PK=A.MENU_MST_FK");
                sb.Append(" AND MA.MENU_MST_FK = V.menu_mst_pk");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND MA.MENU_MST_PK IN(" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  v.MENU_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND V.MENU_TEXT IN (");
                        sb.Append("   select t.MODTYPE from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT V.MENU_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.MENU_TEXT,");
                sb.Append("                V.MENU_TEXT EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VIEW_MODULE_FORMS V, MENU_MST_TBL MA, ATTACH_FILE_DTL_TBL A");
                sb.Append(" WHERE V.MENU_LEVEL=1");
                sb.Append(" AND MA.MENU_MST_PK=A.MENU_MST_FK");
                sb.Append(" AND MA.MENU_MST_FK = V.menu_mst_pk");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND MA.MENU_MST_PK IN(" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND V.MENU_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND V.MENU_TEXT IN (");
                        sb.Append("   select t.MODTYPE from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT V.MENU_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.MENU_TEXT,");
                sb.Append("                V.MENU_TEXT EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VIEW_MODULE_FORMS V, MENU_MST_TBL MA, ATTACH_FILE_DTL_TBL A");
                sb.Append(" WHERE V.MENU_LEVEL=1");
                sb.Append(" AND MA.MENU_MST_PK=A.MENU_MST_FK");
                sb.Append(" AND MA.MENU_MST_FK = V.menu_mst_pk");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND MA.MENU_MST_PK IN(" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  V.MENU_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND V.MENU_TEXT IN (");
                        sb.Append("   select t.MODTYPE from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Module"

        #region "Fetch Feature"

        public DataSet FetchFeature(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT DISTINCT V.MENU_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.MENU_TEXT,");
                sb.Append("                V.MENU_TEXT EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM VIEW_MODULE_FORMS V,ATTACH_FILE_DTL_TBL A");
                sb.Append(" WHERE V.MENU_LEVEL=2");
                sb.Append(" AND V.menu_mst_pk = A.MENU_MST_FK");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND V.MODULE_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  V.MENU_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND V.MENU_MST_PK IN (");
                        sb.Append("   select t.MENU_MST_FK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT V.MENU_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.MENU_TEXT,");
                sb.Append("                V.MENU_TEXT EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VIEW_MODULE_FORMS V,ATTACH_FILE_DTL_TBL A ");
                sb.Append(" WHERE V.MENU_LEVEL=2");
                sb.Append(" AND V.menu_mst_pk = A.MENU_MST_FK");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND V.MODULE_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND V.MENU_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND V.MENU_MST_PK IN (");
                        sb.Append("   select t.MENU_MST_FK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT V.MENU_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                V.MENU_TEXT,");
                sb.Append("                V.MENU_TEXT EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM VIEW_MODULE_FORMS V,ATTACH_FILE_DTL_TBL A");
                sb.Append(" WHERE V.MENU_LEVEL=2");
                sb.Append(" AND V.menu_mst_pk = A.MENU_MST_FK");
                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND V.MODULE_FK IN(" + ConditionPK + ")");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  V.MENU_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(V.MENU_TEXT) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND V.MENU_MST_PK IN (");
                        sb.Append("   select t.MENU_MST_FK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Feature"

        #region "User1"

        public DataSet FetchUser1(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT DISTINCT USER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                USER_ID,");
                sb.Append("                USER_NAME,");
                sb.Append("                PASS_WORD EXTRA5,");
                sb.Append("                LOCATION_NAME ,");
                sb.Append("                EMPLOYEE_NAME ,");
                sb.Append("                DECODE(U.BUSINESS_TYPE,1,'AIR',2,'SEA',3,'BOTH') BUSINESS_TYPE,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM USER_MST_TBL U, LOCATION_MST_TBL L,EMPLOYEE_MST_TBL E");
                sb.Append(" WHERE U.IS_ACTIVATED=1");
                sb.Append(" AND U.DEFAULT_LOCATION_FK=L.LOCATION_MST_PK");
                sb.Append("  AND E.EMPLOYEE_MST_PK = U.EMPLOYEE_MST_FK");
                sb.Append(" AND L.COMP_LOCATION =1");
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  USER_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append(" AND U.DEFAULT_LOCATION_FK=" + ConditionPK);
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND U.BUSINESS_TYPE IN (3, " + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND U.USER_MST_PK IN (");
                        sb.Append("   select t.USER_MST_PK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT USER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                USER_ID,");
                sb.Append("                USER_NAME,");
                sb.Append("                PASS_WORD EXTRA5,");
                sb.Append("                LOCATION_NAME ,");
                sb.Append("                EMPLOYEE_NAME ,");
                sb.Append("                DECODE(U.BUSINESS_TYPE,1,'AIR',2,'SEA',3,'BOTH') BUSINESS_TYPE,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM USER_MST_TBL U, LOCATION_MST_TBL L,EMPLOYEE_MST_TBL E");
                sb.Append(" WHERE U.IS_ACTIVATED=1");
                sb.Append(" AND U.DEFAULT_LOCATION_FK=L.LOCATION_MST_PK");
                sb.Append("  AND E.EMPLOYEE_MST_PK = U.EMPLOYEE_MST_FK");
                sb.Append(" AND L.COMP_LOCATION =1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND USER_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append(" AND U.DEFAULT_LOCATION_FK=" + ConditionPK);
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND U.BUSINESS_TYPE IN (3, " + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(USER_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(USER_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND U.USER_MST_PK IN (");
                        sb.Append("   select t.USER_MST_PK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" ORDER BY USER_ID");
            }
            else
            {
                sb.Append("SELECT DISTINCT USER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                USER_ID,");
                sb.Append("                USER_NAME,");
                sb.Append("                PASS_WORD EXTRA5,");
                sb.Append("                LOCATION_NAME ,");
                sb.Append("                EMPLOYEE_NAME ,");
                sb.Append("                DECODE(U.BUSINESS_TYPE,1,'AIR',2,'SEA',3,'BOTH') BUSINESS_TYPE,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM USER_MST_TBL U, LOCATION_MST_TBL L,EMPLOYEE_MST_TBL E");
                sb.Append(" WHERE U.IS_ACTIVATED=1");
                sb.Append(" AND U.DEFAULT_LOCATION_FK=L.LOCATION_MST_PK");
                sb.Append("  AND E.EMPLOYEE_MST_PK = U.EMPLOYEE_MST_FK");
                sb.Append(" AND L.COMP_LOCATION =1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  USER_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append(" AND U.DEFAULT_LOCATION_FK=" + ConditionPK);
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND U.BUSINESS_TYPE IN (3, " + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(USER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(USER_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(USER_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND U.USER_MST_PK IN (");
                        sb.Append("   select t.USER_MST_PK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" ORDER BY USER_ID");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "User1"

        #region "User2"

        public DataSet FetchUser2(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append("SELECT DISTINCT USER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                USER_ID,");
                sb.Append("                USER_NAME,");
                sb.Append("                PASS_WORD EXTRA5,");
                sb.Append("                LOCATION_NAME ,");
                sb.Append("                EMPLOYEE_NAME ,");
                sb.Append("                DECODE(U.BUSINESS_TYPE,1,'AIR',2,'SEA',3,'BOTH') BUSINESS_TYPE,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM USER_MST_TBL U, LOCATION_MST_TBL L,EMPLOYEE_MST_TBL E");
                sb.Append(" WHERE U.IS_ACTIVATED=1");
                sb.Append(" AND U.DEFAULT_LOCATION_FK=L.LOCATION_MST_PK");
                sb.Append("  AND E.EMPLOYEE_MST_PK = U.EMPLOYEE_MST_FK");
                sb.Append(" AND L.COMP_LOCATION =1");
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  USER_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append(" AND U.DEFAULT_LOCATION_FK=" + ConditionPK);
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND U.BUSINESS_TYPE IN (3, " + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND U.USER_MST_PK IN (");
                        sb.Append("   select t.MODIFIED_USER_PK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }

                sb.Append(" UNION ");

                sb.Append("SELECT DISTINCT USER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                USER_ID,");
                sb.Append("                USER_NAME,");
                sb.Append("                PASS_WORD EXTRA5,");
                sb.Append("                LOCATION_NAME ,");
                sb.Append("                EMPLOYEE_NAME ,");
                sb.Append("                DECODE(U.BUSINESS_TYPE,1,'AIR',2,'SEA',3,'BOTH') BUSINESS_TYPE,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM USER_MST_TBL U, LOCATION_MST_TBL L,EMPLOYEE_MST_TBL E");
                sb.Append(" WHERE U.IS_ACTIVATED=1");
                sb.Append(" AND U.DEFAULT_LOCATION_FK=L.LOCATION_MST_PK");
                sb.Append("  AND E.EMPLOYEE_MST_PK = U.EMPLOYEE_MST_FK");
                sb.Append(" AND L.COMP_LOCATION =1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND USER_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append(" AND U.DEFAULT_LOCATION_FK=" + ConditionPK);
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND U.BUSINESS_TYPE IN (3, " + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(USER_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(USER_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND U.USER_MST_PK IN (");
                        sb.Append("   select t.MODIFIED_USER_PK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY USER_ID");
            }
            else
            {
                sb.Append("SELECT DISTINCT USER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                USER_ID,");
                sb.Append("                USER_NAME,");
                sb.Append("                PASS_WORD EXTRA5,");
                sb.Append("                LOCATION_NAME ,");
                sb.Append("                EMPLOYEE_NAME ,");
                sb.Append("                DECODE(U.BUSINESS_TYPE,1,'AIR',2,'SEA',3,'BOTH') BUSINESS_TYPE,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM USER_MST_TBL U, LOCATION_MST_TBL L,EMPLOYEE_MST_TBL E");
                sb.Append(" WHERE U.IS_ACTIVATED=1");
                sb.Append(" AND U.DEFAULT_LOCATION_FK=L.LOCATION_MST_PK");
                sb.Append("  AND E.EMPLOYEE_MST_PK = U.EMPLOYEE_MST_FK");
                sb.Append(" AND L.COMP_LOCATION =1");

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  USER_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append(" AND U.DEFAULT_LOCATION_FK=" + ConditionPK);
                }

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND U.BUSINESS_TYPE IN (3, " + ConditionPK1 + ")");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(USER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(USER_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(USER_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                    {
                        sb.Append("   AND U.USER_MST_PK IN (");
                        sb.Append("   select t.MODIFIED_USER_PK from VIEW_ATTENQ T");
                        sb.Append(GetAttEnq(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY USER_ID");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "User2"

        #region "Fetch Document Nr"

        public DataSet FetchDocumentNr(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = null;
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                arrAnnSearchPks = AnnSearchPks.Split('$');
            }
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');

            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append(GetDocumentNrQueryString(true, true, PK, TradeID));
                sb.Append(" UNION ");
                sb.Append(GetDocumentNrQueryString(false, false, PK, TradeID));
            }
            else
            {
                sb.Append(GetDocumentNrQueryString(false, true, PK, TradeID));
            }

            //Biz Type:Air
            if (ConditionPK == "1")
            {
                sb.Append("  AND MAINQRY.EXTRA5 IN (1,3)");
                //Biz Type:Sea
            }
            else if (ConditionPK == "2")
            {
                sb.Append("  AND MAINQRY.EXTRA5 IN (2,3)");
                //Biz Type:Both
            }
            else
            {
                sb.Append("  AND MAINQRY.EXTRA5 IN (1,2,3)");
            }
            //Menu Mst FK
            if (!string.IsNullOrEmpty(ConditionPK1))
            {
                sb.Append("   AND MAINQRY.EXTRA6 IN (" + ConditionPK1 + ")");
            }
            if (!string.IsNullOrEmpty(AnnSearchPks))
            {
                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "ATTENQ")
                {
                    sb.Append("   AND MAINQRY.DOCID IN (");
                    sb.Append("   select t.REFNR from VIEW_ATTENQ T");
                    sb.Append(GetAttEnq(AnnSearchPks));
                    sb.Append(")");
                }
            }
            sb.Append(" ORDER BY 10 DESC ");
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        private string GetDocumentNrQueryString(bool Active = false, bool IncludePks = false, string Pks = "", string Ids = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            string _active = (Active ? "'1'" : "'0'");
            sb.Append("SELECT * FROM (");
            sb.Append("SELECT DISTINCT EBST.ENQUIRY_BKG_SEA_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                EBST.ENQUIRY_REF_NO DOCID,");
            sb.Append("                TO_CHAR(EBST.ENQUIRY_DATE, DATEFORMAT) ENQUIRY_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM ATTACH_FILE_DTL_TBL AFDT, ENQUIRY_BKG_SEA_TBL EBST");
            sb.Append(" WHERE AFDT.ENQUIRY_MST_FK = EBST.ENQUIRY_BKG_SEA_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT QST.QUOTATION_MST_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                QST.QUOTATION_REF_NO DOCID,");
            sb.Append("                TO_CHAR(QST.QUOTATION_DATE, DATEFORMAT) QUOTATION_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM ATTACH_FILE_DTL_TBL AFDT, QUOTATION_MST_TBL QST");
            sb.Append(" WHERE AFDT.QUOTATION_MST_FK = QST.QUOTATION_MST_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT BST.BOOKING_MST_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                BST.BOOKING_REF_NO DOCID,");
            sb.Append("                TO_CHAR(BST.BOOKING_DATE, DATEFORMAT) BOOKING_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM BOOKING_MST_TBL BST, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.BOOKING_TRN_FK = BST.BOOKING_MST_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT JCSET.JOB_CARD_TRN_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                JCSET.JOBCARD_REF_NO DOCID,");
            sb.Append("                TO_CHAR(JCSET.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM JOB_CARD_TRN JCSET, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.JOB_CARD_TRN_FK = JCSET.JOB_CARD_TRN_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT HET.HBL_EXP_TBL_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                HET.HBL_REF_NO DOCID,");
            sb.Append("                TO_CHAR(HET.HBL_DATE, DATEFORMAT) HBL_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM HBL_EXP_TBL HET, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.HBL_TRN_FK = HET.HBL_EXP_TBL_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT RMST.RFQ_MAIN_SEA_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                RMST.RFQ_REF_NO DOCID,");
            sb.Append("                TO_CHAR(RMST.RFQ_DATE, DATEFORMAT) RFQ_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM RFQ_MAIN_SEA_TBL RMST, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.RFQ_SEA_FK = RMST.RFQ_MAIN_SEA_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT CMST.CONT_MAIN_SEA_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                CMST.CONTRACT_NO DOCID,");
            sb.Append("                TO_CHAR(CMST.CONTRACT_DATE, DATEFORMAT) CONTRACT_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM CONT_MAIN_SEA_TBL CMST, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.CONT_MAIN_SEA_FK = CMST.CONT_MAIN_SEA_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT DOMT.DELIVERY_ORDER_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                DOMT.DELIVERY_ORDER_REF_NO DOCID,");
            sb.Append("                TO_CHAR(DOMT.DELIVERY_ORDER_DATE, DATEFORMAT) DELIVERY_ORDER_DATE,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM DELIVERY_ORDER_MST_TBL DOMT, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.DELIVERY_ORDER_FK = DOMT.DELIVERY_ORDER_PK ");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT ANT.ANNOUNCEMENT_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                ANT.ANNOUNCEMENT_ID DOCID,");
            sb.Append("                TO_CHAR(ANT.ANNOUNCEMENT_DT, DATEFORMAT) ANNOUNCEMENT_DT,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM ANNOUNCEMENT_TBL ANT, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.ANNOUNCEMENT_FK = ANT.ANNOUNCEMENT_PK");
            sb.Append(" UNION ");
            sb.Append(" SELECT DISTINCT SCT.SALES_CALL_PK PK,");
            sb.Append("                '' EMPTY,");
            sb.Append("                SCT.SALES_CALL_ID DOCID,");
            sb.Append("                TO_CHAR(SCT.SALES_CALL_DT, DATEFORMAT) SALES_CALL_DT,");
            sb.Append("                AFDT.BIZ_TYPE EXTRA5,");
            sb.Append("                AFDT.MENU_MST_FK EXTRA6,");
            sb.Append("                '' EXTRA7,");
            sb.Append("                '' EXTRA8,");
            sb.Append("                '' EXTRA9,");
            sb.Append("                " + _active + " ACTIVE");
            sb.Append("  FROM SALES_CALL_TRN SCT, ATTACH_FILE_DTL_TBL AFDT");
            sb.Append(" WHERE AFDT.SALES_CALL_FK = SCT.SALES_CALL_PK");

            sb.Append(") MAINQRY WHERE 1=1 ");
            string _ids = Ids;

            string _pks = "'" + Pks.Replace(",", "','") + "'";
            if (!string.IsNullOrEmpty(Pks))
            {
                sb.Append(" AND MAINQRY.PK " + (IncludePks ? "IN" : "NOT IN") + " ( " + _pks + " ) ");
            }
            if (!string.IsNullOrEmpty(Ids))
            {
                if (Ids.IndexOf(",") < 0)
                {
                    sb.Append(" AND MAINQRY.DOCID LIKE '%" + _ids.ToUpper() + "%' ");
                }
                else
                {
                    _ids = "'" + Ids.Replace(",", "','") + "'";
                    sb.Append(" AND MAINQRY.DOCID " + (IncludePks ? "IN" : "NOT IN") + " (" + _ids.ToUpper() + ") ");
                }
            }
            return sb.ToString();
        }

        #endregion "Fetch Document Nr"

        #region "JOBCARD"

        public DataSet FetchJobcard(string Jobpk = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string TypedData = "", string JobId = "",
        string JobName = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Jobpk = Jobpk.TrimEnd(',');
            Jobpk = Jobpk.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            //'JOBCARD
            if (Convert.ToInt32(ConditionPK3) == 2)
            {
                if (!string.IsNullOrEmpty(Jobpk))
                {
                    sb.Append("SELECT DISTINCT Q1.JOBPK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                Q1.JOBCARD_REF_NO,");
                    sb.Append("                Q1.JOBCARD_DATE,");
                    sb.Append("                Q1.CUSTPK,");
                    sb.Append("                Q1.CUSTOMER_NAME,");
                    sb.Append("                Q1.POO,");
                    sb.Append("                Q1.COMMODITY_GROUP_CODE,");
                    sb.Append("                Q1.CARGO_TYPE,");
                    sb.Append("                Q1.ACTIVE,");
                    sb.Append("                Q1.CARRIER_MST_FK,");
                    sb.Append("                Q1.VOYAGE_TRN_FK,Q1.FLIGHT_NO");
                    sb.Append("  FROM (SELECT DISTINCT Q.JOBPK,");
                    sb.Append("                        Q.JOBCARD_REF_NO,");
                    sb.Append("                        Q.JOBCARD_DATE,");
                    sb.Append("                        Q.CUSTPK,");
                    sb.Append("                        Q.CUSTOMER_NAME,");
                    sb.Append("                        Q.POO,");
                    sb.Append("                        Q.COMMODITY_GROUP_CODE,");
                    sb.Append("                        Q.CARGO_TYPE,");
                    sb.Append("                        '0' ACTIVE,");
                    sb.Append("                Q.CARRIER_MST_FK,");
                    sb.Append("                Q.VOYAGE_TRN_FK,Q.FLIGHT_NO");
                    sb.Append("          FROM (SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       JOB.JOBCARD_REF_NO,");
                    sb.Append("                       TO_CHAR(JOB.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("                       JOB.SHIPPER_CUST_MST_FK CUSTPK,");
                    sb.Append("                       SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN BKG.CARGO_TYPE = 1 THEN");
                    sb.Append("                          POO.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          FRMPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       CASE WHEN BKG.BUSINESS_TYPE=2 THEN DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') ELSE DECODE(BKG.CARGO_TYPE, 1,'KGS',2,'ULD') END CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("                       BKG.CARRIER_MST_FK,");
                    sb.Append("                       JOB.VOYAGE_TRN_FK,JOB.VOYAGE_FLIGHT_NO FLIGHT_NO,");
                    sb.Append("                       JOB.BUSINESS_TYPE BIZTYPE,");
                    sb.Append("                       JOB.PROCESS_TYPE PROCESSTYPE");
                    sb.Append("                  FROM JOB_CARD_TRN    JOB,");
                    sb.Append("                       BOOKING_MST_TBL         BKG,");
                    sb.Append("                       CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PORT_MST_TBL            POO,");
                    sb.Append("                       PLACE_MST_TBL           FRMPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                    sb.Append("                   AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("                   AND JOB.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND BKG.POO_FK = POO.PORT_MST_PK(+)");
                    sb.Append("                   AND BKG.COL_PLACE_MST_FK = FRMPLC.PLACE_PK(+)");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND JOB.JOB_CARD_STATUS=1");
                    sb.Append("                   AND JOB.PROCESS_TYPE=1");
                    sb.Append("                   AND JOB.JOB_CARD_TRN_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 2");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                UNION");
                    sb.Append("                SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       JOB.JOBCARD_REF_NO,");
                    sb.Append("                       TO_CHAR(JOB.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("                       JOB.CONSIGNEE_CUST_MST_FK CUSTPK,");
                    sb.Append("                       CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN JOB.CARGO_TYPE = 1 THEN");
                    sb.Append("                          PFD.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          TOPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       CASE WHEN JOB.BUSINESS_TYPE=2 THEN DECODE(JOB.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') ELSE DECODE(JOB.CARGO_TYPE, 1,'KGS',2,'ULD') END CARGO_TYPE,");
                    sb.Append("                       POD.LOCATION_MST_FK,");
                    sb.Append("                       JOB.CARRIER_MST_FK,");
                    sb.Append("                       JOB.VOYAGE_TRN_FK, JOB.VOYAGE_FLIGHT_NO FLIGHT_NO,");
                    sb.Append("                       JOB.BUSINESS_TYPE BIZTYPE,");
                    sb.Append("                       JOB.PROCESS_TYPE PROCESSTYPE");
                    sb.Append("                  FROM JOB_CARD_TRN    JOB,");
                    sb.Append("                       CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POD,");
                    sb.Append("                       PORT_MST_TBL            PFD,");
                    sb.Append("                       PLACE_MST_TBL           TOPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE JOB.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("                   AND JOB.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                   AND JOB.PFD_FK = PFD.PORT_MST_PK(+)");
                    sb.Append("                   AND JOB.DEL_PLACE_MST_FK = TOPLC.PLACE_PK(+)");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POD.LOCATION_MST_FK");
                    sb.Append("                   AND JOB.JOB_CARD_STATUS=1");
                    sb.Append("                   AND JOB.PROCESS_TYPE=2");
                    sb.Append("                   AND JOB.JOB_CARD_TRN_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 2");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("             ) Q");

                    sb.Append(" WHERE 1=1 ");
                    if (!string.IsNullOrEmpty(Jobpk))
                    {
                        sb.Append("  AND Q.JOBPK NOT IN (" + Jobpk + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND Q.BIZTYPE IN (" + ConditionPK1 + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("    AND Q.PROCESSTYPE  IN (" + ConditionPK2 + ")");
                    }
                    sb.Append("    AND  Q.LOCATION_MST_FK = " + LoginPK + "");
                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT Q.JOBPK,");
                    sb.Append("                        Q.JOBCARD_REF_NO,");
                    sb.Append("                        Q.JOBCARD_DATE JCDATE,");
                    sb.Append("                        Q.CUSTPK,");
                    sb.Append("                        Q.CUSTOMER_NAME,");
                    sb.Append("                        Q.POO,");
                    sb.Append("                        Q.COMMODITY_GROUP_CODE,");
                    sb.Append("                        Q.CARGO_TYPE,");
                    sb.Append("                        '1' ACTIVE,");
                    sb.Append("                Q.CARRIER_MST_FK,");
                    sb.Append("                Q.VOYAGE_TRN_FK,Q.FLIGHT_NO");
                    sb.Append("          FROM (SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       JOB.JOBCARD_REF_NO,");
                    sb.Append("                       TO_CHAR(JOB.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("                       JOB.SHIPPER_CUST_MST_FK CUSTPK,");
                    sb.Append("                       SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN BKG.CARGO_TYPE = 1 THEN");
                    sb.Append("                          POO.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          FRMPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       CASE WHEN BKG.BUSINESS_TYPE=2 THEN DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') ELSE DECODE(BKG.CARGO_TYPE, 1,'KGS',2,'ULD') END CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("                       BKG.CARRIER_MST_FK,");
                    sb.Append("                       JOB.VOYAGE_TRN_FK,JOB.VOYAGE_FLIGHT_NO FLIGHT_NO,");
                    sb.Append("                       JOB.BUSINESS_TYPE BIZTYPE,");
                    sb.Append("                       JOB.PROCESS_TYPE PROCESSTYPE");
                    sb.Append("                  FROM JOB_CARD_TRN    JOB,");
                    sb.Append("                       BOOKING_MST_TBL         BKG,");
                    sb.Append("                       CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PORT_MST_TBL            POO,");
                    sb.Append("                       PLACE_MST_TBL           FRMPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                    sb.Append("                   AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("                   AND JOB.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND BKG.POO_FK = POO.PORT_MST_PK(+)");
                    sb.Append("                   AND BKG.COL_PLACE_MST_FK = FRMPLC.PLACE_PK(+)");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND JOB.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND JOB.PROCESS_TYPE=1");
                    sb.Append("                   AND JOB.JOB_CARD_TRN_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 2");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                UNION ");
                    sb.Append("                SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       JOB.JOBCARD_REF_NO,");
                    sb.Append("                       TO_CHAR(JOB.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("                       JOB.CONSIGNEE_CUST_MST_FK CUSTPK,");
                    sb.Append("                       CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN JOB.CARGO_TYPE = 1 THEN");
                    sb.Append("                          PFD.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          TOPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       CASE WHEN JOB.BUSINESS_TYPE=2 THEN DECODE(JOB.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') ELSE DECODE(JOB.CARGO_TYPE, 1,'KGS',2,'ULD') END CARGO_TYPE,");
                    sb.Append("                       POD.LOCATION_MST_FK,");
                    sb.Append("                       JOB.CARRIER_MST_FK,");
                    sb.Append("                       JOB.VOYAGE_TRN_FK, JOB.VOYAGE_FLIGHT_NO FLIGHT_NO,");
                    sb.Append("                       JOB.BUSINESS_TYPE BIZTYPE,");
                    sb.Append("                       JOB.PROCESS_TYPE PROCESSTYPE");
                    sb.Append("                  FROM JOB_CARD_TRN    JOB,");
                    sb.Append("                       CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POD,");
                    sb.Append("                       PORT_MST_TBL            PFD,");
                    sb.Append("                       PLACE_MST_TBL           TOPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE JOB.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("                   AND JOB.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND JOB.PORT_MST_POD_FK = POD.PORT_MST_PK");
                    sb.Append("                   AND JOB.PFD_FK = PFD.PORT_MST_PK(+)");
                    sb.Append("                   AND JOB.DEL_PLACE_MST_FK = TOPLC.PLACE_PK(+)");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POD.LOCATION_MST_FK");
                    sb.Append("                   AND JOB.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND JOB.PROCESS_TYPE=2");
                    sb.Append("                   AND JOB.JOB_CARD_TRN_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 2");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("               ) Q");
                    sb.Append(" WHERE 1=1 ");
                    if (!string.IsNullOrEmpty(Jobpk))
                    {
                        sb.Append("  AND Q.JOBPK IN (" + Jobpk + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND Q.BIZTYPE IN (" + ConditionPK1 + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("    AND Q.PROCESSTYPE  IN (" + ConditionPK2 + ")");
                    }

                    sb.Append("    AND  Q.LOCATION_MST_FK = " + LoginPK + "");

                    sb.Append("  ) Q1");
                    sb.Append(" ORDER BY Q1.ACTIVE DESC,TO_DATE(Q1.JOBCARD_DATE) DESC");
                }
                else
                {
                    sb.Append("        SELECT DISTINCT Q.JOBPK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                        Q.JOBCARD_REF_NO,");
                    sb.Append("                        Q.JOBCARD_DATE JCDATE,");
                    sb.Append("                        Q.CUSTPK,");
                    sb.Append("                        Q.CUSTOMER_NAME,");
                    sb.Append("                        Q.POO,");
                    sb.Append("                        Q.COMMODITY_GROUP_CODE,");
                    sb.Append("                        Q.CARGO_TYPE,");
                    sb.Append("                        '0' ACTIVE,");
                    sb.Append("                Q.CARRIER_MST_FK,");
                    sb.Append("                Q.VOYAGE_TRN_FK,Q.FLIGHT_NO");
                    sb.Append("          FROM (SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       JOB.JOBCARD_REF_NO,");
                    sb.Append("                       TO_CHAR(JOB.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("                       JOB.SHIPPER_CUST_MST_FK CUSTPK,");
                    sb.Append("                       SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN BKG.CARGO_TYPE = 1 THEN");
                    sb.Append("                          POO.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          FRMPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       CASE WHEN BKG.BUSINESS_TYPE=2 THEN DECODE(BKG.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') ELSE DECODE(BKG.CARGO_TYPE, 1,'KGS',2,'ULD') END CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("                       BKG.CARRIER_MST_FK,");
                    sb.Append("                       JOB.VOYAGE_TRN_FK,JOB.VOYAGE_FLIGHT_NO FLIGHT_NO,");
                    sb.Append("                       JOB.BUSINESS_TYPE BIZTYPE,");
                    sb.Append("                       JOB.PROCESS_TYPE PROCESSTYPE");
                    sb.Append("                  FROM JOB_CARD_TRN    JOB,");
                    sb.Append("                       BOOKING_MST_TBL         BKG,");
                    sb.Append("                       CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PORT_MST_TBL            POO,");
                    sb.Append("                       PLACE_MST_TBL           FRMPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE JOB.BOOKING_MST_FK = BKG.BOOKING_MST_PK");
                    sb.Append("                   AND JOB.SHIPPER_CUST_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("                   AND JOB.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND BKG.PORT_MST_POL_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND BKG.POO_FK = POO.PORT_MST_PK(+)");
                    sb.Append("                   AND BKG.COL_PLACE_MST_FK = FRMPLC.PLACE_PK(+)");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND JOB.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND JOB.PROCESS_TYPE=1");
                    sb.Append("                   AND JOB.JOB_CARD_TRN_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 2");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                UNION");
                    sb.Append("                SELECT JOB.JOB_CARD_TRN_PK JOBPK,");
                    sb.Append("                       JOB.JOBCARD_REF_NO,");
                    sb.Append("                       TO_CHAR(JOB.JOBCARD_DATE, DATEFORMAT) JOBCARD_DATE,");
                    sb.Append("                       JOB.CONSIGNEE_CUST_MST_FK CUSTPK,");
                    sb.Append("                       CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN JOB.CARGO_TYPE = 1 THEN");
                    sb.Append("                          PFD.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          TOPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       CASE WHEN JOB.BUSINESS_TYPE=2 THEN DECODE(JOB.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') ELSE DECODE(JOB.CARGO_TYPE, 1,'KGS',2,'ULD') END CARGO_TYPE,");
                    sb.Append("                       POD.LOCATION_MST_FK,");
                    sb.Append("                       JOB.CARRIER_MST_FK,");
                    sb.Append("                       JOB.VOYAGE_TRN_FK, JOB.VOYAGE_FLIGHT_NO  FLIGHT_NO,");
                    sb.Append("                       JOB.BUSINESS_TYPE BIZTYPE,");
                    sb.Append("                       JOB.PROCESS_TYPE PROCESSTYPE");
                    sb.Append("                  FROM JOB_CARD_TRN    JOB,");
                    sb.Append("                       CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POD,");
                    sb.Append("                       PORT_MST_TBL            PFD,");
                    sb.Append("                       PLACE_MST_TBL           TOPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE JOB.CONSIGNEE_CUST_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("                   AND JOB.COMMODITY_GROUP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND JOB.PFD_FK = PFD.PORT_MST_PK(+)");
                    sb.Append("                   AND JOB.DEL_PLACE_MST_FK = TOPLC.PLACE_PK(+)");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POD.LOCATION_MST_FK");
                    sb.Append("                   AND JOB.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND JOB.PROCESS_TYPE=2");
                    sb.Append("                   AND JOB.JOB_CARD_TRN_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 2");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                ) Q");
                    sb.Append(" WHERE 1=1 ");
                    if (!string.IsNullOrEmpty(Jobpk))
                    {
                        sb.Append("  AND Q.JOBPK IN (" + Jobpk + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND Q.BIZTYPE IN (" + ConditionPK1 + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("    AND Q.PROCESSTYPE  IN (" + ConditionPK2 + ")");
                    }

                    sb.Append("    AND  Q.LOCATION_MST_FK = " + LoginPK + "");

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(Q.JOBCARD_REF_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(JobId))
                    {
                        sb.Append(" AND UPPER(Q.JOBCARD_REF_NO) LIKE '%" + JobId.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY TO_DATE(Q.JOBCARD_DATE) DESC");
                }
                //'CBJC
            }
            else
            {
                if (!string.IsNullOrEmpty(Jobpk))
                {
                    sb.Append("SELECT DISTINCT Q1.CBJC_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                Q1.CBJC_NO,");
                    sb.Append("                Q1.CBJC_DATE,");
                    sb.Append("                Q1.CUSTPK,");
                    sb.Append("                Q1.CUSTOMER_NAME,");
                    sb.Append("                Q1.POO,");
                    sb.Append("                Q1.COMMODITY_GROUP_CODE,");
                    sb.Append("                Q1.CARGO_TYPE,");
                    sb.Append("                Q1.ACTIVE,");
                    sb.Append("                Q1.OPERATOR_MST_FK,");
                    sb.Append("                Q1.VOYAGE_TRN_FK,Q1.FLIGHT_NO");
                    sb.Append("  FROM (SELECT DISTINCT Q.CBJC_PK,");
                    sb.Append("                        Q.CBJC_NO,");
                    sb.Append("                        Q.CBJC_DATE,");
                    sb.Append("                        Q.CUSTPK,");
                    sb.Append("                        Q.CUSTOMER_NAME,");
                    sb.Append("                        Q.POO,");
                    sb.Append("                        Q.COMMODITY_GROUP_CODE,");
                    sb.Append("                        Q.CARGO_TYPE,");
                    sb.Append("                        '0' ACTIVE,");
                    sb.Append("                Q.OPERATOR_MST_FK,");
                    sb.Append("                Q.VOYAGE_TRN_FK,Q.FLIGHT_NO");
                    sb.Append("          FROM (SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.SHIPPER_MST_FK CUSTPK,");
                    sb.Append("                       SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN CBJC.CARGO_TYPE = 1 THEN");
                    sb.Append("                          POO.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          FRMPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       DECODE(CBJC.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               NVL(CBJC.VOYAGE_TRN_FK,0) VOYAGE_TRN_FK, CBJC.FLIGHT_NO, ");
                    sb.Append("                       2 BIZTYPE,");
                    sb.Append("                       1 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PORT_MST_TBL            POO,");
                    sb.Append("                       PLACE_MST_TBL           FRMPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.SHIPPER_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POL_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = POO.PORT_MST_PK(+) ");
                    sb.Append("                   AND CBJC.PLR_MST_FK = FRMPLC.PLACE_PK(+) ");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 2 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 1 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                UNION ");
                    sb.Append("                SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.SHIPPER_MST_FK CUSTPK,");
                    sb.Append("                       SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                       FRMPLC.PLACE_CODE POO, ");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       '' CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               0 VOYAGE_TRN_FK, CBJC.FLIGHT_NO,");
                    sb.Append("                       1 BIZTYPE,");
                    sb.Append("                       1 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PLACE_MST_TBL           FRMPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.SHIPPER_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POL_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = FRMPLC.PLACE_PK(+) ");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 1 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 1 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                UNION");
                    sb.Append("                SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.CONSIGNEE_MST_FK CUSTPK,");
                    sb.Append("                       CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN CBJC.CARGO_TYPE = 1 THEN");
                    sb.Append("                          PFD.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          TOPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       DECODE(CBJC.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               NVL(CBJC.VOYAGE_TRN_FK,0) VOYAGE_TRN_FK, CBJC.FLIGHT_NO,");
                    sb.Append("                       2 BIZTYPE,");
                    sb.Append("                       2 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PORT_MST_TBL            PFD,");
                    sb.Append("                       PLACE_MST_TBL           TOPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.CONSIGNEE_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POD_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = PFD.PORT_MST_PK(+) ");
                    sb.Append("                   AND CBJC.PLR_MST_FK = TOPLC.PLACE_PK(+) ");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 2 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 2 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                UNION");
                    sb.Append("                SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.CONSIGNEE_MST_FK CUSTPK,");
                    sb.Append("                       CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                       TOPLC.PLACE_CODE POO, ");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       '' CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               0 VOYAGE_TRN_FK, CBJC.FLIGHT_NO, ");
                    sb.Append("                       1 BIZTYPE,");
                    sb.Append("                       2 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PLACE_MST_TBL           TOPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.CONSIGNEE_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POD_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = TOPLC.PLACE_PK(+) ");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 1 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 2 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK) Q");
                    sb.Append("         WHERE 1 = 1");
                    if (!string.IsNullOrEmpty(Jobpk))
                    {
                        sb.Append("  AND Q.CBJC_PK NOT IN (" + Jobpk + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND Q.BIZTYPE IN (" + ConditionPK1 + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("    AND Q.PROCESSTYPE  IN (" + ConditionPK2 + ")");
                    }
                    sb.Append("    AND  Q.LOCATION_MST_FK = " + LoginPK + "");

                    sb.Append("        UNION");
                    sb.Append("        SELECT DISTINCT Q.CBJC_PK,");
                    sb.Append("                        Q.CBJC_NO,");
                    sb.Append("                        Q.CBJC_DATE,");
                    sb.Append("                        Q.CUSTPK,");
                    sb.Append("                        Q.CUSTOMER_NAME,");
                    sb.Append("                        Q.POO,");
                    sb.Append("                        Q.COMMODITY_GROUP_CODE,");
                    sb.Append("                        Q.CARGO_TYPE,");
                    sb.Append("                        '1' ACTIVE,");
                    sb.Append("                Q.OPERATOR_MST_FK,");
                    sb.Append("                Q.VOYAGE_TRN_FK,Q.FLIGHT_NO");
                    sb.Append("          FROM (SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.SHIPPER_MST_FK CUSTPK,");
                    sb.Append("                       SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN CBJC.CARGO_TYPE = 1 THEN");
                    sb.Append("                          POO.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          FRMPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       DECODE(CBJC.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               NVL(CBJC.VOYAGE_TRN_FK,0) VOYAGE_TRN_FK, CBJC.FLIGHT_NO, ");
                    sb.Append("                       2 BIZTYPE,");
                    sb.Append("                       1 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PORT_MST_TBL            POO,");
                    sb.Append("                       PLACE_MST_TBL           FRMPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.SHIPPER_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POL_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = POO.PORT_MST_PK(+) ");
                    sb.Append("                   AND CBJC.PLR_MST_FK = FRMPLC.PLACE_PK(+) ");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 2 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 1 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                UNION");
                    sb.Append("                SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.SHIPPER_MST_FK CUSTPK,");
                    sb.Append("                       SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                       FRMPLC.PLACE_CODE POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       '' CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               0 VOYAGE_TRN_FK, CBJC.FLIGHT_NO,");
                    sb.Append("                       1 BIZTYPE,");
                    sb.Append("                       1 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PLACE_MST_TBL           FRMPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.SHIPPER_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POL_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = FRMPLC.PLACE_PK(+) ");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 1 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 1 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.JC_FK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                UNION");
                    sb.Append("                SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.CONSIGNEE_MST_FK CUSTPK,");
                    sb.Append("                       CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN CBJC.CARGO_TYPE = 1 THEN");
                    sb.Append("                          PFD.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          TOPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       DECODE(CBJC.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               NVL(CBJC.VOYAGE_TRN_FK,0) VOYAGE_TRN_FK, CBJC.FLIGHT_NO,");
                    sb.Append("                       2 BIZTYPE,");
                    sb.Append("                       2 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PORT_MST_TBL            PFD,");
                    sb.Append("                       PLACE_MST_TBL           TOPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.CONSIGNEE_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POD_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = PFD.PORT_MST_PK(+) ");
                    sb.Append("                   AND CBJC.PLR_MST_FK = TOPLC.PLACE_PK(+) ");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 2 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 2 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                UNION");
                    sb.Append("                SELECT CBJC.CBJC_PK,");
                    sb.Append("                       CBJC.CBJC_NO,");
                    sb.Append("                       TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("                       CBJC.CONSIGNEE_MST_FK CUSTPK,");
                    sb.Append("                       CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                       TOPLC.PLACE_CODE POO,");
                    sb.Append("                       COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("                       '' CARGO_TYPE,");
                    sb.Append("                       POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               0 VOYAGE_TRN_FK, CBJC.FLIGHT_NO, ");
                    sb.Append("                       1 BIZTYPE,");
                    sb.Append("                       2 PROCESSTYPE");
                    sb.Append("                  FROM CBJC_TBL                CBJC,");
                    sb.Append("                       CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("                       COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("                       PORT_MST_TBL            POL,");
                    sb.Append("                       PLACE_MST_TBL           TOPLC,");
                    sb.Append("                       LOCATION_MST_TBL        LMT");
                    sb.Append("                 WHERE CBJC.CONSIGNEE_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("                   AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("                   AND CBJC.POD_MST_FK = POL.PORT_MST_PK");
                    sb.Append("                   AND CBJC.PLR_MST_FK = TOPLC.PLACE_PK(+) ");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 1 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 2 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("                   AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK) Q");
                    sb.Append("         WHERE 1 = 1");
                    if (!string.IsNullOrEmpty(Jobpk))
                    {
                        sb.Append("  AND Q.CBJC_PK IN (" + Jobpk + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND Q.BIZTYPE IN (" + ConditionPK1 + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("    AND Q.PROCESSTYPE  IN (" + ConditionPK2 + ")");
                    }

                    sb.Append("    AND  Q.LOCATION_MST_FK = " + LoginPK + "");

                    sb.Append("     ) Q1");
                    sb.Append(" ORDER BY Q1.ACTIVE DESC,TO_DATE(Q1.CBJC_DATE) DESC");
                }
                else
                {
                    sb.Append("SELECT DISTINCT Q.CBJC_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                Q.CBJC_NO,");
                    sb.Append("                Q.CBJC_DATE,");
                    sb.Append("                Q.CUSTPK,");
                    sb.Append("                Q.CUSTOMER_NAME,");
                    sb.Append("                Q.POO,");
                    sb.Append("                Q.COMMODITY_GROUP_CODE,");
                    sb.Append("                Q.CARGO_TYPE,");
                    sb.Append("                '0' ACTIVE,");
                    sb.Append("                Q.OPERATOR_MST_FK,");
                    sb.Append("                Q.VOYAGE_TRN_FK,Q.FLIGHT_NO");
                    sb.Append("  FROM (SELECT CBJC.CBJC_PK,");
                    sb.Append("               CBJC.CBJC_NO,");
                    sb.Append("               TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("               CBJC.SHIPPER_MST_FK CUSTPK,");
                    sb.Append("               SHIPPER.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN CBJC.CARGO_TYPE = 1 THEN");
                    sb.Append("                          POO.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          FRMPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("               COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("               DECODE(CBJC.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("               POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               NVL(CBJC.VOYAGE_TRN_FK,0) VOYAGE_TRN_FK, CBJC.FLIGHT_NO, ");
                    sb.Append("               2 BIZTYPE,");
                    sb.Append("               1 PROCESSTYPE");
                    sb.Append("          FROM CBJC_TBL                CBJC,");
                    sb.Append("               CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("               PORT_MST_TBL            POL,");
                    sb.Append("               PORT_MST_TBL            POO,");
                    sb.Append("               PLACE_MST_TBL           FRMPLC,");
                    sb.Append("               LOCATION_MST_TBL        LMT");
                    sb.Append("         WHERE CBJC.SHIPPER_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("           AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("           AND CBJC.POL_MST_FK = POL.PORT_MST_PK");
                    sb.Append("           AND CBJC.PLR_MST_FK = POO.PORT_MST_PK(+) ");
                    sb.Append("           AND CBJC.PLR_MST_FK = FRMPLC.PLACE_PK(+) ");
                    sb.Append("           AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 2 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 1 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("        UNION");
                    sb.Append("        SELECT CBJC.CBJC_PK,");
                    sb.Append("               CBJC.CBJC_NO,");
                    sb.Append("               TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("               CBJC.SHIPPER_MST_FK CUSTPK,");
                    sb.Append("               SHIPPER.CUSTOMER_NAME,");
                    sb.Append("               FRMPLC.PLACE_CODE POO,");
                    sb.Append("               COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               0 VOYAGE_TRN_FK, CBJC.FLIGHT_NO,");
                    sb.Append("               1 BIZTYPE,");
                    sb.Append("               1 PROCESSTYPE");
                    sb.Append("          FROM CBJC_TBL                CBJC,");
                    sb.Append("               CUSTOMER_MST_TBL        SHIPPER,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("               PORT_MST_TBL            POL,");
                    sb.Append("               PLACE_MST_TBL           FRMPLC,");
                    sb.Append("               LOCATION_MST_TBL        LMT");
                    sb.Append("         WHERE CBJC.SHIPPER_MST_FK = SHIPPER.CUSTOMER_MST_PK");
                    sb.Append("           AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("           AND CBJC.POL_MST_FK = POL.PORT_MST_PK");
                    sb.Append("           AND CBJC.PLR_MST_FK = FRMPLC.PLACE_PK(+) ");
                    sb.Append("           AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 1 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 1 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("           AND CBJC.CBJC_PK NOT IN ");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("        UNION");
                    sb.Append("        SELECT CBJC.CBJC_PK,");
                    sb.Append("               CBJC.CBJC_NO,");
                    sb.Append("               TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("               CBJC.CONSIGNEE_MST_FK CUSTPK,");
                    sb.Append("               CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("                     (CASE");
                    sb.Append("                         WHEN CBJC.CARGO_TYPE = 1 THEN");
                    sb.Append("                          PFD.PORT_ID");
                    sb.Append("                         ELSE");
                    sb.Append("                          TOPLC.PLACE_CODE");
                    sb.Append("                       END) POO,");
                    sb.Append("               COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("               DECODE(CBJC.CARGO_TYPE, 1, 'FCL', 2, 'LCL', 4, 'BBC') CARGO_TYPE,");
                    sb.Append("               POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               NVL(CBJC.VOYAGE_TRN_FK,0) VOYAGE_TRN_FK, CBJC.FLIGHT_NO,");
                    sb.Append("               2 BIZTYPE,");
                    sb.Append("               2 PROCESSTYPE");
                    sb.Append("          FROM CBJC_TBL                CBJC,");
                    sb.Append("               CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("               PORT_MST_TBL            POL,");
                    sb.Append("               PORT_MST_TBL            PFD,");
                    sb.Append("               PLACE_MST_TBL           TOPLC,");
                    sb.Append("               LOCATION_MST_TBL        LMT");
                    sb.Append("         WHERE CBJC.CONSIGNEE_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("           AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("           AND CBJC.POD_MST_FK = POL.PORT_MST_PK");
                    sb.Append("           AND CBJC.PLR_MST_FK = PFD.PORT_MST_PK(+) ");
                    sb.Append("           AND CBJC.PLR_MST_FK = TOPLC.PLACE_PK(+) ");
                    sb.Append("           AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.BIZ_TYPE = 2 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 2 ");
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("        UNION");
                    sb.Append("        SELECT CBJC.CBJC_PK,");
                    sb.Append("               CBJC.CBJC_NO,");
                    sb.Append("               TO_CHAR(CBJC.CBJC_DATE, DATEFORMAT) CBJC_DATE,");
                    sb.Append("               CBJC.CONSIGNEE_MST_FK CUSTPK,");
                    sb.Append("               CONSIGNEE.CUSTOMER_NAME,");
                    sb.Append("               TOPLC.PLACE_CODE POO,");
                    sb.Append("               COMM.COMMODITY_GROUP_CODE,");
                    sb.Append("               '' CARGO_TYPE,");
                    sb.Append("               POL.LOCATION_MST_FK,");
                    sb.Append("               NVL(CBJC.OPERATOR_MST_FK,0) OPERATOR_MST_FK, ");
                    sb.Append("               0 VOYAGE_TRN_FK, CBJC.FLIGHT_NO, ");
                    sb.Append("               1 BIZTYPE,");
                    sb.Append("               2 PROCESSTYPE");
                    sb.Append("          FROM CBJC_TBL                CBJC,");
                    sb.Append("               CUSTOMER_MST_TBL        CONSIGNEE,");
                    sb.Append("               COMMODITY_GROUP_MST_TBL COMM,");
                    sb.Append("               PORT_MST_TBL            POL,");
                    sb.Append("               PLACE_MST_TBL           TOPLC,");
                    sb.Append("               LOCATION_MST_TBL        LMT");
                    sb.Append("         WHERE CBJC.CONSIGNEE_MST_FK = CONSIGNEE.CUSTOMER_MST_PK");
                    sb.Append("           AND CBJC.COMM_GRP_FK = COMM.COMMODITY_GROUP_PK");
                    sb.Append("           AND CBJC.POD_MST_FK = POL.PORT_MST_PK");
                    sb.Append("           AND CBJC.PLR_MST_FK = TOPLC.PLACE_PK(+) ");
                    sb.Append("                   AND CBJC.BIZ_TYPE = 1 ");
                    sb.Append("                   AND CBJC.PROCESS_TYPE = 2 ");
                    sb.Append("                   AND CBJC.JOB_CARD_STATUS=1");
                    //
                    sb.Append("                   AND CBJC.TRANSPORT_REQ=0");
                    //
                    sb.Append("                   AND CBJC.CBJC_PK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                        FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                        FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK ");
                    sb.Append("                        FROM TRANSPORT_INST_SEA_TBL TP");
                    sb.Append("                        WHERE TP.TP_CBJC_JC = 1");
                    sb.Append("                        AND TP.JOB_CARD_FK IS NOT NULL') PK");
                    sb.Append("                                FROM DUAL)))");
                    sb.Append("                 AND ((CBJC.JC_FK NOT IN");
                    sb.Append("                     (SELECT *");
                    sb.Append("                          FROM TABLE (SELECT FN_SPLIT(NVL(PK, 0))");
                    sb.Append("                          FROM (SELECT ROWTOCOL('SELECT TP.JOB_CARD_FK  FROM TRANSPORT_INST_SEA_TBL TP ");
                    sb.Append("                                     WHERE TP.TP_CBJC_JC = 2') PK");
                    sb.Append("                                  FROM DUAL)))) OR (CBJC.JC_FK IS NULL))");
                    sb.Append("           AND LMT.LOCATION_MST_PK = POL.LOCATION_MST_FK) Q");
                    sb.Append(" WHERE 1 = 1");
                    if (!string.IsNullOrEmpty(Jobpk))
                    {
                        sb.Append("  AND Q.CBJC_PK IN (" + Jobpk + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK1))
                    {
                        sb.Append("   AND Q.BIZTYPE IN (" + ConditionPK1 + ")");
                    }

                    if (!string.IsNullOrEmpty(ConditionPK2))
                    {
                        sb.Append("    AND Q.PROCESSTYPE  IN (" + ConditionPK2 + ")");
                    }

                    sb.Append("    AND  Q.LOCATION_MST_FK = " + LoginPK + "");

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(Q.CBJC_NO) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(JobId))
                    {
                        sb.Append(" AND UPPER(Q.CBJC_NO) LIKE '%" + JobId.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY TO_DATE(Q.CBJC_DATE) DESC");
                }
            }
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "JOBCARD"

        #region "Fetch Container Type"

        public DataSet FetchContType(string ContTypePK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TypedData = "", string ContTypeID = "", string ContTypeName = "", string selectedPKs = "", long LoginPK = 0,
        string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            ContTypePK = ContTypePK.TrimEnd(',');
            ContTypePK = ContTypePK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');

            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(ContTypePK))
            {
                sb.Append("SELECT DISTINCT c.container_type_mst_pk,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.container_type_mst_id,");
                sb.Append("       c.container_type_name, ");
                sb.Append("       c.preferences EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE ");
                sb.Append(" FROM container_type_mst_tbl c ");
                sb.Append(" WHERE c.active_flag = 1 ");

                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.container_type_mst_id) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.container_type_name) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" AND c.container_type_mst_pk  IN (" + ContTypePK + ")");
                sb.Append(" UNION ");
                sb.Append("SELECT DISTINCT c.container_type_mst_pk,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.container_type_mst_id,");
                sb.Append("       c.container_type_name, ");
                sb.Append("       c.preferences EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE ");
                sb.Append(" FROM container_type_mst_tbl c ");
                sb.Append(" WHERE c.active_flag = 1 ");
                sb.Append(" AND c.container_type_mst_pk NOT IN (" + ContTypePK + ")");
                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.container_type_mst_id) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.container_type_name) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                sb.Append("SELECT DISTINCT c.container_type_mst_pk,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.container_type_mst_id,");
                sb.Append("       c.container_type_name, ");
                sb.Append("       c.preferences EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE ");
                sb.Append(" FROM container_type_mst_tbl c ");
                sb.Append(" WHERE c.active_flag = 1 ");

                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.container_type_mst_id) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.container_type_name) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }
            }

            sb.Append(" ORDER BY  5,3,4 ");

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Container Type"

        #region "Fetch ICD Ports and Places"

        public DataSet FetchICDPortPlaces(string ContTypePK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TypedData = "", string ContTypeID = "", string ContTypeName = "", string selectedPKs = "", long LoginPK = 0,
        string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            ContTypePK = ContTypePK.TrimEnd(',');
            ContTypePK = ContTypePK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');

            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(ContTypePK))
            {
                sb.Append("SELECT DISTINCT c.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.PORT_ID,");
                sb.Append("       c.PORT_NAME, ");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE ");
                sb.Append(" FROM PORT_MST_TBL c ");
                sb.Append(" WHERE c.PORT_TYPE = 1 ");

                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.PORT_ID) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.PORT_NAME) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" AND c.PORT_MST_PK  IN (" + ContTypePK + ")");
                sb.Append(" UNION ");
                sb.Append("SELECT DISTINCT c.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.PORT_ID,");
                sb.Append("       c.PORT_NAME, ");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE ");
                sb.Append(" FROM PORT_MST_TBL c ");
                sb.Append(" WHERE c.PORT_TYPE = 1 ");
                sb.Append(" AND c.PORT_MST_PK NOT IN (" + ContTypePK + ")");
                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.PORT_ID) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.PORT_NAME) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                sb.Append("SELECT DISTINCT c.PORT_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.PORT_ID,");
                sb.Append("       c.PORT_NAME, ");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE ");
                sb.Append(" FROM PORT_MST_TBL c ");
                sb.Append(" WHERE c.PORT_TYPE = 1 ");

                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.PORT_ID) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.PORT_NAME) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }
            }

            sb.Append(" ORDER BY  5,3,4 ");

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch ICD Ports and Places"

        #region "Fetch Shipping Line"

        public DataSet FetchShippingLine(string ContTypePK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TypedData = "", string ContTypeID = "", string ContTypeName = "", string selectedPKs = "", long LoginPK = 0,
        string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();

            ContTypePK = ContTypePK.TrimEnd(',');
            ContTypePK = ContTypePK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');

            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(ContTypePK))
            {
                sb.Append("SELECT DISTINCT c.operator_mst_pk,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.operator_id,");
                sb.Append("       c.operator_name, ");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE ");
                sb.Append(" FROM operator_mst_tbl c ");
                sb.Append(" WHERE 1 = 1 ");

                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.operator_id) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.operator_name) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" AND c.operator_mst_pk  IN (" + ContTypePK + ")");
                sb.Append(" UNION ");
                sb.Append("SELECT DISTINCT c.operator_mst_pk,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.operator_id,");
                sb.Append("       c.operator_name, ");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE ");
                sb.Append(" FROM operator_mst_tbl c ");
                sb.Append(" WHERE 1 = 1 ");
                sb.Append(" AND c.operator_mst_pk NOT IN (" + ContTypePK + ")");
                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.operator_id) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.operator_name) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }
            }
            else
            {
                sb.Append("SELECT DISTINCT c.operator_mst_pk,");
                sb.Append("       '' EMPTY,");
                sb.Append("       c.operator_id,");
                sb.Append("       c.operator_name, ");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE ");
                sb.Append(" FROM operator_mst_tbl c ");
                sb.Append(" WHERE 1 = 1 ");

                if (!string.IsNullOrEmpty(ContTypeID))
                {
                    sb.Append(" AND UPPER(c.operator_id) LIKE '%" + ContTypeID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ContTypeName))
                {
                    sb.Append(" AND UPPER(c.operator_name) LIKE '%" + ContTypeName.ToUpper().Replace("'", "''") + "%'");
                }
            }

            sb.Append(" ORDER BY  5,3,4 ");

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);

            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Shipping Line"

        #region "Fetch CHA Agent"

        public DataSet FetchCHAAgent(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string TypedData = "", string ID = "", string NAME = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string SerchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            Array arrSearchPks = null;
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(SerchPks))
            {
                arrSearchPks = SerchPks.Split('$');
            }
            if (!string.IsNullOrEmpty(PK))
            {
                sb.Append(" SELECT DISTINCT VMT.VENDOR_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       VMT.VENDOR_ID,");
                sb.Append("       VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '1' ACTIVE");
                sb.Append("  FROM VENDOR_MST_TBL VMT,LOCATION_MST_TBL LMT,VENDOR_CONTACT_DTLS VCDT,VIEW_CUSTOMS_CLEARANCE_CBJC VVC");
                sb.Append(" WHERE VMT.VENDOR_MST_PK = VCDT.VENDOR_MST_FK");
                sb.Append("   AND VCDT.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND VVC.VENDOR_MST_PK = VMT.VENDOR_MST_PK ");
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN(" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    if (Convert.ToInt32(ConditionPK2) == 2)
                    {
                        sb.Append("   AND VVC.BIZ_TYPE IN (2,3)");
                    }
                    else
                    {
                        sb.Append("   AND VVC.BIZ_TYPE IN (1,3)");
                    }
                }
                if (!string.IsNullOrEmpty(SerchPks))
                {
                    if (Convert.ToString(arrSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(1))))
                    {
                        sb.Append("   AND VVC.PROCESS_TYPE IN (" + Convert.ToString(arrSearchPks.GetValue(1)) + ")");
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(2))))
                    {
                        sb.Append("   AND VVC.CARGO_TYPE IN (" + Convert.ToString(arrSearchPks.GetValue(2)) + ")");
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(3))))
                    {
                        sb.Append(" AND TO_DATE(VVC.CBJC_DATE) >= TO_DATE('" + Convert.ToString(arrSearchPks.GetValue(3)) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(4))))
                    {
                        sb.Append(" AND TO_DATE(VVC.CBJC_DATE) <= TO_DATE('" + Convert.ToString(arrSearchPks.GetValue(4)) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(5))))
                    {
                        sb.Append(" AND (VVC.VOYAGE_TRN_FK IN  (" + Convert.ToString(arrSearchPks.GetValue(5)) + ") Or  VVC.OPERATOR_MST_FK IN (" + Convert.ToString(arrSearchPks.GetValue(5)) + ") ) ");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                    {
                        if (Convert.ToInt32(arrSearchPks.GetValue(7)) > 0)
                        {
                            if (Convert.ToInt32(arrSearchPks.GetValue(7)) == 1 & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                            {
                                sb.Append("  AND VVC.JC_FK IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ")");
                            }
                            else if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                            {
                                sb.Append("  AND VVC.CBJC_PK IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ")");
                            }
                        }
                        else
                        {
                            sb.Append("  AND (VVC.JC_FK in (" + Convert.ToString(arrSearchPks.GetValue(6)) + ") Or  VVC.CBJC_PK  IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ") ) ");
                        }
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(8)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(8))))
                    {
                        sb.Append("   AND VVC.COMM_GRP_FK IN (" + Convert.ToString(arrSearchPks.GetValue(8)) + ")");
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(9)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(9))))
                    {
                        sb.Append("   AND VVC.CUSTOMER_MST_FK IN (" + Convert.ToString(arrSearchPks.GetValue(9)) + ")");
                    }
                    if (Convert.ToInt32(arrSearchPks.GetValue(10)) >= 0 & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(10))))
                    {
                        sb.Append("   AND VVC.CC_STATUS IN (" + Convert.ToString(arrSearchPks.GetValue(10)) + ")");
                    }
                }
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND VMT.VENDOR_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append(" SELECT DISTINCT VMT.VENDOR_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       VMT.VENDOR_ID,");
                sb.Append("       VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("  FROM VENDOR_MST_TBL VMT,LOCATION_MST_TBL LMT,VENDOR_CONTACT_DTLS VCDT,VIEW_CUSTOMS_CLEARANCE_CBJC VVC");
                sb.Append(" WHERE VMT.VENDOR_MST_PK = VCDT.VENDOR_MST_FK");
                sb.Append("   AND VCDT.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND VVC.VENDOR_MST_PK = VMT.VENDOR_MST_PK ");

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN(" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    if (Convert.ToInt32(ConditionPK2) == 2)
                    {
                        sb.Append("   AND VVC.BIZ_TYPE IN (2,3)");
                    }
                    else
                    {
                        sb.Append("   AND VVC.BIZ_TYPE IN (1,3)");
                    }
                }
                if (!string.IsNullOrEmpty(SerchPks))
                {
                    if (Convert.ToString(arrSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(1))))
                    {
                        sb.Append("   AND VVC.PROCESS_TYPE IN (" + Convert.ToString(arrSearchPks.GetValue(1)) + ")");
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(2))))
                    {
                        sb.Append("   AND VVC.CARGO_TYPE IN (" + Convert.ToString(arrSearchPks.GetValue(2)) + ")");
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(3))))
                    {
                        sb.Append(" AND TO_DATE(VVC.CBJC_DATE) >= TO_DATE('" + Convert.ToString(arrSearchPks.GetValue(3)) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(4))))
                    {
                        sb.Append(" AND TO_DATE(VVC.CBJC_DATE) <= TO_DATE('" + Convert.ToString(arrSearchPks.GetValue(4)) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(5))))
                    {
                        sb.Append(" AND (VVC.VOYAGE_TRN_FK IN  (" + Convert.ToString(arrSearchPks.GetValue(5)) + ") Or  VVC.OPERATOR_MST_FK IN (" + Convert.ToString(arrSearchPks.GetValue(5)) + ") ) ");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                    {
                        if (Convert.ToInt32(arrSearchPks.GetValue(7)) > 0)
                        {
                            if (Convert.ToInt32(arrSearchPks.GetValue(7)) == 1 & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                            {
                                sb.Append("  AND VVC.JC_FK IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ")");
                            }
                            else if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                            {
                                sb.Append("  AND VVC.CBJC_PK IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ")");
                            }
                        }
                        else
                        {
                            sb.Append("  AND (VVC.JC_FK in (" + Convert.ToString(arrSearchPks.GetValue(6)) + ") Or  VVC.CBJC_PK  IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ") ) ");
                        }
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(8)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(8))))
                    {
                        sb.Append("   AND VVC.COMM_GRP_FK IN (" + Convert.ToString(arrSearchPks.GetValue(8)) + ")");
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(9)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(9))))
                    {
                        sb.Append("   AND VVC.CUSTOMER_MST_FK IN (" + Convert.ToString(arrSearchPks.GetValue(9)) + ")");
                    }
                    if (Convert.ToInt32(arrSearchPks.GetValue(10)) >= 0 & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(10))))
                    {
                        sb.Append("   AND VVC.CC_STATUS IN (" + Convert.ToString(arrSearchPks.GetValue(10)) + ")");
                    }
                }
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND VMT.VENDOR_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append(" SELECT DISTINCT VMT.VENDOR_MST_PK,");
                sb.Append("               '' EMPTY,");
                sb.Append("       VMT.VENDOR_ID,");
                sb.Append("       VMT.VENDOR_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("         '0' ACTIVE");
                sb.Append("  FROM VENDOR_MST_TBL VMT,LOCATION_MST_TBL LMT,VENDOR_CONTACT_DTLS VCDT,VIEW_CUSTOMS_CLEARANCE_CBJC VVC");
                sb.Append(" WHERE VMT.VENDOR_MST_PK = VCDT.VENDOR_MST_FK ");
                sb.Append("   AND VCDT.ADM_LOCATION_MST_FK = LMT.LOCATION_MST_PK");
                sb.Append("   AND VVC.VENDOR_MST_PK = VMT.VENDOR_MST_PK ");

                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND LMT.LOCATION_MST_PK IN(" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(ConditionPK2))
                {
                    if (Convert.ToInt32(ConditionPK2) == 2)
                    {
                        sb.Append("   AND VVC.BIZ_TYPE IN(2,3)");
                    }
                    else
                    {
                        sb.Append("   AND VVC.BIZ_TYPE IN(1,3)");
                    }
                }

                if (!string.IsNullOrEmpty(SerchPks))
                {
                    if (Convert.ToString(arrSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(1))))
                    {
                        sb.Append("   AND VVC.PROCESS_TYPE IN (" + Convert.ToString(arrSearchPks.GetValue(1)) + ")");
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(2))))
                    {
                        sb.Append("   AND VVC.CARGO_TYPE IN (" + Convert.ToString(arrSearchPks.GetValue(2)) + ")");
                    }

                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(3))))
                    {
                        sb.Append(" AND TO_DATE(VVC.CBJC_DATE) >= TO_DATE('" + Convert.ToString(arrSearchPks.GetValue(3)) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(4))))
                    {
                        sb.Append(" AND TO_DATE(VVC.CBJC_DATE) <= TO_DATE('" + Convert.ToString(arrSearchPks.GetValue(4)) + "',dateformat)");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(5))))
                    {
                        sb.Append(" AND (VVC.VOYAGE_TRN_FK IN  (" + Convert.ToString(arrSearchPks.GetValue(5)) + ") Or  VVC.OPERATOR_MST_FK IN (" + Convert.ToString(arrSearchPks.GetValue(5)) + ") ) ");
                    }
                    if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                    {
                        if (Convert.ToInt32(arrSearchPks.GetValue(7)) > 0)
                        {
                            if (Convert.ToInt32(arrSearchPks.GetValue(7)) == 1 & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                            {
                                sb.Append("  AND VVC.JC_FK IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ")");
                            }
                            else if (!string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(6))))
                            {
                                sb.Append("  AND VVC.CBJC_PK IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ")");
                            }
                        }
                        else
                        {
                            sb.Append("  AND (VVC.JC_FK in (" + Convert.ToString(arrSearchPks.GetValue(6)) + ") Or  VVC.CBJC_PK  IN (" + Convert.ToString(arrSearchPks.GetValue(6)) + ") ) ");
                        }
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(8)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(8))))
                    {
                        sb.Append("   AND VVC.COMM_GRP_FK IN (" + Convert.ToString(arrSearchPks.GetValue(8)) + ")");
                    }
                    if (Convert.ToString(arrSearchPks.GetValue(9)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(9))))
                    {
                        sb.Append("   AND VVC.CUSTOMER_MST_FK IN (" + Convert.ToString(arrSearchPks.GetValue(9)) + ")");
                    }
                    if (Convert.ToInt32(arrSearchPks.GetValue(10)) >= 0 & !string.IsNullOrEmpty(Convert.ToString(arrSearchPks.GetValue(10))))
                    {
                        sb.Append("   AND VVC.CC_STATUS IN (" + Convert.ToString(arrSearchPks.GetValue(10)) + ")");
                    }
                }
                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND VMT.VENDOR_MST_PK IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(NAME))
                {
                    sb.Append(" AND UPPER(VMT.VENDOR_NAME) LIKE '%" + NAME.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch CHA Agent"

        #region "P&L Year"

        public DataSet FetchPLYear(string YearPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "", long LoginPK = 0,
        bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            YearPK = YearPK.TrimEnd(',');
            YearPK = YearPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            int No_of_Yrs = 0;
            if ((ConfigurationSettings.AppSettings["No_of_Years"] != null))
            {
                No_of_Yrs = Convert.ToInt32(ConfigurationSettings.AppSettings["No_of_Years"]);
            }
            else
            {
                No_of_Yrs = 5;
            }
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(YearPK))
            {
                sb.Append("SELECT * FROM ( ");
                sb.Append("SELECT DISTINCT EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE + 365, 'YEAR'), (-LEVEL))) AS YEARPK,");
                sb.Append("                '' EXTRA1,");
                sb.Append("EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE + 365, 'YEAR'), (-LEVEL))) AS MONTH_YEAR,");
                sb.Append("                ',' EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM DUAL ");
                sb.Append("   CONNECT BY LEVEL <= " + No_of_Yrs + " * 12 )");

                if (!string.IsNullOrEmpty(YearPK))
                {
                    sb.Append("   WHERE MONTH_YEAR IN ('" + YearPK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT * FROM ( ");
                sb.Append("SELECT DISTINCT EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE + 365, 'YEAR'), (-LEVEL))) AS YEARPK,");
                sb.Append("                '' EXTRA1,");
                sb.Append("EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE + 365, 'YEAR'), (-LEVEL))) AS MONTH_YEAR,");
                sb.Append("                ',' EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM DUAL ");
                sb.Append("   CONNECT BY LEVEL <= " + No_of_Yrs + " * 12 )");

                if (!string.IsNullOrEmpty(YearPK))
                {
                    sb.Append("   WHERE MONTH_YEAR NOT IN ('" + YearPK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" ORDER BY 3 DESC");
            }
            else
            {
                sb.Append("SELECT * FROM ( ");
                sb.Append("SELECT DISTINCT EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE + 365, 'YEAR'), (-LEVEL))) AS YEARPK,");
                sb.Append("                '' EXTRA1,");
                sb.Append("EXTRACT(YEAR FROM ADD_MONTHS(TRUNC(SYSDATE + 365, 'YEAR'), (-LEVEL))) AS MONTH_YEAR,");
                sb.Append("                ',' EXTRA4,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM DUAL ");
                sb.Append("   CONNECT BY LEVEL <= " + No_of_Yrs + " * 12 )");
                sb.Append(" ORDER BY 3 DESC");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "P&L Year"

        #region "P&L Month"

        public DataSet FetchPLMonth(string MonthPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string selectedYears = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrYear = null;
            MonthPK = MonthPK.TrimEnd(',');
            MonthPK = MonthPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            int j = 0;
            i = TypedData.IndexOf(",");
            arrYear = selectedYears.Split(',');
            if (!string.IsNullOrEmpty(MonthPK))
            {
                sb.Append("SELECT * FROM ( ");
                for (j = 0; j <= arrYear.Length - 1; j++)
                {
                    if (j != 0)
                    {
                        sb.Append(" UNION ");
                    }
                    sb.Append(" SELECT TO_CHAR(ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1),'MON-YYYY') DATESPK,");
                    sb.Append("                '' EXTRA1,");
                    sb.Append(" TO_CHAR(ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1),'MON-YYYY') AS DATES,");
                    sb.Append("                ',' EXTRA4,");
                    sb.Append(" ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1) EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append(" FROM DUAL CONNECT BY LEVEL<=CASE WHEN EXTRACT(YEAR FROM SYSDATE)=" + arrYear.GetValue(j) + " THEN  to_number(to_char(SYSDATE,'mm')) ELSE 12 END");
                }
                sb.Append(")   WHERE DATES IN ('" + MonthPK.ToUpper().Replace(",", "','") + "')");
                sb.Append(" UNION ");
                sb.Append("SELECT * FROM ( ");
                for (j = 0; j <= arrYear.Length - 1; j++)
                {
                    if (j != 0)
                    {
                        sb.Append(" UNION ");
                    }
                    sb.Append(" SELECT TO_CHAR(ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1),'MON-YYYY') DATESPK,");
                    sb.Append("                '' EXTRA1,");
                    sb.Append(" TO_CHAR(ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1),'MON-YYYY') AS DATES,");
                    sb.Append("                ',' EXTRA4,");
                    sb.Append(" ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1) EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append(" FROM DUAL CONNECT BY LEVEL<=CASE WHEN EXTRACT(YEAR FROM SYSDATE)=" + arrYear.GetValue(j) + " THEN  to_number(to_char(SYSDATE,'mm')) ELSE 12 END");
                }
                sb.Append(")   WHERE DATES NOT IN ('" + MonthPK.ToUpper().Replace(",", "','") + "')");
                sb.Append(" ORDER BY 5 DESC");
            }
            else
            {
                sb.Append("SELECT * FROM ( ");
                for (j = 0; j <= arrYear.Length - 1; j++)
                {
                    if (j != 0)
                    {
                        sb.Append(" UNION ");
                    }
                    sb.Append(" SELECT TO_CHAR(ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1),'MON-YYYY') DATESPK,");
                    sb.Append("                '' EXTRA1,");
                    sb.Append(" TO_CHAR(ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1),'MON-YYYY') AS DATES,");
                    sb.Append("                ',' EXTRA4,");
                    sb.Append(" ADD_MONTHS(TO_DATE('01/01/" + arrYear.GetValue(j) + "', 'DD/MM/YYYY'), LEVEL - 1) EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append(" FROM DUAL CONNECT BY LEVEL<=CASE WHEN EXTRACT(YEAR FROM SYSDATE)=" + arrYear.GetValue(j) + " THEN  to_number(to_char(SYSDATE,'mm')) ELSE 12 END");
                }
                sb.Append(") ORDER BY 5 DESC");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "P&L Month"

        #region "FetchPortsGroupPort"

        public DataSet FetchPortsGroupPort_Disable(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string PolID = "", string PolName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT distinct PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                //sb.Append("  AND PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                //sb.Append("   AND PMT.PORT_TYPE  = 2 ")
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + selectedPKs + ")");
                }
                sb.Append("  UNION ");
                sb.Append("  SELECT    distinct      PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                //sb.Append("  AND PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                //sb.Append("   AND PMT.PORT_TYPE = 2 ")
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN (" + selectedPKs + ")");
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT   distinct       PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                //sb.Append("    AND PMT.PORT_MST_PK IN")
                //sb.Append("       (SELECT LWPT.PORT_MST_FK")
                //sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT")
                //sb.Append("         WHERE LWPT.LOCATION_MST_FK = " & LoginPK & ")")
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                //sb.Append("   AND PMT.PORT_TYPE  = 2 ")
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PMT.PORT_MST_PK <> 0");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + selectedPKs + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "FetchPortsGroupPort"

        #region "FetchPortsGroupPort"

        public DataSet FetchPortsGroupPod_Disable(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string PolID = "", string PolName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string ConditionPK1 = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                //sb.Append("   AND PMT.PORT_TYPE  = 2 ")

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + selectedPKs + ")");
                }
                sb.Append("  UNION ");
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                //sb.Append("   AND PMT.PORT_TYPE  = 2 ")

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN (" + selectedPKs + ")");
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                //sb.Append("                PGT.PORT_GRP_TRN_PK,")
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");

                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                //sb.Append("   AND PMT.PORT_TYPE = 2 ")
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PMT.PORT_MST_PK <> 0");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "FetchPortsGroupPort"

        #region "POD Group Search"

        public DataSet FetchAllPodGrp(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string ConditionPK1 = "", string ConditionPK2 = "", string ConditionPK3 = "", string ConditionPK4 = "", string ConditionPK5 = "",
        string ConditionPK6 = "", string TypedData = "", string PolID = "", string PolName = "", string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT DISTINCT PGM.PORT_GRP_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PGM.PORT_GRP_ID,");
                sb.Append("       PGM.PORT_GRP_NAME,");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM port_grp_mst_tbl       PGM");
                sb.Append(" WHERE PGM.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PGM.PORT_GRP_MST_PK IN (" + PolPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND PGM.BIZ_TYPE IN (" + ConditionPK + ")");
                }
                sb.Append(" UNION ");
                sb.Append("SELECT DISTINCT PGM.PORT_GRP_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PGM.PORT_GRP_ID,");
                sb.Append("       PGM.PORT_GRP_NAME,");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM port_grp_mst_tbl       PGM");
                sb.Append(" WHERE PGM.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PGM.PORT_GRP_MST_PK NOT IN (" + PolPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND PGM.BIZ_TYPE IN (" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND PGM.PORT_GRP_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT PGM.PORT_GRP_MST_PK,");
                sb.Append("       '' EMPTY,");
                sb.Append("       PGM.PORT_GRP_ID,");
                sb.Append("       PGM.PORT_GRP_NAME,");
                sb.Append("       '' EXTRA5,");
                sb.Append("       '' EXTRA6,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM port_grp_mst_tbl       PGM");
                sb.Append(" WHERE PGM.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("   AND PGM.PORT_GRP_MST_PK NOT IN (" + PolPK + ")");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND PGM.BIZ_TYPE IN (" + ConditionPK + ")");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("   AND PGM.PORT_GRP_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "POD Group Search"

        #region "FetchPortsGroupPort"

        public DataSet FetchPorts_GrpName(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string PolID = "", string PolName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string ConditionPK1 = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(PolPK))
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                sb.Append("                PGMT.port_grp_id,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                if (LoginPK != 0)
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN");
                    sb.Append("       (SELECT LWPT.PORT_MST_FK");
                    sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT");
                    sb.Append("         WHERE LWPT.LOCATION_MST_FK = " + LoginPK + ")");
                }
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                if (Convert.ToInt32(ConditionPK) == 2)
                {
                    sb.Append("   AND PMT.PORT_TYPE  = 2 ");
                }

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + selectedPKs + ")");
                }
                sb.Append("  UNION ");
                sb.Append(" SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                sb.Append("                PGMT.port_grp_id,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                if (LoginPK != 0)
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN");
                    sb.Append("       (SELECT LWPT.PORT_MST_FK");
                    sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT");
                    sb.Append("         WHERE LWPT.LOCATION_MST_FK = " + LoginPK + ")");
                }
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                sb.Append("   AND PMT.ACTIVE_FLAG = 1 ");

                if (Convert.ToInt32(ConditionPK) == 2)
                {
                    sb.Append("   AND PMT.PORT_TYPE = 2 ");
                }

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                if (!string.IsNullOrEmpty(selectedPKs))
                {
                    sb.Append("  AND PMT.PORT_MST_PK IN (" + selectedPKs + ")");
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT DISTINCT PMT.PORT_MST_PK ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                PMT.PORT_ID ,");
                sb.Append("                PMT.PORT_NAME ,");
                sb.Append("                PGMT.port_grp_id,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM PORT_MST_TBL PMT, PORT_GRP_MST_TBL PGMT,PORT_GRP_TRN_TBL PGT");
                sb.Append("   WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
                sb.Append("    AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
                if (LoginPK != 0)
                {
                    sb.Append("    AND PMT.PORT_MST_PK IN");
                    sb.Append("       (SELECT LWPT.PORT_MST_FK");
                    sb.Append("          FROM LOCATION_WORKING_PORTS_TRN LWPT");
                    sb.Append("         WHERE LWPT.LOCATION_MST_FK = " + LoginPK + ")");
                }
                sb.Append("   AND PMT.BUSINESS_TYPE = " + Convert.ToInt32(ConditionPK));
                if (Convert.ToInt32(ConditionPK) == 2)
                {
                    sb.Append("   AND PMT.PORT_TYPE  = 2 ");
                }
                sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                sb.Append("   AND PMT.PORT_MST_PK <> 0");

                if (!string.IsNullOrEmpty(TradeID))
                {
                    sb.Append("    AND PGT.PORT_GRP_MST_FK IN ('" + TradeID.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolID))
                {
                    sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(PolName))
                {
                    sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("  AND PMT.PORT_MST_PK NOT IN (" + ConditionPK1 + ")");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "FetchPortsGroupPort"

        #region "Fetch Operator"

        public DataSet FetchOperator(string CarrierPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CarrierID = "", string CarrierName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CarrierPK = CarrierPK.TrimEnd(',');
            CarrierPK = CarrierPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (ConditionPK == "1")
            {
                if (!string.IsNullOrEmpty(CarrierPK))
                {
                    sb.Append("SELECT          amt.airline_mst_pk,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                amt.airline_id,");
                    sb.Append("                amt.airline_name,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append("  FROM  airline_mst_tbl amt");
                    sb.Append("   WHERE amt.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND amt.airline_mst_pk IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          amt.airline_mst_pk,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                amt.airline_id,");
                    sb.Append("                amt.airline_name,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM  airline_mst_tbl amt");
                    sb.Append("   WHERE amt.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND amt.airline_mst_pk NOT IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(OPR.airline_id) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(OPR.airline_name) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT          amt.airline_mst_pk,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                amt.airline_id,");
                    sb.Append("                amt.airline_name,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM  airline_mst_tbl amt");
                    sb.Append("   WHERE amt.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(amt.airline_id) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(amt.airline_id) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(amt.airline_name) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
            }
            else if (ConditionPK == "2")
            {
                if (!string.IsNullOrEmpty(CarrierPK))
                {
                    sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                OPR.OPERATOR_ID,");
                    sb.Append("                OPR.OPERATOR_NAME,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append("  FROM  OPERATOR_MST_TBL OPR");
                    sb.Append("   WHERE OPR.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND OPR.OPERATOR_MST_PK IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                OPR.OPERATOR_ID,");
                    sb.Append("                OPR.OPERATOR_NAME,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM OPERATOR_MST_TBL OPR");
                    sb.Append("   WHERE OPR.ACTIVE_FLAG =1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND OPR.OPERATOR_MST_PK NOT IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                OPR.OPERATOR_ID,");
                    sb.Append("                OPR.OPERATOR_NAME,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM OPERATOR_MST_TBL OPR ");
                    sb.Append("   WHERE OPR.ACTIVE_FLAG = 1");

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" ORDER BY 2, 3");
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(CarrierPK))
                {
                    sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                OPR.OPERATOR_ID,");
                    sb.Append("                OPR.OPERATOR_NAME,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append("  FROM  OPERATOR_MST_TBL OPR");
                    sb.Append("   WHERE OPR.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND OPR.OPERATOR_MST_PK IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          amt.airline_mst_pk,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                amt.airline_id,");
                    sb.Append("                amt.airline_name,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append("  FROM  airline_mst_tbl amt");
                    sb.Append("   WHERE amt.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND amt.airline_mst_pk IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                OPR.OPERATOR_ID,");
                    sb.Append("                OPR.OPERATOR_NAME,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM OPERATOR_MST_TBL OPR");
                    sb.Append("   WHERE OPR.ACTIVE_FLAG =1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND OPR.OPERATOR_MST_PK NOT IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          amt.airline_mst_pk,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                amt.airline_id,");
                    sb.Append("                amt.airline_name,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM  airline_mst_tbl amt");
                    sb.Append("   WHERE amt.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(CarrierPK))
                    {
                        sb.Append("   AND amt.airline_mst_pk NOT IN ('" + CarrierPK.ToUpper().Replace(",", "','") + "')");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(OPR.airline_id) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(OPR.airline_name) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT          OPR.OPERATOR_MST_PK,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                OPR.OPERATOR_ID,");
                    sb.Append("                OPR.OPERATOR_NAME,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM OPERATOR_MST_TBL OPR ");
                    sb.Append("   WHERE OPR.ACTIVE_FLAG = 1");

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_ID) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(OPR.OPERATOR_NAME) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          amt.airline_mst_pk,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                amt.airline_id,");
                    sb.Append("                amt.airline_name,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM  airline_mst_tbl amt");
                    sb.Append("   WHERE amt.ACTIVE_FLAG=1");

                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(amt.airline_id) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierID))
                    {
                        sb.Append(" AND UPPER(amt.airline_id) LIKE '%" + CarrierID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(CarrierName))
                    {
                        sb.Append(" AND UPPER(amt.airline_name) LIKE '%" + CarrierName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
            }
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Operator"

        #region "Fetch Shipper"

        public DataSet FetchCustomerByType(string CustomerPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CustomerID = "", string CustomerName = "", string selectedPKs = "",
        string LoginPK = "", bool IsAdmin = false, string CustType = "SHIPPER")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CustomerPK = CustomerPK.TrimEnd(',');
            CustomerPK = CustomerPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");

            if (!string.IsNullOrEmpty(CustomerPK))
            {
                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  CUSTOMER_MST_TBL CMT ");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append("  , CUSTOMER_CONTACT_DTLS CCD ");
                }

                sb.Append(" WHERE CMT.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
                    sb.Append(" AND CCD.ADM_LOCATION_MST_FK IN (" + LoginPK + ") ");
                }

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                sb.Append(" AND CMT.CUSTOMER_MST_PK IN (SELECT CCT.CUSTOMER_MST_FK FROM CUSTOMER_CATEGORY_TRN CCT WHERE CCT.CUSTOMER_CATEGORY_MST_FK IN");
                sb.Append(" (SELECT CCMT.CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL CCMT WHERE 1=1 AND UPPER(CCMT.CUSTOMER_CATEGORY_ID)='" + CustType + "'))");

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL CMT");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append("  , CUSTOMER_CONTACT_DTLS CCD ");
                }

                sb.Append("   WHERE CMT.ACTIVE_FLAG =1");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
                    sb.Append(" AND CCD.ADM_LOCATION_MST_FK IN (" + LoginPK + ") ");
                }

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }

                if (!string.IsNullOrEmpty(CustomerPK))
                {
                    sb.Append("   AND CMT.CUSTOMER_MST_PK  NOT IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" AND CMT.CUSTOMER_MST_PK IN (SELECT CCT.CUSTOMER_MST_FK FROM CUSTOMER_CATEGORY_TRN CCT WHERE CCT.CUSTOMER_CATEGORY_MST_FK IN");
                sb.Append(" (SELECT CCMT.CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL CCMT WHERE 1=1 AND UPPER(CCMT.CUSTOMER_CATEGORY_ID)='" + CustType + "'))");

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          CMT.CUSTOMER_MST_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("                CMT.CUSTOMER_ID,");
                sb.Append("                CMT.CUSTOMER_NAME,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL CMT ");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append("  , CUSTOMER_CONTACT_DTLS CCD ");
                }

                sb.Append("   WHERE CMT.ACTIVE_FLAG = 1");

                if (!string.IsNullOrEmpty(LoginPK) & LoginPK != "0")
                {
                    sb.Append(" AND CMT.CUSTOMER_MST_PK = CCD.CUSTOMER_MST_FK ");
                    sb.Append(" AND CCD.ADM_LOCATION_MST_FK IN (" + LoginPK + ") ");
                }

                if (ConditionPK == "2")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (2,3)");
                }
                else if (ConditionPK == "1")
                {
                    sb.Append("   AND CMT.BUSINESS_TYPE IN (1,3)");
                }
                sb.Append(" AND CMT.CUSTOMER_MST_PK IN (SELECT CCT.CUSTOMER_MST_FK FROM CUSTOMER_CATEGORY_TRN CCT WHERE CCT.CUSTOMER_CATEGORY_MST_FK IN");
                sb.Append(" (SELECT CCMT.CUSTOMER_CATEGORY_MST_PK FROM CUSTOMER_CATEGORY_MST_TBL CCMT WHERE 1=1 AND UPPER(CCMT.CUSTOMER_CATEGORY_ID)='" + CustType + "'))");
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerID))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_ID) LIKE '%" + CustomerID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CustomerName))
                {
                    sb.Append(" AND UPPER(CMT.CUSTOMER_NAME) LIKE '%" + CustomerName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Shipper"

        #region "Fetch POO/PFD"

        public DataSet FetchPOOPFD(string PolPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string PolID = "", string PolName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PolPK = PolPK.TrimEnd(',');
            PolPK = PolPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (ConditionPK == "1")
            {
                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("SELECT          PLC.PLACE_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PLC.PLACE_CODE ,");
                    sb.Append("                PLC.PLACE_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append("  FROM PLACE_MST_TBL PLC");
                    sb.Append("   WHERE PLC.ACTIVE_FLAG = 1");
                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PLC.PLACE_PK IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          PLC.PLACE_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PLC.PLACE_CODE ,");
                    sb.Append("                PLC.PLACE_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM PLACE_MST_TBL PLC WHERE PLC.ACTIVE_FLAG = 1");
                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PLC.PLACE_PK NOT IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }
                    if (!string.IsNullOrEmpty(PolID))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_CODE) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(PolName))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT          PLC.PLACE_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PLC.PLACE_CODE ,");
                    sb.Append("                PLC.PLACE_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM PLACE_MST_TBL PLC WHERE PLC.ACTIVE_FLAG = 1");
                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PLC.PLACE_PK NOT IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }
                    if (!string.IsNullOrEmpty(PolID))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_CODE) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                    }
                    if (!string.IsNullOrEmpty(PolName))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(PolPK))
                {
                    sb.Append("SELECT          PMT.PORT_MST_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PMT.PORT_ID ,");
                    sb.Append("                PMT.PORT_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append("  FROM PORT_MST_TBL PMT");
                    sb.Append("   WHERE PMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND PMT.PORT_TYPE =1");
                    sb.Append("   AND PMT.PORT_MST_PK <> 0");
                    sb.Append("  AND PMT.BUSINESS_TYPE = 2");

                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PMT.PORT_MST_PK IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }
                    sb.Append(" UNION ");

                    sb.Append("SELECT          PLC.PLACE_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PLC.PLACE_CODE ,");
                    sb.Append("                PLC.PLACE_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '1' ACTIVE");
                    sb.Append("  FROM PLACE_MST_TBL PLC");
                    sb.Append("   WHERE PLC.ACTIVE_FLAG = 1");
                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PLC.PLACE_PK IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          PMT.PORT_MST_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PMT.PORT_ID ,");
                    sb.Append("                PMT.PORT_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM PORT_MST_TBL PMT");
                    sb.Append("   WHERE PMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND PMT.PORT_TYPE = 1");
                    sb.Append("   AND PMT.PORT_MST_PK <> 0");
                    sb.Append("  AND PMT.BUSINESS_TYPE = 2");
                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PMT.PORT_MST_PK NOT IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }
                    if (!string.IsNullOrEmpty(PolID))
                    {
                        sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(PolName))
                    {
                        sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" UNION ");

                    sb.Append("SELECT          PLC.PLACE_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PLC.PLACE_CODE ,");
                    sb.Append("                PLC.PLACE_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM PLACE_MST_TBL PLC WHERE PLC.ACTIVE_FLAG = 1");
                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PLC.PLACE_PK NOT IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }
                    if (!string.IsNullOrEmpty(PolID))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_CODE) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(PolName))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
                else
                {
                    sb.Append("SELECT          PMT.PORT_MST_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PMT.PORT_ID ,");
                    sb.Append("                PMT.PORT_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM PORT_MST_TBL PMT");
                    sb.Append("   WHERE 1 = 1");
                    sb.Append("   AND PMT.PORT_TYPE = 1");
                    sb.Append("   AND PMT.ACTIVE_FLAG = 1");
                    sb.Append("   AND PMT.PORT_MST_PK <> 0");
                    sb.Append("   AND PMT.BUSINESS_TYPE = 2");
                    if (!string.IsNullOrEmpty(TypedData))
                    {
                        sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(PolID))
                    {
                        sb.Append(" AND UPPER(PMT.PORT_ID) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                    }

                    if (!string.IsNullOrEmpty(PolName))
                    {
                        sb.Append(" AND UPPER(PMT.PORT_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                    }
                    sb.Append(" UNION ");

                    sb.Append("SELECT          PLC.PLACE_PK ,");
                    sb.Append("                '' EMPTY,");
                    sb.Append("                PLC.PLACE_CODE ,");
                    sb.Append("                PLC.PLACE_NAME ,");
                    sb.Append("                '' EXTRA5,");
                    sb.Append("                '' EXTRA6,");
                    sb.Append("                '' EXTRA7,");
                    sb.Append("                '' EXTRA8,");
                    sb.Append("                '' EXTRA9,");
                    sb.Append("                '0' ACTIVE");
                    sb.Append("  FROM PLACE_MST_TBL PLC WHERE PLC.ACTIVE_FLAG = 1");
                    if (!string.IsNullOrEmpty(PolPK))
                    {
                        sb.Append("   AND PLC.PLACE_PK NOT IN ('" + PolPK.ToUpper().Replace(",", "','") + "')");
                    }
                    if (!string.IsNullOrEmpty(PolID))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_CODE) LIKE '%" + PolID.ToUpper().Replace("'", "''") + "%'");
                    }
                    if (!string.IsNullOrEmpty(PolName))
                    {
                        sb.Append(" AND UPPER(PLC.PLACE_NAME) LIKE '%" + PolName.ToUpper().Replace("'", "''") + "%'");
                    }

                    sb.Append(" ORDER BY 2, 3");
                }
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch POO/PFD"

        #region "Fetch IncoTerms"

        public DataSet FetchIncoTerms(string IncoTermsPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string TermsID = "", string TermsName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            IncoTermsPK = IncoTermsPK.TrimEnd(',');
            IncoTermsPK = IncoTermsPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(IncoTermsPK))
            {
                sb.Append("SELECT          stmt.shipping_terms_mst_pk ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                stmt.inco_code ,");
                sb.Append("                stmt.inco_code_description ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM shipping_terms_mst_tbl stmt");
                sb.Append("   WHERE stmt.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(IncoTermsPK))
                {
                    sb.Append("   AND stmt.shipping_terms_mst_pk IN ('" + IncoTermsPK.ToUpper().Replace(",", "','") + "')");
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          stmt.shipping_terms_mst_pk ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                stmt.inco_code ,");
                sb.Append("                stmt.inco_code_description ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM shipping_terms_mst_tbl stmt");
                sb.Append("   WHERE stmt.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(IncoTermsPK))
                {
                    sb.Append("   AND stmt.shipping_terms_mst_pk NOT IN ('" + IncoTermsPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TermsID))
                {
                    sb.Append(" AND UPPER(stmt.inco_code) LIKE '%" + TermsID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(TermsName))
                {
                    sb.Append(" AND UPPER(stmt.inco_code_description) LIKE '%" + TermsName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          stmt.shipping_terms_mst_pk ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                stmt.inco_code ,");
                sb.Append("                stmt.inco_code_description ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM shipping_terms_mst_tbl stmt");
                sb.Append("   WHERE stmt.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(IncoTermsPK))
                {
                    sb.Append("   AND stmt.shipping_terms_mst_pk NOT IN ('" + IncoTermsPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(TermsID))
                {
                    sb.Append(" AND UPPER(stmt.inco_code) LIKE '%" + TermsID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(TermsName))
                {
                    sb.Append(" AND UPPER(stmt.inco_code_description) LIKE '%" + TermsName.ToUpper().Replace("'", "''") + "%'");
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch IncoTerms"

        #region "Fetch IncoTerms"

        public DataSet FetchCommGrp(string CommGrpPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string CommGrpID = "", string CommGrpName = "", string PortGroup = "",
        string selectedPKs = "", long LoginPK = 0, bool IsAdmin = false, string EnCondition = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CommGrpPK = CommGrpPK.TrimEnd(',');
            CommGrpPK = CommGrpPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            Array arrAnnSearchPks = EnCondition.Split('$');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(CommGrpPK))
            {
                sb.Append("SELECT          cgmt.commodity_group_pk ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                cgmt.commodity_group_code ,");
                sb.Append("                cgmt.commodity_group_desc ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM commodity_group_mst_tbl cgmt");
                sb.Append("   WHERE cgmt.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(CommGrpPK))
                {
                    sb.Append("   AND cgmt.commodity_group_pk IN ('" + CommGrpPK.ToUpper().Replace(",", "','") + "')");
                }

                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("   AND cgmt.commodity_group_pk IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.COMMODITY_GROUP_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.COMMODITY_GROUP_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    'sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T")
                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append("  AND cgmt.commodity_group_pk IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("select  t.COMMODITY_GROUP_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("select  t.COMMODITY_GROUP_MST_FK from VIEW_CUST_CONTRACT_SEA  T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("select t.COMMODITY_GROUP_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("select t.COMMODITY_GROUP_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" UNION ");

                sb.Append("SELECT          cgmt.commodity_group_pk ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                cgmt.commodity_group_code ,");
                sb.Append("                cgmt.commodity_group_desc ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM commodity_group_mst_tbl cgmt");
                sb.Append("   WHERE cgmt.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(CommGrpPK))
                {
                    sb.Append("   AND cgmt.commodity_group_pk NOT IN ('" + CommGrpPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(CommGrpID))
                {
                    sb.Append(" AND UPPER(cgmt.commodity_group_code) LIKE '%" + CommGrpID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CommGrpName))
                {
                    sb.Append(" AND UPPER(cgmt.commodity_group_desc) LIKE '%" + CommGrpName.ToUpper().Replace("'", "''") + "%'");
                }

                //If Convert.ToString(arrAnnSearchPks.GetValue(0)) = "TARIFF" Then
                //    sb.Append("   AND cgmt.commodity_group_pk IN (")
                //    If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) = 1 Then
                //        sb.Append("   select t.COMMODITY_GROUP_FK from VIEW_AIR_RATES_FREIGHT T")
                //    Else
                //        sb.Append("   select t.COMMODITY_GROUP_FK from VIEW_SEA_RATES_FREIGHT T")
                //    End If

                //    'sb.Append("   select t.POLFK from VIEW_AIR_RATES_FREIGHT T")
                //    sb.Append(GetTariff(EnCondition))
                //End If

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append("  AND cgmt.commodity_group_pk IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("select  t.COMMODITY_GROUP_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("select  t.COMMODITY_GROUP_MST_FK from VIEW_CUST_CONTRACT_SEA  T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("select t.COMMODITY_GROUP_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("select t.COMMODITY_GROUP_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT          cgmt.commodity_group_pk ,");
                sb.Append("                '' EMPTY,");
                sb.Append("                cgmt.commodity_group_code ,");
                sb.Append("                cgmt.commodity_group_desc ,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM commodity_group_mst_tbl cgmt");
                sb.Append("   WHERE cgmt.ACTIVE_FLAG = 1");
                if (!string.IsNullOrEmpty(CommGrpPK))
                {
                    sb.Append("   AND cgmt.commodity_group_pk NOT IN ('" + CommGrpPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(CommGrpID))
                {
                    sb.Append(" AND UPPER(cgmt.commodity_group_code) LIKE '%" + CommGrpID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(CommGrpName))
                {
                    sb.Append(" AND UPPER(cgmt.commodity_group_desc) LIKE '%" + CommGrpName.ToUpper().Replace("'", "''") + "%'");
                }

                if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "TARIFF")
                {
                    sb.Append("  AND cgmt.commodity_group_pk IN (");
                    if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("select  t.COMMODITY_GROUP_MST_FK from VIEW_CUST_CONTRACT_AIR T");
                        }
                        else
                        {
                            sb.Append("select  t.COMMODITY_GROUP_MST_FK from VIEW_CUST_CONTRACT_SEA  T");
                        }
                    }
                    else
                    {
                        if (Convert.ToInt32(arrAnnSearchPks.GetValue(1)) == 1)
                        {
                            sb.Append("select t.COMMODITY_GROUP_FK from VIEW_AIR_RATES_FREIGHT T");
                        }
                        else
                        {
                            sb.Append("select t.COMMODITY_GROUP_FK from VIEW_SEA_RATES_FREIGHT T");
                        }
                    }
                    sb.Append(GetTariff(EnCondition));
                }

                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch IncoTerms"

        #region "Get Enhance Search for Announcement"

        private string GetAnnQuery(string AnnSearchPK, Int16 SearchType)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            if (SearchType == 0)
            {
                strsql = "   AND LMT.LOCATION_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.location_fk from announcement_tbl t where t.type_flag=0";
            }
            else if (SearchType == 1)
            {
                strsql = "   AND DMT.DEPARTMENT_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.DEPARTMENT_MST_FK from announcement_tbl t where t.type_flag=0";
            }
            else if (SearchType == 2)
            {
                strsql = "   AND DSMT.DESIGNATION_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.DESIGNATION_MST_FK from announcement_tbl t where t.type_flag=0";
            }
            else if (SearchType == 3)
            {
                strsql = "   AND UMT.USER_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.USERS_MST_FK from announcement_tbl t where t.type_flag=0";
            }
            else if (SearchType == 4)
            {
                strsql = "   AND EMT.EMPLOYEE_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.MANAGEMENT_MST_FK from announcement_tbl t where t.type_flag=0";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))))
            {
                arrPKS = arrAnnSearchPks.GetValue(1).ToString().Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  or instr(t.location_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
            {
                arrPKS = arrAnnSearchPks.GetValue(2).ToString().Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  or instr(t.department_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
            {
                arrPKS = arrAnnSearchPks.GetValue(3).ToString().Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  or instr(t.designation_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
            {
                arrPKS = arrAnnSearchPks.GetValue(3).ToString().Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  or instr(t.users_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
            {
                arrPKS = arrAnnSearchPks.GetValue(5).ToString().Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  or instr(t.management_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
            {
                strsql += "  and to_date(t.valid_from,''dd/mm/yyyy'') >=to_date(''" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + "'',''dd/mm/yyyy'')";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7))))
            {
                strsql += " and to_date(t.valid_to,''dd/mm/yyyy'')<=to_date(''" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "'',''dd/mm/yyyy'')";
            }
            if (Convert.ToInt32(arrAnnSearchPks.GetValue(8)) == 0)
            {
                strsql += " and ((to_date(sysdate,''dd/mm/yyyy'') between VALID_FROM and VALID_TO ) and ( STATUS   = 0 or STATUS  = 4 ))";
            }
            else if (Convert.ToInt32(arrAnnSearchPks.GetValue(8)) == 1)
            {
                strsql += " and (VALID_TO between to_date((add_months(sysdate,-1)-1),''dd/mm/yyyy'') and to_date(sysdate-1,''dd/mm/yyyy'')) and  STATUS  in (0,1,4)";
            }
            else if (Convert.ToInt32(arrAnnSearchPks.GetValue(8)) == 2)
            {
                strsql += " and VALID_TO < to_date((add_months(sysdate,-1)-1),''dd/mm/yyyy'') and STATUS <>3";
            }
            else if (Convert.ToInt32(arrAnnSearchPks.GetValue(8)) == 3)
            {
                strsql += " and STATUS =3";
            }
            else if (Convert.ToInt32(arrAnnSearchPks.GetValue(8)) == 4)
            {
                strsql += " and VALID_FROM > to_date(sysdate,''dd/mm/yyyy'') and STATUS = 4";
            }
            else if (Convert.ToInt32(arrAnnSearchPks.GetValue(8)) == 5)
            {
                strsql += " and STATUS >= 0 AND STATUS<>3 ";
            }
            strsql += "') pk from dual)))";
            return strsql;
        }

        #endregion "Get Enhance Search for Announcement"

        #region "Get Enhance Search for Announcement External"

        private string GetAnnQueryExt(string AnnSearchPK, Int16 SearchType)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            if (SearchType == 0)
            {
                strsql = "   AND LMT.LOCATION_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.Location_Fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 1)
            {
                strsql = "   AND RMT.REGION_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.region_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 2)
            {
                strsql = "   AND TD.TRADE_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.trade_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 3)
            {
                strsql = "   AND PMT.PORT_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.pol_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 4)
            {
                strsql = "   AND AMT.AREA_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.area_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 5)
            {
                strsql = "   AND SM.SECTOR_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.sector_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 6)
            {
                strsql = "   AND CMT.COUNTRY_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.country_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 7)
            {
                strsql = "   AND PMT.PORT_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.pod_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 8)
            {
                strsql = "   AND AMT.AGENT_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.agent_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 9)
            {
                strsql = "   AND CMT.CUSTOMER_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.customer_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 10)
            {
                strsql = "   AND PGM.PORT_GRP_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.portgroup_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            else if (SearchType == 11)
            {
                strsql = "   AND CMT.COMMODITY_MST_PK IN (";
                strsql += "  select * from table(";
                strsql += "  SELECT FN_SPLIT(PK) from(";
                strsql += "  select rowtocol('";
                strsql += "  select t.commodity_mst_fk from announcement_tbl t where t.type_flag=1";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(0)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(0))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(0)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.Location_Fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))))
            {
                arrPKS = arrAnnSearchPks.GetValue(1).ToString().Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.region_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(2)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.trade_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(3)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.pol_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(4)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.area_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(5)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.sector_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }

            if (Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(6)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.country_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(7)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(7)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.pod_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))))
            {
                arrPKS = Convert.ToString(arrAnnSearchPks.GetValue(8)).Split(',');
                strsql += " and (1=1";
                for (i = 0; i <= arrPKS.Length - 1; i++)
                {
                    strsql += "  and instr(t.agent_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                }
                strsql += ")";
            }
            if (arrAnnSearchPks.Length > 9)
            {
                if (arrAnnSearchPks.GetValue(9).ToString() != "0" & !string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString()))
                {
                    arrPKS = arrAnnSearchPks.GetValue(9).ToString().Split(',');
                    strsql += " and (1=1";
                    for (i = 0; i <= arrPKS.Length - 1; i++)
                    {
                        strsql += "  and instr(t.customer_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                    }
                    strsql += ")";
                }
                if (arrAnnSearchPks.GetValue(10).ToString() != "0" & !string.IsNullOrEmpty(arrAnnSearchPks.GetValue(10).ToString()))
                {
                    arrPKS = arrAnnSearchPks.GetValue(10).ToString().Split(',');
                    strsql += " and (1=1";
                    for (i = 0; i <= arrPKS.Length - 1; i++)
                    {
                        strsql += "  and instr(t.portgroup_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                    }
                    strsql += ")";
                }
                if (arrAnnSearchPks.GetValue(11).ToString() != "0" & !string.IsNullOrEmpty(arrAnnSearchPks.GetValue(11).ToString()))
                {
                    arrPKS = arrAnnSearchPks.GetValue(11).ToString().Split(',');
                    strsql += " and (1=1";
                    for (i = 0; i <= arrPKS.Length - 1; i++)
                    {
                        strsql += "  and instr(t.commodity_mst_fk ||'','',''" + arrPKS.GetValue(i) + ",'')>0 ";
                    }
                    strsql += ")";
                }

                if (Convert.ToInt32(arrAnnSearchPks.GetValue(14)) != 5)
                {
                    if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(12).ToString()))
                    {
                        strsql += "  and to_date(t.valid_from_ext,''dd/mm/yyyy'') >=to_date(''" + arrAnnSearchPks.GetValue(12) + "'',''dd/mm/yyyy'')";
                    }
                    //If arrAnnSearchPks(13) <> "" Then
                    //    strsql &= " and to_date(t.valid_to_ext,''dd/mm/yyyy'')<=to_date(''" & arrAnnSearchPks(13) & "'',''dd/mm/yyyy'')"
                    //End If
                }

                if (Convert.ToInt32(arrAnnSearchPks.GetValue(14)) == 0)
                {
                    strsql += " and ((to_date(sysdate,''dd/mm/yyyy'') between to_date(valid_from_ext,''dd/mm/yyyy'') and valid_to_ext ) and ( STATUS   = 0 or STATUS  = 4 ))";
                }
                else if (Convert.ToInt32(arrAnnSearchPks.GetValue(14)) == 1)
                {
                    strsql += " and (valid_to_ext between to_date((add_months(sysdate,-1)-1),''dd/mm/yyyy'') and valid_to_ext(sysdate-1,''dd/mm/yyyy'')) and  STATUS  in (0,1,4)";
                }
                else if (Convert.ToInt32(arrAnnSearchPks.GetValue(14)) == 2)
                {
                    strsql += " and valid_to_ext < to_date((add_months(sysdate,-1)-1),''dd/mm/yyyy'') and STATUS <>3";
                }
                else if (Convert.ToInt32(arrAnnSearchPks.GetValue(14)) == 3)
                {
                    strsql += " and STATUS =3";
                }
                else if (Convert.ToInt32(arrAnnSearchPks.GetValue(14)) == 4)
                {
                    strsql += " and valid_from_ext > to_date(sysdate,''dd/mm/yyyy'') and STATUS = 4";
                }
                else if (Convert.ToInt32(arrAnnSearchPks.GetValue(14)) == 5)
                {
                    strsql += " and STATUS >= 0 AND STATUS<>3 ";
                }
            }
            strsql += "') pk from dual)))";
            return strsql;
        }

        #endregion "Get Enhance Search for Announcement External"

        #region "Get Enhance Search for Customer Stmt of Acc"

        private string GetCustStmt(string AnnSearchPK)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            strsql = "   WHERE 1=1";
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))) & Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0")
            {
                strsql += " and t.business_type=" + Convert.ToInt32(arrAnnSearchPks.GetValue(1));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))) & Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0")
            {
                strsql += " and t.PROCESS_TYPE=" + Convert.ToString(arrAnnSearchPks.GetValue(2));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))) & Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0")
            {
                strsql += " and t.CARGO_TYPE=" + Convert.ToString(arrAnnSearchPks.GetValue(3));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))) & Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0")
            {
                strsql += " and t.COUNTRY_MST_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))) & Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0")
            {
                strsql += " and t.LOCATION_MST_PK in (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))) & Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0")
            {
                strsql += " and t.REF_GROUP_CUST_PK in (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7))) & Convert.ToString(arrAnnSearchPks.GetValue(7)) != "0")
            {
                strsql += " and t.CUSTOMER_MST_PK in (" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + ")";
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "2")
            {
                if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))) & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0")
                {
                    strsql += " and t.VOYAGE_TRN_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + ")";
                }
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "1")
            {
                if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString()) & arrAnnSearchPks.GetValue(9).ToString() != "0")
                {
                    strsql += " and t.VOYAGE_FLIGHT_NO in (" + arrAnnSearchPks.GetValue(9) + ")";
                }
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(10).ToString()) & arrAnnSearchPks.GetValue(10).ToString() != "0")
            {
                strsql += " and t.polfk in (" + arrAnnSearchPks.GetValue(10) + ")";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(11).ToString()) & arrAnnSearchPks.GetValue(11).ToString() != "0")
            {
                strsql += " and t.podfk in (" + arrAnnSearchPks.GetValue(11) + ")";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(12).ToString()) & arrAnnSearchPks.GetValue(12).ToString() != "0")
            {
                strsql += " and t.invoice_date>=to_date('" + arrAnnSearchPks.GetValue(12).ToString() + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(13).ToString()) & arrAnnSearchPks.GetValue(13).ToString() != "0")
            {
                strsql += " and t.invoice_date<=to_date('" + arrAnnSearchPks.GetValue(13).ToString() + "',dateformat)";
            }
            return strsql;
        }

        #endregion "Get Enhance Search for Customer Stmt of Acc"

        #region "Get Enhance Search for Customer Stmt of Acc (By Trans)"

        private string GetCustStmtByTrans(string AnnSearchPK)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            strsql = "   WHERE 1=1";
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))) & Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0")
            {
                strsql += " and t.business_type=" + Convert.ToInt32(arrAnnSearchPks.GetValue(1));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))) & Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0")
            {
                strsql += " and t.PROCESS_TYPE=" + Convert.ToString(arrAnnSearchPks.GetValue(2));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))) & Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0")
            {
                strsql += " and t.CARGO_TYPE=" + Convert.ToString(arrAnnSearchPks.GetValue(3));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
            {
                strsql += " and t.ref_date>=to_date('" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
            {
                strsql += " and t.ref_date<=to_date('" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + "',dateformat)";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))) & Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0")
            {
                strsql += " and t.COUNTRY_MST_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7))) & Convert.ToString(arrAnnSearchPks.GetValue(7)) != "0")
            {
                strsql += " and t.LOCATION_MST_PK in (" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))) & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0")
            {
                strsql += " and t.REF_GROUP_CUST_PK in (" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + ")";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString()) & arrAnnSearchPks.GetValue(9).ToString() != "0")
            {
                strsql += " and t.CUSTOMER_MST_PK in (" + arrAnnSearchPks.GetValue(9) + ")";
            }
            return strsql;
        }

        #endregion "Get Enhance Search for Customer Stmt of Acc (By Trans)"

        #region "Get Enhance Search for Supplier Sales Report"

        private string GetSupSales(string AnnSearchPK)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            strsql = "   WHERE 1=1";
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3" & !string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))) & Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0")
            {
                strsql += " and t.business_type=" + Convert.ToInt32(arrAnnSearchPks.GetValue(1));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))) & Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0")
            {
                strsql += " and t.PROCESS_TYPE=" + Convert.ToString(arrAnnSearchPks.GetValue(2));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
            {
                strsql += " and t.REFDATE>=to_date('" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
            {
                strsql += " and t.REFDATE<=to_date('" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + "',dateformat)";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))) & Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0")
            {
                strsql += " and t.CARGO_TYPE in (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))) & Convert.ToString(arrAnnSearchPks.GetValue(6)) != "-1")
            {
                strsql += " and t.VENDOR_TYPE_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7))) & Convert.ToString(arrAnnSearchPks.GetValue(7)) != "0")
            {
                strsql += " and t.POLFK in (" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))) & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0")
            {
                strsql += " and t.PODFK in (" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + ")";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString()) & arrAnnSearchPks.GetValue(9).ToString() != "0")
            {
                strsql += " and t.VENDOR_MST_PK in (" + arrAnnSearchPks.GetValue(9) + ")";
            }
            if (arrAnnSearchPks.GetValue(10).ToString() == "2")
            {
                strsql += " AND T.INV_SUPPLIER_PK IS  NULL AND T.INV_SUPPLIER_TBL_FK IS  NULL";
            }
            else if (arrAnnSearchPks.GetValue(10).ToString() == "3")
            {
                strsql += " AND T.INV_SUPPLIER_PK IS NOT NULL AND T.INV_SUPPLIER_TBL_FK IS NULL";
            }
            else if (arrAnnSearchPks.GetValue(10).ToString() == "4")
            {
                strsql += " AND T.INV_SUPPLIER_TBL_FK IS NOT NULL";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(11).ToString()))
            {
                strsql += " AND UPPER(T.VESSEL_NAME) ='" + arrAnnSearchPks.GetValue(11).ToString().ToUpper() + "'";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(12).ToString()))
            {
                strsql += " AND UPPER(T.VOYAGE) ='" + arrAnnSearchPks.GetValue(12).ToString().ToUpper() + "'";
            }
            return strsql;
        }

        #endregion "Get Enhance Search for Supplier Sales Report"

        #region "Get Enhance Search for Attachment Enquiry Screen"

        private string GetAttEnq(string AnnSearchPK)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            strsql = "   WHERE 1=1";
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))) & Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0" & Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3")
            {
                strsql += " and t.BIZ_TYPE=" + Convert.ToInt32(arrAnnSearchPks.GetValue(1));
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
            {
                strsql += " and UPPER(t.REFNR) IN('" + Convert.ToString(arrAnnSearchPks.GetValue(2)).ToUpper().Replace(",", "','") + "')";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
            {
                strsql += " and MODIFIED_USER_PK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
            {
                strsql += " and UPPER(t.MODTYPE) IN('" + Convert.ToString(arrAnnSearchPks.GetValue(4)).ToUpper().Replace(",", "','") + "')";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))) & Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0")
            {
                strsql += " and t.MENU_MST_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))) & Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0")
            {
                strsql += " and t.USER_MST_PK in (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))
            {
                strsql += " and TO_DATE(t.DT,'DD/MM/YYYY') >=TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "','DD/MM/YYYY')";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8)).Trim()))
            {
                strsql += " and TO_DATE(t.DT,'DD/MM/YYYY') <=TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + "','DD/MM/YYYY')";
            }

            return strsql;
        }

        #endregion "Get Enhance Search for Attachment Enquiry Screen"

        #region "Get Enhance Search for Qtn Report"

        private string GetQTNRpt(string AnnSearchPK)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            strsql = "   WHERE 1=1";
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))) & Convert.ToString(arrAnnSearchPks.GetValue(1)) != "0")
            {
                strsql += " and t.CUSTOMER_MST_FK=" + Convert.ToInt32(arrAnnSearchPks.GetValue(1));
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))))
            {
                strsql += " and t.DEFAULT_LOCATION_FK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))))
            {
                strsql += " and t.QUOTATION_MST_PK =" + Convert.ToString(arrAnnSearchPks.GetValue(3));
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))))
            {
                strsql += " and t.POLFK =" + Convert.ToString(arrAnnSearchPks.GetValue(4));
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))))
            {
                strsql += " and t.PODFK =" + Convert.ToString(arrAnnSearchPks.GetValue(5));
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))))
            {
                strsql += " and TO_DATE(t.VALID_TO,'DD/MM/YYYY') >=TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + "','DD/MM/YYYY')";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7)).Trim()))
            {
                strsql += " and TO_DATE(t.VALID_TO,'DD/MM/YYYY') <=TO_DATE('" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + "','DD/MM/YYYY')";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))) & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0" & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "3")
            {
                strsql += " and t.BIZ_TYPE =" + Convert.ToString(arrAnnSearchPks.GetValue(8));
            }

            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString()) & arrAnnSearchPks.GetValue(9).ToString() != "0")
            {
                if (Convert.ToString(arrAnnSearchPks.GetValue(8)) == "2")
                {
                    strsql += " and t.CARGO_TYPE =" + arrAnnSearchPks.GetValue(9);
                }
            }
            return strsql;
        }

        #endregion "Get Enhance Search for Qtn Report"

        #region "Get Enhance Search for Pontential vs Actual"

        private string GetPOTACT(string AnnSearchPK)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            strsql = "   WHERE 1=1";
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(1))) & Convert.ToString(arrAnnSearchPks.GetValue(1)) != "3")
            {
                strsql += " and t.BUSINESS_TYPE=" + Convert.ToInt32(arrAnnSearchPks.GetValue(1));
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "2" & Convert.ToString(arrAnnSearchPks.GetValue(2)) == "3")
            {
                strsql += " AND REG.REG_BUSTYPE =2";
            }
            else if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "2" & Convert.ToString(arrAnnSearchPks.GetValue(2)) == "3")
            {
                strsql += " AND REG.REG_BUSTYPE =0";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(3))) & Convert.ToString(arrAnnSearchPks.GetValue(3)) != "0")
            {
                strsql += " and t.CUST_CUSTOMER_MST_FK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(3)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))) & Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0")
            {
                strsql += " and t.POLFK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))) & Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0")
            {
                strsql += " and t.ADM_LOCATION_MST_FK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))) & Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0")
            {
                strsql += " and t.PODFK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(7))) & Convert.ToString(arrAnnSearchPks.GetValue(7)) != "0")
            {
                strsql += " and t.COUNTRY_MST_FK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(7)) + ")";
            }

            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))) & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0")
            {
                strsql += " and t.REGION_MST_FK IN(" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + ")";
            }

            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString().Trim()))
            {
                strsql += " and TO_DATE(t.BOOKING_DATE,'DD/MM/YYYY') >=TO_DATE('" + arrAnnSearchPks.GetValue(9) + "','DD/MM/YYYY')";
            }

            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(10).ToString().Trim()))
            {
                strsql += " and TO_DATE(t.BOOKING_DATE,'DD/MM/YYYY') <=TO_DATE('" + arrAnnSearchPks.GetValue(10) + "','DD/MM/YYYY')";
            }

            return strsql;
        }

        #endregion "Get Enhance Search for Pontential vs Actual"

        #region "Fetch Voyage/FlightNo"

        public DataSet FetchVoyageFlightNo(string FlightPK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string FlightNo = "", string FlightName = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false, string AnnSearchPks = "")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            Array arrAnnSearchPks = AnnSearchPks.Split('$');
            Array SplitData = null;
            FlightPK = FlightPK.TrimEnd(',');
            FlightPK = FlightPK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(TypedData))
            {
                sb.Append("SELECT  DISTINCT  V.VOYAGE_TRN_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("               V.CARRIER_NAME,");
                sb.Append("               V.VOYAGE,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '1' ACTIVE");
                sb.Append("  FROM  VIEW_FRTOUT_VSLVOY V WHERE 1=1");
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append("   AND  V.VOYAGE IN ('" + TypedData.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(FlightPK))
                {
                    sb.Append("   AND  V.VOYAGE_TRN_PK IN (" + FlightPK + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND (V.VOYAGE_TRN_PK,V.VOYAGE) IN (");
                        sb.Append("   select T.VOYAGE_TRN_FK,t.VOYAGE_FLIGHT_NO from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" UNION ");

                sb.Append("SELECT  DISTINCT  V.VOYAGE_TRN_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("               V.CARRIER_NAME,");
                sb.Append("               V.VOYAGE,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  VIEW_FRTOUT_VSLVOY V WHERE 1=1");
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append("   AND  V.VOYAGE NOT IN ('" + TypedData.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(FlightPK))
                {
                    sb.Append("   AND  V.VOYAGE_TRN_PK NOT IN (" + FlightPK + ")");
                }
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND (V.VOYAGE_TRN_PK,V.VOYAGE) IN (");
                        sb.Append("   select T.VOYAGE_TRN_FK,t.VOYAGE_FLIGHT_NO from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT  DISTINCT  V.VOYAGE_TRN_PK,");
                sb.Append("                '' EMPTY,");
                sb.Append("               V.CARRIER_NAME,");
                sb.Append("               V.VOYAGE,");
                sb.Append("                '' EXTRA5,");
                sb.Append("                '' EXTRA6,");
                sb.Append("                '' EXTRA7,");
                sb.Append("                '' EXTRA8,");
                sb.Append("                '' EXTRA9,");
                sb.Append("                '0' ACTIVE");
                sb.Append("  FROM  VIEW_FRTOUT_VSLVOY V WHERE 1=1");
                if (!string.IsNullOrEmpty(AnnSearchPks))
                {
                    if (Convert.ToString(arrAnnSearchPks.GetValue(0)) == "CUSTSTMT")
                    {
                        sb.Append("   AND (V.VOYAGE_TRN_PK,V.VOYAGE) IN (");
                        sb.Append("   select T.VOYAGE_TRN_FK,t.VOYAGE_FLIGHT_NO from VIEW_CUSTSTMT T");
                        sb.Append(GetCustStmt(AnnSearchPks));
                        sb.Append(")");
                    }
                }
                sb.Append(" ORDER BY 2");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Voyage/FlightNo"

        #region "Get Agent SOA Query Condition"

        public string GetExtendedQueryAgentSOA(string Flag, string AdditionalPks)
        {
            StringBuilder sbNew = new StringBuilder();
            try
            {
                string _paramVal = "";
                int _paramIndex = 1;
                while (AdditionalPks.Split('$').Length > _paramIndex)
                {
                    _paramVal = AdditionalPks.Split('$')[_paramIndex];

                    if (_paramIndex == 1 & Convert.ToInt32(_paramVal) > 0)
                    {
                        //'BIZ TYPE
                        sbNew.Append(" AND VAS.BIZ_TYPE IN (" + Convert.ToInt32(_paramVal) + ")");
                    }
                    else if (_paramIndex == 2 & Convert.ToInt32(_paramVal) > 0)
                    {
                        //'PROCESS TYPE
                        sbNew.Append(" AND VAS.PROCESS_TYPE=" + Convert.ToInt32(_paramVal));
                    }
                    else if (_paramIndex == 3 & Convert.ToInt32(_paramVal) > 0)
                    {
                        //'CARGO TYPE
                        sbNew.Append(" AND VAS.CARGO_TYPE=" + Convert.ToInt32(_paramVal));
                    }
                    else if (_paramIndex == 4 & _paramVal.Length > 0 & Flag.ToUpper() != "COUNTRY")
                    {
                        //'COUNTRY FK
                        sbNew.Append(" AND VAS.COUNTRY_MST_FK IN (" + _paramVal + ")");
                    }
                    else if (_paramIndex == 5 & _paramVal.Length > 0 & Flag.ToUpper() != "LOCATION")
                    {
                        //LOCATION FK
                        sbNew.Append(" AND VAS.LOCATION_MST_PK IN (" + _paramVal + ")");
                        //ElseIf _paramIndex = 6 And Val(_paramVal) > 0 Then//NOT REQUIRED
                        //    'CURRENCY FK
                        //    sbNew.Append(" AND VAS.CURRENCY_FK=" & Val(_paramVal))
                    }
                    else if (_paramIndex == 7 & _paramVal.Length > 0 & Flag.ToUpper() != "AGENT")
                    {
                        //AGENT FK
                        sbNew.Append(" AND VAS.AGENT_MST_PK IN (" + _paramVal + ")");
                    }
                    else if (_paramIndex == 8 & _paramVal.Length > 0 & Flag.ToUpper() != "VSLVOY")
                    {
                        //VOYAGE TRN FK/AIRLINE FK
                        sbNew.Append(" AND VAS.VOYAGE_TRN_FK IN (" + _paramVal + ")");
                    }
                    else if (_paramIndex == 9 & _paramVal.Length > 0)
                    {
                        //VOYAGE/FLIGHT
                        //sbNew.Append(" AND VAS.VOY_FLIGHT_NO_JC=" & Val(_paramVal))
                    }
                    else if (_paramIndex == 10 & _paramVal.Length > 0 & Flag.ToUpper() != "CARRIER")
                    {
                        //CARRIER FK
                        sbNew.Append(" AND VAS.CARRIER_FK IN (" + _paramVal + ")");
                    }
                    else if (_paramIndex == 11 & _paramVal.Length > 0 & Flag.ToUpper() != "POL")
                    {
                        //POL FK
                        sbNew.Append(" AND VAS.POL_FK IN (" + _paramVal + ")");
                    }
                    else if (_paramIndex == 12 & _paramVal.Length > 0 & Flag.ToUpper() != "POD")
                    {
                        //POD FK
                        sbNew.Append(" AND VAS.POD_FK IN (" + _paramVal + ")");
                    }
                    else if (_paramIndex == 13 & _paramVal.Length > 0)
                    {
                        //FROM DATE
                        sbNew.Append(" AND TO_DATE(VAS.REF_DATE,DATEFORMAT) >= TO_DATE('" + _paramVal.ToUpper() + "',DATEFORMAT) ");
                    }
                    else if (_paramIndex == 14 & _paramVal.Length > 0)
                    {
                        //TO DATE
                        sbNew.Append(" AND TO_DATE(VAS.REF_DATE,DATEFORMAT) <= TO_DATE('" + _paramVal.ToUpper() + "',DATEFORMAT) ");
                    }

                    _paramIndex += 1;
                }
                if (Flag.ToUpper() == "COUNTRY")
                {
                    return "SELECT DISTINCT VAS.COUNTRY_MST_FK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
                else if (Flag.ToUpper() == "LOCATION")
                {
                    return "SELECT DISTINCT VAS.LOCATION_MST_PK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
                else if (Flag.ToUpper() == "CURRENCY")
                {
                    return "SELECT DISTINCT VAS.REF_CURRENCY_FK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
                else if (Flag.ToUpper() == "AGENT")
                {
                    return "SELECT DISTINCT VAS.AGENT_MST_PK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
                else if (Flag.ToUpper() == "VSLVOY")
                {
                    return "SELECT DISTINCT VAS.VOYAGE_TRN_FK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
                else if (Flag.ToUpper() == "CARRIER")
                {
                    return "SELECT DISTINCT VAS.CARRIER_FK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
                else if (Flag.ToUpper() == "POL")
                {
                    return "SELECT DISTINCT VAS.POL_FK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
                else if (Flag.ToUpper() == "POD")
                {
                    return "SELECT DISTINCT VAS.POD_FK FROM VIEW_AGENT_SOA VAS WHERE 1=1 " + sbNew.ToString();
                }
            }
            catch (Exception ex)
            {
                return "0";
            }
            return "";
        }

        #endregion "Get Agent SOA Query Condition"

        #region "Fetch ApplytoCustomer"

        public DataSet FetchApplytoCustomer(string PK = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, string ASC_OR_DESC = "ASC", string ConditionPK = "", string TradeID = "", string TypedData = "", string ID = "", string NAME = "", string selectedPKs = "",
        long LoginPK = 0, bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            PK = PK.TrimEnd(',');
            PK = PK.TrimStart(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            TypedData = TypedData.TrimStart(',');
            TypedData = TypedData.TrimEnd(',');
            int i = 0;
            i = TypedData.IndexOf(",");
            if (!string.IsNullOrEmpty(PK))
            {
                //sb.Append("  SELECT P.PARTY_MST_PK, P.PARTY_ID, P.PARTY_NAME,'1' CheckBox,TO_CHAR(p.PARTY_TYPE)  PARTY_TYPE ")
                //sb.Append("    FROM VIEW_DOC_MESSAGING_PARTY P  ")

                sb.Append("  SELECT DISTINCT P.PARTY_MST_PK, ");
                sb.Append("               '' Empty,");
                sb.Append("               P.PARTY_ID,");
                sb.Append("               P.PARTY_NAME,");
                sb.Append(" '' Extra5,");
                sb.Append(" '' Extra6,");
                sb.Append(" '' Extra7,");
                sb.Append(" '' Extra8,");
                sb.Append(" '' Extra9,");
                sb.Append("  '1' ACTIVE");
                sb.Append("    FROM VIEW_DOC_MESSAGING_PARTY P  WHERE 1=1  ");

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND P.PARTY_MST_PK NOT IN (SELECT CUST_MST_FK FROM QCOR_MC_M_MESSAGE_SETUP_CUST ");
                    sb.Append("   WHERE MESSAGE_SETUP_MST_FK IN ('" + ConditionPK.ToUpper().Replace(",", "','") + "'))");
                }

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  P.PARTY_MST_PK  IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    // sb.Append("   AND AND P.PARTY_TYPE IN (" & ConditionPK & ")")
                    sb.Append(" AND  P.PARTY_MST_PK IN ('" + ConditionPK.ToUpper().Replace(",", "','") + "')");
                }

                //If PK <> "" Then
                //    sb.Append("   AND  CMT.COMMODITY_MST_PK IN ('" & PK.ToUpper.Replace(",", "','") & "')")
                //End If

                sb.Append(" UNION ALL ");

                sb.Append("  SELECT DISTINCT P.PARTY_MST_PK, ");
                sb.Append("               '' Empty,");
                sb.Append("               P.PARTY_ID,");
                sb.Append("               P.PARTY_NAME,");
                sb.Append(" '' Extra5,");
                sb.Append(" '' Extra6,");
                sb.Append(" '' Extra7,");
                sb.Append(" '' Extra8,");
                sb.Append(" '' Extra9,");
                sb.Append("  '0' ACTIVE");
                sb.Append("    FROM VIEW_DOC_MESSAGING_PARTY P WHERE 1=1  ");

                //sb.Append("   SELECT P.PARTY_MST_PK, P.PARTY_ID, P.PARTY_NAME,'0' CheckBox,TO_CHAR(p.PARTY_TYPE) PARTY_TYPE  ")
                //sb.Append("    FROM VIEW_DOC_MESSAGING_PARTY P  ")

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    sb.Append("   AND P.PARTY_MST_PK NOT IN (SELECT CUST_MST_FK FROM QCOR_MC_M_MESSAGE_SETUP_CUST ");
                    //  sb.Append("   WHERE MESSAGE_SETUP_MST_FK <>  " & ConditionPK & " ) ")
                    sb.Append("   WHERE MESSAGE_SETUP_MST_FK IN ('" + ConditionPK.ToUpper().Replace(",", "','") + "'))");
                }

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    //sb.Append("   AND AND P.PARTY_TYPE IN (" & ConditionPK & ")")
                    sb.Append(" AND  P.PARTY_MST_PK IN ('" + ConditionPK.ToUpper().Replace(",", "','") + "')");
                }

                //sb.Append(" ) Q order by CheckBox DESC,UPPER(Q.PARTY_NAME))QRY ")

                if (!string.IsNullOrEmpty(PK))
                {
                    sb.Append("   AND  P.PARTY_MST_PK NOT IN ('" + PK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(P.PARTY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }

                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(P.PARTY_NAME) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("  SELECT DISTINCT P.PARTY_MST_PK, ");
                sb.Append("               '' Empty,");
                sb.Append("               P.PARTY_ID,");
                sb.Append("               P.PARTY_NAME,");
                sb.Append(" '' Extra5,");
                sb.Append(" '' Extra6,");
                sb.Append(" '' Extra7,");
                sb.Append(" '' Extra8,");
                sb.Append(" '' Extra9,");
                sb.Append("  '0' ACTIVE");
                sb.Append(" FROM VIEW_DOC_MESSAGING_PARTY P WHERE 1=1 ");

                //sb.Append("   SELECT P.PARTY_MST_PK, P.PARTY_ID, P.PARTY_NAME,'0' CheckBox,TO_CHAR(p.PARTY_TYPE) PARTY_TYPE  ")
                //sb.Append("    FROM VIEW_DOC_MESSAGING_PARTY P  ")

                if (!string.IsNullOrEmpty(ConditionPK))
                {
                    //sb.Append("   AND P.PARTY_MST_PK IN(" & ConditionPK & ")")
                    sb.Append(" AND  P.PARTY_MST_PK IN ('" + ConditionPK.ToUpper().Replace(",", "','") + "')");
                }
                if (!string.IsNullOrEmpty(ID))
                {
                    sb.Append(" AND UPPER(P.PARTY_ID) LIKE '%" + ID.ToUpper().Replace("'", "''") + "%'");
                }
                if (!string.IsNullOrEmpty(TypedData))
                {
                    sb.Append(" AND UPPER(P.PARTY_NAME) LIKE '%" + TypedData.ToUpper().Replace("'", "''") + "%'");
                }
                sb.Append(" ORDER BY 2, 3");
            }

            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch ApplytoCustomer"

        #region "Fetch Affialiates"

        public DataSet FetchAffialiates(string CustomerPK = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0, string GroupRefPK = "0", string ConditionPK1 = "0", string ConditionPK2 = "0", string ID = "", string NAME = "", string selectedPKs = "", long LoginPK = 0,
        bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CustomerPK = (string.IsNullOrEmpty(CustomerPK) ? "0" : CustomerPK.TrimEnd(','));
            CustomerPK = CustomerPK.TrimEnd(',');
            CustomerPK = CustomerPK.TrimStart(',');
            if (ConditionPK1 != "0" & !string.IsNullOrEmpty(ConditionPK1))
            {
                sb.Append("SELECT CMT.CUSTOMER_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("       CMT.CUSTOMER_ID,  ");
                sb.Append("       CMT.CUSTOMER_NAME ,");
                sb.Append("       CN.COUNTRY_NAME,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '1' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       COUNTRY_MST_TBL       CN");
                sb.Append("     WHERE CMT.COUNTRY_MST_FK = CN.COUNTRY_MST_PK(+) ");
                sb.Append("     AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
                sb.Append("     AND CCD.ADM_LOCATION_MST_FK  = LMT.LOCATION_MST_PK ");
                sb.Append("     AND CMT.ACTIVE_FLAG = 1");
                sb.Append("     AND CMT.REF_GROUP_CUST_PK IN ('" + GroupRefPK + "')");
                if (ConditionPK1 != "0" & !string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("AND CMT.CUSTOMER_MST_PK IN ( ");
                    sb.Append("SELECT A.CUST_MST_FK");
                    sb.Append("  FROM AFFILIATE_CUSTOMER_DTLS A");
                    sb.Append("   WHERE A.REFERENCE_MST_FK IN ('" + ConditionPK1 + "')");
                    sb.Append("   AND A.CUST_MST_FK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                    sb.Append("   AND A.REFERENCE_TYPE IN ('" + ConditionPK2 + "'))");
                }
                sb.Append(" UNION ");
                sb.Append("SELECT CMT.CUSTOMER_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("       CMT.CUSTOMER_ID,  ");
                sb.Append("       CMT.CUSTOMER_NAME ,");
                sb.Append("       CN.COUNTRY_NAME,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       COUNTRY_MST_TBL       CN");
                sb.Append("     WHERE CMT.COUNTRY_MST_FK = CN.COUNTRY_MST_PK(+) ");
                sb.Append("     AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
                sb.Append("     AND CCD.ADM_LOCATION_MST_FK  = LMT.LOCATION_MST_PK ");
                sb.Append("     AND CMT.ACTIVE_FLAG = 1");
                sb.Append("     AND CMT.REF_GROUP_CUST_PK IN ('" + GroupRefPK + "')");

                if (ConditionPK1 != "0" & !string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("AND CMT.CUSTOMER_MST_PK NOT IN ( ");
                    sb.Append("SELECT A.CUST_MST_FK");
                    sb.Append("  FROM AFFILIATE_CUSTOMER_DTLS A");
                    sb.Append("   WHERE A.REFERENCE_MST_FK IN ('" + ConditionPK1 + "')");
                    sb.Append("   AND A.CUST_MST_FK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                    sb.Append("   AND A.REFERENCE_TYPE IN ('" + ConditionPK2 + "'))");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            else
            {
                sb.Append("SELECT CMT.CUSTOMER_MST_PK, ");
                sb.Append("               '' EMPTY,");
                sb.Append("       CMT.CUSTOMER_ID,  ");
                sb.Append("       CMT.CUSTOMER_NAME ,");
                sb.Append("       CN.COUNTRY_NAME,");
                sb.Append("       LMT.LOCATION_NAME,");
                sb.Append("       '' EXTRA7,");
                sb.Append("       '' EXTRA8,");
                sb.Append("       '' EXTRA9,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL      CMT,");
                sb.Append("       CUSTOMER_CONTACT_DTLS CCD,");
                sb.Append("       LOCATION_MST_TBL      LMT,");
                sb.Append("       COUNTRY_MST_TBL       CN");
                sb.Append("     WHERE CMT.COUNTRY_MST_FK = CN.COUNTRY_MST_PK(+) ");
                sb.Append("     AND CCD.CUSTOMER_MST_FK = CMT.CUSTOMER_MST_PK ");
                sb.Append("     AND CCD.ADM_LOCATION_MST_FK  = LMT.LOCATION_MST_PK ");
                sb.Append("     AND CMT.ACTIVE_FLAG = 1");
                sb.Append("     AND CMT.REF_GROUP_CUST_PK IN ('" + GroupRefPK + "')");
                if (ConditionPK1 != "0" & !string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append("AND CMT.CUSTOMER_MST_PK IN ( ");
                    sb.Append("SELECT A.CUST_MST_FK");
                    sb.Append("  FROM AFFILIATE_CUSTOMER_DTLS A");
                    sb.Append("   WHERE A.REFERENCE_MST_FK IN ('" + ConditionPK1 + "')");
                    sb.Append("   AND A.CUST_MST_FK IN ('" + CustomerPK.ToUpper().Replace(",", "','") + "')");
                    sb.Append("   AND A.REFERENCE_TYPE IN ('" + ConditionPK2 + "'))");
                }
                sb.Append(" ORDER BY 2, 3");
            }
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        public DataSet FetchAffialitesNavigate(string CustomerPK = "0", Int32 CurrentPage = 0, Int32 TotalPage = 0, string GroupRefPK = "0", string ConditionPK1 = "0", string ConditionPK2 = "0", string ID = "", string NAME = "", string selectedPKs = "", long LoginPK = 0,
        bool IsAdmin = false)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            CustomerPK = (string.IsNullOrEmpty(CustomerPK) ? "0" : CustomerPK.TrimEnd(','));
            CustomerPK = CustomerPK.TrimEnd(',');
            CustomerPK = CustomerPK.TrimStart(',');
            if (ConditionPK1 != "0" & !string.IsNullOrEmpty(ConditionPK1))
            {
                sb.Append("SELECT CMT.CUSTOMER_MST_PK, ");
                sb.Append("       CMT.CUSTOMER_ID,  ");
                sb.Append("       CMT.CUSTOMER_NAME ,");
                sb.Append("       '0' ACTIVE");
                sb.Append("  FROM CUSTOMER_MST_TBL      CMT");
                sb.Append("     WHERE CMT.ACTIVE_FLAG = 1");
                sb.Append("     AND (CMT.CUSTOMER_MST_PK IN ('" + GroupRefPK + "')");
                if (ConditionPK1 != "0" & !string.IsNullOrEmpty(ConditionPK1))
                {
                    sb.Append(" OR CMT.CUSTOMER_MST_PK IN ( ");
                    sb.Append("SELECT A.CUST_MST_FK ");
                    sb.Append("  FROM AFFILIATE_CUSTOMER_DTLS A,");
                    sb.Append("  CUSTOMER_MST_TBL C");
                    if (Convert.ToInt32(ConditionPK2) == 5)
                    {
                        sb.Append(" , SRR_SEA_TBL  CT");
                        sb.Append("  WHERE A.REFERENCE_MST_FK = CT.SRR_SEA_PK ");
                    }
                    else if (Convert.ToInt32(ConditionPK2) == 6)
                    {
                        sb.Append(" , SRR_AIR_TBL  CT");
                        sb.Append("  WHERE A.REFERENCE_MST_FK = CT.SRR_AIR_PK ");
                    }
                    else if (Convert.ToInt32(ConditionPK2) == 7 | Convert.ToInt32(ConditionPK2) == 8)
                    {
                        sb.Append(" , QUOTATION_MST_TBL  CT");
                        sb.Append("  WHERE A.REFERENCE_MST_FK = CT.QUOTATION_MST_PK ");
                    }
                    else
                    {
                        sb.Append("  WHERE 1=1 ");
                    }
                    sb.Append("   AND A.CUST_MST_FK= C.CUSTOMER_MST_PK  ");
                    sb.Append("   AND A.REFERENCE_MST_FK IN ('" + ConditionPK1 + "'))");
                }
                sb.Append(")");
            }
            System.Text.StringBuilder strCount = new System.Text.StringBuilder();
            strCount.Append(" SELECT COUNT(*) from  ");
            strCount.Append((" (" + sb.ToString() + ")"));
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(strCount.ToString()));

            TotalPage = TotalRecords / M_MasterPageSize;
            if (TotalRecords % M_MasterPageSize != 0)
            {
                TotalPage += 1;
            }
            if (CurrentPage > TotalPage)
                CurrentPage = 1;
            if (TotalRecords == 0)
                CurrentPage = 0;

            last = CurrentPage * M_MasterPageSize;
            start = (CurrentPage - 1) * M_MasterPageSize + 1;
            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT ROWNUM \"SL.NR\", T.*  FROM ");
            sqlstr.Append("  (" + sb.ToString() + " ");
            sqlstr.Append("  ) T) QRY  WHERE \"SL.NR\"  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sqlstr.ToString());
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

        #endregion "Fetch Affialiates"

        #region "Get Enhance Search for Customer Stmt of Acc"

        private string GetTariff(string AnnSearchPK)
        {
            string strsql = null;
            Array arrAnnSearchPks = AnnSearchPK.Split('$');
            Array arrPKS = null;
            Int16 i = default(Int16);
            strsql = "   WHERE 1=1";
            //If Convert.ToInt32(arrAnnSearchPks.GetValue(1)) <> "3" And Convert.ToInt32(arrAnnSearchPks.GetValue(1)) <> "" And Convert.ToInt32(arrAnnSearchPks.GetValue(1)) <> "0" Then
            //    strsql &= " and t.business_type=" & Convert.ToInt32(arrAnnSearchPks.GetValue(1))
            //End If
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(2))) & Convert.ToString(arrAnnSearchPks.GetValue(2)) != "0")
            {
                strsql += " and t.FROMCOUNTRYPK in (" + Convert.ToString(arrAnnSearchPks.GetValue(2)) + ")";
            }
            //If Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "" And Convert.ToString(arrAnnSearchPks.GetValue(3)) <> "0" Then
            //    strsql &= " and t.CARGO_TYPE=" & Convert.ToString(arrAnnSearchPks.GetValue(3))
            //End If
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(4))) & Convert.ToString(arrAnnSearchPks.GetValue(4)) != "0")
            {
                strsql += " and t.TOCOUNTRYPK in (" + Convert.ToString(arrAnnSearchPks.GetValue(4)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(5))) & Convert.ToString(arrAnnSearchPks.GetValue(5)) != "0")
            {
                strsql += " and t.FREIGHT_ELEMENT_MST_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(5)) + ")";
            }
            if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(6))) & Convert.ToString(arrAnnSearchPks.GetValue(6)) != "0")
            {
                strsql += " and t.CUSTOMER_MST_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(6)) + ")";
            }
            //If Convert.ToString(arrAnnSearchPks.GetValue(7)) <> "" And Convert.ToString(arrAnnSearchPks.GetValue(7)) <> "0" Then
            //    strsql &= " and t.CUSTOMER_MST_PK in (" & Convert.ToString(arrAnnSearchPks.GetValue(7)) & ")"
            //End If
            if (Convert.ToString(arrAnnSearchPks.GetValue(1)) == "1")
            {
                if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))) & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0")
                {
                    strsql += " and t.AIRLINE_MST_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + ")";
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(Convert.ToString(arrAnnSearchPks.GetValue(8))) & Convert.ToString(arrAnnSearchPks.GetValue(8)) != "0")
                {
                    strsql += " and t.OPERATOR_MST_FK in (" + Convert.ToString(arrAnnSearchPks.GetValue(8)) + ")";
                }
            }
            if (Convert.ToString(arrAnnSearchPks.GetValue(7)) == "2")
            {
                if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString()) & arrAnnSearchPks.GetValue(9).ToString() != "0")
                {
                    strsql += " and t.COMMODITY_GROUP_MST_FK in (" + arrAnnSearchPks.GetValue(9).ToString() + ")";
                }
            }
            else
            {
                if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(9).ToString()) & arrAnnSearchPks.GetValue(9).ToString() != "0")
                {
                    strsql += " and t.COMMODITY_GROUP_FK in (" + arrAnnSearchPks.GetValue(9).ToString() + ")";
                }
            }

            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(10).ToString()) & arrAnnSearchPks.GetValue(10).ToString() != "0")
            {
                strsql += " and t.polfk in (" + arrAnnSearchPks.GetValue(10).ToString() + ")";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(11).ToString()) & arrAnnSearchPks.GetValue(11).ToString() != "0")
            {
                strsql += " and t.podfk in (" + arrAnnSearchPks.GetValue(11).ToString() + ")";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(12).ToString()) & arrAnnSearchPks.GetValue(12).ToString() != "0")
            {
                strsql += " and TO_DATE(t.VALID_FROM, DATEFORMAT)>=to_date('" + arrAnnSearchPks.GetValue(12).ToString() + "',dateformat)";
            }
            if (!string.IsNullOrEmpty(arrAnnSearchPks.GetValue(13).ToString()) & arrAnnSearchPks.GetValue(13).ToString() != "0")
            {
                strsql += " and TO_DATE(t.VALID_TO, DATEFORMAT)<=to_date('" + arrAnnSearchPks.GetValue(13).ToString() + "',dateformat)";
            }
            strsql += "  )";
            return strsql;
        }

        #endregion "Get Enhance Search for Customer Stmt of Acc"
    }
}