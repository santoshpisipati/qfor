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
using System;
using System.Collections;
using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace Quantum_QFOR
{
    public class cls_MstTariffGroup : CommonFeatures
    {
        #region "List of Members of Class"

        private Int16 M_TARIFF_GROUP_PK;
        private string M_TARIFF_GROUP_CODE;
        private string M_TARIFF_GROUP_DESC;

        #endregion "List of Members of Class"

        private static DataSet M_DataSet = new DataSet();

        #region "List of Properties"

        public Int16 CommGrp_Pk
        {
            get { return M_TARIFF_GROUP_PK; }
            set { M_TARIFF_GROUP_PK = value; }
        }

        public string CommGrp_Code
        {
            get { return M_TARIFF_GROUP_CODE; }
            set { M_TARIFF_GROUP_CODE = value; }
        }

        public string CommGrp_Desc
        {
            get { return M_TARIFF_GROUP_DESC; }
            set { M_TARIFF_GROUP_DESC = value; }
        }

        public static DataSet MyDataSet
        {
            get { return M_DataSet; }
        }

        #endregion "List of Properties"

        #region "Constructor"

        public cls_MstTariffGroup(bool SelectAll = false)
        {
            string Sql = null;
            if (SelectAll)
            {
                Sql += " select 0 COMMODITY_GROUP_PK,";
                Sql += " '<ALL>' COMMODITY_GROUP_CODE, ";
                Sql += " ' ' COMMODITY_GROUP_DESC, ";
                Sql += " 0 VERSION_NO from dual UNION ";
            }
            Sql += " SELECT CG.COMMODITY_GROUP_PK,CG.COMMODITY_GROUP_CODE, ";
            Sql += " CG.COMMODITY_GROUP_DESC,CG.VERSION_NO ";
            Sql += " FROM COMMODITY_GROUP_MST_TBL CG ";
            Sql += " WHERE CG.ACTIVE_FLAG=1 ";
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

        #region "Fetch Listing"

        public DataSet FetchListing(Int16 P_Tariff_Group_Pk = 0, string P_Tariff_Group_Code = "", string P_Tariff_Group_Desc = "", string SearchType = "", string strColumnName = "", Int32 CurrentPage = 0, Int32 TotalPage = 0, Int16 SortCol = 2, Int16 isActive = 1, bool blnSortAscending = false,
        Int32 flag = 0)
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            string SQLQuery = null;
            string strCondition = null;
            Int32 TotalRecords = default(Int32);
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            if (isActive == 1)
            {
                strCondition += " AND TGM.ACTIVE_FLAG = 1 ";
            }
            else
            {
                //strCondition &= vbCrLf & "  TGM.ACTIVE_FLAG = 0 "
            }
            if (flag == 0)
            {
                strCondition += " AND 1=2 ";
            }
            else
            {
                strCondition += " AND 1=1 ";
            }
            if (P_Tariff_Group_Pk > 0)
            {
                strCondition = strCondition + " AND TGM.TARIFF_GRP_MST_PK = " + P_Tariff_Group_Pk;
            }
            if (P_Tariff_Group_Code.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(TGM.TARIFF_GRP_ID) LIKE '" + P_Tariff_Group_Code.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(TGM.TARIFF_GRP_ID) LIKE '%" + P_Tariff_Group_Code.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(TGM.TARIFF_GRP_ID) LIKE '%" + P_Tariff_Group_Code.ToUpper().Replace("'", "''") + "%'";
                }
            }
            if (P_Tariff_Group_Desc.Trim().Length > 0)
            {
                if (SearchType.ToString().Trim().Length > 0)
                {
                    if (SearchType == "S")
                    {
                        strCondition += " AND UPPER(TGM.TARIFF_GRP_NAME) LIKE '" + P_Tariff_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                    else
                    {
                        strCondition += " AND UPPER(TGM.TARIFF_GRP_NAME) LIKE '%" + P_Tariff_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                    }
                }
                else
                {
                    strCondition += " AND UPPER(TGM.TARIFF_GRP_NAME) LIKE '%" + P_Tariff_Group_Desc.ToUpper().Replace("'", "''") + "%'";
                }
            }
            sb.Append(" SELECT COUNT(*)");
            sb.Append("  FROM TARIFF_GRP_MST_TBL TGM");
            sb.Append("  WHERE 1=1");
            sb.Append("" + strCondition + "");

            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler(sb.ToString()));
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

            sb.Length = 0;
            sb.Append(" SELECT * FROM (SELECT ROWNUM SLNO,Q.* FROM");
            sb.Append(" (SELECT TGM.ACTIVE_FLAG,");
            sb.Append("       TGM.TARIFF_GRP_MST_PK,");
            sb.Append("       TGM.TARIFF_GRP_ID,");
            sb.Append("       TGM.TARIFF_GRP_NAME,");
            //sb.Append("       TGM.BIZ_TYPE,")
            sb.Append("       DECODE(TGM.BIZ_TYPE,1,'AIR',2,'SEA')BIZ_TYPE,");
            sb.Append("       TGM.VERSION_NO,");
            sb.Append("       '' DELFLAG,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM TARIFF_GRP_MST_TBL TGM");
            sb.Append(" WHERE 1=1");
            sb.Append("" + strCondition + "");

            if (!strColumnName.Equals("SLNO"))
            {
                sb.Append(" ORDER BY " + strColumnName);
            }
            if (!blnSortAscending & !strColumnName.Equals("SLNO"))
            {
                sb.Append(" DESC");
            }
            sb.Append(" )Q ) WHERE SLNO  BETWEEN " + start + " AND " + last);
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Listing"

        #region "Fetch Entry Header"

        public DataSet FetchHeader(string TariffMstPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT TGM.TARIFF_GRP_MST_PK,");
            sb.Append("       TGM.TARIFF_GRP_ID,");
            sb.Append("       TGM.TARIFF_GRP_NAME,");
            sb.Append("       TGM.BIZ_TYPE,");
            sb.Append("       TGM.POL_GRP_MST_FK,");
            sb.Append("       POLGRP.PORT_GRP_ID POL_GRP_ID,");
            sb.Append("       POLGRP.PORT_GRP_NAME POL_GRP_NAME,");
            sb.Append("       TGM.POD_GRP_MST_FK,");
            sb.Append("       PODGRP.PORT_GRP_ID POD_GRP_ID,");
            sb.Append("       PODGRP.PORT_GRP_NAME POD_GRP_NAME,");
            sb.Append("       TGM.ACTIVE_FLAG,");
            sb.Append("       TGM.VERSION_NO");
            sb.Append("  FROM TARIFF_GRP_MST_TBL TGM,");
            sb.Append("       PORT_GRP_MST_TBL   POLGRP,");
            sb.Append("       PORT_GRP_MST_TBL   PODGRP");
            sb.Append(" WHERE TGM.POL_GRP_MST_FK = POLGRP.PORT_GRP_MST_PK");
            sb.Append("   AND TGM.POD_GRP_MST_FK = PODGRP.PORT_GRP_MST_PK");
            sb.Append("  AND TGM.TARIFF_GRP_MST_PK IN (" + TariffMstPK + ")");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Entry Header"

        #region "FetchAllPortPair"

        public DataSet FetchAllPortPair(string POLGrpPK = "0", string PODGrpPK = "0", string BizType = "0", int CheckFlag = 0, string TariffMstPK = "0", Int32 CurrentPage = -1, Int32 TotalPage = -1)
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            System.Text.StringBuilder sbCondition = new System.Text.StringBuilder(5000);
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);

            sb.Append("SELECT ");
            sb.Append("       QRY.SELFLAG,");
            sb.Append("       QRY.TARIFF_GRP_TRN_PK,");
            sb.Append("       QRY.PORT_PAIR_ID,");
            sb.Append("       QRY.POL_PK,");
            sb.Append("       QRY.POL_NAME,");
            sb.Append("       QRY.POD_PK,");
            sb.Append("       QRY.POD_NAME,");
            sb.Append("       QRY.CHGFLAG ");
            sb.Append(" FROM (SELECT ROWNUM SLNO,'' SELFLAG,");
            sb.Append("         '0' TARIFF_GRP_TRN_PK,");
            sb.Append("         POL.PORT_ID || '-' || POD.PORT_ID PORT_PAIR_ID,");
            sb.Append("         POL.PORT_MST_PK POL_PK,");
            sb.Append("         POL.PORT_NAME POL_NAME,");
            sb.Append("         POD.PORT_MST_PK POD_PK,");
            sb.Append("         POD.PORT_NAME POD_NAME,");
            sb.Append("         '' CHGFLAG");

            sbCondition.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD");
            sbCondition.Append("     WHERE POL.PORT_MST_PK IN");
            sbCondition.Append("       (SELECT POL.PORT_MST_PK");
            sbCondition.Append("          FROM PORT_GRP_TRN_TBL POLTRN, PORT_MST_TBL POL");
            sbCondition.Append("         WHERE POLTRN.PORT_MST_FK = POL.PORT_MST_PK");
            sbCondition.Append("           AND POLTRN.PORT_GRP_MST_FK IN ( " + POLGrpPK + " ) )");
            sbCondition.Append("      ");
            sbCondition.Append("   AND POD.PORT_MST_PK IN");
            sbCondition.Append("       (SELECT POD.PORT_MST_PK");
            sbCondition.Append("          FROM PORT_GRP_TRN_TBL PODTRN, PORT_MST_TBL POD");
            sbCondition.Append("         WHERE PODTRN.PORT_MST_FK = POD.PORT_MST_PK");
            sbCondition.Append("           AND PODTRN.PORT_GRP_MST_FK IN( " + PODGrpPK + " ) )");
            if (TariffMstPK != "0")
            {
                sbCondition.Append("       AND (POL.PORT_MST_PK, POD.PORT_MST_PK) NOT IN");
                sbCondition.Append("       (SELECT T.POL_MST_FK, T.POD_MST_FK FROM TARIFF_GRP_TRN_TBL T");
                sbCondition.Append("           WHERE T.TARIFF_GRP_MST_FK = " + TariffMstPK);
                sbCondition.Append("        )");
            }
            sbCondition.Append(" AND POL.BUSINESS_TYPE =" + BizType);
            sbCondition.Append(" AND POD.BUSINESS_TYPE =" + BizType);
            if (CheckFlag != 0)
            {
                sbCondition.Append(" AND 1 = 2");
            }
            TotalRecords = Convert.ToInt32(objWF.ExecuteScaler("Select count(*) " + sbCondition.ToString()));
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

            sb.Append(sbCondition);
            sb.Append(" ) QRY");
            sb.Append(" WHERE QRY.SLNO  BETWEEN " + start + " AND " + last);
            sb.Append("  ORDER BY QRY.PORT_PAIR_ID");

            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "FetchAllPortPair"

        #region "Fetch Grid Details"

        public DataSet FetchDetails(string txtTariffMstPK = "0", string POLGrpPK = "0", string PODGrpPK = "0", string BizType = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append(" SELECT * FROM (");
            sb.Append("  SELECT PGT.PORT_GRP_TRN_PK,");
            sb.Append("       '0' DELFLAG,");
            sb.Append("       CMT.COUNTRY_MST_PK,");
            sb.Append("       CMT.COUNTRY_ID,");
            sb.Append("       CMT.COUNTRY_NAME,");
            sb.Append("       PMT.PORT_MST_PK PORT_MST_FK,");
            sb.Append("       PMT.PORT_ID,");
            sb.Append("       PMT.PORT_NAME,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM PORT_GRP_MST_TBL PGMT,");
            sb.Append("       PORT_GRP_TRN_TBL PGT,");
            sb.Append("       COUNTRY_MST_TBL  CMT,");
            sb.Append("       PORT_MST_TBL     PMT");
            sb.Append(" WHERE PGMT.PORT_GRP_MST_PK = PGT.PORT_GRP_MST_FK");
            sb.Append("   AND PGT.PORT_MST_FK = PMT.PORT_MST_PK");
            sb.Append("   AND PMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("   AND PGMT.PORT_GRP_MST_PK IN (" + txtTariffMstPK + ")");
            sb.Append(" UNION ");
            sb.Append(" SELECT 0 PORT_GRP_TRN_PK,");
            sb.Append("       '0' DELFLAG,");
            sb.Append("       CMT.COUNTRY_MST_PK,");
            sb.Append("       CMT.COUNTRY_ID,");
            sb.Append("       CMT.COUNTRY_NAME,");
            sb.Append("       PMT.PORT_MST_PK PORT_MST_FK,");
            sb.Append("       PMT.PORT_ID,");
            sb.Append("       PMT.PORT_NAME,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM COUNTRY_MST_TBL CMT, PORT_MST_TBL PMT");
            sb.Append(" WHERE PMT.COUNTRY_MST_FK = CMT.COUNTRY_MST_PK");
            sb.Append("   AND PMT.PORT_MST_PK IN (" + txtTariffMstPK + ")");
            sb.Append(" )ORDER BY COUNTRY_NAME, PORT_ID");

            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Grid Details"

        #region "Fetch Selected Port Grid"

        public DataSet FetchSelPortGrid(string TariffMstPK = "0", string POLPK = "(0,0)", string PODPK = "0", string BizType = "0", string TrnGrpPK = "0")
        {
            WorkFlow objWF = new WorkFlow();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);

            sb.Append("SELECT * FROM (");
            sb.Append(" SELECT '' SELFLAG,");
            sb.Append("       TGT.TARIFF_GRP_TRN_PK,");
            sb.Append("       POL.PORT_ID || '-' || POD.PORT_ID PORT_PAIR_ID,");
            sb.Append("       POL.PORT_MST_PK POL_PK,");
            sb.Append("       POL.PORT_NAME POL_NAME,");
            sb.Append("       POD.PORT_MST_PK POD_PK,");
            sb.Append("       POD.PORT_NAME POD_NAME,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM TARIFF_GRP_TRN_TBL TGT, PORT_MST_TBL POL, PORT_MST_TBL POD");
            sb.Append(" WHERE TGT.POL_MST_FK = POL.PORT_MST_PK");
            sb.Append("   AND TGT.POD_MST_FK = POD.PORT_MST_PK");
            sb.Append("   AND TGT.TARIFF_GRP_MST_FK = " + TariffMstPK);
            sb.Append("   AND POL.BUSINESS_TYPE = " + BizType);
            sb.Append("   AND POD.BUSINESS_TYPE = " + BizType);
            //sb.Append("   AND TGT.TARIFF_GRP_TRN_PK NOT IN (" & TrnGrpPK & " )")
            sb.Append(" UNION ");
            sb.Append(" SELECT '' SELFLAG,");
            sb.Append("       0 TARIFF_GRP_TRN_PK,");
            sb.Append("       POL.PORT_ID || '-' || POD.PORT_ID PORT_PAIR_ID,");
            sb.Append("       POL.PORT_MST_PK POL_PK,");
            sb.Append("       POL.PORT_NAME POL_NAME,");
            sb.Append("       POD.PORT_MST_PK POD_PK,");
            sb.Append("       POD.PORT_NAME POD_NAME,");
            sb.Append("       '' CHGFLAG");
            sb.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD");
            if (TariffMstPK != "0")
            {
                sb.Append("   ,TARIFF_GRP_TRN_TBL TGL, TARIFF_GRP_TRN_TBL TGD");
            }
            sb.Append(" WHERE (POL.PORT_MST_PK, POD.PORT_MST_PK) IN (" + POLPK + " )");
            if (TariffMstPK != "0")
            {
                sb.Append("  AND (TGL.POL_MST_FK,TGD.POD_MST_FK) NOT IN (" + POLPK + " )");
                sb.Append("  AND TGL.TARIFF_GRP_MST_FK = " + TariffMstPK);
                sb.Append("  AND TGD.TARIFF_GRP_MST_FK = " + TariffMstPK);
            }

            //'sb.Append(" AND POL.PORT_MST_PK = TGT.POL_MST_FK")
            //'sb.Append("  AND POD.PORT_MST_PK = TGT.POD_MST_FK")
            //'sb.Append("  AND TGT.TARIFF_GRP_MST_FK NOT IN")
            //'sb.Append(" (SELECT TGT.TARIFF_GRP_TRN_PK")
            //'sb.Append("  FROM TARIFF_GRP_TRN_TBL TGT")
            ///sb.Append("  WHERE TGT.TARIFF_GRP_TRN_PK IN (" & TrnGrpPK & " )")
            ///sb.Append("  AND (TGT.POL_MST_FK, TGT.POD_MST_FK) IN")
            //'sb.Append("  WHERE ")
            //'sb.Append("  (TGT.POL_MST_FK, TGT.POD_MST_FK) IN")
            //'sb.Append("   (" & POLPK & " ))")
            sb.Append("  ");
            sb.Append(" AND POL.BUSINESS_TYPE =" + BizType);
            sb.Append(" AND POD.BUSINESS_TYPE =" + BizType);
            sb.Append("  )ORDER BY PORT_PAIR_ID");
            try
            {
                return objWF.GetDataSet(sb.ToString());
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

        #endregion "Fetch Selected Port Grid"

        #region "Save Function"

        public ArrayList save(DataSet M_DataSet, DataSet GridDS)
        {
            WorkFlow objWK = new WorkFlow();
            objWK.OpenConnection();
            OracleTransaction TRAN = null;
            TRAN = objWK.MyConnection.BeginTransaction();
            int TariffGrpPK = 0;
            long lngI = 0;
            Int32 RecAfct = default(Int32);
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            OracleCommand delCommand = new OracleCommand();

            try
            {
                DataTable DtTbl = new DataTable();
                DataRow DtRw = null;
                int i = 0;
                DtTbl = M_DataSet.Tables[0];
                var _with1 = insCommand;
                _with1.Connection = objWK.MyConnection;
                _with1.CommandType = CommandType.StoredProcedure;
                _with1.CommandText = objWK.MyUserName + ".TARIFF_GRP_MST_TBL_PKG.TARIFF_GRP_MST_TBL_INS";
                var _with2 = _with1.Parameters;
                _with2.Add("TARIFF_GRP_ID_IN", M_DataSet.Tables[0].Rows[0]["TARIFF_GRP_ID"]).Direction = ParameterDirection.Input;
                _with2.Add("TARIFF_GRP_NAME_IN", M_DataSet.Tables[0].Rows[0]["TARIFF_GRP_NAME"]).Direction = ParameterDirection.Input;
                _with2.Add("POL_GRP_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["POL_GRP_MST_FK"]).Direction = ParameterDirection.Input;
                _with2.Add("POD_GRP_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["POD_GRP_MST_FK"]).Direction = ParameterDirection.Input;
                _with2.Add("BIZ_TYPE_IN", M_DataSet.Tables[0].Rows[0]["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                _with2.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[0]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
                _with2.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                _with2.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with2.Add("RETURN_VALUE", OracleDbType.Int32, 10, "TARIFF_GRP_MST_PK").Direction = ParameterDirection.Output;

                var _with3 = updCommand;
                _with3.Connection = objWK.MyConnection;
                _with3.CommandType = CommandType.StoredProcedure;
                _with3.CommandText = objWK.MyUserName + ".TARIFF_GRP_MST_TBL_PKG.TARIFF_GRP_MST_TBL_UPD";
                var _with4 = _with3.Parameters;
                _with4.Add("TARIFF_GRP_MST_PK_IN", M_DataSet.Tables[0].Rows[0]["TARIFF_GRP_MST_PK"]).Direction = ParameterDirection.Input;
                _with4.Add("TARIFF_GRP_ID_IN", M_DataSet.Tables[0].Rows[0]["TARIFF_GRP_ID"]).Direction = ParameterDirection.Input;
                _with4.Add("TARIFF_GRP_NAME_IN", M_DataSet.Tables[0].Rows[0]["TARIFF_GRP_NAME"]).Direction = ParameterDirection.Input;
                _with4.Add("POL_GRP_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["POL_GRP_MST_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("POD_GRP_MST_FK_IN", M_DataSet.Tables[0].Rows[0]["POD_GRP_MST_FK"]).Direction = ParameterDirection.Input;
                _with4.Add("BIZ_TYPE_IN", M_DataSet.Tables[0].Rows[0]["BIZ_TYPE"]).Direction = ParameterDirection.Input;
                _with4.Add("ACTIVE_FLAG_IN", M_DataSet.Tables[0].Rows[0]["ACTIVE_FLAG"]).Direction = ParameterDirection.Input;
                _with4.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                _with4.Add("VERSION_NO_IN", M_DataSet.Tables[0].Rows[0]["VERSION_NO"]).Direction = ParameterDirection.Input;
                _with4.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                _with4.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                var _with5 = objWK.MyDataAdapter;
                if (string.IsNullOrEmpty(M_DataSet.Tables[0].Rows[0]["TARIFF_GRP_MST_PK"].ToString()))
                {
                    _with5.InsertCommand = insCommand;
                    _with5.InsertCommand.Transaction = TRAN;
                    RecAfct = _with5.InsertCommand.ExecuteNonQuery();
                    TariffGrpPK = Convert.ToInt32(insCommand.Parameters["RETURN_VALUE"].Value);
                }
                else
                {
                    _with5.UpdateCommand = updCommand;
                    _with5.UpdateCommand.Transaction = TRAN;
                    RecAfct = _with5.UpdateCommand.ExecuteNonQuery();
                    TariffGrpPK = Convert.ToInt32(updCommand.Parameters["RETURN_VALUE"].Value);
                }

                if (RecAfct > 0)
                {
                    arrMessage = (ArrayList)SaveTariffGroupGrid(TariffGrpPK, GridDS, TRAN);
                    if (arrMessage.Count == 0)
                    {
                        TRAN.Commit();
                        arrMessage.Add("All Data Saved Successfully");
                        arrMessage.Add(TariffGrpPK);
                        return arrMessage;
                    }
                    else
                    {
                        TRAN.Rollback();
                        return arrMessage;
                    }
                }
                else
                {
                    arrMessage.Add("Error");
                    TRAN.Rollback();
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                objWK.CloseConnection();
            }
        }

        #endregion "Save Function"

        #region "Save Tariff Group Grid"

        public object SaveTariffGroupGrid(int TariffGrpPK = 0, DataSet dsGrid = null, OracleTransaction TRAN = null)
        {
            Int32 RecAfct = default(Int32);
            Int32 i = default(Int32);
            WorkFlow objWK = new WorkFlow();
            OracleCommand insCommand = new OracleCommand();
            OracleCommand updCommand = new OracleCommand();
            objWK.MyConnection = TRAN.Connection;
            try
            {
                if (dsGrid.Tables[0].Rows.Count > 0)
                {
                    for (i = 0; i <= dsGrid.Tables[0].Rows.Count - 1; i++)
                    {
                        var _with6 = insCommand;
                        insCommand.Parameters.Clear();
                        _with6.Connection = objWK.MyConnection;
                        _with6.CommandType = CommandType.StoredProcedure;
                        _with6.CommandText = objWK.MyUserName + ".TARIFF_GRP_TRN_TBL_PKG.TARIFF_GRP_TRN_TBL_INS";
                        var _with7 = _with6.Parameters;
                        _with7.Add("TARIFF_GRP_MST_FK_IN", TariffGrpPK).Direction = ParameterDirection.Input;
                        _with7.Add("POL_MST_FK_IN", dsGrid.Tables[0].Rows[i]["POL_PK"]).Direction = ParameterDirection.Input;
                        _with7.Add("POD_MST_FK_IN", dsGrid.Tables[0].Rows[i]["POD_PK"]).Direction = ParameterDirection.Input;
                        _with7.Add("CREATED_BY_FK_IN", M_CREATED_BY_FK).Direction = ParameterDirection.Input;
                        _with7.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                        _with7.Add("RETURN_VALUE", OracleDbType.Int32, 10, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        var _with8 = updCommand;
                        updCommand.Parameters.Clear();
                        _with8.Connection = objWK.MyConnection;
                        _with8.CommandType = CommandType.StoredProcedure;
                        _with8.CommandText = objWK.MyUserName + ".TARIFF_GRP_TRN_TBL_PKG.TARIFF_GRP_TRN_TBL_UPD";
                        var _with9 = _with8.Parameters;
                        _with9.Add("TARIFF_GRP_TRN_PK_IN", dsGrid.Tables[0].Rows[i]["TARIFF_GRP_TRN_PK"]).Direction = ParameterDirection.Input;
                        _with9.Add("TARIFF_GRP_MST_FK_IN", TariffGrpPK).Direction = ParameterDirection.Input;
                        _with9.Add("POL_MST_FK_IN", dsGrid.Tables[0].Rows[i]["POL_PK"]).Direction = ParameterDirection.Input;
                        _with9.Add("POD_MST_FK_IN", dsGrid.Tables[0].Rows[i]["POD_PK"]).Direction = ParameterDirection.Input;
                        _with9.Add("LAST_MODIFIED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                        _with9.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                        _with9.Add("RETURN_VALUE", OracleDbType.Int32, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;

                        var _with10 = objWK.MyDataAdapter;
                        if (Convert.ToInt32(dsGrid.Tables[0].Rows[i]["TARIFF_GRP_TRN_PK"]) == 0)
                        {
                            _with10.InsertCommand = insCommand;
                            _with10.InsertCommand.Transaction = TRAN;
                            RecAfct = _with10.InsertCommand.ExecuteNonQuery();
                        }
                        else
                        {
                            _with10.UpdateCommand = updCommand;
                            _with10.UpdateCommand.Transaction = TRAN;
                            RecAfct = _with10.UpdateCommand.ExecuteNonQuery();
                        }
                    }
                }
                return arrMessage;
            }
            catch (OracleException oraEx)
            {
                arrMessage.Add(oraEx.Message);
                return arrMessage;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Save Tariff Group Grid"

        #region "DeleteSavedTariffPortPair"

        public ArrayList DeleteSavedTariffPortPair(ArrayList DeletedRow, int TariffMstPK)
        {
            WorkFlow objWK = new WorkFlow();
            OracleTransaction oraTran = null;
            OracleCommand delCommand = new OracleCommand();
            string strReturn = null;
            string[] arrRowDetail = null;
            Int32 i = default(Int32);
            try
            {
                objWK.OpenConnection();
                for (i = 0; i <= DeletedRow.Count - 1; i++)
                {
                    oraTran = objWK.MyConnection.BeginTransaction();
                    var _with11 = objWK.MyCommand;
                    _with11.Transaction = oraTran;
                    _with11.Connection = objWK.MyConnection;
                    _with11.CommandType = CommandType.StoredProcedure;

                    _with11.CommandText = objWK.MyUserName + ".TARIFF_GRP_TRN_TBL_PKG.TARIFF_GRP_TRN_TBL_DEL";
                    arrRowDetail = Convert.ToString(DeletedRow[i]).Split(',');
                    _with11.Parameters.Clear();
                    var _with12 = _with11.Parameters;
                    //.Add("TARIFF_GRP_TRN_PK_IN", CType(TariffMstPK, Integer)).Direction = ParameterDirection.Input
                    _with12.Add("TARIFF_GRP_TRN_PK_IN", Convert.ToInt64(arrRowDetail[0])).Direction = ParameterDirection.Input;
                    _with12.Add("DELETED_BY_FK_IN", M_LAST_MODIFIED_BY_FK).Direction = ParameterDirection.Input;
                    _with12.Add("CONFIG_MST_FK_IN", M_Configuration_PK).Direction = ParameterDirection.Input;
                    _with12.Add("RETURN_VALUE", OracleDbType.Varchar2, 50, "RETURN_VALUE").Direction = ParameterDirection.Output;
                    _with11.Parameters["RETURN_VALUE"].SourceVersion = DataRowVersion.Current;
                    try
                    {
                        if (Convert.ToInt32(_with11.ExecuteNonQuery()) > 0)
                        {
                            oraTran.Commit();
                        }
                        else
                        {
                            arrMessage.Add("Child Record Found, Record(s)cannot be deleted");
                            oraTran.Rollback();
                        }
                    }
                    catch (Exception ex)
                    {
                        arrMessage.Add("Child Record Found, Record(s)cannot be deleted");
                        oraTran.Rollback();
                    }
                }
                if (arrMessage.Count > 0)
                {
                    return arrMessage;
                }
                else
                {
                    arrMessage.Add("Success");
                    return arrMessage;
                }
            }
            catch (OracleException oraexp)
            {
                throw oraexp;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (objWK.MyConnection.State == ConnectionState.Open)
                {
                    objWK.MyConnection.Close();
                }
            }
        }

        #endregion "DeleteSavedTariffPortPair"

        #region "FetchNavigationSector"

        public DataSet FetchNavigationSector()
        {
            DataSet objds = new DataSet();
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = " SELECT R.REGION_MST_PK RPK, R.REGION_CODE, R.REGION_NAME REGION_NAME FROM REGION_MST_TBL R WHERE R.ACTIVE_FLAG=1  ORDER BY R.REGION_NAME";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[0].TableName = "REGION";

                str = " SELECT A.AREA_MST_PK APK, A.AREA_ID, A.AREA_NAME AREA_NAME, A.REGION_MST_FK RFK FROM AREA_MST_TBL A WHERE A.ACTIVE_FLAG = 1 ORDER BY A.AREA_NAME";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[1].TableName = "AREA";

                DataRelation objRel1 = new DataRelation("REL_REG_AREA", objds.Tables[0].Columns["RPK"], objds.Tables[1].Columns["RFK"]);

                objRel1.Nested = true;
                objds.Relations.Add(objRel1);
                return objds;
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

        #endregion "FetchNavigationSector"

        #region "FetchNavigationPorts"

        public DataSet FetchNavigationPorts(string RegionPK = "0", string AreaPK = "0", string BizType = "0")
        {
            DataSet objds = new DataSet();
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = " SELECT A.AREA_MST_PK APK, A.AREA_ID, A.AREA_NAME AREA_NAME FROM AREA_MST_TBL A WHERE A.ACTIVE_FLAG = 1  AND A.AREA_MST_PK IN (" + AreaPK + ") ORDER BY A.AREA_NAME";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[0].TableName = "AREA";

                str = " SELECT C.COUNTRY_MST_PK CPK,  C.COUNTRY_ID, C.COUNTRY_NAME COUNTRY_NAME,  C.AREA_MST_FK AFK FROM COUNTRY_MST_TBL C WHERE C.ACTIVE_FLAG = 1  AND C.AREA_MST_FK IN(" + AreaPK + ") ORDER BY C.COUNTRY_NAME ";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[1].TableName = "COUNTRY";

                str = "SELECT DISTINCT P.PORT_MST_PK PPK, P.PORT_ID, P.PORT_NAME PORT_NAME, P.COUNTRY_MST_FK CFK FROM PORT_MST_TBL P,COUNTRY_MST_TBL C WHERE P.ACTIVE_FLAG = 1 AND P.COUNTRY_MST_FK = C.COUNTRY_MST_PK  AND C.AREA_MST_FK IN (" + AreaPK + ")";
                if (BizType != "0")
                {
                    str += " AND P.BUSINESS_TYPE = " + BizType;
                }
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[2].TableName = "PORT";

                DataRelation objRel1 = new DataRelation("REL_AREA_CON", objds.Tables[0].Columns["APK"], objds.Tables[1].Columns["AFK"]);
                DataRelation objRel2 = new DataRelation("REL_CON_LOC", objds.Tables[1].Columns["CPK"], objds.Tables[2].Columns["CFK"]);

                objRel1.Nested = true;
                objRel2.Nested = true;

                objds.Relations.Add(objRel1);
                objds.Relations.Add(objRel2);

                return objds;
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

        #endregion "FetchNavigationPorts"

        #region "FetchNavigationForLocation"

        public DataSet FetchNavigationForLocation()
        {
            DataSet objds = new DataSet();
            string str = null;
            WorkFlow objWF = new WorkFlow();
            try
            {
                str = "SELECT CP.CORPORATE_MST_PK COPK, CP.CORPORATE_ID, CP.CORPORATE_NAME CORPORATE_NAME FROM CORPORATE_MST_TBL CP";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[0].TableName = "Corporate";

                str = "SELECT R.REGION_MST_PK RPK, R.REGIONCODE, R.REGIONNAME REGIONNAME,R.CORPORATE_MST_FK COFK FROM REGION_MST_TBL R ";
                objds.Tables.Add(objWF.GetDataTable(str));
                objds.Tables[1].TableName = "REGION";

                str = "SELECT L.Location_MST_PK LPK,l.location_id,l.location_name LOCATIONNAME,L.REPORTING_TO_FK RFK FROM LOCATION_MST_TBL l ";
                objds.Tables.Add(objWF.GetDataTable(str));
                DataRelation objRel1 = new DataRelation("REL_REG_LOC", objds.Tables[1].Columns["RPK"], objds.Tables[2].Columns["RFK"]);
                DataRelation objRel2 = new DataRelation("REL_COR_REG", objds.Tables[0].Columns["COPK"], objds.Tables[1].Columns["COFK"]);

                objRel1.Nested = true;

                objRel2.Nested = true;
                objds.Relations.Add(objRel1);
                objds.Relations.Add(objRel2);
                return objds;
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

        #endregion "FetchNavigationForLocation"

        #region "GenerateSectors"

        public DataSet generateSectors(string RegionPK = "", string AreaPK = "", string CountryPk = "", string Ports = "", string Flag = "0")
        {
            Int32 last = default(Int32);
            Int32 start = default(Int32);
            Int32 TotalRecords = default(Int32);
            DataSet CountDS = null;
            System.Text.StringBuilder strMainSql = new System.Text.StringBuilder();
            WorkFlow objWF = new WorkFlow();

            RegionPK = RegionPK.TrimStart(',');
            AreaPK = AreaPK.TrimStart(',');
            CountryPk = CountryPk.TrimStart(',');
            Ports = Ports.TrimStart(',');

            strMainSql.Append(" SELECT ROWNUM \"SL_NR\", 'false' Sel,");
            strMainSql.Append(" ' ' TLI, ");
            strMainSql.Append("       POL.PORT_ID || '-' || POD.PORT_ID SECTOR_DESC,");
            strMainSql.Append("       POL.PORT_MST_PK POL_FK,");
            strMainSql.Append("       POD.PORT_MST_PK POD_FK,");
            strMainSql.Append("       POL.PORT_ID FROMPORT,");
            strMainSql.Append("       POD.PORT_ID TOPORT,");
            strMainSql.Append("       NULL DISTANCE_IN_MILES");
            strMainSql.Append("  FROM PORT_MST_TBL POL, PORT_MST_TBL POD");
            strMainSql.Append(" Where pol.port_mst_pk in");
            strMainSql.Append("       (SELECT PT.PORT_MST_PK");
            strMainSql.Append("          FROM PORT_MST_TBL    PT,");
            strMainSql.Append("               COUNTRY_MST_TBL CT,");
            strMainSql.Append("                 AREA_MST_TBL AR,");
            strMainSql.Append("                 REGION_MST_TBL  RG");
            strMainSql.Append("         WHERE PT.COUNTRY_MST_FK = CT.COUNTRY_MST_PK");
            strMainSql.Append("           AND CT.AREA_MST_FK = AR.AREA_MST_PK");
            strMainSql.Append("           AND AR.REGION_MST_FK = RG.REGION_MST_PK ");

            if (!string.IsNullOrEmpty(RegionPK) & RegionPK != "0")
            {
                strMainSql.Append(" AND RG.REGION_MST_PK IN (" + RegionPK + ")");
            }
            if (!string.IsNullOrEmpty(AreaPK) & AreaPK != "0")
            {
                strMainSql.Append(" AND AR.AREA_MST_PK IN (" + AreaPK + ")");
            }
            if (!string.IsNullOrEmpty(CountryPk) & CountryPk != "0")
            {
                strMainSql.Append(" AND CT.COUNTRY_MST_PK IN (" + CountryPk + ")");
            }
            if (!string.IsNullOrEmpty(Ports) & Ports != "0")
            {
                strMainSql.Append(" AND PT.PORT_MST_PK IN (" + Ports + ")");
            }

            strMainSql.Append("     )");

            strMainSql.Append("   and pod.port_mst_pk in");
            strMainSql.Append("       (SELECT PT.PORT_MST_PK");
            strMainSql.Append("          FROM PORT_MST_TBL    PT,");
            strMainSql.Append("               COUNTRY_MST_TBL CT, ");
            strMainSql.Append("                 AREA_MST_TBL AR,");
            strMainSql.Append("                 REGION_MST_TBL  RG");
            strMainSql.Append("         WHERE PT.COUNTRY_MST_FK = CT.COUNTRY_MST_PK");
            strMainSql.Append("           AND CT.AREA_MST_FK = AR.AREA_MST_PK");
            strMainSql.Append("           AND AR.REGION_MST_FK = RG.REGION_MST_PK ");
            strMainSql.Append("           And pol.port_mst_pk != pod.port_mst_pk ");

            //If ToRegionPK <> "" And ToRegionPK <> "0" Then
            //    strMainSql.Append(" AND RG.REGION_MST_PK IN (" & ToRegionPK & ")")
            //End If
            //If ToAreaPK <> "" And ToAreaPK <> "0" Then
            //    strMainSql.Append(" AND AR.AREA_MST_PK IN (" & ToAreaPK & ")")
            //End If
            //If ToCountryPk <> "" And ToCountryPk <> "0" Then
            //    strMainSql.Append(" AND CT.COUNTRY_MST_PK IN (" & ToCountryPk & ")")
            //End If
            //If ToPorts <> "" And ToPorts <> "0" Then
            //    strMainSql.Append(" AND PT.PORT_MST_PK IN (" & ToPorts & ")")
            //End If
            strMainSql.Append("     )");

            strMainSql.Append("     AND NOT EXISTS");
            strMainSql.Append("       (SELECT 1");
            strMainSql.Append("                FROM SECTOR_MST_TBL SEC");
            strMainSql.Append("               WHERE SEC.FROM_PORT_FK =POL.PORT_MST_PK");
            strMainSql.Append("               AND SEC.TO_PORT_FK= POD.PORT_MST_PK )");
            if (Flag == "1")
            {
                strMainSql.Append("   AND 1=2 ");
            }

            strMainSql.Append("   ORDER BY SECTOR_DESC");

            System.Text.StringBuilder sqlstr = new System.Text.StringBuilder();
            sqlstr.Append(" SELECT QRY.* FROM ");
            sqlstr.Append("(SELECT T.*  FROM ");
            sqlstr.Append("  (" + strMainSql.ToString() + " ");
            sqlstr.Append("  ) T) QRY WHERE SL_NR  Between " + start + " and " + last + "  ");

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

        #endregion "GenerateSectors"

        #region "Fetch Area"

        public DataSet CheckExisiting(string POLGrpPK = "0", string PODGrpPK = "0", string BizType = "0")
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT T.TARIFF_GRP_ID");
            sb.Append("  FROM TARIFF_GRP_MST_TBL T");
            sb.Append(" WHERE T.POL_GRP_MST_FK = " + POLGrpPK);
            sb.Append("   AND T.POD_GRP_MST_FK = " + PODGrpPK);
            sb.Append("   AND T.BIZ_TYPE = " + BizType);
            WorkFlow objWF = new WorkFlow();
            try
            {
                return objWF.GetDataSet(sb.ToString());
            }
            catch (OracleException oraEx)
            {
                throw oraEx;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion "Fetch Area"
    }
}