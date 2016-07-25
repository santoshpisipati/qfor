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
using System.Web;

namespace Quantum_QFOR
{
    /// <summary>
    ///
    /// </summary>
    /// <seealso cref="Quantum_QFOR.CommonFeatures" />
    public class Cls_Parameters_TBL : CommonFeatures
    {
        #region "SaveParameters"

        /// <summary>
        /// Saves the parameters.
        /// </summary>
        /// <param name="generalcargo">The generalcargo.</param>
        /// <param name="reefercargo">The reefercargo.</param>
        /// <param name="dangerouscargo">The dangerouscargo.</param>
        /// <param name="opencargo">The opencargo.</param>
        /// <param name="precarriage">The precarriage.</param>
        /// <param name="oncarriage">The oncarriage.</param>
        /// <param name="bof">The bof.</param>
        /// <param name="afc">The afc.</param>
        /// <param name="kg">The kg.</param>
        /// <param name="ton">The ton.</param>
        /// <param name="lbs">The LBS.</param>
        /// <param name="frt_bof">The frt_bof.</param>
        /// <param name="frt_afc">The frt_afc.</param>
        /// <param name="detention">The detention.</param>
        /// <param name="demurage">The demurage.</param>
        /// <param name="oper_cost">The oper_cost.</param>
        /// <param name="tran_cost">The tran_cost.</param>
        /// <param name="sales_manager">The sales_manager.</param>
        /// <param name="sales_executive">The sales_executive.</param>
        /// <param name="FRT_AIF">The fr t_ aif.</param>
        /// <param name="FAC">The fac.</param>
        /// <param name="FRT_MIS">The fr t_ mis.</param>
        /// <param name="incentive">The incentive.</param>
        /// <param name="bbcCargo">The BBC cargo.</param>
        /// <returns></returns>
        public int SaveParameters(int generalcargo, int reefercargo, int dangerouscargo, int opencargo, int precarriage, int oncarriage, int bof, int afc, int kg, int ton,
        int lbs, int frt_bof, int frt_afc, int detention, int demurage, int oper_cost, int tran_cost, int sales_manager, int sales_executive, int FRT_AIF,
        int FAC, int FRT_MIS, int incentive, int bbcCargo)
        {
            WorkFlow objWF = new WorkFlow();
            objWF.OpenConnection();
            try
            {
                objWF.MyCommand.Parameters.Clear();
                objWF.MyCommand.CommandType = CommandType.StoredProcedure;
                objWF.MyCommand.CommandText = objWF.MyUserName + ".PARAMETERS_UPD_PKG.PARAMETER_UPD";
                var _with1 = objWF.MyCommand.Parameters;
                _with1.Add("GENERAL_CARGO_FK_IN", (generalcargo == 0 ? 0 : generalcargo)).Direction = ParameterDirection.Input;
                _with1.Add("HAZ_CARGO_FK_IN", (dangerouscargo == 0 ? 0 : dangerouscargo)).Direction = ParameterDirection.Input;
                _with1.Add("REEFER_CARGO_FK_IN", (reefercargo == 0 ? 0 : reefercargo)).Direction = ParameterDirection.Input;
                _with1.Add("ODC_CARGO_FK_IN", (opencargo == 0 ? 0 : opencargo)).Direction = ParameterDirection.Input;
                _with1.Add("BBC_CARGO_FK_IN", (bbcCargo == 0 ? 0 : bbcCargo)).Direction = ParameterDirection.Input;
                _with1.Add("COST_PRECARRIAGE_FK_IN", (precarriage == 0 ? 0 : precarriage)).Direction = ParameterDirection.Input;
                _with1.Add("COST_ONCARRIAGE_FK_IN", (oncarriage == 0 ? 0 : oncarriage)).Direction = ParameterDirection.Input;
                _with1.Add("COST_BOF_FK_IN", (bof == 0 ? 0 : bof)).Direction = ParameterDirection.Input;
                _with1.Add("COST_AFC_FK_IN", (afc == 0 ? 0 : afc)).Direction = ParameterDirection.Input;
                _with1.Add("KGS_IN", (kg == 0 ? 0 : kg)).Direction = ParameterDirection.Input;
                _with1.Add("TONS_IN", (ton == 0 ? 0 : ton)).Direction = ParameterDirection.Input;
                _with1.Add("LBS_IN", (lbs == 0 ? 0 : lbs)).Direction = ParameterDirection.Input;
                _with1.Add("FRT_BOF_FK_IN", (frt_bof == 0 ? 0 : frt_bof)).Direction = ParameterDirection.Input;
                _with1.Add("FRT_AFC_FK_IN", (frt_afc == 0 ? 0 : frt_afc)).Direction = ParameterDirection.Input;
                _with1.Add("FRT_AIF_FK_IN", (FRT_AIF == 0 ? 0 : FRT_AIF)).Direction = ParameterDirection.Input;
                _with1.Add("FRT_MIS_FK_IN", (FRT_MIS == 0 ? 0 : FRT_MIS)).Direction = ParameterDirection.Input;
                _with1.Add("FRT_DET_CHARGE_FK_IN", (detention == 0 ? 0 : detention)).Direction = ParameterDirection.Input;
                _with1.Add("FRT_DEM_CHARGE_FK_IN", (demurage == 0 ? 0 : demurage)).Direction = ParameterDirection.Input;
                _with1.Add("COST_FRT_FK_IN", (oper_cost == 0 ? 0 : oper_cost)).Direction = ParameterDirection.Input;
                _with1.Add("COST_TPC_FK_IN", (tran_cost == 0 ? 0 : tran_cost)).Direction = ParameterDirection.Input;
                _with1.Add("FRT_FAC_FK_IN", (FAC == 0 ? 0 : FAC)).Direction = ParameterDirection.Input;
                _with1.Add("SALES_MANAGER_IN", (sales_manager == 0 ? 0 : sales_manager)).Direction = ParameterDirection.Input;
                _with1.Add("SALES_EXECUTIVE_IN", (sales_executive == 0 ? 0 : sales_executive)).Direction = ParameterDirection.Input;
                _with1.Add("COST_INCENTIVE_FK_IN", (incentive == 0 ? 0 : incentive)).Direction = ParameterDirection.Input;
                _with1.Add("USER_PK_IN", HttpContext.Current.Session["USER_PK"]).Direction = ParameterDirection.Input;
                return objWF.MyCommand.ExecuteNonQuery();
            }
            catch (OracleException OraExp)
            {
                throw OraExp;
            }
            catch (Exception ex)
            {
                return 0;
            }
            finally
            {
                objWF.CloseConnection();
            }
        }

        #endregion "SaveParameters"

        #region "Fetch"

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        /// <returns></returns>
        public PARAMETERS GetParameters()
        {
            WorkFlow objWf = new WorkFlow();
            DataSet dsParam = new DataSet();
            PARAMETERS _param = new PARAMETERS();
            System.Text.StringBuilder sb = new System.Text.StringBuilder(5000);
            sb.Append("SELECT NVL(P.GENERAL_CARGO_FK, 0) GENERAL_CARGO_FK,");
            sb.Append("       NVL(P.HAZ_CARGO_FK, 0) HAZ_CARGO_FK,");
            sb.Append("       NVL(P.REEFER_CARGO_FK, 0) REEFER_CARGO_FK,");
            sb.Append("       NVL(P.ODC_CARGO_FK, 0) ODC_CARGO_FK,");
            sb.Append("       NVL(P.BBC_CARGO_FK, 0) BBC_CARGO_FK,");
            sb.Append("       NVL(P.COST_PRECARRIAGE_FK, 0) COST_PRECARRIAGE_FK,");
            sb.Append("       NVL(P.COST_ONCARRIAGE_FK, 0) COST_ONCARRIAGE_FK,");
            sb.Append("       NVL(P.COST_BOF_FK, 0) COST_BOF_FK,");
            sb.Append("       NVL(P.COST_AFC_FK, 0) COST_AFC_FK,");
            sb.Append("       NVL(P.UOM_KG, 0) UOM_KG,");
            sb.Append("       NVL(P.UOM_TON, 0) UOM_TON,");
            sb.Append("       NVL(P.UOM_LBS, 0) UOM_LBS,");
            sb.Append("       NVL(P.FRT_BOF_FK, 0) FRT_BOF_FK,");
            sb.Append("       NVL(P.FRT_AFC_FK, 0) FRT_AFC_FK,");
            sb.Append("       NVL(P.COST_FRT_FK, 0) COST_FRT_FK,");
            sb.Append("       NVL(P.SALES_MANAGER, 0) SALES_MANAGER,");
            sb.Append("       NVL(P.SALES_EXECUTIVE, 0) SALES_EXECUTIVE,");
            sb.Append("       NVL(P.CONTAINER_CHECK, 0) CONTAINER_CHECK,");
            sb.Append("       NVL(P.FRT_DET_CHARGE_FK, 0) FRT_DET_CHARGE_FK,");
            sb.Append("       NVL(P.FRT_DEM_CHARGE_FK, 0) FRT_DEM_CHARGE_FK,");
            sb.Append("       NVL(P.COST_TPC_FK, 0) COST_TPC_FK,");
            sb.Append("       NVL(P.CREATED_BY_FK, 0) CREATED_BY_FK,");
            sb.Append("       NVL(P.LAST_MODIFIED_BY_FK, 0) LAST_MODIFIED_BY_FK,");
            sb.Append("       NVL(P.PARAMETERS_MST_PK, 0) PARAMETERS_MST_PK,");
            sb.Append("       NVL(P.FRT_AIF_FK, 0) FRT_AIF_FK,");
            sb.Append("       NVL(P.FRT_FAC_FK, 0) FRT_FAC_FK,");
            sb.Append("       NVL(P.FRT_MIS_FK, 0) FRT_MIS_FK,");
            sb.Append("       NVL(P.COST_INCENTIVE_FK, 0) COST_INCENTIVE_FK");
            sb.Append("  FROM PARAMETERS_TBL P");

            try
            {
                dsParam = objWf.GetDataSet(sb.ToString());
                if (dsParam.Tables[0].Rows.Count > 0)
                {
                    var _with2 = dsParam.Tables[0].Rows[0];
                    _param.GENERAL_CARGO_FK = Convert.ToInt32(_with2["GENERAL_CARGO_FK"]);
                    _param.HAZ_CARGO_FK = Convert.ToInt32(_with2["HAZ_CARGO_FK"]);
                    _param.REEFER_CARGO_FK = Convert.ToInt32(_with2["REEFER_CARGO_FK"]);
                    _param.ODC_CARGO_FK = Convert.ToInt32(_with2["ODC_CARGO_FK"]);
                    _param.BBC_CARGO_FK = Convert.ToInt32(_with2["BBC_CARGO_FK"]);
                    _param.COST_PRECARRIAGE_FK = Convert.ToInt32(_with2["COST_PRECARRIAGE_FK"]);
                    _param.COST_ONCARRIAGE_FK = Convert.ToInt32(_with2["COST_ONCARRIAGE_FK"]);
                    _param.COST_BOF_FK = Convert.ToInt32(_with2["COST_BOF_FK"]);
                    _param.COST_AFC_FK = Convert.ToInt32(_with2["COST_AFC_FK"]);
                    _param.UOM_KG = Convert.ToInt32(_with2["UOM_KG"]);
                    _param.UOM_TON = Convert.ToInt32(_with2["UOM_TON"]);
                    _param.UOM_LBS = Convert.ToInt32(_with2["UOM_LBS"]);
                    _param.FRT_BOF_FK = Convert.ToInt32(_with2["FRT_BOF_FK"]);
                    _param.FRT_AFC_FK = Convert.ToInt32(_with2["FRT_AFC_FK"]);
                    _param.COST_FRT_FK = Convert.ToInt32(_with2["COST_FRT_FK"]);
                    _param.SALES_MANAGER = Convert.ToInt32(_with2["SALES_MANAGER"]);
                    _param.SALES_EXECUTIVE = Convert.ToInt32(_with2["SALES_EXECUTIVE"]);
                    _param.CONTAINER_CHECK = Convert.ToInt32(_with2["CONTAINER_CHECK"]);
                    _param.FRT_DET_CHARGE_FK = Convert.ToInt32(_with2["FRT_DET_CHARGE_FK"]);
                    _param.FRT_DEM_CHARGE_FK = Convert.ToInt32(_with2["FRT_DEM_CHARGE_FK"]);
                    _param.COST_TPC_FK = Convert.ToInt32(_with2["COST_TPC_FK"]);
                    _param.CREATED_BY_FK = Convert.ToInt32(_with2["CREATED_BY_FK"]);
                    _param.LAST_MODIFIED_BY_FK = Convert.ToInt32(_with2["LAST_MODIFIED_BY_FK"]);
                    _param.PARAMETERS_MST_PK = Convert.ToInt32(_with2["PARAMETERS_MST_PK"]);
                    _param.FRT_AIF_FK = Convert.ToInt32(_with2["FRT_AIF_FK"]);
                    _param.FRT_FAC_FK = Convert.ToInt32(_with2["FRT_FAC_FK"]);
                    _param.FRT_MIS_FK = Convert.ToInt32(_with2["FRT_MIS_FK"]);
                    _param.COST_INCENTIVE_FK = Convert.ToInt32(_with2["COST_INCENTIVE_FK"]);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return _param;
        }

        #endregion "Fetch"
    }

    /// <summary>
    ///
    /// </summary>
    public class PARAMETERS
    {
        /// <summary>
        /// The genera l_ carg o_ fk
        /// </summary>
        public int GENERAL_CARGO_FK;

        /// <summary>
        /// The ha z_ carg o_ fk
        /// </summary>
        public int HAZ_CARGO_FK;

        /// <summary>
        /// The reefe r_ carg o_ fk
        /// </summary>
        public int REEFER_CARGO_FK;

        /// <summary>
        /// The od c_ carg o_ fk
        /// </summary>
        public int ODC_CARGO_FK;

        /// <summary>
        /// The bb c_ carg o_ fk
        /// </summary>
        public int BBC_CARGO_FK;

        /// <summary>
        /// The cos t_ precarriag e_ fk
        /// </summary>
        public int COST_PRECARRIAGE_FK;

        /// <summary>
        /// The cos t_ oncarriag e_ fk
        /// </summary>
        public int COST_ONCARRIAGE_FK;

        /// <summary>
        /// The cos t_ bo f_ fk
        /// </summary>
        public int COST_BOF_FK;

        /// <summary>
        /// The cos t_ af c_ fk
        /// </summary>
        public int COST_AFC_FK;

        /// <summary>
        /// The uo m_ kg
        /// </summary>
        public int UOM_KG;

        /// <summary>
        /// The uo m_ ton
        /// </summary>
        public int UOM_TON;

        /// <summary>
        /// The uo m_ LBS
        /// </summary>
        public int UOM_LBS;

        /// <summary>
        /// The fr t_ bo f_ fk
        /// </summary>
        public int FRT_BOF_FK;

        /// <summary>
        /// The fr t_ af c_ fk
        /// </summary>
        public int FRT_AFC_FK;

        /// <summary>
        /// The cos t_ fr t_ fk
        /// </summary>
        public int COST_FRT_FK;

        /// <summary>
        /// The sale s_ manager
        /// </summary>
        public int SALES_MANAGER;

        /// <summary>
        /// The sale s_ executive
        /// </summary>
        public int SALES_EXECUTIVE;

        /// <summary>
        /// The containe r_ check
        /// </summary>
        public int CONTAINER_CHECK;

        /// <summary>
        /// The fr t_ de t_ charg e_ fk
        /// </summary>
        public int FRT_DET_CHARGE_FK;

        /// <summary>
        /// The fr t_ de m_ charg e_ fk
        /// </summary>
        public int FRT_DEM_CHARGE_FK;

        /// <summary>
        /// The cos t_ tp c_ fk
        /// </summary>
        public int COST_TPC_FK;

        /// <summary>
        /// The create d_ b y_ fk
        /// </summary>
        public int CREATED_BY_FK;

        /// <summary>
        /// The las t_ modifie d_ b y_ fk
        /// </summary>
        public int LAST_MODIFIED_BY_FK;

        /// <summary>
        /// The parameter s_ ms t_ pk
        /// </summary>
        public int PARAMETERS_MST_PK;

        /// <summary>
        /// The fr t_ ai f_ fk
        /// </summary>
        public int FRT_AIF_FK;

        /// <summary>
        /// The fr t_ fa c_ fk
        /// </summary>
        public int FRT_FAC_FK;

        /// <summary>
        /// The fr t_ mi s_ fk
        /// </summary>
        public int FRT_MIS_FK;

        /// <summary>
        /// The cos t_ incentiv e_ fk
        /// </summary>
        public int COST_INCENTIVE_FK;
    }
}