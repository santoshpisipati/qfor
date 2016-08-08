using System;

namespace Quantum_QFOR.Models
{
    public class JobCardSea
    {
        public string jobrefNO { get; set; }
        public string bookingNo { get; set; }
        public string HblNo { get; set; }
        public string polID { get; set; }
        public string podId { get; set; }
        public string polPK { get; set; }
        public string podPK { get; set; }
        public string jcStatus { get; set; }
        public string shipper { get; set; }
        public string consignee { get; set; }
        public string agent { get; set; }
        public string bizType { get; set; }
        public string processType { get; set; }
        public string cargoType { get; set; }
        public Double SearchFor { get; set; }
        public Int32 SearchFortime { get; set; }
        public string SortColumn { get; set; }
        public Int32 CurrentPage { get; set; }
        public Int32 TotalPage { get; set; }
        public string SortType { get; set; }
        public bool BOOKING { get; set; }
        public string MblNo { get; set; }
        public Int64 lngUsrLocFk { get; set; }
        public string containerno { get; set; }
        public Int32 jctype { get; set; }
        public Int32 flag { get; set; }
        public string hdnPlrpk { get; set; }
        public string hdnPfdpk { get; set; }
        public string carrierFk { get; set; }
        public string vesselPk { get; set; }
        public string UcrNr { get; set; }
        public string Commpk { get; set; }
        public bool flgXBkg { get; set; }
        public bool flgCL { get; set; }
        public string VesselName { get; set; }
        public string VoyageFlightNo { get; set; }
        public string PONumber { get; set; }
        public bool IsNominated { get; set; }
        public Int32 SalesExecMstFk { get; set; }
        public Int32 OtherStatus { get; set; }
        public Int64 CustomerPK { get; set; }
        public Int64 NotifyPK { get; set; }
    }
}

