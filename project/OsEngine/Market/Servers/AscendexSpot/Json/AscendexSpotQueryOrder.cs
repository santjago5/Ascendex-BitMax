using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{
    class AscendexSpotQueryOrderResponse
    {
        public string code { get; set; }

        public string accountCategory { get; set; }

        public string accountId { get; set; }

        public AscendexSpotQueryOrderMessage data { get; set; }
    }

    class AscendexSpotQueryOrderMessage
    {
        public string symbol { get; set; }

        public string price { get; set; }

        public string orderQty { get; set; }

        public string orderType { get; set; }

        public string avgPx { get; set; }

        public string cumFee { get; set; }

        public string cumFilledQty { get; set; }

        public string errorCode { get; set; }

        public string feeAsset { get; set; }

        public string lastExecTime { get; set; }

        public string orderId { get; set; }

        public string seqNum { get; set; }

        public string side { get; set; }

        public string status { get; set; }

        public string stopPrice { get; set; }

        public string execInst { get; set; }
    }
}