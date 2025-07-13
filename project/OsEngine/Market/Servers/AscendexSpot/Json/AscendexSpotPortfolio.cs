using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{

    public class AscendexSpotBalance
    {
        public string asset { get; set; }

        public string totalBalance { get; set; }

        public string availableBalance { get; set; }
    }

    public class AscendexSpotBalanceResponse
    {
        public string code { get; set; }

        public List<AscendexSpotBalance> data { get; set; }
    }

}

