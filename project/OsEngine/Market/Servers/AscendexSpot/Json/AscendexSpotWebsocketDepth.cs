using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{
    class AscendexSpotDepthSnapshotResponse
    {
        public string code { get; set; }
        public AscendexSpotDepthWrapper data { get; set; }
    }

    class AscendexSpotDepthWrapper
    {
        public string m { get; set; } // "depth-snapshot"
        public string symbol { get; set; }
        public AscendexSpotDepthSnapshotData data { get; set; }
    }

    class AscendexSpotDepthSnapshotData
    {
        public string seqnum { get; set; }
        public string ts { get; set; }

        // Массивы цен и объёмов: [price, quantity]
        public string[][] asks { get; set; }
        public string[][] bids { get; set; }
    }

}
