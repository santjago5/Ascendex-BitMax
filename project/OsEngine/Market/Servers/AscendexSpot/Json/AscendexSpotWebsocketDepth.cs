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
        public AscendexSpotDepthMessage data { get; set; }
    }

    class AscendexSpotDepthMessage
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
//Если size > 0 и цена отсутствует — добавляем уровень.

//Если size > 0 и цена уже есть — обновляем количество.

//Если size == 0 — удаляем уровень по цене.

public class AscendexSpotDepthEntry
{
    public decimal Price;
    public decimal Quantity;
}

public class AscendexSpotDepth
{
    public List<AscendexSpotDepthEntry> Bids = new List<AscendexSpotDepthEntry>();
    public List<AscendexSpotDepthEntry> Asks = new List<AscendexSpotDepthEntry>();
}
