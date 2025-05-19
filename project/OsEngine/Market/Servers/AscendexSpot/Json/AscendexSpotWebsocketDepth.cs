using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{
    class AscendexSpotDepthResponse
    {
        public string code { get; set; }
        public AscendexSpotDepthMessage data { get; set; }
    }

    class AscendexSpotDepthMessage
    {
        public string m { get; set; } // "depth-snapshot"
        public string symbol { get; set; }
        public AscendexSpotDepthtData data { get; set; }
    }

    class AscendexSpotDepthtData
    {
        public string seqnum { get; set; }
        public string ts { get; set; }

        // Массивы цен и объёмов: [price, quantity]
        //public string[][] asks { get; set; }
        //public string[][] bids { get; set; }
        public List<List<string>> asks { get; set; }
        public List<List<string>> bids { get; set; }
    }

}

//public class AscendexDepthSnapshotResponse
//{
//    public string m { get; set; }
//    public string symbol { get; set; }
//    public AscendexDepthData data { get; set; }
//}

//public class AscendexDepthData
//{
//    public long seqnum { get; set; }
//    public long ts { get; set; }
//    public List<List<string>> asks { get; set; }
//    public List<List<string>> bids { get; set; }
//}



//Если size > 0 и цена отсутствует — добавляем уровень.

//Если size > 0 и цена уже есть — обновляем количество.

//Если size == 0 — удаляем уровень по цене.

//public class AscendexSpotDepthEntry
//{
//    public decimal Price;
//    public decimal Quantity;
//}

//public class AscendexSpotDepth
//{
//    public List<AscendexSpotDepthEntry> Bids = new List<AscendexSpotDepthEntry>();
//    public List<AscendexSpotDepthEntry> Asks = new List<AscendexSpotDepthEntry>();
//}
