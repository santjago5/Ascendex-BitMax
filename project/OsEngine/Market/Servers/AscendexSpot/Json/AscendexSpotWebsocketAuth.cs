using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{

    public class AscendexSpotWebsocketAuth
    {
        public string m { get; set; }       // Тип сообщения: всегда "auth"
        public string id { get; set; }      // Идентификатор, переданный в запросе (например: "abc123")
        public string code { get; set; }    // Код результата: 0 — успех, любое другое значение — ошибка
        public string err { get; set; }     // Сообщение об ошибке (может быть null или отсутствовать при code == 0)
    }


    //class AscendexSpotCancelOrderResponse
    //{
    //    public string code { get; set; }
    //    public AscendexSpotCancelOrderData data { get; set; }
    //}

    //class AscendexSpotCancelOrderData
    //{
    //    public string orderId { get; set; }
    //    public string status { get; set; } // Например: "Canceled"
    //}


}
