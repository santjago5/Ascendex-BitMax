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
////using System.Collections.Generic;
//using Newtonsoft.Json;

//// Корневой объект JSON-ответа
//public class OrderHistoryResponse
//{
//    [JsonProperty("code")]
//    public string Code { get; set; } // Код ответа как строка

//    [JsonProperty("accountCategory")]
//    public string AccountCategory { get; set; } // Категория аккаунта

//    [JsonProperty("accountId")]
//    public string AccountId { get; set; } // Идентификатор аккаунта

//    [JsonProperty("data")]
//    public List<OrderItem> Data { get; set; } // Список ордеров
//}

//// Класс для представления ордера
//public class OrderItem
//{
//    [JsonProperty("symbol")]
//    public string Symbol { get; set; } // Торговая пара

//    [JsonProperty("price")]
//    public string Price { get; set; } // Цена ордера

//    [JsonProperty("orderQty")]
//    public string OrderQty { get; set; } // Количество в ордере

//    [JsonProperty("orderType")]
//    public string OrderType { get; set; } // Тип ордера

//    [JsonProperty("avgPx")]
//    public string AvgPx { get; set; } // Средняя цена исполнения

//    [JsonProperty("cumFee")]
//    public string CumFee { get; set; } // Совокупная комиссия

//    [JsonProperty("cumFilledQty")]
//    public string CumFilledQty { get; set; } // Совокупное исполненное количество

//    [JsonProperty("errorCode")]
//    public string ErrorCode { get; set; } // Код ошибки (если есть)

//    [JsonProperty("feeAsset")]
//    public string FeeAsset { get; set; } // Валюта комиссии

//    [JsonProperty("lastExecTime")]
//    public string LastExecTime { get; set; } // Время последнего исполнения как строка

//    [JsonProperty("orderId")]
//    public string OrderId { get; set; } // Идентификатор ордера

//    [JsonProperty("seqNum")]
//    public string SeqNum { get; set; } // Порядковый номер как строка

//    [JsonProperty("side")]
//    public string Side { get; set; } // Направление (Buy/Sell)

//    [JsonProperty("status")]
//    public string Status { get; set; } // Статус ордера

//    [JsonProperty("stopPrice")]
//    public string StopPrice { get; set; } // Стоп-цена (если есть)

//    [JsonProperty("execInst")]
//    public string ExecInst { get; set; } // Инструкция исполнения
//}
