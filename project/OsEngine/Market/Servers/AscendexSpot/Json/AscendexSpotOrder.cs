using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{

    public class AscendexSpotOrderResponse
    {
        public string code { get; set; }             // Код ответа / Response code
        public AscendexSpotOrderDataRest data { get; set; }     // Данные ответа / Response data
    }

    // Класс для свойства 'data' / Class for 'data' field
    public class AscendexSpotOrderDataRest
    {
        public string accountId { get; set; }    // Идентификатор аккаунта / Account ID
        public string ac { get; set; }               // Тип аккаунта / Account type
        public string action { get; set; }           // Действие (например, place-order) / Action (e.g. place-order)/(cancel-order)/(cancel-all)
        public string status { get; set; }           // Статус запроса (например, ACCEPT) / Request status (e.g. ACCEPT)
        public AscendexSpotOrderInfo info { get; set; }     // Подробная информация о заказе / Detailed order info

    }


    public class AscendexSpotOrderInfo
    {
        public string id { get; set; }               //
        public string avgPx { get; set; }            // Средняя цена / Average price
        public string cumFee { get; set; }           // Совокупная комиссия / Cumulative fee
        public string cumFilledQty { get; set; }     // Совокупное исполненное количество / Cumulative filled quantity
        public string errorCode { get; set; }        // Код ошибки / Error code
        public string feeAsset { get; set; }         // Валюта комиссии / Fee asset
        public string lastExecTime { get; set; }     // Время последнего исполнения / Last execution time
        public string orderId { get; set; }          // Идентификатор ордера / Order ID
        public string orderQty { get; set; }         // Количество ордера / Order quantity
        public string orderType { get; set; }        // Тип ордера / Order type
        public string price { get; set; }            // Цена / Price
        public string seqNum { get; set; }           // Номер последовательности / Sequence number
        public string side { get; set; }             // Сторона (Buy/Sell) / Order side (Buy/Sell)
        public string status { get; set; }           // Статус ордера / Order status
        public string stopPrice { get; set; }        // Стоп-цена / Stop price
        public string symbol { get; set; }           // Торговая пара / Trading pair
        public string execInst { get; set; }         // Инструкция исполнения / Execution instruction
    }
    public class AscendexSpotOpenOrdersResponse
    {
        public string ac { get; set; }                     //      AccountCategory 
        public string accountId { get; set; }
        public string code { get; set; }
        public List<AscendexSpotOrderInfo> data { get; set; }
    }

    public class AscendexSpotCancelOrderResponse
    {
        public string code { get; set; }
        public string accountId { get; set; }
        public string ac { get; set; }
        public string action { get; set; }
        public string status { get; set; }
        public string message { get; set; }
        public string reason { get; set; }
        public AscendexSpotCancelOrderData data { get; set; }
    }

    public class AscendexSpotCancelOrderInfo
    {
        public string id { get; set; }
        public string orderId { get; set; }
        public string orderType { get; set; }
        public string symbol { get; set; }
        public string timestamp { get; set; }
    }

    public class AscendexSpotCancelOrderData
    {
        public string accountId { get; set; }
        public string ac { get; set; }
        public string action { get; set; }
        public string status { get; set; }
        public AscendexSpotCancelOrderInfo info { get; set; }
    }
}
