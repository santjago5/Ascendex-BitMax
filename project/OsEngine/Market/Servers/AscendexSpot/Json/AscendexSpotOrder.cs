using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{
    // Класс для корневого объекта ответа / Root response object
    public class AscendexSpotOrderResponse
    {
        public string code { get; set; }             // Код ответа / Response code
        public AscendexSpotOrderDataRest data { get; set; }     // Данные ответа / Response data
    }

    // Класс для свойства 'data' / Class for 'data' field
    public class AscendexSpotOrderDataRest
    {
        public string ac { get; set; }               // Тип аккаунта / Account type
        public string accountId { get; set; }        // Идентификатор аккаунта / Account ID
        public string action { get; set; }           // Действие (например, place-order) / Action (e.g. place-order)
        public AscendexSpotOrderInfo info { get; set; }     // Подробная информация о заказе / Detailed order info
        public string status { get; set; }           // Статус запроса (например, ACCEPT) / Request status (e.g. ACCEPT)
    }

    // Класс для свойства 'info' / Class for 'info' field
    public class AscendexSpotOrderInfo
    {
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
    public class AscendexSpotOrderErrorResponse
    {
        public string code { get; set; }         // Код ошибки / Error code
        public string message { get; set; }      // Сообщение об ошибке / Error message
        public string reason { get; set; }       // Причина / Reason
        public string accountId { get; set; }    // ID аккаунта / Account ID
        public string ac { get; set; }           // Тип аккаунта / Account type
    }
}


//// Класс для корневого объекта ответа / Root response object
//public class PlaceOrderResponse
//{
//    public string code { get; set; }             // Код ответа / Response code
//    public PlaceOrderData data { get; set; }     // Данные ответа / Response data
//}

//// Класс для свойства 'data' / Class for 'data' field
//public class PlaceOrderData
//{
//    public string ac { get; set; }               // Тип аккаунта / Account type
//    public string accountId { get; set; }        // Идентификатор аккаунта / Account ID
//    public string action { get; set; }           // Действие (например, place-order) / Action (e.g. place-order)
//    public PlaceOrderInfo info { get; set; }     // Подробная информация о заказе / Detailed order info
//    public string status { get; set; }           // Статус запроса (например, ACCEPT или DONE) / Request status (e.g. ACCEPT or DONE)
//}

//// Класс для свойства 'info' / Class for 'info' field
//public class PlaceOrderInfo
//{
//    public string avgPx { get; set; }            // Средняя цена / Average price
//    public string cumFee { get; set; }           // Совокупная комиссия / Cumulative fee
//    public string cumFilledQty { get; set; }     // Совокупное исполненное количество / Cumulative filled quantity
//    public string errorCode { get; set; }        // Код ошибки / Error code
//    public string feeAsset { get; set; }         // Валюта комиссии / Fee asset
//    public string id { get; set; }               // Уникальный ID записи / Unique record ID
//    public string lastExecTime { get; set; }     // Время последнего исполнения / Last execution time
//    public string orderId { get; set; }          // Идентификатор ордера / Order ID
//    public string orderQty { get; set; }         // Количество ордера / Order quantity
//    public string orderType { get; set; }        // Тип ордера / Order type
//    public string price { get; set; }            // Цена / Price
//    public string seqNum { get; set; }           // Номер последовательности / Sequence number
//    public string side { get; set; }             // Сторона (Buy/Sell) / Order side (Buy/Sell)
//    public string status { get; set; }           // Статус ордера / Order status
//    public string stopPrice { get; set; }        // Стоп-цена / Stop price
//    public string symbol { get; set; }           // Торговая пара / Trading pair
//    public string execInst { get; set; }         // Инструкция исполнения / Execution instruction
//}
