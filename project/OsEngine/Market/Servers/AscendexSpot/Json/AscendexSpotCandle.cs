using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OsEngine.Market.Servers.AscendexSpot.Json
{
    public class AscendexSpotCandleResponse
    {
        public string code { get; set; }
        public List<AscendexSpotCandleEntry> data { get; set; }
    }

    public class AscendexSpotCandleEntry
    {
        public string m { get; set; }        // Тип ("bar")
        public string s { get; set; }        // Символ (e.g. BTC/USDT)
        public AscendexSpotCandleData data { get; set; }    // Вложенные данные свечи
    }

    public class AscendexSpotCandleData
    {
        public string i { get; set; }        // Интервал
        public string ts { get; set; }         // Timestamp (Unix ms)
        public string o { get; set; }        // Open
        public string c { get; set; }        // Close
        public string h { get; set; }        // High
        public string l { get; set; }        // Low
        public string v { get; set; }        // Volume
    }
}
//Запрашиваемый диапазон времени определяется тремя параметрами — to, from и n — в соответствии со следующими правилами:

//from / to указывают начальную и конечную временные метки первой и последней свечи соответственно.

//Параметр to всегда учитывается. Если он не задан, его значение устанавливается как текущее системное время.

//Для параметров from и to действуют следующие правила:

//Если указан только from, диапазон запроса определяется как [from, to], включая оба конца. Однако, если диапазон слишком широк, сервер увеличит значение from, чтобы количество возвращаемых свечей не превышало 500.

//Если указан только n, сервер вернёт последние n свечей до момента to. Однако, если n больше 500, будет возвращено только 500 свечей.

//Если указаны оба параметра — from и n, сервер выберет тот вариант, при котором будет возвращено меньшее количество свечей.