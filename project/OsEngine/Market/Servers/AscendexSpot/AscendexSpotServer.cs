using OsEngine.Entity;
using OsEngine.Language;
using OsEngine.Logging;
using OsEngine.Market.Servers.Entity;
using RestSharp;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using Method = RestSharp.Method;
using OsEngine.Market.Servers.AscendexSpot.Json;
using Order = OsEngine.Entity.Order;
using Security = OsEngine.Entity.Security;
using Candle = OsEngine.Entity.Candle;
using Trade = OsEngine.Entity.Trade;
using ErrorEventArgs = OsEngine.Entity.WebSocketOsEngine.ErrorEventArgs;
using Side = OsEngine.Entity.Side;
using System.Globalization;
using WebSocketState = OsEngine.Entity.WebSocketOsEngine.WebSocketState;
using WebSocket = OsEngine.Entity.WebSocketOsEngine.WebSocket;
using CloseEventArgs = OsEngine.Entity.WebSocketOsEngine.CloseEventArgs;
using MessageEventArgs = OsEngine.Entity.WebSocketOsEngine.MessageEventArgs;
using Timer = System.Timers.Timer;
using static OsEngine.Market.Servers.AscendexSpot.AscendexSpotServerRealization;
using OsEngine.Market.Servers.Transaq.TransaqEntity;
using OsEngine.Market.Servers;










namespace OsEngine.Market.Servers.AscendexSpot
{
    public class AscendexSpotServer : AServer
    {
        public AscendexSpotServer(int uniqueNumber)
        {
            ServerNum = uniqueNumber;
            AscendexSpotServerRealization realization = new AscendexSpotServerRealization();
            ServerRealization = realization;

            CreateParameterString(OsLocalization.Market.ServerParamPublicKey, "");
            CreateParameterPassword(OsLocalization.Market.ServerParameterSecretKey, "");
        }
    }

    public class AscendexSpotServerRealization : IServerRealization
    {
        #region 1 Constructor, Status, Connection

        public AscendexSpotServerRealization()
        {
            ServerStatus = ServerConnectStatus.Disconnect;

            Thread threadForPublicMessagesMarketDepths = new Thread(PublicMessageMarketDepthsReader);
            threadForPublicMessagesMarketDepths.IsBackground = true;
            threadForPublicMessagesMarketDepths.Name = "PublicMarketDepthsMessageReaderAscendexSpot";
            threadForPublicMessagesMarketDepths.Start();

            Thread threadForPublicTradesMessages = new Thread(PublicMessageTradesReader);
            threadForPublicTradesMessages.IsBackground = true;
            threadForPublicTradesMessages.Name = "PublicTradeMessageReaderAscendexSpot";
            threadForPublicTradesMessages.Start();

            Thread threadForPrivateMessages = new Thread(PrivateMessageReader);
            threadForPrivateMessages.IsBackground = true;
            threadForPrivateMessages.Name = "PrivateMessageReaderAscendexSpot";
            threadForPrivateMessages.Start();

            //Thread threadCheckAliveWebSocket = new Thread(CheckAliveWebSocket);
            //threadCheckAliveWebSocket.IsBackground = true;
            //threadCheckAliveWebSocket.Name = "CheckAliveWebSocket";
            //threadCheckAliveWebSocket.Start();



        }


        public static List<OrderTracker> _orderTracker = new List<OrderTracker>();

        public class OrderTracker
        {
            public int OsOrderNumberUser;
            public string OrderNumberMarket;
            public string OrderCancelId;

        }


        public DateTime ServerTime { get; set; }


        public void Connect(WebProxy proxy = null)
        {
            try
            {

                _publicKey = ((ServerParameterString)ServerParameters[0]).Value;
                _secretKey = ((ServerParameterPassword)ServerParameters[1]).Value;

                if (string.IsNullOrEmpty(_publicKey) || string.IsNullOrEmpty(_secretKey))
                {
                    SendLogMessage("Error:Invalid public or secret key.", LogMessageType.Error);
                    return;
                }

                string _apiPath = "/api/pro/v2/assets";


                IRestResponse response = CreatePublicQuery(_apiPath, Method.GET);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    string responseBody = response.Content;

                    AscendexSpotSecurityResponse result = JsonConvert.DeserializeObject<AscendexSpotSecurityResponse>(responseBody);

                    if (result != null && result.code == "0")
                    {
                        FIFOListWebSocketPublicMarketDepthsMessage = new ConcurrentQueue<string>();
                        FIFOListWebSocketPublicTradesMessage = new ConcurrentQueue<string>();
                        FIFOListWebSocketPrivateMessage = new ConcurrentQueue<string>();
                        CreatePublicWebSocketMarketDepthsConnect();
                        CreatePublicWebSocketTradesConnect();
                        CreatePrivateWebSocketConnect();
                        CheckActivationSockets();

                        StartClientPingTimer();

                        SendLogMessage("Start Ascendex Connection", LogMessageType.System);
                    }
                    else
                    {
                        SendLogMessage("Status: Maintenance mode", LogMessageType.System);
                    }
                }
                else
                {
                    SendLogMessage($"No connection to Ascendex server. Code:{response.StatusCode}, Error:{response.Content}", LogMessageType.Error);
                    ServerStatus = ServerConnectStatus.Disconnect;
                    DisconnectEvent();
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Connection cannot be open Ascendex. exception:" + exception.ToString(), LogMessageType.Error);
                ServerStatus = ServerConnectStatus.Disconnect;
                DisconnectEvent();
            }
        }

        public void Dispose()
        {
            try
            {
                for (int i = 0; i < _securities.Count; i++)
                {
                    Security security = _securities[i];

                    if (string.IsNullOrWhiteSpace(security.Name))
                    {
                        SendLogMessage("Cannot unsubscribe — security is null or empty.", LogMessageType.Error);
                        return;
                    }

                    UnsubscribeFromAllWebSockets();

                }

                DeleteWebSocketConnection();

            }
            catch (Exception exception)
            {
                SendLogMessage("Dispose method error: " + exception.ToString(), LogMessageType.Error);
            }

            FIFOListWebSocketPrivateMessage = null;
            FIFOListWebSocketPublicMarketDepthsMessage = null;
            FIFOListWebSocketPublicTradesMessage = null;

            Disconnect();
        }

        public void Disconnect()
        {
            if (ServerStatus != ServerConnectStatus.Disconnect)
            {
                if (_clientPingTimer != null)
                {
                    _clientPingTimer.Stop();
                    _clientPingTimer.Dispose();
                    _clientPingTimer = null;
                }

                ServerStatus = ServerConnectStatus.Disconnect;
                DisconnectEvent();
            }
        }
        public ServerType ServerType
        {
            get { return ServerType.AscendexSpot; }
        }

        public event Action ConnectEvent;

        public event Action DisconnectEvent;

        #endregion

        private Timer _clientPingTimer;
        private void StartClientPingTimer()
        {
            // создаём таймер на 15 секунд
            _clientPingTimer = new Timer(10000);

            // подписка на событие
            _clientPingTimer.Elapsed += (sender, e) =>
            {
                // создаём ping-сообщение
                var ping = new { op = "ping" };

                string json = JsonConvert.SerializeObject(ping);

                for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
                {
                    WebSocket socket = _webSocketPublicMarketDepths[i];
                    if (socket.ReadyState == WebSocketState.Open)
                    {
                        socket.Send(json);
                    }
                }

                // отправка в публичные сокеты трейдов
                for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                {
                    WebSocket socket = _webSocketPublicTrades[i];
                    if (socket.ReadyState == WebSocketState.Open)
                    {
                        socket.Send(json);
                    }
                }

                // отправка в приватный сокет
                if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
                {
                    _webSocketPrivate.Send(json);
                }
            };

            _clientPingTimer.AutoReset = true;
            _clientPingTimer.Start(); // запускаем таймер

        }

        #region 2 Properties 
        public List<IServerParameter> ServerParameters { get; set; }
        public ServerConnectStatus ServerStatus { get; set; }

        private string _publicKey = "";

        private string _secretKey = "";

        private string _baseUrl = "https://ascendex.com";

        #endregion

        #region 3 Securities

        private List<Security> _securities = new List<Security>();

        private RateGate _rateGateSecurity = new RateGate(1, TimeSpan.FromMilliseconds(2100));

        public void GetSecurities()
        {
            try
            {
                _rateGateSecurity.WaitToProceed();

                string _apiPath = "api/pro/v1/cash/products";

                IRestResponse response = CreatePublicQuery(_apiPath, Method.GET/*, _myProxy*/);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    string jsonResponse = response.Content;

                    AscendexSpotSecurityResponse securityList = JsonConvert.DeserializeObject<AscendexSpotSecurityResponse>(jsonResponse);

                    if (securityList == null)
                    {
                        SendLogMessage("GetSecurities> Deserialization resulted in null", LogMessageType.Error);
                        return;
                    }

                    if (securityList.data.Count > 0)
                    {
                        SendLogMessage("Securities loaded. Count: " + securityList.data.Count, LogMessageType.System);
                        SecurityEvent?.Invoke(_securities);
                    }

                    if (securityList != null && securityList.code == "0")
                    {
                        List<Security> securities = new List<Security>();

                        for (int i = 0; i < securityList.data.Count; i++)
                        {
                            string symbol = securityList.data[i].symbol;
                            string price = securityList.data[i].tickSize;
                            string domain = securityList.data[i].domain;
                            string statusCode = securityList.data[i].statusCode;

                            if (symbol.Contains("$") || domain.Contains("LeveragedETF") || statusCode != "Normal")

                            {
                                continue;
                            }

                            Security newSecurity = new Security();

                            newSecurity.Exchange = ServerType.AscendexSpot.ToString();
                            newSecurity.Name = symbol;
                            newSecurity.NameFull = symbol;
                            newSecurity.NameClass = GetNameClass(symbol);
                            newSecurity.NameId = symbol;
                            newSecurity.SecurityType = SecurityType.CurrencyPair;
                            newSecurity.Lot = 1;
                            newSecurity.State = SecurityStateType.Activ;
                            newSecurity.PriceStep = securityList.data[i].tickSize.ToString().ToDecimal();
                            newSecurity.Decimals = price.DecimalsCount() == 0 ? 1 : price.DecimalsCount();

                            if (newSecurity.PriceStep == 0)
                            {
                                newSecurity.PriceStep = 1;
                            }

                            newSecurity.PriceStepCost = newSecurity.PriceStep;
                            newSecurity.DecimalsVolume = Convert.ToInt32(securityList.data[i].priceScale);
                            newSecurity.MinTradeAmount = securityList.data[i].minQty.ToString().ToDecimal();
                            newSecurity.MinTradeAmountType = MinTradeAmountType.Contract;
                            newSecurity.VolumeStep = newSecurity.DecimalsVolume.GetValueByDecimals();
                            securities.Add(newSecurity);
                        }

                        if (SecurityEvent != null)
                        {
                            SecurityEvent(securities);
                        }
                    }
                }
                else
                {
                    SendLogMessage($"Securities request error. Code:{response.StatusCode}, Error:{response.Content}", LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Securities request exception" + exception.ToString(), LogMessageType.Error);
            }
        }

        private string GetAccountGroup()
        {

            string fullPath = $"/api/pro/v1/info";

            IRestResponse response = CreatePrivateQuery(fullPath, null, null, null, Method.GET/*, null*/);

            if (response == null || response.StatusCode != HttpStatusCode.OK)
            {
                SendLogMessage("Failed to get account group", LogMessageType.Error);
                return string.Empty;
            }

            ApiKeyInfoResponse responses = JsonConvert.DeserializeObject<ApiKeyInfoResponse>(response.Content);
            return responses.data.accountGroup;
        }

        private string GetNameClass(string security)
        {
            switch (security)
            {
                case string s when s.EndsWith("USD"):
                    return "USD";
                case string s when s.EndsWith("USDT"):
                    return "USDT";
                case string s when s.EndsWith("BTC"):
                    return "BTC";
            }

            return "CurrencyPair";
        }

        #endregion

        #region 4 Portfolios

        private List<Portfolio> _portfolios = new List<Portfolio>();

        public event Action<List<Portfolio>> PortfolioEvent;

        private RateGate _rateGatePortfolio = new RateGate(1, TimeSpan.FromMilliseconds(750));

        public void GetPortfolios()
        {

            CreateQueryPortfolio();

            if (_portfolios.Count != 0)
            {
                PortfolioEvent?.Invoke(_portfolios);
            }
        }

        private void CreateQueryPortfolio()
        {
            try
            {
                _rateGatePortfolio.WaitToProceed();

                string accountGroup = GetAccountGroup();
                string fullPath = $"/{accountGroup}/api/pro/v1/cash/balance";

                IRestResponse response = CreatePrivateQuery(fullPath, null, null, accountGroup, Method.GET/*, _myProxy*/);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    Portfolio portfolio = new Portfolio();

                    portfolio.Number = "AscendexSpotPortfolio";
                    portfolio.ValueBegin = 1;
                    portfolio.ValueCurrent = 1;

                    AscendexSpotBalanceResponse wallets = JsonConvert.DeserializeObject<AscendexSpotBalanceResponse>(response.Content);

                    for (int i = 0; i < wallets.data.Count; i++)
                    {

                        PositionOnBoard position = new PositionOnBoard();

                        position.PortfolioName = "AscendexSpotPortfolio";
                        position.SecurityNameCode = wallets.data[i].asset;
                        position.ValueBegin = wallets.data[i].totalBalance.ToDecimal();
                        position.ValueCurrent = wallets.data[i].availableBalance.ToDecimal();
                        position.ValueBlocked = position.ValueBegin - position.ValueCurrent;

                        portfolio.SetNewPosition(position);

                    }

                    _portfolios.Add(portfolio);

                    if (_portfolios.Count != 0)
                    {
                        PortfolioEvent?.Invoke(_portfolios);
                    }
                }
                else
                {
                    SendLogMessage($"Portfolio request error. Code:{response.StatusCode}, Error:{response.Content}", LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        #endregion

        #region 5 Data
        public List<Candle> GetCandleDataToSecurity(Security security, TimeFrameBuilder timeFrameBuilder, DateTime startTime, DateTime endTime, DateTime actualTime)
        {
            startTime = DateTime.SpecifyKind(startTime, DateTimeKind.Utc);
            endTime = DateTime.SpecifyKind(endTime, DateTimeKind.Utc);
            actualTime = DateTime.SpecifyKind(actualTime, DateTimeKind.Utc);

            if (startTime != actualTime)
            {
                startTime = actualTime;
            }

            int tfTotalMinutes = (int)timeFrameBuilder.TimeFrameTimeSpan.TotalMinutes;

            if (!CheckTf(tfTotalMinutes))
            {
                return null;
            }

            if (endTime > DateTime.UtcNow)
            {
                endTime = DateTime.UtcNow;
            }

            if (!CheckTime(startTime, endTime, actualTime))
            {
                return null;
            }

            int countNeedToLoad = GetCountCandlesFromPeriod(startTime, endTime, timeFrameBuilder.TimeFrameTimeSpan);

            return GetCandleHistory(security.NameFull, timeFrameBuilder.TimeFrameTimeSpan, true, countNeedToLoad, endTime);
        }

        public List<Candle> GetCandleHistory(string nameSec, TimeSpan tf, bool isOsData, int countToLoad, DateTime timeEnd)
        {
            string timeFrame = GetInterval(tf);  // Интервал в формате API
            int limit = 480;                     // Лимит за 1 запрос

            List<Candle> allCandles = new List<Candle>(); // Общий список свечей
            HashSet<DateTime> uniqueTimes = new HashSet<DateTime>(); // Для исключения дубликатов

            int candlesLoaded = 0;
            DateTime periodEnd = timeEnd; // Начнем с заданного конца


            if (periodEnd > DateTime.UtcNow)
            {
                periodEnd = DateTime.UtcNow;
            }
            while (candlesLoaded < countToLoad)
            {
                // Сколько свечей нужно запросить в этой итерации
                int candlesToLoad = Math.Min(limit, countToLoad - candlesLoaded);

                // Получаем порцию свечей до periodEnd
                List<Candle> rangeCandles = CreateQueryCandles(nameSec, timeFrame, periodEnd, candlesToLoad);

                // Если ошибка или пусто — прекращаем
                if (rangeCandles == null || rangeCandles.Count == 0)
                {
                    break;
                }

                // Добавляем только уникальные свечи
                for (int i = 0; i < rangeCandles.Count; i++)
                {
                    if (uniqueTimes.Add(rangeCandles[i].TimeStart))
                    {
                        allCandles.Add(rangeCandles[i]);
                    }
                }

                candlesLoaded += rangeCandles.Count;

                // Следующий "конец" периода — начало первой свечи из этого блока
                periodEnd = rangeCandles[0].TimeStart;

                // Если ушли раньше нужного диапазона — завершаем
                if (periodEnd <= timeEnd - TimeSpan.FromMinutes(tf.TotalMinutes * countToLoad))
                {
                    break;
                }
            }

            // Удаляем свечи позже указанного времени
            for (int i = allCandles.Count - 1; i >= 0; i--)
            {
                if (allCandles[i].TimeStart > timeEnd)
                {
                    allCandles.RemoveAt(i);
                }
            }

            // Сортировка на случай, если порядок сбился
            allCandles.Sort((a, b) => a.TimeStart.CompareTo(b.TimeStart));

            return allCandles;
        }


        public List<Trade> GetTickDataToSecurity(Security security, DateTime startTime, DateTime endTime, DateTime actualTime)
        {
            return null;
        }

        public List<Candle> GetLastCandleHistory(Security security, TimeFrameBuilder timeFrameBuilder, int candleCount)
        {

            int tfTotalMinutes = (int)timeFrameBuilder.TimeFrameTimeSpan.TotalMinutes;
            DateTime timeEnd = DateTime.UtcNow;
            DateTime timeStart = timeEnd.AddMinutes(-tfTotalMinutes * candleCount);

            return GetCandleDataToSecurity(security, timeFrameBuilder, timeStart, timeEnd, timeStart);

        }

        private bool CheckTime(DateTime startTime, DateTime endTime, DateTime actualTime)
        {
            if (startTime >= endTime ||
                startTime >= DateTime.UtcNow ||
                actualTime > endTime ||
                actualTime > DateTime.UtcNow)
            {
                SendLogMessage("Error: The date is incorrect", LogMessageType.User);
                return false;
            }

            return true;
        }

        private bool CheckTf(int timeFrameMinutes)
        {
            if (timeFrameMinutes == 1 ||
                timeFrameMinutes == 5 ||
                timeFrameMinutes == 15 ||
                timeFrameMinutes == 30 ||
                timeFrameMinutes == 60 ||
                timeFrameMinutes == 120 ||
                timeFrameMinutes == 240 ||
                timeFrameMinutes == 360 ||
                timeFrameMinutes == 720 ||
                timeFrameMinutes == 1440)

            {
                return true;
            }
            return false;
        }

        private string GetInterval(TimeSpan tf)
        {
            if (tf.Days > 0)
            {
                return $"{tf.Days}d";
            }
            else if (tf.TotalMinutes > 0)
            {
                return (tf.TotalMinutes).ToString();
            }
            else
            {
                SendLogMessage("Error:The timeframe is incorrect", LogMessageType.User);
                return null;
            }
        }

        private int GetCountCandlesFromPeriod(DateTime startTime, DateTime endTime, TimeSpan tf)
        {
            TimeSpan timePeriod = endTime - startTime;

            if (tf.Days > 0)
            {
                return Convert.ToInt32(timePeriod.TotalDays / tf.TotalDays);
            }
            else if (tf.Hours > 0)
            {
                return Convert.ToInt32(timePeriod.TotalHours / tf.TotalHours);
            }
            else if (tf.Minutes > 0)
            {
                return Convert.ToInt32(timePeriod.TotalMinutes / tf.TotalMinutes);
            }
            else
            {
                SendLogMessage(" Timeframe must be defined in days, hours, or minutes.", LogMessageType.Error);
            }

            return 0;
        }


        private RateGate _rateGateCandleHistory = new RateGate(1, TimeSpan.FromMilliseconds(2100));

        private List<Candle> CreateQueryCandles(string symbol, string interval/*, DateTime startTime*/, DateTime endTime, int limit)
        {
            _rateGateCandleHistory.WaitToProceed();

            try
            {
                // long startDate = TimeManager.GetTimeStampMilliSecondsToDateTime(startTime);
                long endDate = TimeManager.GetTimeStampMilliSecondsToDateTime(endTime);

                string _apiPath = $"/api/pro/v1/barhist?symbol={symbol}&interval={interval}&n={limit}&to={endDate}";

                IRestResponse response = CreatePublicQuery(_apiPath, Method.GET/*, _myProxy*/);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCandleResponse json = JsonConvert.DeserializeObject<AscendexSpotCandleResponse>(response.Content);

                    // Проверка: если объект пустой или вернулся неуспешный код
                    if (json == null || json.code != "0" || json.data == null)
                    {
                        
                        SendLogMessage($"{ json.code }, {json.data}, Data format error or response code != 0" , LogMessageType.Error );
                        return null;
                       // return new List<Candle>();
                    }

                    List<AscendexSpotCandleData> candleList = new List<AscendexSpotCandleData>();

                    for (int i = 0; i < json.data.Count; i++)
                    {
                        AscendexSpotCandleData candleData = json.data[i].data;

                        Candle candle = new Candle();

                        candle.TimeStart = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(candleData.ts));
                        candle.Open = Convert.ToDecimal(candleData.o);
                        candle.Close = Convert.ToDecimal(candleData.c);
                        candle.High = Convert.ToDecimal(candleData.h);
                        candle.Low = Convert.ToDecimal(candleData.l);
                        candle.Volume = Convert.ToDecimal(candleData.v);

                        candleList.Add(candleData);
                    }

                    return ConvertToCandles(candleList);
                }
                else
                {
                    SendLogMessage($"Failed to query candles. Code: {response.StatusCode}, Error: {response.Content}", LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {

                SendLogMessage($"Request error: {exception.Message}", LogMessageType.Error);
            }

            return null;
        }


        private List<Candle> ConvertToCandles(List<AscendexSpotCandleData> candleList)
        {
            List<Candle> candles = new List<Candle>();

            try
            {
                for (int i = 0; i < candleList.Count; i++)
                {
                    AscendexSpotCandleData candle = candleList[i];

                    try
                    {
                        if (string.IsNullOrEmpty(candle.o) ||
                            string.IsNullOrEmpty(candle.c) || string.IsNullOrEmpty(candle.h) ||
                            string.IsNullOrEmpty(candle.l) || string.IsNullOrEmpty(candle.v))
                        {
                            SendLogMessage("Candle data contains null or empty values", LogMessageType.Error);
                            continue;
                        }

                        if ((candle.o).ToDecimal() == 0 || (candle.c).ToDecimal() == 0 ||
                            (candle.h.ToDecimal() == 0 || (candle.l).ToDecimal() == 0 ||
                            (candle.v).ToDecimal() == 0))
                        {

                            continue;
                        }

                        Candle newCandle = new Candle();

                        newCandle.State = CandleState.Finished;
                        newCandle.TimeStart = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(candle.ts));
                        newCandle.Open = candle.o.ToDecimal();
                        newCandle.Close = candle.c.ToDecimal();
                        newCandle.High = candle.h.ToDecimal();
                        newCandle.Low = candle.l.ToDecimal();
                        newCandle.Volume = candle.v.ToDecimal();

                        candles.Add(newCandle);
                    }
                    catch (Exception exception)
                    {
                        SendLogMessage($"Format exception: {exception.Message}", LogMessageType.Error);
                    }
                }

                return candles;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return null;
            }
        }

        #endregion



        #region 6 WebSocket creation


        private void PublicMessageMarketDepthsReader()
        {
            Thread.Sleep(2000);

            while (true)
            {

                try
                {
                    if (ServerStatus == ServerConnectStatus.Disconnect)
                    {
                        Thread.Sleep(2000);
                        continue;
                    }

                    if (FIFOListWebSocketPublicMarketDepthsMessage.IsEmpty)
                    {
                        Thread.Sleep(1);
                        continue;
                    }
                    //{"m":"connected","type":"unauth"}
                    //{ "op":"connected","type":"auth"}
                    // { "op":"connected","type":"unauth"}
                    //{"m":"sub","ch":"depth:1INCH/USDT","code":0}

                    FIFOListWebSocketPublicMarketDepthsMessage.TryDequeue(out string message);
                  
                    if (message == null)
                    {
                        continue;
                    }

                    else if (message.Contains("\"m\":\"ping\""))
                    {
                        for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
                        {
                            WebSocket socket = _webSocketPublicMarketDepths[i];
                            if (socket.ReadyState == WebSocketState.Open)
                            {
                                socket.Send("{\"op\":\"pong\"}");
                                SendLogMessage(">>> [Pong] Responded to server ping from depth socket", LogMessageType.System);
                            }
                        }
                        return;
                    }

                    else if (message.Contains("\"m\":\"error\""))
                    {
                        SendLogMessage($"/MarketDepths-{message}", LogMessageType.Error);


                        return;
                    }

                    if (message.Contains("\"m\":\"depth-snapshot\""))
                    {
                        SnapshotDepth(message);
                        continue;
                    }

                    else if (message.Contains("\"m\":\"depth\""))

                    {
                        UpdateDepth(message);
                        continue;
                    }
                }
                catch (Exception exception)
                {
                    Thread.Sleep(2000);
                    SendLogMessage(exception.ToString(), LogMessageType.Error);
                }
            }
        }

        private void PublicMessageTradesReader()
        {
            while (true)
            {
                try
                {
                    if (ServerStatus == ServerConnectStatus.Disconnect)
                    {
                        Thread.Sleep(2000);
                        continue;
                    }

                    if (FIFOListWebSocketPublicTradesMessage.IsEmpty)
                    {
                        Thread.Sleep(1);
                        continue;
                    }

                    FIFOListWebSocketPublicTradesMessage.TryDequeue(out string message);

                    if (message == null)
                    {
                        continue;
                    }

                    else if (message.Contains("\"m\":\"error\""))
                    {
                        SendLogMessage($"//PublicTrades-{message}", LogMessageType.Error);

                        return;
                    }
                    if (message.Contains("\"m\":\"trades\""))
                    {
                        UpdateTrade(message);
                    }

                    else if (message.Contains("\"m\":\"ping\""))
                    {
                        for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                        {
                            WebSocket socket = _webSocketPublicTrades[i];
                            if (socket.ReadyState == WebSocketState.Open)
                            {
                                socket.Send("{\"op\":\"pong\"}");
                                SendLogMessage(">>> [Pong] Responded to server ping from Trades socket", LogMessageType.System);
                            }
                        }
                        return;
                    }
                }
                catch (Exception exception)
                {
                    Thread.Sleep(2000);
                    SendLogMessage(exception.ToString(), LogMessageType.Error);
                }
            }
        }
        //		message	"{\"m\":\"connected\",\"type\":\"unauth\"}"	string

        //"{\"m\":\"auth\",\"id\":\"auth-req5a9bdd16-257b-475d-b43f-c39c53b77542\",\"code\":0}"
        // {"m":"error","code":150001,"reason":"INVALID_JSON_FORMAT","info":"Unable to parse json: pong"}
        private void PrivateMessageReader()
        {
            Thread.Sleep(1000);

            while (true)
            {
                try
                {
                    if (ServerStatus == ServerConnectStatus.Disconnect)
                    {
                        Thread.Sleep(2000);
                        continue;
                    }

                    if (FIFOListWebSocketPrivateMessage.IsEmpty)
                    {
                        Thread.Sleep(1);
                        continue;
                    }

                    FIFOListWebSocketPrivateMessage.TryDequeue(out string message);

                    if (message == null)
                    {
                        continue;
                    }

                    else if (message.Contains("\"m\":\"error\""))
                    {
                        SendLogMessage($"///Private - {message}", LogMessageType.Error);

                        return;
                    }

                    // если пришёл ping от сервера "{\"m\":\"ping\",\"hp\":3}"	

                    if (message.Contains("\"m\":\"ping\""))
                    {
                        SendLogMessage(">>> Responding with pong: {\"op\":\"pong\"}", LogMessageType.System);

                        _webSocketPrivate.Send("{\"op\":\"pong\"}");
                    }

                    if (message.Contains("\"op\":\"auth\""))
                    {
                        SendLogMessage("WebSocket private opened", LogMessageType.System);

                        AscendexSpotWebsocketAuth authResponse = JsonConvert.DeserializeObject<AscendexSpotWebsocketAuth>(message);

                        if (authResponse.code == "0")
                        {

                            SendLogMessage("WebSocket authentication successful", LogMessageType.System);
                        }
                        else
                        {
                            ServerStatus = ServerConnectStatus.Disconnect;
                            DisconnectEvent();
                            SendLogMessage($"WebSocket authentication error: Invalid public or secret key: {authResponse.err}", LogMessageType.Error);
                        }
                    }

                    else if (message.Contains("\"m\":\"error\""))
                    {
                        SendLogMessage($"Private {message}", LogMessageType.Error);
                        return;
                    }

                    else if (message.Contains("\"m\":\"order\""))
                    {
                        var orderMessage = JsonConvert.DeserializeObject<WebSocketMessage<AscendexSpotOrderData>>(message);

                        UpdateOrder(orderMessage);

                    }
                    else if (message.Contains("\"m\":\"balance\""))
                    {
                        var portfolioMessage = JsonConvert.DeserializeObject<WebSocketMessage<AscendexSpotPortfolio>>(message);

                        UpdatePortfolio(portfolioMessage);
                    }
                }
                catch (Exception exception)
                {
                    Thread.Sleep(5000);
                    SendLogMessage(exception.ToString(), LogMessageType.Error);
                }
            }
        }

        private ConcurrentQueue<string> FIFOListWebSocketPrivateMessage = new ConcurrentQueue<string>();
        private ConcurrentQueue<string> FIFOListWebSocketPublicMarketDepthsMessage = new ConcurrentQueue<string>();
        private ConcurrentQueue<string> FIFOListWebSocketPublicTradesMessage = new ConcurrentQueue<string>();

        private List<WebSocket> _webSocketPublicTrades = new List<WebSocket>();
        private List<WebSocket> _webSocketPublicMarketDepths = new List<WebSocket>();

        private WebSocket _webSocketPrivate;

        private string _webSocketUrl = "wss://ascendex.com/1/api/pro/v1/stream";

        private void CreatePublicWebSocketMarketDepthsConnect()
        {
            try
            {
                if (FIFOListWebSocketPublicMarketDepthsMessage == null)
                {
                    FIFOListWebSocketPublicMarketDepthsMessage = new ConcurrentQueue<string>();
                }

                _webSocketPublicMarketDepths.Add(CreateNewPublicMarketDepthsSocket());
            }
            catch (Exception exception)
            {
                SendLogMessage($"{exception.Message} {exception.StackTrace}", LogMessageType.Error);
            }
        }

        private int _webSocketConnectAttempts = 0;
        private DateTime _lastWebSocketConnectTime = DateTime.MinValue;
        private static readonly int _minReconnectIntervalSec = 10;
        private WebSocket CreateNewPublicMarketDepthsSocket()
        {
            try
            {
                Thread.Sleep(2000);

                DateTime now = DateTime.UtcNow;
                double secondsSinceLastConnect = (now - _lastWebSocketConnectTime).TotalSeconds;

                if (secondsSinceLastConnect < _minReconnectIntervalSec)
                {
                    double waitTime = _minReconnectIntervalSec - secondsSinceLastConnect;
                    SendLogMessage($" Delay before connecting to MarketDepths WebSocket: {waitTime} sec.", LogMessageType.System);
                    Thread.Sleep((int)(waitTime * 1000));
                }

                _webSocketConnectAttempts++;
                _lastWebSocketConnectTime = DateTime.UtcNow;

                SendLogMessage($"Try to connect to WebSocket MarketDepths #{_webSocketConnectAttempts} # {_lastWebSocketConnectTime}", LogMessageType.System);


                WebSocket webSocketPublicMarketDepthsNew = new WebSocket(_webSocketUrl);

                //if (_myProxy != null)
                //{
                //    webSocketPublicNew.SetProxy(_myProxy);
                //}

                webSocketPublicMarketDepthsNew.EmitOnPing = true;
                webSocketPublicMarketDepthsNew.OnOpen += WebSocketPublicMarketDepthsNew_OnOpen;
                webSocketPublicMarketDepthsNew.OnClose += WebSocketPublicMarketDepthsNew_OnClose;
                webSocketPublicMarketDepthsNew.OnMessage += WebSocketPublicMarketDepthsNew_OnMessage;
                webSocketPublicMarketDepthsNew.OnError += WebSocketPublicMarketDepthsNew_OnError;
                webSocketPublicMarketDepthsNew.Connect();

                return webSocketPublicMarketDepthsNew;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return null;
            }
        }

        private void CreatePublicWebSocketTradesConnect()
        {
            try
            {
                if (FIFOListWebSocketPublicTradesMessage == null)
                {
                    FIFOListWebSocketPublicTradesMessage = new ConcurrentQueue<string>();
                }

                _webSocketPublicTrades.Add(CreateNewPublicTradesSocket());
            }
            catch (Exception exception)
            {
                SendLogMessage($"{exception.Message} {exception.StackTrace}", LogMessageType.Error);
            }
        }

        private WebSocket CreateNewPublicTradesSocket()
        {
            try
            {
                Thread.Sleep(2000);

                if (_webSocketPublicTrades.Count >= 5)
                {
                    SendLogMessage(" WebSocket Trades limit exceeded: not creating new connection.", LogMessageType.Error);
                    return null;
                }


                DateTime now = DateTime.UtcNow;
                double secondsSinceLastConnect = (now - _lastWebSocketConnectTime).TotalSeconds;

                if (secondsSinceLastConnect < _minReconnectIntervalSec)
                {
                    double waitTime = _minReconnectIntervalSec - secondsSinceLastConnect;
                    SendLogMessage($" Delay before connecting to Trades WebSocket: {waitTime} сек.", LogMessageType.System);
                    Thread.Sleep((int)(waitTime * 1000));
                }

                _webSocketConnectAttempts++;
                _lastWebSocketConnectTime = DateTime.UtcNow;

                SendLogMessage($" Try to connect to WebSocket (Trades) #{_webSocketConnectAttempts} # {_lastWebSocketConnectTime}", LogMessageType.System);


                WebSocket webSocketPublicTradesNew = new WebSocket(_webSocketUrl);

                webSocketPublicTradesNew.EmitOnPing = true;
                webSocketPublicTradesNew.OnOpen += WebSocketPublicTradesNew_OnOpen;
                webSocketPublicTradesNew.OnClose += WebSocketPublicTradesNew_OnClose;
                webSocketPublicTradesNew.OnMessage += WebSocketPublicTradesNew_OnMessage;
                webSocketPublicTradesNew.OnError += WebSocketPublicTradesNew_OnError;
                webSocketPublicTradesNew.Connect();

                return webSocketPublicTradesNew;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return null;
            }
        }

        private void WebSocketPublicTradesNew_OnError(object sender, ErrorEventArgs e)
        {
            try
            {
                if (e.Exception != null)
                {
                    SendLogMessage($"AscendexSpot WebSocket Trades Error: {e.Exception}", LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("AscendexSpot Data socket Trades exception: " + exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPublicTradesNew_OnMessage(object sender, MessageEventArgs e)
        {
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect
                    || e?.Data == null
                    || string.IsNullOrEmpty(e.Data))
                {
                    return;
                }

                if (FIFOListWebSocketPublicTradesMessage == null)
                {
                    return;
                }


                if (e.Data.Contains("\"m\":\"ping\""))
                {
                    for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                    {
                        WebSocket socket = _webSocketPublicTrades[i];
                        if (socket.ReadyState == WebSocketState.Open)
                        {
                            socket.Send("{\"op\":\"pong\"}"); // правильно!

                        }
                    }
                    return;
                }

                FIFOListWebSocketPublicTradesMessage?.Enqueue(e.Data);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPublicTradesNew_OnClose(object sender, CloseEventArgs e)
        {
            try
            {
                Disconnect();

                SendLogMessage($"AscendexSpot Public Trades WebSocket closed by AscendexSpot. Code: {e.Code}", LogMessageType.Error);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPublicTradesNew_OnOpen(object sender, EventArgs e)
        {
            try
            {
                CheckActivationSockets();

                SendLogMessage("WebSocket public Trades AscendexSpot open.", LogMessageType.System);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void CreatePrivateWebSocketConnect()
        {
            try
            {
                Thread.Sleep(2000);

                if (_webSocketPrivate != null)
                {
                    return;
                }

                _webSocketPrivate = new WebSocket(_webSocketUrl);

                //if (_myProxy != null)
                //{
                //    _webSocketPrivate.SetProxy(_myProxy);
                //}

                _webSocketPrivate.EmitOnPing = true;
                _webSocketPrivate.OnOpen += _webSocketPrivate_OnOpen;
                _webSocketPrivate.OnClose += _webSocketPrivate_OnClose;
                _webSocketPrivate.OnMessage += _webSocketPrivate_OnMessage;
                _webSocketPrivate.OnError += _webSocketPrivate_OnError;

                _webSocketPrivate.Connect();
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }
        private void DeleteWebSocketConnection()
        {
            if (_webSocketPublicMarketDepths != null)
            {
                try
                {
                    for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
                    {
                        WebSocket webSocketPublicMarketDepthsNew = _webSocketPublicMarketDepths[i];

                        webSocketPublicMarketDepthsNew.OnOpen -= WebSocketPublicMarketDepthsNew_OnOpen;
                        webSocketPublicMarketDepthsNew.OnClose -= WebSocketPublicMarketDepthsNew_OnClose;
                        webSocketPublicMarketDepthsNew.OnMessage -= WebSocketPublicMarketDepthsNew_OnMessage;
                        webSocketPublicMarketDepthsNew.OnError -= WebSocketPublicMarketDepthsNew_OnError;

                        if (webSocketPublicMarketDepthsNew.ReadyState == WebSocketState.Open)
                        {
                            webSocketPublicMarketDepthsNew.CloseAsync();
                        }
                        webSocketPublicMarketDepthsNew = null;
                    }
                }
                catch
                {
                    // ignore
                }

                _webSocketPublicMarketDepths.Clear();
            }
            if (_webSocketPublicTrades != null)
            {
                try
                {
                    for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                    {
                        WebSocket webSocketPublicTradesNew = _webSocketPublicTrades[i];

                        webSocketPublicTradesNew.OnOpen -= WebSocketPublicTradesNew_OnOpen;
                        webSocketPublicTradesNew.OnClose -= WebSocketPublicTradesNew_OnClose;
                        webSocketPublicTradesNew.OnMessage -= WebSocketPublicTradesNew_OnMessage;
                        webSocketPublicTradesNew.OnError -= WebSocketPublicTradesNew_OnError;

                        if (webSocketPublicTradesNew.ReadyState == WebSocketState.Open)
                        {
                            webSocketPublicTradesNew.CloseAsync();
                        }
                        webSocketPublicTradesNew = null;
                    }
                }
                catch
                {
                    // ignore
                }

                _webSocketPublicTrades.Clear();
            }

            if (_webSocketPrivate != null)
            {
                try
                {
                    _webSocketPrivate.OnOpen -= _webSocketPrivate_OnOpen;
                    _webSocketPrivate.OnClose -= _webSocketPrivate_OnClose;
                    _webSocketPrivate.OnMessage -= _webSocketPrivate_OnMessage;
                    _webSocketPrivate.OnError -= _webSocketPrivate_OnError;

                    _webSocketPrivate.CloseAsync();
                }
                catch
                {
                    // ignore
                }

                _webSocketPrivate = null;
            }
        }

        private void GenerateAuthenticate()
        {
            try
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string payload = timestamp + "+stream";
                string signature = CreateSignatureBase64(_secretKey, payload);
                string idGuid = Guid.NewGuid().ToString();

                var auth = new
                {
                    op = "auth",
                    id = "auth-req" + idGuid,
                    t = timestamp,
                    key = _publicKey,
                    sig = signature
                };

                string authJson = JsonConvert.SerializeObject(auth);
                _webSocketPrivate.Send(authJson);

                SendLogMessage("Auth sent: " + authJson, LogMessageType.Trade);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        public static string CreateSignatureBase64(string secret, string payload)
        {
            byte[] keyBytes = Encoding.UTF8.GetBytes(secret);
            byte[] payloadBytes = Encoding.UTF8.GetBytes(payload);

            using (var hmac = new HMACSHA256(keyBytes))
            {
                byte[] hash = hmac.ComputeHash(payloadBytes);
                return Convert.ToBase64String(hash);
            }
        }

        #endregion

        #region 7 WebSocket events

        //private void WebSocketPublicNew_OnOpen(object sender, EventArgs e)
        //{
        //    SendLogMessage("Ascendex WebSocket Public connected", LogMessageType.System);
        //    //webSocketPublic.Send($"{{\"op\":\"sub\",\"ch\":\"depth:{security.Name}\"}}");
        //    //webSocketPublic.Send($"{{\"op\":\"sub\",\"ch\":\"trades:{security.Name}\"}}");
        //    _webSocketPublic[0].Send("${\"op\":\"sub\",\"ch\":\"trades:{BTC/USDT}\"}"); 
        //    _webSocketPublic[0].Send("${\"op\":\"sub\",\"ch\":\"depth:{BTC/USDT}\"}");
        //}
        private void WebSocketPublicMarketDepthsNew_OnOpen(object sender, EventArgs e)
        {
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    CheckActivationSockets();
                    SendLogMessage("AscendexSpot WebSocket public MarketDepths connection open", LogMessageType.System);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }
        private void WebSocketPublicMarketDepthsNew_OnClose(object sender, CloseEventArgs e)
        {
            try
            {
                Disconnect();

                SendLogMessage($"Public MarketDeptns WebSocket closed by AscendexSpot. Code:{e.Code}", LogMessageType.Error);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }
        private void WebSocketPublicMarketDepthsNew_OnMessage(object sender, MessageEventArgs e)
        {
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect
                    || e?.Data == null
                    || string.IsNullOrEmpty(e.Data))
                {
                    return;
                }

                if (FIFOListWebSocketPublicMarketDepthsMessage == null)
                {
                    return;
                }

                if (e.IsText && e.Data.Contains("\"m\":\"connected\""))
                {
                    if (e.Data.Contains("\"type\":\"unauth\""))
                    {

                        SendLogMessage("WebSocket publicDepth opened", LogMessageType.System);
                    }

                    else
                    {
                        ServerStatus = ServerConnectStatus.Disconnect;
                        DisconnectEvent();
                        SendLogMessage($"WebSocket publicDepth  error {e.Data}", LogMessageType.Error);
                    }
                }

                if (e.Data.Contains("\"m\":\"ping\""))
                {
                    for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
                    {
                        WebSocket socket = _webSocketPublicMarketDepths[i];

                        if (socket.ReadyState == WebSocketState.Open)
                        {
                            socket.Send("{\"op\":\"pong\"}"); // правильно!
                            SendLogMessage(">>> [Pong] Responded to server ping", LogMessageType.System);
                        }
                    }

                    return;
                }

                if (e.IsText)
                {
                    FIFOListWebSocketPublicMarketDepthsMessage.Enqueue(e.Data);
                }

            }
            catch (Exception error)
            {
                SendLogMessage(error.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPublicMarketDepthsNew_OnError(object sender, ErrorEventArgs e)///переделать как в битфайнекс
        {
            if (e.Exception != null)
            {
                SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
            }
            else
            {
                SendLogMessage("AscendexSpot WebSocket Public error" + e.ToString(), LogMessageType.Error);
            }
        }

        private void _webSocketPrivate_OnOpen(object sender, EventArgs e)
        {
            try
            {

                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    GenerateAuthenticate();
                    CheckActivationSockets();

                    SendLogMessage("Connection to private data is Open", LogMessageType.System);

                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }

        }
        private void _webSocketPrivate_OnClose(object sender, CloseEventArgs e)
        {
            try
            {
                Disconnect();

                SendLogMessage($"Connection Closed by AscendexSpot. {e.Code} {e.Reason}. WebSocket Private Closed Event", LogMessageType.Error);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }
        private void _webSocketPrivate_OnMessage(object sender, MessageEventArgs e)
        {
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect
                   || e?.Data == null
                   || string.IsNullOrEmpty(e?.Data))
                {
                    return;
                }

                if (FIFOListWebSocketPrivateMessage == null)
                {
                    return;
                }

                else if (e.IsText && e.Data.Contains("\"op\":\"auth\"") && e.Data.Contains("\"code\":0"))
                {

                    SendLogMessage("Authorization to private channels", LogMessageType.System);
                }

                if (e.Data.Contains("\"m\":\"ping\""))
                {
                    if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
                    {
                        _webSocketPrivate.Send("{\"op\":\"pong\"}");
                        SendLogMessage(">>> [Pong] Responded to server ping (Private socket)", LogMessageType.System);
                    }
                    return;
                }


                FIFOListWebSocketPrivateMessage.Enqueue(e.Data);

            }
            catch (Exception error)
            {
                SendLogMessage(error.ToString(), LogMessageType.Error);

            }
        }
        private void _webSocketPrivate_OnError(object sender, ErrorEventArgs e)
        {
            try

            {
                if (e.Exception != null)
                {
                    SendLogMessage($"WebSocket private Error: {e.Exception}", LogMessageType.Error);
                }

            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        #endregion


        private string _socketActivateLocker = "socketActivateLocker";
        private List<string> _subscribedSecurities = new List<string>();
        private void CheckActivationSockets()
        {
            lock (_socketActivateLocker)
            {
                try
                {
                    if (_webSocketPrivate == null
                       || _webSocketPrivate?.ReadyState != WebSocketState.Open)
                    {
                        Disconnect();
                        return;
                    }

                    if (_webSocketPublicMarketDepths.Count == 0)
                    {
                        Disconnect();
                        return;
                    }

                    WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[0];


                    if (webSocketPublicMarketDepths == null
                        || webSocketPublicMarketDepths?.ReadyState != WebSocketState.Open)
                    {
                        Disconnect();
                        return;
                    }

                    if (_webSocketPublicTrades.Count == 0)
                    {
                        Disconnect();
                        return;
                    }

                    WebSocket webSocketPublicTrades = _webSocketPublicTrades[0];

                    if (webSocketPublicTrades == null
                        || webSocketPublicTrades?.ReadyState != WebSocketState.Open)
                    {
                        Disconnect();
                        return;
                    }

                    if (ServerStatus != ServerConnectStatus.Connect)
                    {
                        ServerStatus = ServerConnectStatus.Connect;
                        ConnectEvent();
                    }

                    SendLogMessage("All sockets activated.", LogMessageType.System);
                }
                catch (Exception exception)
                {
                    SendLogMessage(exception.Message, LogMessageType.Error);
                }
            }
        }



        #region 8 WebSocket check alive
        private void CheckAliveWebSocket()
        {
            while (true)
            {
                try
                {
                    Thread.Sleep(2000);

                    if (ServerStatus == ServerConnectStatus.Disconnect)
                    {
                        continue;
                    }

                    for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
                    {
                        WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[i];
                        if (webSocketPublicMarketDepths != null
                            && webSocketPublicMarketDepths?.ReadyState == WebSocketState.Open)
                        {

                            webSocketPublicMarketDepths?.Send("{\"op\":\"ping\"}");
                        }
                        else
                        {
                            Disconnect();
                        }
                    }

                    for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                    {
                        WebSocket webSocketPublicTrades = _webSocketPublicTrades[i];
                        if (webSocketPublicTrades != null
                            && webSocketPublicTrades?.ReadyState == WebSocketState.Open)
                        {

                            webSocketPublicTrades?.Send("{\"op\":\"ping\"}");
                        }
                        else
                        {
                            Disconnect();
                        }

                    }


                    if (_webSocketPrivate != null
                        && (_webSocketPrivate.ReadyState == WebSocketState.Open
                    || _webSocketPrivate.ReadyState == WebSocketState.Connecting))
                    {

                        _webSocketPrivate?.Send("{\"op\":\"ping\"}");
                    }
                    else
                    {
                        Disconnect();
                    }
                }
                catch (Exception error)
                {
                    SendLogMessage(error.ToString(), LogMessageType.Error);
                }
            }
        }

        #endregion

        #region  9  WebSocket security subscribe


        private RateGate _rateGateSubscribed = new RateGate(1, TimeSpan.FromMilliseconds(2500));

        public void Subscrible(Security security)//////ошибка в слове Subscrible
        {
            try
            {
                _rateGateSubscribed.WaitToProceed();

                CreateSubscribeMessageWebSocket(security);
                Thread.Sleep(100);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void CreateSubscribeMessageWebSocket(Security security)
        {
            try
            {
                _rateGateSubscribed.WaitToProceed();

                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    return;
                }

                for (int i = 0; i < _subscribedSecurities.Count; i++)
                {
                    if (_subscribedSecurities[i].Equals(security.Name))
                    {
                        return;
                    }
                }

                _subscribedSecurities.Add(security.Name);


                if (_webSocketPublicMarketDepths.Count == 0
                    || _webSocketPublicTrades.Count == 0)
                {
                    return;
                }

                WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[_webSocketPublicMarketDepths.Count - 1];
                WebSocket webSocketPublicTrades = _webSocketPublicTrades[_webSocketPublicTrades.Count - 1];


                int MaxWebSocketCount = 5; // максимум подписок на один сокет
                int MaxSubsPerSocket = 60;// Максимум подписок на один сокет


                if (webSocketPublicMarketDepths.ReadyState == WebSocketState.Open
                    && webSocketPublicTrades.ReadyState == WebSocketState.Open
                    && _subscribedSecurities.Count != 0
                    && _subscribedSecurities.Count % MaxSubsPerSocket == 0)
                {
                    //{
                    //    if (_webSocketPublic.Count >= MaxWebSocketCount)
                    //    {
                    //        SendLogMessage("WebSocket connections limit exceeded. Subscription will be postponed.", LogMessageType.Error);
                    //        return;
                    //    }
                    // creating a new socket
                    WebSocket newSocketMarketDepths = CreateNewPublicMarketDepthsSocket();
                    WebSocket newSocketTrades = CreateNewPublicTradesSocket();

                    DateTime timeEndMarketDepths = DateTime.Now.AddSeconds(10);
                    while (newSocketMarketDepths.ReadyState != WebSocketState.Open)
                    {
                        Thread.Sleep(500);

                        if (timeEndMarketDepths < DateTime.Now)
                        {
                            break;
                        }
                    }

                    if (newSocketMarketDepths.ReadyState == WebSocketState.Open)
                    {
                        _webSocketPublicMarketDepths.Add(newSocketMarketDepths);
                        webSocketPublicMarketDepths = newSocketMarketDepths;
                    }

                    DateTime timeEndTrades = DateTime.Now.AddSeconds(10);
                    while (newSocketTrades.ReadyState != WebSocketState.Open)
                    {
                        Thread.Sleep(500);

                        if (timeEndTrades < DateTime.Now)
                        {
                            break;
                        }
                    }

                    if (newSocketTrades.ReadyState == WebSocketState.Open)
                    {
                        _webSocketPublicTrades.Add(newSocketTrades);
                        webSocketPublicTrades = newSocketTrades;
                    }
                }

                if (webSocketPublicMarketDepths != null
                    && webSocketPublicTrades != null)
                {

                    webSocketPublicMarketDepths.Send($"{{\"op\":\"req\",\"action\":\"depth-snapshot\",\"args\":{{\"symbol\":\"{security.Name}\"}}}}");
                    webSocketPublicMarketDepths.Send($"{{\"op\":\"sub\",\"ch\":\"depth:{security.Name}\"}}");
                    webSocketPublicTrades.Send($"{{\"op\":\"sub\",\"ch\":\"trades:{security.Name}\"}}");
                    webSocketPublicTrades.Send($"{{\"op\":\"req\",\"action\":\"market-trades\",\"args\":{{\"symbol\":\"{security.Name}\"}}}}");//&&&&&&&&&&


                }

                if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
                {
                    _webSocketPrivate.Send("{\"op\":\"sub\",\"ch\":\"order:cash\"}");

                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        // Метод отписки от всех подписок
        private void UnsubscribeFromAllWebSockets()
        {
            try
            {
                // Проверка статуса подключения
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    return;
                }

                // Отписка от depth по всем публичным WebSocket'ам стакана
                for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
                {
                    WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[i];

                    // Проверяем, что сокет открыт
                    if (webSocketPublicMarketDepths != null && webSocketPublicMarketDepths.ReadyState == WebSocketState.Open)
                    {
                        try
                        {
                            // Если есть подписанные инструменты
                            if (_subscribedSecurities != null && _subscribedSecurities.Count > 0)
                            {
                                for (int j = 0; j < _subscribedSecurities.Count; j++)
                                {
                                    string symbol = _subscribedSecurities[j];

                                    // Отписка от стакана
                                    webSocketPublicMarketDepths.Send($"{{\"op\":\"unsub\",\"ch\":\"depth:{symbol}\"}}");
                                }
                            }
                        }
                        catch (Exception exception)
                        {
                            // Логируем ошибку конкретного сокета
                            SendLogMessage($"Unsubscribe error on public depth socket: {exception.Message} {exception.StackTrace}", LogMessageType.Error);
                        }
                    }
                }

                // Отписка от trades по всем публичным WebSocket'ам сделок
                for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                {
                    WebSocket webSocketPublicTrades = _webSocketPublicTrades[i];

                    // Проверяем, что сокет открыт
                    if (webSocketPublicTrades != null && webSocketPublicTrades.ReadyState == WebSocketState.Open)
                    {
                        try
                        {
                            // Если есть подписанные инструменты
                            if (_subscribedSecurities != null && _subscribedSecurities.Count > 0)
                            {
                                for (int j = 0; j < _subscribedSecurities.Count; j++)
                                {
                                    string symbol = _subscribedSecurities[j];

                                    // Отписка от сделок
                                    webSocketPublicTrades.Send($"{{\"op\":\"unsub\",\"ch\":\"trades:{symbol}\"}}");
                                }
                            }

                            // Очищаем словарь сделок
                            // _tradeDictionary.Clear();
                        }
                        catch (Exception exception)
                        {
                            SendLogMessage($"Unsubscribe error on public trades socket: {exception.Message} {exception.StackTrace}", LogMessageType.Error);
                        }
                    }
                }

                // Отписка от приватных каналов (ордеры)
                if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
                {
                    try
                    {
                        _webSocketPrivate.Send("{\"op\":\"unsub\",\"ch\":\"order:cash\"}");
                    }
                    catch (Exception ex)
                    {
                        SendLogMessage($"Unsubscribe error on private socket: {ex.Message} {ex.StackTrace}", LogMessageType.Error);
                    }
                }

                // Очищаем список подписанных инструментов
                _subscribedSecurities.Clear();

                // Уведомляем об успешной отписке
                SendLogMessage("All subscriptions have been successfully removed", LogMessageType.System);
            }
            catch (Exception exception)
            {
                // Общая ошибка
                SendLogMessage($"General unsubscribe error: {exception.Message} {exception.StackTrace}", LogMessageType.Error);
            }
        }


        #endregion

        #region  10 WebSocket parsing the messages

        public event Action<List<Security>> SecurityEvent;
        public event Action<News> NewsEvent;
        public event Action<MarketDepth> MarketDepthEvent;
        public event Action<Trade> NewTradesEvent;
        public event Action<Order> MyOrderEvent;
        public event Action<MyTrade> MyTradeEvent;
        public event Action<OptionMarketDataForConnector> AdditionalMarketDataEvent;


        // Обработка стакана: инициализация снапшотом и обновлениями
        private Dictionary<string, AscendexSpotDepthResponse> _depths = new Dictionary<string, AscendexSpotDepthResponse>();

        // Храним все активные MarketDepth по инструментам
        private List<MarketDepth> _allDepths = new List<MarketDepth>();
        private bool _snapshotInitialized = false;
        private long _lastSeqNum = -1;

        private DateTime _lastTimeMd = DateTime.MinValue;


        private void SnapshotDepth(string message)
        {
            try
            {
                AscendexSpotDepthMessage snapshot = JsonConvert.DeserializeObject<AscendexSpotDepthMessage>(message);

                // Если данные некорректны — выходим
                if (snapshot == null || snapshot.data == null)
                    return;

                // Обновляем текущий seqnum и флаг инициализации
                _lastSeqNum = Convert.ToInt64(snapshot.data.seqnum);
                _snapshotInitialized = true;

                // Создаем новый объект стакана
                MarketDepth newDepth = new MarketDepth();
                newDepth.SecurityNameCode = snapshot.symbol;
                newDepth.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(snapshot.data.ts));

                // Добавляем уровни BID (покупки)

                for (int i = 0; i < snapshot.data.bids.Count && i < 25; i++)
                {
                    var level = snapshot.data.bids[i];
                    newDepth.Bids.Add(new MarketDepthLevel
                    {
                        Price = level[0].ToDecimal(),
                        Bid = level[1].ToDecimal()
                    });
                }

                // Добавляем уровни ASK (продажи)
                for (int i = 0; i < snapshot.data.asks.Count && i < 25; i++)
                {
                    var level = snapshot.data.asks[i];
                    newDepth.Asks.Add(new MarketDepthLevel
                    {
                        Price = level[0].ToDecimal(),
                        Ask = level[1].ToDecimal()
                    });
                }


                newDepth.Time = DateTime.UtcNow;

                // если текущее время меньше или равно предыдущему — увеличиваем _lastTimeMd
                if (newDepth.Time <= _lastTimeMd)
                {
                    _lastTimeMd = _lastTimeMd.AddTicks(1);
                    newDepth.Time = _lastTimeMd;
                }
                else
                {
                    _lastTimeMd = newDepth.Time;
                }


                // Обновляем локальное хранилище стаканов
                var needDepth = _allDepths.Find(d => d.SecurityNameCode == newDepth.SecurityNameCode);

                if (needDepth != null)
                {
                    _allDepths.Remove(needDepth);
                }
                _allDepths.Add(newDepth);

                MarketDepthEvent?.Invoke(newDepth.GetCopy());
            }
            catch (Exception error)
            {
                SendLogMessage(error.ToString(), LogMessageType.Error);
            }

        }

        // Метод обновления стакана по дельте
        private void UpdateDepth(string json)
        {
            try
            {
                // Десериализуем входящее сообщение
                var update = JsonConvert.DeserializeObject<AscendexSpotDepthMessage>(json);

                // Находим соответствующий стакан
                var depth = _allDepths.Find(d => d.SecurityNameCode == update.symbol);

                if (depth == null)
                    return;

                if (update?.data == null || update.symbol != depth.SecurityNameCode)
                    return;

                if (!_snapshotInitialized) return;


                // Проверка: если seqnum пропущен — нужно обновить снапшот
                if (_lastSeqNum != -1 && Convert.ToInt64(update.data.seqnum) != _lastSeqNum + 1)
                {
                    _snapshotInitialized = false;
                    _lastSeqNum = -1;
                    RequestSnapshot(depth.SecurityNameCode);
                    return;
                }

                _lastSeqNum = Convert.ToInt64(update.data.seqnum);

                depth.Time = DateTime.UtcNow;

                if (depth.Time < _lastTimeMd)
                {
                    depth.Time = _lastTimeMd;
                }
                else if (depth.Time == _lastTimeMd)
                {
                    _lastTimeMd = DateTime.FromBinary(_lastTimeMd.Ticks + 1);

                    depth.Time = _lastTimeMd;
                }

                _lastTimeMd = depth.Time;

                // Применяем изменения
                ApplyLevels(update.data.bids, depth.Bids, isBid: true);
                ApplyLevels(update.data.asks, depth.Asks, isBid: false);

                // Обновляем время и передаём дальше
                depth.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(update.data.ts));
                //depth.Bids = depth.Bids.OrderByDescending(x => x.Price).Take(25).ToList();
                //depth.Asks = depth.Asks.OrderBy(x => x.Price).Take(25).ToList();
                // Сортировка бидов по убыванию и обрезка до 25
                depth.Bids.Sort((a, b) => b.Price.CompareTo(a.Price));

                List<MarketDepthLevel> topBids = new List<MarketDepthLevel>();
                for (int i = 0; i < depth.Bids.Count && i < 25; i++)
                {
                    topBids.Add(depth.Bids[i]);
                }
                depth.Bids = topBids;

                // Сортировка асков по возрастанию и обрезка до 25
                depth.Asks.Sort((a, b) => a.Price.CompareTo(b.Price));

                List<MarketDepthLevel> topAsks = new List<MarketDepthLevel>();
                for (int i = 0; i < depth.Asks.Count && i < 25; i++)
                {
                    topAsks.Add(depth.Asks[i]);
                }
                depth.Asks = topAsks;

                MarketDepthEvent?.Invoke(depth.GetCopy());
            }
            catch (Exception exception)
            {
                SendLogMessage("Depth of Market update error: " + exception.Message, LogMessageType.Error);
            }
        }


        // Метод запроса снапшота стакана
        private void RequestSnapshot(string symbol)
        {
            WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[_webSocketPublicMarketDepths.Count - 1];

            // Проверка, открыт ли сокет
            if (webSocketPublicMarketDepths.ReadyState == WebSocketState.Open)
            {
                webSocketPublicMarketDepths.Send($"{{\"op\":\"req\",\"action\":\"depth-snapshot\",\"args\":{{\"symbol\":\"{symbol}\"}}}}");
            }
        }


        // Метод применяет список изменений к уровням стакана
        private void ApplyLevels(List<List<string>> updates, List<MarketDepthLevel> levels, bool isBid)
        {
            for (int i = 0; i < updates.Count; i++)
            {
                decimal price = updates[i][0].ToDecimal();
                decimal size = updates[i][1].ToDecimal();

                var existing = levels.Find(x => x.Price == price);

                if (size == 0)
                {
                    if (existing != null)
                    {
                        levels.Remove(existing);
                    }
                }
                else
                {
                    if (existing != null)
                    {
                        if (isBid) existing.Bid = size;
                        else existing.Ask = size;
                    }
                    else
                    {
                        var level = new MarketDepthLevel { Price = price };
                        if (isBid) level.Bid = size;
                        else level.Ask = size;
                        levels.Add(level);
                    }
                }
            }

            if (isBid)
                levels.Sort((a, b) => b.Price.CompareTo(a.Price));
            else
                levels.Sort((a, b) => a.Price.CompareTo(b.Price));
        }

        // Вставка уровня вручную (если потребуется)
        private void InsertLevel(decimal price, decimal value, Side side, MarketDepth marketDepth)
        {
            var levels = side == Side.Buy ? marketDepth.Bids : marketDepth.Asks;
            var level = levels.Find(l => l.Price == price);

            if (level != null)
            {
                if (side == Side.Buy)
                    level.Bid = value;
                else
                    level.Ask = value;
            }
            else
            {
                level = new MarketDepthLevel();
                level.Price = price;
                if (side == Side.Buy) level.Bid = value;
                else level.Ask = value;
                levels.Add(level);
            }
        }

        // Удаление уровня по цене
        private void DeleteLevel(decimal price, Side side, MarketDepth marketDepth)
        {
            var levels = side == Side.Buy ? marketDepth.Bids : marketDepth.Asks;
            var level = levels.Find(l => l.Price == price);
            if (level != null)
                levels.Remove(level);
        }

        // Сортировка BID — по убыванию цены
        private void SortBids(List<MarketDepthLevel> levels)
        {
            levels.Sort((a, b) => b.Price.CompareTo(a.Price));
        }

        // Сортировка ASK — по возрастанию цены
        private void SortAsks(List<MarketDepthLevel> levels)
        {
            levels.Sort((a, b) => a.Price.CompareTo(b.Price));
        }

        private void UpdateTrade(string message)
        {
            try
            {

                AscendexSpotPublicTradesResponse response = JsonConvert.DeserializeObject<AscendexSpotPublicTradesResponse>(message);

                if (response == null || response.data == null || response.data == null)
                {
                    SendLogMessage("UpdateTrade> Received empty  json", LogMessageType.Error);
                    return;
                }

                for (int i = 0; i < response.data.Count; i++)
                {
                    AscendexSpotPublicTradeItem json = response.data[i];

                    Trade newTrade = new Trade();

                    newTrade.SecurityNameCode = response.symbol;
                    newTrade.Id = json.seqnum;
                    newTrade.Price = json.p.ToString().ToDecimal();
                    newTrade.Volume = json.q.ToString().ToDecimal();
                    newTrade.Side = (json.bm == "true") ? Side.Sell : Side.Buy;
                    newTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(json.ts));

                    NewTradesEvent?.Invoke(newTrade);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }



        private void UpdateMyTrade(string json)
        {
            try
            {
                AscendexSpotQueryOrderResponse response = JsonConvert.DeserializeObject<AscendexSpotQueryOrderResponse>(json);

                if (response == null || response.code != "0" || response.data == null /*|| response.data.Count == 0*/)
                {
                    SendLogMessage("UpdateMyTrade> Received empty or invalid json", LogMessageType.Error);
                    return;
                }


                //// Обрабатываем все сделки в цикле
                //for (int i = 0; i < response.data.Count; i++)
                //{
                //    AscendexSpotQueryOrderMessage item = response.data[i];

                MyTrade myTrade = new MyTrade();


                myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(response.data.lastExecTime));

                myTrade.SecurityNameCode = response.data.symbol;

                myTrade.Price = response.data.price.ToDecimal();

                myTrade.NumberTrade = response.data.seqNum;

                myTrade.NumberOrderParent = response.data.orderId;

                myTrade.Volume = response.data.orderQty.ToDecimal();

                myTrade.Side = (response.data.side == "Buy") ? Side.Buy : Side.Sell;

                //myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(item.lastExecTime));

                //myTrade.SecurityNameCode = item.symbol;

                //myTrade.Price = item.price.ToDecimal();

                //myTrade.NumberTrade = item.seqNum;

                //myTrade.NumberOrderParent = item.orderId;

                //myTrade.Volume = item.orderQty.ToDecimal();

                //myTrade.Side = (item.side == "Buy") ? Side.Buy : Side.Sell;


                MyTradeEvent?.Invoke(myTrade);

                SendLogMessage(myTrade.ToString(), LogMessageType.Trade);
                //}
            }

            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void UpdateOrder(WebSocketMessage<AscendexSpotOrderData> json)
        {

            try
            {
                if (json == null || json.m != "order" || json.data == null)
                {

                    SendLogMessage("UpdateOrder> Received empty json", LogMessageType.Error);
                    return;
                }
                //if (!_myOrderIds.Contains(json.data.orderId))
                //{
                //    SendLogMessage("SKIP not my order. MarketId = " + json.data.orderId, LogMessageType.System);
                //    return;
                //}


                if (json != null && json.m == "order" && json.data != null)
                {
                    //action": "cancel-Order",
                    //action": "cancel-All"

                    Order updateOrder = new Order();

                    var data = json.data;
                    OrderTracker tracker = _orderTracker.Find(t => t.OrderNumberMarket == json.data.orderId);

                    if (tracker != null)
                    {
                        updateOrder.NumberUser = tracker.OsOrderNumberUser;
                    }

                    updateOrder.SecurityNameCode = json.data.s;
                    updateOrder.SecurityClassCode = GetNameClass(json.data.s);
                    updateOrder.State = GetOrderState(json.data.st);
                    updateOrder.NumberMarket = json.data.orderId;
                    //updateOrder.NumberUser = _orderTracker.;
                    updateOrder.Side = (json.data.sd == "Buy") ? Side.Buy : Side.Sell;
                    updateOrder.TypeOrder = (json.data.ot == "Limit") ? OrderPriceType.Limit : OrderPriceType.Market;
                    updateOrder.Price = (json.data.p).ToDecimal();
                    updateOrder.Volume = (json.data.q).ToDecimal();
                    updateOrder.TimeCreate = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(json.data.t));
                    updateOrder.ServerType = ServerType.AscendexSpot;
                    updateOrder.PortfolioNumber = "AscendexSpotPortfolio";

                    MyOrderEvent?.Invoke(updateOrder);

                    if (json.data.st == "PartiallyFilled" || json.data.st == "Filled")
                    {

                        if (string.IsNullOrEmpty(data.cfq) || string.IsNullOrEmpty(data.ap))
                        {
                            SendLogMessage("UpdateOrder> Trade skipped due to missing data (cfq or ap)", LogMessageType.Error);
                            return;
                        }

                        MyTrade myTrade = new MyTrade
                        {
                            NumberOrderParent = data.orderId,
                            Side = data.sd == "Buy" ? Side.Buy : Side.Sell,
                            SecurityNameCode = data.s,
                            Price = data.ap.ToDecimal(),
                            Volume = data.cfq.ToDecimal(),
                            NumberTrade = data.sn.ToString(),
                            Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(data.t)),
                        };

                        MyTradeEvent?.Invoke(myTrade);
                    }
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void UpdatePortfolio(WebSocketMessage<AscendexSpotPortfolio> json)
        {
            try
            {
                if (json == null)
                {
                    return;
                }

                Portfolio portfolio = new Portfolio();

                portfolio.Number = "AscendexSpotPortfolio";
                portfolio.ValueBegin = 1;
                portfolio.ValueCurrent = 1;
                portfolio.ServerType = ServerType.AscendexSpot;


                if (json != null && json.m == "balance" && json.data != null)
                {

                    PositionOnBoard position = new PositionOnBoard();

                    position.PortfolioName = "AscendexSpotPortfolio";
                    position.SecurityNameCode = json.data.a;
                    position.ValueCurrent = json.data.ab.ToString().ToDecimal();
                    position.ValueBegin = json.data.tb.ToString().ToDecimal();

                    position.ValueBlocked = position.ValueBegin.ToString().ToDecimal() - position.ValueCurrent.ToString().ToDecimal();

                    portfolio.SetNewPosition(position);

                    _portfolios.Add(portfolio);
                }

                if (_portfolios.Count > 0)
                {
                    PortfolioEvent?.Invoke(_portfolios);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        #endregion

        //public void RemoveAllCompletedOrders(List<AscendexSpotOrderInfo> orders)
        //{
        //    for (int i = 0; i < orders.Count; i++)
        //    {
        //        RemoveCompletedOrder(orders[i].orderId, orders[i].status);
        //    }
        //}



        //public string GetOrderIdByNumberUser(int numberUser)
        //{
        //    if (userToOrderMap.TryGetValue(numberUser, out string orderId))
        //    {
        //        return orderId;
        //    }

        //    return null;
        //}

        //public int GetNumberUserByOrderId(string orderId)
        //{
        //    if (orderToUserMap.TryGetValue(orderId, out int numberUser))
        //    {
        //        return numberUser;
        //    }

        //    return 0; // Если не найден
        //}


        // Удалить связь, если ордер завершён
        //public void RemoveCompletedOrder(string orderId, string status)
        //{
        //    if (IsOrderFinal(status) && orderToUserMap.TryGetValue(orderId, out int numberUser))
        //    {
        //        // Удаляем ордер из обоих словарей
        //        orderToUserMap.Remove(orderId);
        //        userToOrderMap.Remove(numberUser);

        //        // Логируем удаление
        //        SendLogMessage($"[OrderLinkManager] Completed order removed: OrderId={orderId}, Status={status}", LogMessageType.Error);
        //    }
        //}

        // Массовое удаление завершённых ордеров
        //public void RemoveAllCompletedOrders(List<AscendexSpotOrderInfo> orders)
        //{
        //    for (int i = 0; i < orders.Count; i++)
        //    {
        //        RemoveCompletedOrder(orders[i].orderId, orders[i].status);
        //    }
        //}

        /// Возвращает true, если ордер завершён (неактивен)
        public bool IsOrderFinal(string status)
        {
            return status == "Canceled" ||
                   status == "Filled" ||
                   status == "Rejected" ||
                   status == "Expired" ||
                   status == "Failed";
        }


        #region  11 Trade
        public void SendOrder(Order order)

        {
            _rateGateOrder.WaitToProceed();
            try
            {
                string accountGroup = GetAccountGroup();
                long time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string orderSide = order.Side == Side.Buy ? "Buy" : "Sell";
                string typeOrder = order.TypeOrder == OrderPriceType.Limit ? "Limit" : "Market";

                string body;

                //  if (order.TypeOrder == OrderPriceType.Limit)
                if (typeOrder == "Limit")
                {
                    body = $"{{" +
                                  $"\"id\": \"{order.NumberUser.ToString()}\", " +
                                  $"\"time\": {time}, " +
                                  $"\"symbol\": \"{order.SecurityNameCode}\", " +
                                  $"\"orderPrice\": \"{order.Price.ToString(CultureInfo.InvariantCulture)}\", " +
                                  $"\"orderQty\": \"{order.Volume.ToString(CultureInfo.InvariantCulture)}\", " +
                                  $"\"orderType\": \"{typeOrder}\", " +
                                  $"\"side\": \"{orderSide}\"" +
                                  $"}}";
                }
                else
                {
                    body = $"{{" +
                                  $"\"id\": \"{order.NumberUser.ToString()}\", " +
                                  $"\"time\": {time}, " +
                                  $"\"symbol\": \"{order.SecurityNameCode}\", " +
                                  $"\"orderQty\": \"{order.Volume.ToString(CultureInfo.InvariantCulture)}\", " +
                                  $"\"orderType\": \"{typeOrder}\", " +
                                  $"\"side\": \"{orderSide}\"" +
                                  $"}}";

                }

                string fullPath = $"/{accountGroup}/api/pro/v1/cash/order";

                IRestResponse request = CreatePrivateQuery(fullPath, body, accountGroup, null, Method.POST);

                if (request == null)
                {
                    SendLogMessage("Deserialization resulted in null", LogMessageType.Error);
                    return;
                }


                AscendexSpotOrderResponse response = JsonConvert.DeserializeObject<AscendexSpotOrderResponse>(request.Content);

                if (request.StatusCode == HttpStatusCode.OK && response.code != "0")//844629
                {
                    SendLogMessage($"Error : {request.ErrorMessage}, StatusCode {request.StatusCode}", LogMessageType.Error);
                    order.State = OrderStateType.Fail;
                    MyOrderEvent?.Invoke(order);
                }

                if (response != null && response.code == "0" && response.data != null)
                {
                    SendLogMessage($" Order send: status {response.data.status} OrderId :{response.data.info.orderId}", LogMessageType.Error);//trade


                    if (response.data.status == "Ack" || response.data.status == "New" || response.data.info.status == "Done")
                    {
                        var orderTracker = new OrderTracker()
                        {
                            OsOrderNumberUser = order.NumberUser,
                            OrderNumberMarket = response.data.info.orderId
                        };

                        _orderTracker.Add(orderTracker);
                        order.State = GetOrderState(response.data.status);
                        order.NumberMarket = response.data.info.orderId;

                        // Записываем построчно в CSV-файл с датой
                        try
                        {
                            string time1 = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"); // текущее время
                            string line = $"{time1},{orderTracker.OsOrderNumberUser},{orderTracker.OrderNumberMarket}";

                            string path = "order_trackers.txt";

                            // Проверяем, нужно ли добавить заголовки
                            bool addHeader = !System.IO.File.Exists(path);

                            using (System.IO.StreamWriter writer = new System.IO.StreamWriter(path, true)) // append = true
                            {
                                if (addHeader)
                                {
                                    writer.WriteLine("Time,OsOrderNumberUser,OrderNumberMarket");
                                }

                                writer.WriteLine(line);
                            }
                        }
                        catch (Exception ex)
                        {
                            SendLogMessage("Ошибка при записи в order_trackers.txt: " + ex.Message, LogMessageType.Error);
                        }


                        MyOrderEvent?.Invoke(order);
                    }

                    //GetOrderState(order.NumberMarket);
                    MyOrderEvent?.Invoke(order);
                }
                //else
                //{
                //    SendLogMessage($"Error Send Order : {message}, StatusCode {code}", LogMessageType.Error);
                //    order.State = OrderStateType.Fail;
                //    MyOrderEvent?.Invoke(order);
                //}
            }
            catch (Exception exception)
            {
                SendLogMessage("Order send exception " + exception.ToString(), LogMessageType.Error);
            }

        }
        private RateGate _rateGateCancelOrder = new RateGate(1, TimeSpan.FromMilliseconds(7000));
        public void CancelAllOrders()
        {
            try
            {
                _rateGateCancelOrder.WaitToProceed();

                string accountGroup = GetAccountGroup();

                string accountCategory = "cash";

                string path = $"/{accountGroup}/api/pro/v1/{accountCategory}/order/all";


                IRestResponse response = CreatePrivateQuery(path, null, accountGroup, accountCategory, Method.DELETE/*, _myProxy*/);

                if (response == null)
                {
                    return;
                }

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                    if (cancelResult != null && cancelResult.code == "0")
                    {
                        SendLogMessage($"All active orders cancelled", LogMessageType.Error);
                        GetPortfolios();
                    }
                    else
                    {
                        SendLogMessage($"Error: code={cancelResult?.code}", LogMessageType.Error);
                    }
                }
                else
                {
                    SendLogMessage($"Error Order canceled {response.StatusCode}", LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Order canceled exception " + exception.ToString(), LogMessageType.Error);
            }
        }

        public void CancelOrder(Order order)
        {
            try
            {
                _rateGateCancelOrder.WaitToProceed();
                string accountGroup = GetAccountGroup();

                string path = $"/{accountGroup}/api/pro/v1/cash/order";

                long time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string body;


                if (order.TypeOrder == OrderPriceType.Limit)
                {
                    body = $"{{" +
                           $"\"orderId\": \"{order.NumberMarket.ToString()}\", " +                   // orderId — обязательный
                           $"\"orderType\": \"{order.TypeOrder.ToString()}\", " +                   // orderType — только если это Limit
                           $"\"symbol\": \"{order.SecurityNameCode}\", " +                          // symbol — обязательный
                           $"\"time\": {time}, " +                                                  // time — обязательный
                           $"\"orderNumberUser\": \"{order.NumberUser.ToString()}\"" +              // НЕобязательное поле (если оно поддерживается)
                           $"}}";
                }
                else
                {
                    body = $"{{" +
                           $"\"orderId\": \"{order.NumberMarket.ToString()}\", " +                  // orderId
                           $"\"symbol\": \"{order.SecurityNameCode}\", " +                          // symbol
                           $"\"time\": {time}, " +                                                  // time
                           $"\"orderNumberUser\": \"{order.NumberUser.ToString()}\"" +              // user id
                           $"}}";
                }

                IRestResponse response = CreatePrivateQuery(path, body, accountGroup, null, Method.DELETE/*, _myProxy*/);
                //{\"code\":100004,\"message\":\"Invalid Http Request Input\"}"

                if (response == null)
                {

                    // GetOrderStatus(order);
                    //  GetOrderState(order.NumberMarket);
                    // SendLogMessage("CancelOrder> Deserialization resulted in null", LogMessageType.Error);
                    return;
                }



                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                    if (cancelResult != null && cancelResult.code == "0")
                    {
                        if (cancelResult.data.status == "Ack")
                        {
                            //OrderTracker OrderTracker = _orderTracker.Find(c => c.OrderNumberMarket == cancelResult.data.info.orderId);


                            //if (OrderTracker == null)
                            //{

                            //}
                            Order cancelOrd = new Order();

                            cancelOrd.NumberMarket = cancelResult.data.info.orderId;
                            cancelOrd.NumberUser = order.NumberUser;//(OrderTracker.OsOrderNumberUser).ToString(); 
                            cancelOrd.TimeCancel = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(cancelResult.data.info.timestamp));

                            //CancelOrderSuccessResponse success = JsonConvert.DeserializeObject<CancelOrderSuccessResponse>(response.Content);

                            SendLogMessage("The order has been cancelled . OrderId: " + cancelOrd.NumberMarket + "NumberUser:" + cancelOrd.NumberUser, LogMessageType.Error);

                            GetOrderStatus(order);

                        }
                    }
                    else
                    {
                        SendLogMessage($" Cancel error: code={response.StatusCode},message {response.Content},{response.ErrorMessage} ", LogMessageType.Error);
                        GetOrderStatus(order);
                    }
                }
                else
                {
                    GetOrderStatus(order);

                    SendLogMessage($" Error Order cancellation:  {response.Content},{response.ErrorMessage}", LogMessageType.Error);
                }

                GetPortfolios();
            }

            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        public void CancelAllOrdersToSecurity(Security security)
        {
            try
            {
                string accountGroup = GetAccountGroup();
                string accountCategory = "cash";

                string path = $"/{accountGroup}/api/pro/v1/{accountCategory}/order/all";

                string body = $"{{" +
                              $"\"symbol\": \"{security.Name}\"" +
                              $"}}";


                IRestResponse response = CreatePrivateQuery(path, body, accountGroup, accountCategory, Method.DELETE/*, _myProxy*/);

                if (response == null)
                {
                    SendLogMessage(" Error: No response from server.", LogMessageType.Error);
                    return;
                }

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                    if (cancelResult != null && cancelResult.code == "0")
                    {
                        SendLogMessage($" Orders cancelled: {cancelResult.data.info.orderId} |Status: {cancelResult.data.status}", LogMessageType.Error);

                    }
                    else
                    {
                        SendLogMessage($" Cancel error: code={cancelResult?.code}", LogMessageType.Error);
                    }
                }
                else
                {
                    SendLogMessage($" Error: {response.StatusCode} : {response.Content}", LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        public void ChangeOrderPrice(Order order, decimal newPrice)
        {

        }

        public List<Order> GetAllOpenOrders()
        {
            _rateGateOrder.WaitToProceed();
            try
            {
                List<Order> orders = new List<Order>();

                string accountGroup = GetAccountGroup();
                string accountCategory = "cash";
                string path = $"/{accountGroup}/api/pro/v1/cash/order/open";

                IRestResponse response = CreatePrivateQuery(path, null, accountGroup, accountCategory, Method.GET/*, _myProxy*/);

                if (response == null)
                {
                    SendLogMessage($" {response.StatusCode}", LogMessageType.Error);
                    return new List<Order>();
                }

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotOpenOrdersResponse result = JsonConvert.DeserializeObject<AscendexSpotOpenOrdersResponse>(response.Content);

                    if (result != null && result.code == "0")
                    {
                        if (result.data.Count == 0)
                        {
                            return new List<Order>();
                        }

                        for (int i = 0; i < result.data.Count; i++)
                        {
                            AscendexSpotOrderInfo order = result.data[i];
                            OrderTracker orderTracker = _orderTracker.Find(c => c.OrderNumberMarket == order.orderId);


                            if (orderTracker == null)
                            {
                                SendLogMessage("orderTracker == null", LogMessageType.Error);
                            }

                            Order activeOrder = new Order();
                            activeOrder.TimeCallBack = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(order.lastExecTime));
                            activeOrder.ServerType = ServerType.AscendexSpot;
                            activeOrder.SecurityNameCode = order.symbol;
                            activeOrder.NumberMarket = order.orderId;
                            activeOrder.NumberUser = orderTracker.OsOrderNumberUser; //GetNumberUserByOrderId(order.orderId);
                            activeOrder.Side = order.side == "Buy" ? Side.Buy : Side.Sell;
                            activeOrder.State = GetOrderState(order.status);
                            activeOrder.TypeOrder = order.orderType == "Limit" ? OrderPriceType.Limit : OrderPriceType.Market;
                            activeOrder.Volume = (order.orderQty).ToDecimal();
                            activeOrder.Price = order.price.ToDecimal();
                            activeOrder.PortfolioNumber = "AscendexSpotPortfolio";

                            orders.Add(activeOrder);


                            try
                            {
                                string time1 = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"); // текущее время
                                string line = $"{time1},{order.orderId},{result.accountId}";

                                string path1 = "ActiveOrder.txt";

                                // Проверяем, нужно ли добавить заголовки
                                bool addHeader = !System.IO.File.Exists(path1);

                                using (System.IO.StreamWriter writer = new System.IO.StreamWriter(path1, true)) // append = true
                                {
                                    if (addHeader)
                                    {
                                        writer.WriteLine("Time,OsOrderNumberUser,OrderNumberMarket");
                                    }

                                    writer.WriteLine(line);
                                }
                            }
                            catch { SendLogMessage(" не могу записать ActiveOrder.txt", LogMessageType.Error); }
                        }
                    }
                    else
                    {

                        SendLogMessage($" GetOrderStatus Error: code={result?.code}, {response.Content}", LogMessageType.Error);
                    }
                }
                else
                {
                    SendLogMessage($" HTTP Error:{response.StatusCode},{response.Content}", LogMessageType.Error);

                }

                for (int i = 0; i < orders.Count; i++)
                {
                    MyOrderEvent?.Invoke(orders[i]);
                }

                return orders;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return new List<Order>();
            }
        }
        public void GetAllActivOrders()
        {
            List<Order> orders = GetAllOpenOrders();

            if (orders == null
                || orders.Count == 0)
            {
                return;
            }

            for (int i = 0; i < orders.Count; i++)
            {
                MyOrderEvent?.Invoke(orders[i]);
            }
        }

        public void GetOrderStatus(Order order)
        {
            try
            {
                if (order == null)
                {
                    SendLogMessage("GetOrderStatus > Order is null", LogMessageType.Error);
                    return;
                }

                if (string.IsNullOrWhiteSpace(order.NumberMarket))
                {
                    SendLogMessage("GetOrderStatus > Order.NumberMarket is empty", LogMessageType.Error);
                    return;
                }

                Order orderOnMarket = null;

                List<Order> ordersActive = GetAllOpenOrders();

                if (ordersActive != null)
                {
                    for (int i = 0; i < ordersActive.Count; i++)
                    {
                        if (ordersActive[i].NumberMarket == order.NumberMarket)
                        {
                            orderOnMarket = ordersActive[i];
                            break;
                        }
                    }
                }

                if (orderOnMarket == null)
                {
                    List<Order> ordersHistory = GetHistoryOrders();

                    if (ordersHistory != null)
                    {
                        for (int i = 0; i < ordersHistory.Count; i++)
                        {
                            if (ordersHistory[i].NumberMarket == order.NumberMarket)
                            {
                                orderOnMarket = ordersHistory[i];
                                break;
                            }
                        }
                    }
                }

                if (orderOnMarket == null)
                {
                    SendLogMessage($"GetOrderStatus > Order not found: {order.NumberMarket}", LogMessageType.Error);
                    return;
                }

                MyOrderEvent?.Invoke(orderOnMarket);

                if (orderOnMarket.State == OrderStateType.Done || orderOnMarket.State == OrderStateType.Partial)
                {
                    MyTrade myTrade = new MyTrade();

                    myTrade.SecurityNameCode = orderOnMarket.SecurityNameCode;
                    myTrade.NumberOrderParent = orderOnMarket.NumberMarket;
                    myTrade.Price = orderOnMarket.Price.ToString().ToDecimal();
                    myTrade.Volume = orderOnMarket.VolumeExecute.ToString().ToDecimal();
                    myTrade.Side = orderOnMarket.Side;
                    myTrade.Time = orderOnMarket.TimeDone;

                    MyTradeEvent?.Invoke(myTrade);

                    SendLogMessage(myTrade.ToString(), LogMessageType.Trade);
                }
            }
            catch (Exception ex)
            {
                SendLogMessage("GetOrderStatus > Exception: " + ex.Message, LogMessageType.Error);
            }
        }


        public Order GetOrderStatusById(Order order)
        {
            try
            {

                if (order == null)
                {
                    SendLogMessage("GetOrderStatus> Order is null", LogMessageType.Error);
                    return new Order();
                }

                string accountGroup = GetAccountGroup();
                string accountCategory = "cash";

                string path = $"/{accountGroup}/api/pro/v1/cash/order/status?orderId={order.NumberMarket}";


                IRestResponse request = CreatePrivateQuery(path, null, accountGroup, accountCategory, Method.GET);

                if (request == null)
                {

                    SendLogMessage("GetOrderStatus> Request returned null", LogMessageType.Error);
                    return new Order();
                }


                if (request.StatusCode == HttpStatusCode.OK)
                {

                    AscendexSpotQueryOrderResponse response = JsonConvert.DeserializeObject<AscendexSpotQueryOrderResponse>(request.Content);

                    if (response != null)
                    {
                        SendLogMessage($"Error status order: {response.code}, message: {request.Content}", LogMessageType.Error);
                        return new Order();
                    }

                    if (response != null && response.data != null && response.code == "0")
                    {
                        AscendexSpotQueryOrderMessage orderData = response.data;

                        order.SecurityNameCode = orderData.symbol;
                        order.State = GetOrderState(orderData.status);
                        order.NumberMarket = orderData.orderId;
                        order.Price = orderData.price.ToDecimal();
                        //order.NumberUser = order.NumberUser; //GetNumberUserByOrderId(orderData.orderId);
                        order.PortfolioNumber = "AscendexSpotPortfolio";
                        order.SecurityClassCode = GetNameClass(orderData.symbol);
                        order.Side = orderData.side == "Buy" ? Side.Buy : Side.Sell;
                        order.TypeOrder = orderData.orderType == "Limit" ? OrderPriceType.Limit : OrderPriceType.Market;
                        order.Volume = orderData.orderQty.ToDecimal();
                        order.TimeCallBack = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(orderData.lastExecTime));


                        SendLogMessage($"Order Status: {orderData.orderId} | Статус: {orderData.status}", LogMessageType.Error);

                    }
                    else
                    {
                        SendLogMessage("GetOrderStatus> response.data is null", LogMessageType.Error);

                    }
                }
                else
                {
                    SendLogMessage($"HTTP Error: {request.StatusCode}, content={request.Content}", LogMessageType.Error);
                }

                /* MyOrderEvent?.Invoke(order);*/
                return order;
            }
            catch (Exception exception)
            {

                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return new Order();
            }
        }

        private RateGate _rateGateOrder = new RateGate(1, TimeSpan.FromMilliseconds(7000));

        //private void CreateMyTrade(string symbol, int numberUser)
        //{
        //    _rateGateOrder.WaitToProceed();

        //    try
        //    {
        //        //string fullpath = $"/api/pro/v1/trades";


        //        IRestResponse request = CreatePublicQuery(fullpath, Method.GET);

        //        if (request.StatusCode == HttpStatusCode.OK)
        //        {
        //            AscendexSpotOrderResponse response = JsonConvert.DeserializeObject<AscendexSpotOrderResponse>(request.Content);

        //            if (response != null && response.code == "0" && response.data != null)
        //            {
        //                int numUser = GetNumberUserByOrderId(response.data.info.orderId);

        //                if (numberUser == numUser)
        //                {
        //                    MyTrade myTrade = new MyTrade();

        //                    myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(response.data.info.lastExecTime));
        //                    myTrade.SecurityNameCode = response.data.info.symbol;
        //                    myTrade.NumberOrderParent = response.data.info.orderId;
        //                    myTrade.Price = (response.data.info.price).ToDecimal();
        //                    myTrade.NumberTrade = response.data.info.seqNum;
        //                    myTrade.Volume = (response.data.info.cumFilledQty).ToDecimal();
        //                    myTrade.Side = (response.data.info.side) == "Buy" ? Side.Buy : Side.Sell;
        //                    string commissionSecName = response.data.info.cumFee;

        //                    myTrade.Volume = (myTrade.Volume + commissionSecName).ToDecimal();

        //                    MyTradeEvent?.Invoke(myTrade);
        //                }

        //            }
        //            else
        //            {
        //                SendLogMessage($"CreateMyTrade>. Http State Code: {response.data.info.errorCode}", LogMessageType.Error);
        //            }
        //        }

        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}
        public List<Order> GetHistoryOrders()
        {
            _rateGateOrder.WaitToProceed();
            try
            {
                List<Order> orders = new List<Order>();

                string accountGroup = GetAccountGroup();
                string accountCategory = "cash";
                string path = $"/{accountGroup}/api/pro/v1/cash/order/hist/current";


                IRestResponse response = CreatePrivateQuery(path, null, accountGroup, accountCategory, Method.GET/*, _myProxy*/);

                if (response == null)
                {
                    SendLogMessage($" response is null", LogMessageType.Error);
                    return new List<Order>();
                }
                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotOpenOrdersResponse result = JsonConvert.DeserializeObject<AscendexSpotOpenOrdersResponse>(response.Content);

                    if (result != null && result.code == "0")
                    {
                        if (result.data.Count == 0)
                        {
                            return new List<Order>();
                        }

                        for (int i = 0; i < result.data.Count; i++)
                        {
                            AscendexSpotOrderInfo order = result.data[i];

                            Order historyOrder = new Order();
                            historyOrder.NumberMarket = order.orderId;
                            ///historyOrder.NumberUser = //orderTracker.OsOrderNumberUser;// GetNumberUserByOrderId(order.orderId);
                            historyOrder.TimeCallBack = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(order.lastExecTime));
                            historyOrder.ServerType = ServerType.AscendexSpot;
                            historyOrder.SecurityNameCode = order.symbol;
                            historyOrder.Side = order.side == "Buy" ? Side.Buy : Side.Sell;
                            historyOrder.State = GetOrderState(order.status);
                            historyOrder.Volume = (order.orderQty).ToDecimal();
                            historyOrder.Price = order.price.ToDecimal();
                            historyOrder.PortfolioNumber = "AscendexSpotPortfolio";
                            historyOrder.TypeOrder = order.orderType == "Limit" ? OrderPriceType.Limit : OrderPriceType.Market;
                            orders.Add(historyOrder);

                            try
                            {
                                string time1 = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"); // текущее время
                                string line = $"{time1},{order.orderId},{result.accountId},{order.orderType},{order.status}";

                                string path2 = "HistoryOrders.txt";

                                // Проверяем, нужно ли добавить заголовки
                                bool addHeader = !System.IO.File.Exists(path2);

                                using (System.IO.StreamWriter writer = new System.IO.StreamWriter(path2, true)) // append = true
                                {
                                    if (addHeader)
                                    {
                                        writer.WriteLine("Time,OrderNumber,AccoundId");
                                    }

                                    writer.WriteLine(line);
                                }
                            }
                            catch (Exception ex)
                            {
                                SendLogMessage("Ошибка при записи в HistoryOrders.txt: " + ex.Message, LogMessageType.Error);
                            }
                        }
                    }
                    else
                    {

                        SendLogMessage($"GetOrderStatus Error: code={result?.code}, {response.Content}", LogMessageType.Error);
                    }
                }
                else
                {
                    SendLogMessage($"HTTP Error:{response.StatusCode},{response.Content}", LogMessageType.Error);

                }

                for (int i = 0; i < orders.Count; i++)
                {
                    MyOrderEvent?.Invoke(orders[i]);
                }

                return orders;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return new List<Order>();
            }

        }

        private OrderStateType GetOrderState(string orderStateResponse)
        {

            if (orderStateResponse.StartsWith("New") || orderStateResponse.StartsWith("Ack") || orderStateResponse.StartsWith("Done"))
            {
                return OrderStateType.Active;
            }

            else if (orderStateResponse.StartsWith("Filled"))
            {
                return OrderStateType.Done;
            }

            else if (orderStateResponse.StartsWith("PartiallyFilled"))
            {
                return OrderStateType.Partial;
            }

            else if (orderStateResponse.StartsWith("Rejected"))
            {
                return OrderStateType.Fail;
            }
            else if (orderStateResponse.StartsWith("Canceled"))
            {
                return OrderStateType.Cancel;
            }
            //else if (orderStateResponse.StartsWith("Ack") || orderStateResponse.StartsWith("Done"))
            //{
            //    return OrderStateType.Pending;
            //}

            return OrderStateType.None;
        }

        #endregion

        public bool SubscribeNews()
        {
            return false;
        }

        #region  12 Queries

        private IRestResponse CreatePublicQuery(string path, Method method/*, IWebProxy proxy = null*/)
        {
            try
            {
                RestClient client = new RestClient(_baseUrl);

                //if (proxy != null)
                //{
                //    client.Proxy = proxy;
                //}

                RestRequest request = new RestRequest(path, method);
                IRestResponse response = client.Execute(request);

                return response;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.Message, LogMessageType.Error);
                return null;
            }
        }


        private readonly Dictionary<string, string> SignaturePaths = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {

            ["/1/api/pro/v1/cash/order"] = "order",
            ["/1/api/pro/v1/margin/order"] = "order",
            ["/1/api/pro/v1/cash/order/all"] = "order/all",
            ["/1/api/pro/v1/margin/order/all"] = "order/all",
            ["/1/api/pro/v1/cash/order/status"] = "order/status",
            ["/1/api/pro/v1/margin/order/status"] = "order/status",
            ["/1/api/pro/v1/cash/order/open"] = "order/open",
            ["/1/api/pro/v1/margin/order/open"] = "order/open",
            ["/1/api/pro/v1/cash/order/hist/current"] = "order/hist/current",
            ["/1/api/pro/v1/margin/order/hist/current"] = "order/hist/current",
            ["/api/pro/data/v2/order/hist"] = "data/v2/order/hist",
            ["/api/pro/data/v1/cash/balance/snapshot"] = "data/v1/cash/balance/snapshot",
            ["/api/pro/data/v1/margin/balance/snapshot"] = "data/v1/margin/balance/snapshot",
            ["/api/pro/data/v1/cash/balance/history"] = "data/v1/cash/balance/history",
            ["/api/pro/data/v1/margin/balance/history"] = "data/v1/margin/balance/history",
            ["/1/api/pro/v1/cash/balance"] = "balance", //[$"/{accountGroup}/api/pro/v1/cash/balance"] = "balance"
            ["/1/api/pro/v1/margin/balance"] = "balance",


        };

        private string BuildPrehashMessage(string fullPath, long timestamp)
        {
            string pathOnly = fullPath.Contains("?")
        ? fullPath.Substring(0, fullPath.IndexOf("?", StringComparison.Ordinal))
        : fullPath;


            string prehashPath;

            //  Если путь есть в словаре — используем его
            if (SignaturePaths.TryGetValue(pathOnly, out prehashPath))
            {
                return $"{timestamp}+{prehashPath}";
            }

            //  Если нет — извлекаем всё после /v1/
            int idx = fullPath.IndexOf("/v1/", StringComparison.OrdinalIgnoreCase);
            prehashPath = (idx >= 0) ? fullPath.Substring(idx + 4) : fullPath.Trim('/');

            return $"{timestamp}+{prehashPath}";
        }

        private IRestResponse CreatePrivateQuery(string fullPath, object body = null, string accountGroup = null, string accountCategory = null, Method method = Method.GET)
        {
            try
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                string message = BuildPrehashMessage(fullPath, timestamp);

                string signature = GenerateSignature(message, _secretKey);

                RestClient client = new RestClient(_baseUrl);
                RestRequest request = new RestRequest(fullPath, method);

                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("x-auth-key", _publicKey);
                request.AddHeader("x-auth-timestamp", timestamp.ToString());
                request.AddHeader("x-auth-signature", signature);

                if (body != null)
                {
                    string jsonBody = JsonConvert.SerializeObject(body);
                    request.AddParameter("application/json", body, ParameterType.RequestBody);
                }

                return client.Execute(request);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.Message, LogMessageType.Error);
                return null;
            }
        }


        static string GenerateSignature(string message, string secret)
        {

            byte[] keyBytes = Encoding.UTF8.GetBytes(secret);
            byte[] messageBytes = Encoding.UTF8.GetBytes(message);

            using (var hmac = new HMACSHA256(keyBytes))
            {
                byte[] hash = hmac.ComputeHash(messageBytes);
                return Convert.ToBase64String(hash);
            }
        }

        #endregion

        #region 13 Log

        public event Action<string, LogMessageType> LogMessageEvent;

        private void SendLogMessage(string message, LogMessageType messageType)
        {
            LogMessageEvent(message, messageType);
        }

        #endregion
    }
}