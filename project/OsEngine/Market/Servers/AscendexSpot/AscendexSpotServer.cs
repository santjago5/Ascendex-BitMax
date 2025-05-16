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
using OsEngine.Entity.WebSocketOsEngine;
using OsEngine.Market.Servers.BitMax;
using OsEngine.Market.Servers.Transaq.TransaqEntity;




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
            threadForPublicMessagesMarketDepths.Name = "PublicMarketDepthsMessageReaderAscendex";
            threadForPublicMessagesMarketDepths.Start();

            Thread threadForPublicTradesMessages = new Thread(PublicMessageTradesReader);
            threadForPublicTradesMessages.IsBackground = true;
            threadForPublicTradesMessages.Name = "PublicTradeMessageReaderAscendex";
            threadForPublicTradesMessages.Start();

            Thread threadForPrivateMessages = new Thread(PrivateMessageReader);
            threadForPrivateMessages.IsBackground = true;
            threadForPrivateMessages.Name = "PrivateMessageReaderAscendex";
            threadForPrivateMessages.Start();

            Thread threadCheckAliveWebSocket = new Thread(CheckAliveWebSocket);
            threadCheckAliveWebSocket.IsBackground = true;
            threadCheckAliveWebSocket.Name = "CheckAliveWebSocket";
            threadCheckAliveWebSocket.Start();
        }

        public DateTime ServerTime { get; set; }

      //  private WebProxy _myProxy;
        public void Connect(WebProxy proxy = null)
        {
            try
            {
                // _myProxy = proxy;

                _publicKey = ((ServerParameterString)ServerParameters[0]).Value;
                _secretKey = ((ServerParameterPassword)ServerParameters[1]).Value;

                if (string.IsNullOrEmpty(_publicKey) || string.IsNullOrEmpty(_secretKey))
                {
                    SendLogMessage("Error:Invalid public or secret key.", LogMessageType.Error);
                    return;
                }


                string _apiPath = "/api/pro/v2/assets";

                //IRestResponse response = CreatePublicQuery(_apiPath, Method.GET, _myProxy);
                IRestResponse response = CreatePublicQuery(_apiPath, Method.GET);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    string responseBody = response.Content;

                    //if (responseBody.Contains("0"))
                    //{
                    FIFOListWebSocketPublicMarketDepthsMessage = new ConcurrentQueue<string>();
                    FIFOListWebSocketPublicTradesMessage = new ConcurrentQueue<string>();
                    FIFOListWebSocketPrivateMessage = new ConcurrentQueue<string>();
                    CreatePublicWebSocketMarketDepthsConnect();
                    CreatePublicWebSocketTradesConnect();
                    CreatePrivateWebSocketConnect();
                    CheckActivationSockets();

                    SendLogMessage("Start Ascendex Connection", LogMessageType.System);
                    //}
                    //else
                    //{
                    //    SendLogMessage("Status: Maintenance mode", LogMessageType.System);
                    //}
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
                        SendLogMessage(" Cannot unsubscribe — security is null or empty.", LogMessageType.Error);
                        return;
                    }
                    UnsubscribeFromAllChannels(security);
                }

                DeleteWebSocketConnection();
            }
            catch (Exception exception)
            {
                SendLogMessage("Dispose method error: " + exception.ToString(), LogMessageType.System);
            }

            FIFOListWebSocketPublicMarketDepthsMessage = null;
            FIFOListWebSocketPublicTradesMessage = null;
            FIFOListWebSocketPrivateMessage = null;

            Disconnect();
        }

        public void Disconnect()
        {
            if (ServerStatus != ServerConnectStatus.Disconnect)
            {
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

                    List<Security> securities = new List<Security>();

                    for (int i = 0; i < securityList.data.Count; i++)
                    {
                        string symbol = securityList.data[i].symbol;
                        string price = securityList.data[i].tickSize;

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

            IRestResponse response = CreatePrivateQuery(_baseUrl, null, fullPath);

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
            int limit = 4990;

            List<Candle> allCandles = new List<Candle>();

            DateTime startTime = timeEnd - TimeSpan.FromMinutes(tf.TotalMinutes * countToLoad);
            HashSet<DateTime> uniqueTimes = new HashSet<DateTime>();

            int candlesLoaded = 0;
            string timeFrame = GetInterval(tf);

            DateTime periodEnd = startTime;

            while (candlesLoaded < countToLoad && periodEnd < timeEnd)
            {
                int candlesToLoad = Math.Min(limit, countToLoad - candlesLoaded);
                DateTime periodStart = startTime;

                periodEnd = periodStart.AddMinutes(tf.TotalMinutes * candlesToLoad);

                if (periodEnd > DateTime.UtcNow)
                {
                    periodEnd = DateTime.UtcNow;
                }

                List<Candle> rangeCandles = CreateQueryCandles(nameSec, timeFrame, periodStart, periodEnd, candlesToLoad);

                if (rangeCandles == null)
                {
                    return null;
                }

                if (rangeCandles.Count == 0)
                {
                    return null;
                }

                for (int i = 0; i < rangeCandles.Count; i++)
                {
                    if (uniqueTimes.Add(rangeCandles[i].TimeStart))
                    {
                        allCandles.Add(rangeCandles[i]);
                    }
                }

                int actualCandlesLoaded = rangeCandles.Count;

                candlesLoaded += actualCandlesLoaded;
                startTime = allCandles[allCandles.Count - 1].TimeStart;

                if (periodEnd >= timeEnd)
                {
                    break;
                }
            }

            for (int i = allCandles.Count - 1; i >= 0; i--)
            {
                if (allCandles[i].TimeStart > timeEnd)
                {
                    allCandles.RemoveAt(i);
                }
            }

            for (int i = allCandles.Count - 1; i > 0; i--)
            {
                if (allCandles[i].TimeStart == allCandles[i - 1].TimeStart)
                {
                    allCandles.RemoveAt(i);
                }
            }

            return allCandles;
        }
        public List<Trade> GetTickDataToSecurity(Security security, DateTime startTime, DateTime endTime, DateTime actualTime)
        {
            throw new NotImplementedException();
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
                timeFrameMinutes == 240 ||
                timeFrameMinutes == 360 ||
                timeFrameMinutes == 720 ||
                timeFrameMinutes == 1440 ||
                timeFrameMinutes == 10080 ||
                timeFrameMinutes == 43829)
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
            else if (tf.Minutes > 0)
            {
                return $"{tf.Minutes}m";
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

        private List<Candle> CreateQueryCandles(string symbol, string interval, DateTime startTime, DateTime endTime, int limit)
        {
            _rateGateCandleHistory.WaitToProceed();

            try
            {
                long startDate = TimeManager.GetTimeStampMilliSecondsToDateTime(startTime);
                long endDate = TimeManager.GetTimeStampMilliSecondsToDateTime(endTime);


                string _apiPath = $"/api/pro/v1/barhist?symbol={symbol}&interval={interval}&start={startDate}&end={endDate}&n={limit}";


                IRestResponse response = CreatePublicQuery(_apiPath, Method.GET/*, _myProxy*/);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCandleResponse json = JsonConvert.DeserializeObject<AscendexSpotCandleResponse>(response.Content);

                    // Проверка: если объект пустой или вернулся неуспешный код
                    if (json == null || json.code != "0" || json.data == null)
                    {
                        Console.WriteLine("❌ Ошибка формата данных или код ответа != 0");
                        return new List<Candle>();
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
                        if (string.IsNullOrEmpty(candle.ts) || string.IsNullOrEmpty(candle.o) ||
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
                            SendLogMessage("Candle data contains zero values", LogMessageType.Error);
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


        #region  6 WebSocket creation

        private string _webSocketUrl = "wss://ascendex.com/1/api/pro/v1/stream";

        private string _lockerCheckActivateionSockets = "lockerCheckActivateionSockets";

        private List<WebSocket> _webSocketPublicMarketDepths = new List<WebSocket>();

        private List<WebSocket> _webSocketPublicTrades = new List<WebSocket>();

        private WebSocket _webSocketPrivate;

        private ConcurrentQueue<string> FIFOListWebSocketPublicMarketDepthsMessage = new ConcurrentQueue<string>();

        private ConcurrentQueue<string> FIFOListWebSocketPublicTradesMessage = new ConcurrentQueue<string>();

        private ConcurrentQueue<string> FIFOListWebSocketPrivateMessage = new ConcurrentQueue<string>();

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
            catch (Exception ex)
            {
                SendLogMessage($"{ex.Message} {ex.StackTrace}", LogMessageType.Error);
            }
        }

        private WebSocket CreateNewPublicMarketDepthsSocket()
        {
            try
            {
                WebSocket webSocketPublicMarketDepthsNew = new WebSocket(_webSocketUrl);

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
            catch (Exception ex)
            {
                SendLogMessage($"{ex.Message} {ex.StackTrace}", LogMessageType.Error);
            }
        }

        private WebSocket CreateNewPublicTradesSocket()
        {
            try
            {
                WebSocket _webSocketPublicTradesNew = new WebSocket(_webSocketUrl);

                //if (_myProxy != null)
                //{
                //    _webSocketPublicTradesNew.SetProxy(_myProxy);
                //}

                _webSocketPublicTradesNew.EmitOnPing = true;
                _webSocketPublicTradesNew.OnOpen += WebSocketPublicTradesNew_OnOpen;
                _webSocketPublicTradesNew.OnClose += WebSocketPublicTradesNew_OnClose;
                _webSocketPublicTradesNew.OnMessage += WebSocketPublicTradesNew_OnMessage;
                _webSocketPublicTradesNew.OnError += WebSocketPublicTradesNew_OnError;
                _webSocketPublicTradesNew.Connect();

                return _webSocketPublicTradesNew;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return null;
            }
        }

        private void CreatePrivateWebSocketConnect()
        {
            try
            {
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
                _webSocketPrivate.OnOpen += WebSocketPrivate_Opened;
                _webSocketPrivate.OnClose += WebSocketPrivate_Closed;
                _webSocketPrivate.OnMessage += WebSocketPrivate_MessageReceived;
                _webSocketPrivate.OnError += WebSocketPrivate_Error;

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
                    _webSocketPrivate.OnOpen -= WebSocketPrivate_Opened;
                    _webSocketPrivate.OnClose -= WebSocketPrivate_Closed;
                    _webSocketPrivate.OnMessage -= WebSocketPrivate_MessageReceived;
                    _webSocketPrivate.OnError -= WebSocketPrivate_Error;
                    _webSocketPrivate.CloseAsync();
                }
                catch
                {
                    // ignore
                }

                _webSocketPrivate = null;
            }
        }

        #endregion

        #region 8 WebSocket check alive
        private void CheckAliveWebSocket()
        {
            while (true)
            {
                try
                {
                    Thread.Sleep(20000);

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
                            webSocketPublicMarketDepths?.Send("{\"event\":\"ping\", \"cid\":1204}");
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
                            webSocketPublicTrades.Send("{\"event\":\"ping\", \"cid\":1254}");
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
                        _webSocketPrivate.Send("{\"event\":\"ping\", \"cid\":1274}");
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
        public static void SendPong(WebSocket webSocket)
        {
            var pong = new { op = "pong" };
            string json = JsonConvert.SerializeObject(pong);
            webSocket.Send(json);

        }
        #endregion
     


        #region  7 WebSocket events

        private void WebSocketPublicMarketDepthsNew_OnError(object sender, ErrorEventArgs e)
        {
            try
            {
                if (e.Exception != null)
                {
                    ////    SendLogMessage($(" WebSocket MarketDepths"(e.Exception.ToString(), LogMessageType.Error);
                    SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Data socket MarketDepths exception: " + exception.ToString(), LogMessageType.Error);
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

                if (e.Data.Contains("pong"))
                { // pong message
                  // SendPing;
                }
                if (e.Data.Contains("\"m\":\"depth\""))
                {
                    var depth = JsonConvert.DeserializeObject<AscendexSpotDepthSnapshotResponse>(e.Data);

                }

                FIFOListWebSocketPublicMarketDepthsMessage?.Enqueue(e.Data);
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

                SendLogMessage($"Public MarketDeptns WebSocket closed by  AscendexSpot. Code:{e.Code}", LogMessageType.Error);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPublicMarketDepthsNew_OnOpen(object sender, EventArgs e)
        {
            try
            {
                CheckActivationSockets();

                SendLogMessage("WebSocket public MarketDepths  AscendexSpot open.", LogMessageType.System);

            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPublicTradesNew_OnError(object sender, ErrorEventArgs e)
        {
            try
            {
                //if (!string.IsNullOrEmpty(e.Message))
                //{
                //    SendLogMessage($"WebSocket Trades Error: {e.Message}", LogMessageType.Error);
                //}
                if (e.Exception != null)
                {
                    SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Data socket Trades exception: " + exception.ToString(), LogMessageType.Error);
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

                if (e.Data.Contains("pong"))
                { // pong message
                    return;
                }

                if (e.Data.Contains("\"m\":\"trades\""))
                {
                    var trades = JsonConvert.DeserializeObject<AscendexSpotPublicTradesResponse>(e.Data);

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

                SendLogMessage($"Public Trades WebSocket closed by  AscendexSpot. Code: {e.Code}", LogMessageType.Error);
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

                SendLogMessage("WebSocket public Trades  AscendexSpot open.", LogMessageType.System);

            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPrivate_Opened(object sender, EventArgs e)
        {
            try
            {
                string fullPath = "wss://ascendex.com/1/api/pro/v1/stream";

                GenerateAuthenticate(_webSocketPrivate, _publicKey, _secretKey, fullPath);

                CheckActivationSockets();

                SendLogMessage("Connection to private data is Open", LogMessageType.System);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPrivate_Closed(object sender, CloseEventArgs e)
        {
            try
            {
                Disconnect();
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }

            SendLogMessage($"Connection Closed by  AscendexSpot. WebSocket Private closed. Code: {e.Code}", LogMessageType.Error);
        }

        private void WebSocketPrivate_Error(object sender, ErrorEventArgs e)
        {
            try
            {
                //if (!string.IsNullOrEmpty(e.Message))
                //{
                //    SendLogMessage($"WebSocket private Error: {e.Message}", LogMessageType.Error);
                //}
                if (e.Exception != null)
                {
                    SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void WebSocketPrivate_MessageReceived(object sender, MessageEventArgs e)
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

                if (e.Data.Contains("pong"))
                { // pong message
                    return;
                }


                if (e.Data.Contains("\"m\":\"auth\"") && e.Data.Contains("\"code\":0"))
                {

                }


                FIFOListWebSocketPrivateMessage?.Enqueue(e.Data);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void GenerateAuthenticate(WebSocket webSocket, string apiKey, string secretKey, string fullPath)
        {
            try
            {
                string apiPath = fullPath.Substring(fullPath.LastIndexOf('/') + 1);// для подписи
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string message = timestamp + "+" + apiPath;
                string signature = GenerateSignature(secretKey, message);

                var authPayload = new
                {
                    op = "auth",
                    //id = "abc123",
                    t = timestamp,
                    key = apiKey,
                    sig = signature
                };

                string json = JsonConvert.SerializeObject(authPayload);

                webSocket.Send(json);

            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void CheckActivationSockets()
        {
            lock (_lockerCheckActivateionSockets)
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
                    SendLogMessage(exception.ToString(), LogMessageType.Error);
                }
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

        private void PublicMessageMarketDepthsReader()
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

                    if (FIFOListWebSocketPublicMarketDepthsMessage.IsEmpty)
                    {
                        Thread.Sleep(1);
                        continue;
                    }

                    FIFOListWebSocketPublicMarketDepthsMessage.TryDequeue(out string message);

                    if (message == null)
                    {
                        continue;
                    }

                }
                catch (Exception exception)
                {
                    Thread.Sleep(5000);
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
                    if (message.Contains("\"m\":\"depth-snapshot\""))
                    {
                        SnapshotDepth(message);
                    }
                    if (message.Contains("\"m\":\"depth\""))
                    {
                        UpdateDepth(message);
                    }
                    if (message.Contains("\"m\":\"trades\""))
                    {
                        UpdateTrade(message);
                    }
                }
                catch (Exception exception)
                {
                    Thread.Sleep(5000);
                    SendLogMessage(exception.ToString(), LogMessageType.Error);
                }
            }
        }



        private void PrivateMessageReader()
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
                    else if (message.Contains("\"m\":\"ping\""))
                    {
                        _webSocketPrivate.Send("{\"op\":\"pong\"}");

                    }
                    //if (message.Contains("\"m\":\"ping\""))
                    //{
                    //    SendPong(_webSocketPrivate);
                    //    return;
                    //}

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

                    else if (message.Contains("\"m\":\"trade\""))
                    {
                        var tradeMessage = JsonConvert.DeserializeObject<WebSocketMessage<AscendexSpotMyTradeData>>(message);
                        UpdateMyTrade(message);
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


        private List<MarketDepth> _allDepths = new List<MarketDepth>();
        private void SnapshotDepth(string message)
        {
            AscendexSpotDepthSnapshotResponse snapshot =
                JsonConvert.DeserializeObject<AscendexSpotDepthSnapshotResponse>(message);

            if (snapshot == null || snapshot.data == null || snapshot.data.data == null)
            {
                return;
            }

            MarketDepth newDepth = new MarketDepth();
            newDepth.Time = DateTime.UtcNow;
            newDepth.SecurityNameCode = snapshot.data.symbol;

            string[][] bids = snapshot.data.data.bids;
            if (bids != null)
            {
                for (int i = 0; i < bids.Length; i++)
                {
                    newDepth.Bids.Add(new MarketDepthLevel()
                    {
                        Price = bids[i][0].ToDecimal(),
                        Bid = bids[i][1].ToDecimal()
                    });
                }
            }

            string[][] asks = snapshot.data.data.asks;
            if (asks != null)
            {
                for (int i = 0; i < asks.Length; i++)
                {
                    newDepth.Asks.Add(new MarketDepthLevel()
                    {
                        Price = asks[i][0].ToDecimal(),
                        Ask = asks[i][1].ToDecimal()
                    });
                }
            }


            var needDepth = _allDepths.Find(d => d.SecurityNameCode == newDepth.SecurityNameCode);
            if (needDepth != null)
            {
                _allDepths.Remove(needDepth);
            }

            _allDepths.Add(newDepth);
        }

        private void UpdateDepth(string message)
        {
            AscendexSpotDepthWrapper wrapper =
                JsonConvert.DeserializeObject<AscendexSpotDepthWrapper>(message);

            if (wrapper == null || wrapper.data == null)
            {
                return;
            }

            Depth depthUpdate = new Depth();
            depthUpdate.Symbol = wrapper.symbol;

            DepthData data = new DepthData();
            data.Bids = wrapper.data.bids;
            data.Asks = wrapper.data.asks;
            depthUpdate.Data = data;

            //  UpdateDepth(depthUpdate);
        }

        //private MarketDepth UpdateDepth(Depth quotes)
        //{
        //    var needDepth = _allDepths.Find(d => d.SecurityNameCode == quotes.Symbol);
        //    if (needDepth == null)
        //    {
        //        return null;
        //    }

        //    if (quotes.Data.Bids != null)
        //    {
        //        string[][] bidsLevels = quotes.Data.Bids;

        //        for (int i = 0; i < bidsLevels.Length; i++)
        //        {
        //            decimal price = bidsLevels[i][0].ToDecimal();
        //            decimal bid = bidsLevels[i][1].ToDecimal();

        //            if (bid != 0)
        //            {
        //                InsertLevel(price, bid, Side.Buy, needDepth);
        //            }
        //            else
        //            {
        //                DeleteLevel(price, Side.Buy, needDepth);
        //            }
        //        }

        //        SortBids(needDepth.Bids);
        //    }

        //    if (quotes.Data.Asks != null)
        //    {
        //        string[][] asksLevels = quotes.Data.Asks;

        //        for (int i = 0; i < asksLevels.Length; i++)
        //        {
        //            decimal price = asksLevels[i][0].ToDecimal();
        //            decimal ask = asksLevels[i][1].ToDecimal();

        //            if (ask != 0)
        //            {
        //                InsertLevel(price, ask, Side.Sell, needDepth);
        //            }
        //            else
        //            {
        //                DeleteLevel(price, Side.Sell, needDepth);
        //            }
        //        }

        //        SortAsks(needDepth.Asks);
        //    }

        //    return needDepth.GetCopy();
        //}

        private void InsertLevel(decimal price, decimal value, Side side, MarketDepth marketDepth)
        {
            var levels = side == Side.Buy ? marketDepth.Bids : marketDepth.Asks;
            var level = levels.Find(l => l.Price == price);

            if (level != null)
            {
                if (side == Side.Buy) { level.Bid = value; } else { level.Ask = value; }
            }
            else
            {
                level = new MarketDepthLevel();
                level.Price = price;
                if (side == Side.Buy) { level.Bid = value; } else { level.Ask = value; }

                levels.Add(level);
            }
        }

        private void DeleteLevel(decimal price, Side side, MarketDepth marketDepth)
        {
            var levels = side == Side.Buy ? marketDepth.Bids : marketDepth.Asks;
            var level = levels.Find(l => l.Price == price);
            if (level != null) { levels.Remove(level); }
        }

        private void SortBids(List<MarketDepthLevel> levels)
        {
            levels.Sort((a, b) => b.Price.CompareTo(a.Price));
        }

        private void SortAsks(List<MarketDepthLevel> levels)
        {
            levels.Sort((a, b) => a.Price.CompareTo(b.Price));
        }


        //private void SnapshotDepth(string message)
        //{
        //    AscendexSpotDepthSnapshotResponse snapshot =
        // JsonConvert.DeserializeObject<AscendexSpotDepthSnapshotResponse>(message);

        //    // Проверяем, что данные получены корректно
        //    if (snapshot == null || snapshot.data == null || snapshot.data.data == null)
        //    {
        //        return;
        //    }

        //    // Создаём новый объект MarketDepth
        //    MarketDepth newDepth = new MarketDepth();

        //    // Устанавливаем время получения данных
        //    newDepth.Time = DateTime.UtcNow;

        //    // Устанавливаем символ инструмента
        //    newDepth.SecurityNameCode = snapshot.data.symbol;

        //    // Обрабатываем заявки на покупку (bids)
        //    string[][] bids = snapshot.data.data.bids;
        //    if (bids != null)
        //    {
        //        for (int i = 0; i < bids.Length; i++)
        //        {
        //            // Добавляем уровень заявки
        //            newDepth.Bids.Add(new MarketDepthLevel()
        //            {
        //                Price = bids[i][0].ToDecimal(), // Цена
        //                Bid = bids[i][1].ToDecimal()    // Объём
        //            });
        //        }
        //    }

        //    // Обрабатываем заявки на продажу (asks)
        //    string[][] asks = snapshot.data.data.asks;
        //    if (asks != null)
        //    {
        //        for (int i = 0; i < asks.Length; i++)
        //        {
        //            // Добавляем уровень заявки
        //            newDepth.Asks.Add(new MarketDepthLevel()
        //            {
        //                Price = asks[i][0].ToDecimal(), // Цена
        //                Ask = asks[i][1].ToDecimal()    // Объём
        //            });
        //        }
        //    }

        //    // Ищем, существует ли уже такой инструмент в списке
        //    var needDepth = _allDepths.Find(d => d.SecurityNameCode == newDepth.SecurityNameCode);

        //    if (needDepth != null)
        //    {
        //        // Удаляем старый MarketDepth
        //        _allDepths.Remove(needDepth);
        //    }

        //    // Добавляем обновлённый MarketDepth
        //    _allDepths.Add(newDepth);
        //}



        //public MarketDepth Create(string message)
        //{
        //    var depth = JsonConvert.DeserializeAnonymousType(message, new Depth());

        //    var need = _allDepths.Find(d => d.SecurityNameCode == depth.Symbol);

        //    if (need == null)
        //    {
        //        return CreateNew(depth);
        //    }

        //    return UpdateDepth(depth);
        //}


        //private MarketDepth CreateNew(Depth quotes)
        //{
        //    var newDepth = new MarketDepth();

        //    newDepth.Time = DateTime.UtcNow;

        //    newDepth.SecurityNameCode = quotes.Symbol;

        //    var needDepth = _allDepths.Find(d => d.SecurityNameCode == newDepth.SecurityNameCode);

        //    if (needDepth != null)
        //    {
        //        _allDepths.Remove(needDepth);
        //    }

        //    var bids = quotes.Data.Bids;
        //    var asks = quotes.Data.Asks;

        //    foreach (var bid in bids)
        //    {
        //        newDepth.Bids.Add(new MarketDepthLevel()
        //        {
        //            Price = bid[0].ToDecimal(),
        //            Bid = bid[1].ToDecimal(),
        //        });
        //    }

        //    foreach (var ask in asks)
        //    {
        //        newDepth.Asks.Add(new MarketDepthLevel()
        //        {
        //            Price = ask[0].ToDecimal(),
        //            Ask = ask[1].ToDecimal(),
        //        });
        //    }

        //    _allDepths.Add(newDepth);

        //    return newDepth.GetCopy();
        //}

        //  private void UpdateDepth(string message)
        //  {
        //      AscendexSpotDepthWrapper wrapper =
        //JsonConvert.DeserializeObject<AscendexSpotDepthWrapper>(message);

        //      // Проверяем наличие данных
        //      if (wrapper == null || wrapper.data == null)
        //      {
        //          return;
        //      }

        //      // Создаём временный объект типа Depth для совместимости с UpdateDepth
        //      Depth depthUpdate = new Depth();

        //      // Устанавливаем символ
        //      depthUpdate.Symbol = wrapper.symbol;

        //      // Создаём объект Data
        //      DepthData data = new DepthData();

        //      // Присваиваем bids и asks из входящего сообщения
        //      data.Bids = wrapper.data.bids;
        //      data.Asks = wrapper.data.asks;

        //      depthUpdate.Data = data;

        //      // Вызываем метод обновления
        //      UpdateDepth(depthUpdate);
        //  }

        //private MarketDepth UpdateDepth(Depth quotes)
        //{
        //    var needDepth = _allDepths.Find(d => d.SecurityNameCode == quotes.Symbol);

        //    if (needDepth == null)
        //    {
        //        throw new ArgumentNullException("BitMax: MarketDepth for updates not found");
        //    }

        //    if (quotes.Data.Bids != null)
        //    {
        //        var bidsLevels = quotes.Data.Bids;

        //        foreach (var bidLevel in bidsLevels)
        //        {
        //            decimal price = bidLevel[0].ToDecimal();
        //            decimal bid = bidLevel[1].ToDecimal();

        //            if (bid != 0)
        //            {
        //                InsertLevel(price, bid, Side.Buy, needDepth);
        //            }
        //            else
        //            {
        //                DeleteLevel(price, Side.Buy, needDepth);
        //            }
        //        }
        //        SortBids(needDepth.Bids);
        //    }

        //    if (quotes.Data.Asks != null)
        //    {
        //        var asksLevels = quotes.Data.Asks;

        //        foreach (var askLevel in asksLevels)
        //        {
        //            decimal price = askLevel[0].ToDecimal();
        //            decimal ask = askLevel[1].ToDecimal();

        //            if (ask != 0)
        //            {
        //                InsertLevel(price, ask, Side.Sell, needDepth);
        //            }
        //            else
        //            {
        //                DeleteLevel(price, Side.Sell, needDepth);
        //            }
        //        }
        //        SortAsks(needDepth.Asks);
        //    }

        //    return needDepth.GetCopy();
        //}


        //protected void InsertLevel(decimal price, decimal value, Side side, MarketDepth marketDepth)
        //{
        //    var needDepthPart = side == Side.Buy ? marketDepth.Bids : marketDepth.Asks;

        //    var needLevel = needDepthPart.Find(level => level.Price == price);

        //    if (needLevel != null)
        //    {
        //        if (side == Side.Buy)
        //        {
        //            needLevel.Bid = value;
        //        }
        //        else
        //        {
        //            needLevel.Ask = value;
        //        }
        //    }
        //    else
        //    {
        //        needLevel = new MarketDepthLevel();
        //        needLevel.Price = price;

        //        if (side == Side.Buy)
        //        {
        //            needLevel.Bid = value;
        //        }
        //        else
        //        {
        //            needLevel.Ask = value;
        //        }

        //        needDepthPart.Add(needLevel);
        //        SortBids(needDepthPart);
        //    }
        //}

        //protected void DeleteLevel(decimal price, Side side, MarketDepth marketDepth)
        //{
        //    var needDepthPart = side == Side.Buy ? marketDepth.Bids : marketDepth.Asks;

        //    var needLevel = needDepthPart.Find(level => level.Price == price);

        //    needDepthPart.Remove(needLevel);
        //}

        //protected void SortBids(List<MarketDepthLevel> levels)
        //{
        //    levels.Sort((a, b) =>
        //    {
        //        if (a.Price > b.Price)
        //        {
        //            return -1;
        //        }
        //        else if (a.Price < b.Price)
        //        {
        //            return 1;
        //        }
        //        else
        //        {
        //            return 0;
        //        }
        //    });
        //}

        //protected void SortAsks(List<MarketDepthLevel> levels)
        //{
        //    levels.Sort((a, b) =>
        //    {
        //        if (a.Price > b.Price)
        //        {
        //            return 1;
        //        }
        //        else if (a.Price < b.Price)
        //        {
        //            return -1;
        //        }
        //        else
        //        {
        //            return 0;
        //        }
        //    });
        //}
        private void UpdateTrade(string message)
        {
            try
            {
                AscendexSpotPublicTradesResponse response = JsonConvert.DeserializeObject<AscendexSpotPublicTradesResponse>(message);

                if (response == null || response.data == null || response.data.data == null)
                {
                    SendLogMessage("UpdateTrade> Received empty  json", LogMessageType.Error);
                    return;
                }

                for (int i = 0; i < response.data.data.Count; i++)
                {
                    AscendexSpotPublicTradeItem json = response.data.data[i];

                    Trade newTrade = new Trade();

                    newTrade.SecurityNameCode = response.data.symbol;
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

        private void UpdateMyTrade(string message)
        {
            try
            {

                AscendexSpotMyTradeData json = JsonConvert.DeserializeObject<AscendexSpotMyTradeData>(message);

                if (json == null)
                {
                    SendLogMessage("UpdateMyTrade> Received empty json", LogMessageType.Error);
                    return;
                }

                MyTrade myTrade = new MyTrade();

                myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(json.t));
                myTrade.SecurityNameCode = json.s; ;
                myTrade.Price = json.p.ToString().ToDecimal();
                myTrade.NumberTrade = json.orderId;
                myTrade.Volume = json.q.ToString().ToDecimal();
                myTrade.Side = (json.side.ToLower() == "buy") ? Side.Buy : Side.Sell;


                MyTradeEvent?.Invoke(myTrade);

                SendLogMessage(myTrade.ToString(), LogMessageType.Trade);
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


                if (json != null && json.m == "order" && json.data != null)
                {

                }

                Order updateOrder = new Order();

                updateOrder.SecurityNameCode = json.data.s;
                updateOrder.NumberMarket = json.data.orderId;
                updateOrder.State = GetOrderState(json.data.st);
                updateOrder.Side = (json.data.sd.ToLower() == "buy") ? Side.Buy : Side.Sell;
                updateOrder.TypeOrder = (json.data.ot.ToLower() == "limit") ? OrderPriceType.Limit : OrderPriceType.Market;
                updateOrder.Price = (json.data.p).ToDecimal();
                updateOrder.Volume = (json.data.q).ToDecimal();
                updateOrder.TimeCreate = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(json.data.t));
                updateOrder.ServerType = ServerType.AscendexSpot;

                updateOrder.PortfolioNumber = "AscendexSpotPortfolio";

                MyOrderEvent?.Invoke(updateOrder);


            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private OrderStateType GetOrderState(string orderStateResponse)
        {
            if (orderStateResponse.StartsWith("ACTIVE"))
            {
                return OrderStateType.Active;
            }
            else if (orderStateResponse.StartsWith("EXECUTED"))
            {
                return OrderStateType.Done;
            }
            else if (orderStateResponse.StartsWith("PARTIALLY FILLED"))
            {
                return OrderStateType.Partial;
            }
            else if (orderStateResponse.StartsWith("CANCELED"))
            {
                return OrderStateType.Cancel;
            }

            return OrderStateType.None;
        }
        private void UpdatePortfolio(WebSocketMessage<AscendexSpotPortfolio> json)
        {
            try
            {
                // AscendexSpotPortfolio json = JsonConvert.DeserializeObject<AscendexSpotPortfolio>(message);


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


        #region  9  WebSocket security subscrible

        private List<string> _subscribedSecurities = new List<string>();

        private RateGate _rateGateSubscribed = new RateGate(1, TimeSpan.FromMilliseconds(2500));

        public void Subscrible(Security security)
        {
            try
            {
                _rateGateSubscribed.WaitToProceed();

                CreateSubscribleMessageWebSocket(security);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void CreateSubscribleMessageWebSocket(Security security)
        {
            try
            {
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

                if (webSocketPublicMarketDepths.ReadyState == WebSocketState.Open
                    && webSocketPublicTrades.ReadyState == WebSocketState.Open
                    && _subscribedSecurities.Count != 0
                    && _subscribedSecurities.Count % 15 == 0)
                {
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
                    webSocketPublicMarketDepths.Send($"{{\"op\":\"sub\",\"ch\":\"depth:{security.Name}\"}}");
                    webSocketPublicTrades.Send($"{{\"op\":\"sub\",\"ch\":\"trades:{security.Name}\"}}");
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void UnsubscribeFromAllChannels(Security security)
        {
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    return;
                }

                for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
                {
                    WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[i];

                    if (webSocketPublicMarketDepths != null && webSocketPublicMarketDepths?.ReadyState == WebSocketState.Open)
                    {
                        //  { "op": "unsub", "id": "abc123", "ch":"trades:ASD/USDT" }
                        string message = $"{{\"op\":\"unsub\",\"ch\":\"depth:{security.Name}\"}}";

                        webSocketPublicMarketDepths.Send(message);
                    }
                }

                for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                {
                    WebSocket webSocketPublicTrades = _webSocketPublicTrades[i];

                    if (webSocketPublicTrades != null && webSocketPublicTrades?.ReadyState == WebSocketState.Open)
                    {
                        string message = $"{{\"op\":\"unsub\",\"ch\":\"trades:{security.Name}\"}}";

                        webSocketPublicTrades.Send(message);
                    }

                    SendLogMessage("All subscriptions have been successfully removed", LogMessageType.System);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Error unsubscribing from channels:" + exception.ToString(), LogMessageType.Error);
            }
        }

        #endregion


        #region  11 Trade
        public void SendOrder(Order order)
        {
            //POST <account-group>/api/pro/v1/{account - category}/order

        }
        public void CancelAllOrders()
        {
            //DELETE <account-group>/api/pro/v1/{account-category}/order/all
            string accountGroup = GetAccountGroup();

            string accountCategory = "cash";

            string path = $"/{accountGroup}/api/pro/v1/{accountCategory}/order/all";


            IRestResponse response = CreatePrivateQuery(path, accountGroup, accountCategory, null, Method.DELETE/*, _myProxy*/);

            if (response == null)
            {
                Console.WriteLine("❌ Ошибка: нет ответа от сервера.");
                return;
            }

            if (response.StatusCode == HttpStatusCode.OK)
            {
                Console.WriteLine("📩 Ответ на отмену ордера:");
                Console.WriteLine(response.Content);

                AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                if (cancelResult != null && cancelResult.code == 0)
                {
                    Console.WriteLine($"✅ Ордера отменены: {cancelResult.data.orderId} | Статус: {cancelResult.data.status}");
                }
                else
                {
                    Console.WriteLine($"❌ Ошибка отмены: code={cancelResult?.code}");
                }
            }
            else
            {
                Console.WriteLine("❌ HTTP ошибка: " + response.StatusCode);
                Console.WriteLine(response.Content);
            }

        }
        // получаем номер группы
        public void CancelOrder(Order order)
        { // DELETE < account - group >/ api / pro / v1 /{ account - category}/ order

            string accountGroup = GetAccountGroup();

            string path = $"/{accountGroup}/api/pro/v1/cash/order";

            var body = new
            {
                orderId = order,
                symbol = order.SecurityNameCode,
                time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
            };

            IRestResponse response = CreatePrivateQuery(path, body, accountGroup, null, Method.DELETE/*, _myProxy*/);

            if (response == null)
            {
                Console.WriteLine("❌ Ошибка: нет ответа от сервера.");
                return;
            }

            if (response.StatusCode == HttpStatusCode.OK)
            {

                Console.WriteLine(response.Content);

                AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                if (cancelResult != null && cancelResult.code == 0)
                {
                    Console.WriteLine($"✅ Ордер отменён: {cancelResult.data.orderId} | Статус: {cancelResult.data.status}");
                }
                else
                {
                    Console.WriteLine($"❌ Ошибка отмены: code={cancelResult?.code}");
                }
            }
            else
            {
                Console.WriteLine("❌ HTTP ошибка: " + response.StatusCode);
                Console.WriteLine(response.Content);
            }
        }

        public void CancelAllOrdersToSecurity(Security security)
        {

            string accountGroup = GetAccountGroup();
            string accountCategory = "cash";

            string path = $"/{accountGroup}/api/pro/v1/{accountCategory}/order/all";

            var body = new { symbol = security };


            IRestResponse response = CreatePrivateQuery(path, body, accountGroup, accountCategory, Method.DELETE/*, _myProxy*/);

            if (response == null)
            {
                Console.WriteLine("❌ Ошибка: нет ответа от сервера.");
                return;
            }

            if (response.StatusCode == HttpStatusCode.OK)
            {
                Console.WriteLine("📩 Ответ на отмену ордера:");
                Console.WriteLine(response.Content);

                AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                if (cancelResult != null && cancelResult.code == 0)
                {
                    Console.WriteLine($"✅ Ордера отменены: {cancelResult.data.orderId} | Статус: {cancelResult.data.status}");
                }
                else
                {
                    Console.WriteLine($"❌ Ошибка отмены: code={cancelResult?.code}");
                }
            }
            else
            {
                Console.WriteLine("❌ HTTP ошибка: " + response.StatusCode);
                Console.WriteLine(response.Content);
            }
        }

        public void ChangeOrderPrice(Order order, decimal newPrice)
        {
            throw new NotImplementedException();
        }

        public void GetAllActivOrders()
        {
            //GET <account-group>/api/pro/v1/{account-category}/order/open
        }

        public void GetOrderStatus(Order order)
        {
            //GET <account-group>/api/pro/v1/{account-category}/order/status?orderId={orderId}
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
            catch (Exception ex)
            {
                SendLogMessage(ex.Message, LogMessageType.Error);
                return null;
            }
        }

        private IRestResponse CreatePrivateQuery(string fullPath, object body = null, string accountGroup = null, string accountCategory = null, Method method = Method.GET, IWebProxy proxy = null)
        {
            try
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string shortPath = fullPath.Substring(fullPath.LastIndexOf('/') + 1);// для подписи
                string message = timestamp + "+" + shortPath;
                string signature = GenerateSignature(message, _secretKey);

                RestClient client = new RestClient(_baseUrl);

                //if (_myProxy != null)
                //{
                //    client.Proxy = _myProxy;
                //}
                //RestRequest request = new RestRequest(fullPath, Method.GET);
                RestRequest request = new RestRequest(fullPath, method);
                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("x-auth-key", _publicKey);
                request.AddHeader("x-auth-timestamp", timestamp.ToString());
                request.AddHeader("x-auth-signature", signature);

                // если передаётся тело запроса
                if (body != null)
                {
                    string jsonBody = JsonConvert.SerializeObject(body);
                    request.AddParameter("application/json", jsonBody, ParameterType.RequestBody);
                }
                IRestResponse response = client.Execute(request);

                return response;

            }
            catch (Exception ex)
            {
                SendLogMessage(ex.Message, LogMessageType.Error);
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