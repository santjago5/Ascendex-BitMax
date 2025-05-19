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
using System.Linq;
using Tinkoff.InvestApi.V1;
using System.Net.Sockets;
using System.Windows.Interop;







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

            Thread threadForPrivateMessages = new Thread(PrivateMessageReader);
            threadForPrivateMessages.IsBackground = true;
            threadForPrivateMessages.Name = "PrivateMessageReaderAscendexSpot";
            threadForPrivateMessages.Start();

            Thread threadCheckAliveWebSocket = new Thread(CheckAliveWebSocket);
            threadCheckAliveWebSocket.IsBackground = true;
            threadCheckAliveWebSocket.Name = "CheckAliveWebSocket";
            threadCheckAliveWebSocket.Start();

            Thread messageReaderPublic = new Thread(PublicMessageReader);
            messageReaderPublic.IsBackground = true;
            messageReaderPublic.Name = "PublicMessageReaderAscendexSpot";
            messageReaderPublic.Start();
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

                    AscendexSpotSecurityResponse result = JsonConvert.DeserializeObject<AscendexSpotSecurityResponse>(responseBody);

                    if (result != null && result.code == "0")
                    {
                        FIFOListWebSocketPublicMessage = new ConcurrentQueue<string>();
                        FIFOListWebSocketPrivateMessage = new ConcurrentQueue<string>();
                        CreatePrivateWebSocketConnect();
                        CheckSocketsActivate();

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
                        SendLogMessage(" Cannot unsubscribe — security is null or empty.", LogMessageType.Error);
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
            FIFOListWebSocketPublicMessage = null;

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

                        if (symbol.Contains("$"))
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

            IRestResponse response = CreatePrivateQuery(fullPath, null, null, null, Method.GET, null);

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
            else if (tf.TotalMinutes > 0)
            {
                return ((int)tf.TotalMinutes) + "m";
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
            //else if (tf.Hours > 0)
            //{
            //    return Convert.ToInt32(timePeriod.TotalHours / tf.TotalHours);
            //}
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



        #region 6 WebSocket creation

        private void PublicMessageReader()
        {
            Thread.Sleep(5000);

            while (true)
            {
                try
                {
                    if (ServerStatus == ServerConnectStatus.Disconnect)
                    {
                        Thread.Sleep(1000);
                        continue;
                    }

                    if (FIFOListWebSocketPublicMessage.IsEmpty)
                    {
                        Thread.Sleep(1);
                        continue;
                    }

                    if (FIFOListWebSocketPublicMessage.TryDequeue(out string message))
                    {
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
                        else if (message.Contains("\"m\":\"trades\""))
                        {
                            UpdateTrade(message);
                            continue;
                        }
                        else if (message.Contains("\"m\":\"ping\"")) 
                        {
                            webSocketPublic.Send("pong");
                            return;
                        }
                    }
                }
                catch (Exception exception)
                {
                    Thread.Sleep(2000);
                    SendLogMessage(exception.ToString(), LogMessageType.Error);
                }
            }
        }


        private ConcurrentQueue<string> FIFOListWebSocketPrivateMessage = new ConcurrentQueue<string>();
        private ConcurrentQueue<string> FIFOListWebSocketPublicMessage = new ConcurrentQueue<string>();

        private List<WebSocket> _webSocketPublic = new List<WebSocket>();
        private WebSocket webSocketPublic;
        private WebSocket _webSocketPrivate;
        private const string _webSocketUrl = "wss://ascendex.com/1/api/pro/v1/stream";

        private WebSocket CreateNewPublicSocket()
        {
            try
            {
                WebSocket webSocketPublicNew = new WebSocket(_webSocketUrl);

                //if (_myProxy != null)
                //{
                //    webSocketPublicNew.SetProxy(_myProxy);
                //}

                webSocketPublicNew.EmitOnPing = true;
                webSocketPublicNew.OnOpen += WebSocketPublicNew_OnOpen;
                webSocketPublicNew.OnClose += WebSocketPublicNew_OnClose;
                webSocketPublicNew.OnMessage += WebSocketPublicNew_OnMessage;
                webSocketPublicNew.OnError += WebSocketPublicNew_OnError;
                webSocketPublicNew.Connect();

                return webSocketPublicNew;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return null;
            }
        }

        private void CreatePrivateWebSocketConnect()
        {
            if (_webSocketPrivate != null)
                return;

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

        private void GenerateAuthenticate()
        {
            try
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string payload = timestamp + "+stream";
                string signature = CreateSignatureBase64(_secretKey, payload);

                var auth = new
                {
                    op = "auth",
                    t = timestamp,
                    key = _publicKey,
                    sig = signature
                };

                string authJson = JsonConvert.SerializeObject(auth);
                _webSocketPrivate.Send(authJson);
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

        /// <summary>
        /// Отписаться от канала Ascendex WebSocket
        /// </summary>
        /// <param name="socket">WebSocket объект</param>
        /// <param name="channel">Имя канала, например: order:cash</param>
        private void UnsubscribeChannel(WebSocket socket, string channel)
        {
            try
            {
                if (socket != null && socket.ReadyState == WebSocketState.Open)
                {
                    var unsub = new
                    {
                        op = "unsub",
                        ch = channel
                    };

                    string json = JsonConvert.SerializeObject(unsub);
                    socket.Send(json);
                    SendLogMessage("Unsubscribe: " + channel, LogMessageType.System);
                }
            }
            catch (Exception ex)
            {
                SendLogMessage("Error Unsubscribe " + channel + ": " + ex.Message, LogMessageType.Error);
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
        private void WebSocketPublicNew_OnOpen(object sender, EventArgs e)
        {
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    SendLogMessage("AscendexSpot WebSocket Public connection open", LogMessageType.System);
                    CheckSocketsActivate();
                }
            }
            catch (Exception ex)
            {
                SendLogMessage(ex.ToString(), LogMessageType.Error);
            }
        }
        private void WebSocketPublicNew_OnClose(object sender, CloseEventArgs e)
        {
            if (ServerStatus != ServerConnectStatus.Disconnect)
            {
                SendLogMessage($"Connection Closed by AscendexSpot. {e.Code} {e.Reason}. WebSocket Public Closed Event", LogMessageType.Error);
                ServerStatus = ServerConnectStatus.Disconnect;
                DisconnectEvent();
            }

        }
        private void WebSocketPublicNew_OnMessage(object sender, MessageEventArgs e)
        {

            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    return;
                }

                if (e == null)
                {
                    return;
                }


                if (FIFOListWebSocketPublicMessage == null)
                {
                    return;
                }
                //if (e.IsText)
                //{
                //    if (e.Data.Contains("ping"))
                //    {
                //        for (int i = 0; i < _webSocketPublic.Count; i++)
                //        {
                //            _webSocketPublic[i].Send("pong");
                //        }

                //        return;
                //    }
                //}

                if (e.IsText)
                {
                    FIFOListWebSocketPublicMessage.Enqueue(e.Data);
                }

            }
            catch (Exception error)
            {
                SendLogMessage(error.ToString(), LogMessageType.Error);
            }
        }
        private void WebSocketPublicNew_OnError(object sender, ErrorEventArgs e)
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

                    CheckSocketsActivate();

                    SendLogMessage("Subscribed to private channels (order & balance)", LogMessageType.System);

                }
            }
            catch (Exception ex)
            {
                SendLogMessage(ex.ToString(), LogMessageType.Error);
            }

        }
        private void _webSocketPrivate_OnClose(object sender, CloseEventArgs e)
        {
            if (ServerStatus != ServerConnectStatus.Disconnect)
            {
                SendLogMessage($"Connection Closed by AscendexSpot. {e.Code} {e.Reason}. WebSocket Private Closed Event", LogMessageType.Error);
                ServerStatus = ServerConnectStatus.Disconnect;
                DisconnectEvent();
            }
        }
        private void _webSocketPrivate_OnMessage(object sender, MessageEventArgs e)
        {
            try
            {
                if (ServerStatus != ServerConnectStatus.Connect)
                {
                    return;
                }

                if (e == null)
                {
                    return;
                }

                if (string.IsNullOrEmpty(e.ToString()))
                {
                    return;
                }

                if (FIFOListWebSocketPrivateMessage == null)
                {
                    return;
                }
                if (e.IsText && e.Data.Contains("\"m\":\"connected\"") && e.Data.Contains("\"type\":\"unauth\""))
                {

                    SendLogMessage("Connected to private channels ", LogMessageType.System);
                }

                if (e.IsText && e.Data.Contains("\"op\":\"auth\"") && e.Data.Contains("\"code\":0"))
                {

                    SendLogMessage("Authorization to private channels", LogMessageType.System);
                }

                else if (e.IsText && e.Data.Contains("ping"))
                {
                    _webSocketPrivate.Send("pong");
                    return;
                }
                else
                {
                    FIFOListWebSocketPrivateMessage.Enqueue(e.Data);
                }
            }
            catch (Exception error)
            {
                SendLogMessage(error.ToString(), LogMessageType.Error);
                SendLogMessage($"WebSocket Private Error message read. Error: {error}", LogMessageType.Error);
            }
        }
        private void _webSocketPrivate_OnError(object sender, ErrorEventArgs e)
        {

            if (e.Exception != null)
            {
                SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
            }
            else
            {
                SendLogMessage("WebSocket Private error" + e.ToString(), LogMessageType.Error);
            }
        }

        #endregion


        #region  7 WebSocket events


        //private void WebSocketPrivate_Opened(object sender, EventArgs e)
        //{
        //    try
        //    {
        //        string fullPath = "wss://ascendex.com/1/api/pro/v1/stream";

        //        GenerateAuthenticate();


        //        try
        //        {
        //            string apiPath = fullPath.Substring(fullPath.LastIndexOf('/') + 1);// для подписи
        //            long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        //            string message = timestamp + "+" + apiPath;
        //            string signature = GenerateSignature(_secretKey, message);

        //            var authPayload = new
        //            {
        //                op = "auth",
        //                //id = "abc123",
        //                t = timestamp,
        //                key = _publicKey,
        //                sig = signature
        //            };

        //            string json = JsonConvert.SerializeObject(authPayload);

        //            _webSocketPrivate.Send(json);

        //        }
        //        catch (Exception exception)
        //        {
        //            SendLogMessage(exception.ToString(), LogMessageType.Error);
        //        }


        //        CheckActivationSockets();

        //        SendLogMessage("Connection to private data is Open", LogMessageType.System);
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}


        //private void WebSocketPrivate_Closed(object sender, CloseEventArgs e)
        //{
        //    try
        //    {
        //        Disconnect();
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }

        //    SendLogMessage($"Connection Closed by  AscendexSpot. WebSocket Private closed. Code: {e.Code}", LogMessageType.Error);
        //}

        //private void WebSocketPrivate_Error(object sender, ErrorEventArgs e)
        //{
        //    try
        //    {
        //        //if (!string.IsNullOrEmpty(e.Message))
        //        //{
        //        //    SendLogMessage($"WebSocket private Error: {e.Message}", LogMessageType.Error);
        //        //}
        //        if (e.Exception != null)
        //        {
        //            SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
        //        }
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}

        //private void WebSocketPrivate_MessageReceived(object sender, MessageEventArgs e)
        //{
        //    try
        //    {
        //        if (ServerStatus == ServerConnectStatus.Disconnect
        //            || e?.Data == null
        //            || string.IsNullOrEmpty(e?.Data))
        //        {
        //            return;
        //        }

        //        if (FIFOListWebSocketPrivateMessage == null)
        //        {
        //            return;
        //        }

        //        if (e.Data.Contains("pong"))
        //        {

        //        }

        //        if (e.Data.Contains("\"m\":\"auth\"") && e.Data.Contains("\"code\":0"))
        //        {


        //        }

        //        FIFOListWebSocketPrivateMessage?.Enqueue(e.Data);
        //    }

        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}





        private string _socketActivateLocker = "socketActivateLocker";
        private List<string> _subscribledSecutiries = new List<string>();
        private void CheckSocketsActivate()
        {
            try
            {
                lock (_socketActivateLocker)
                {
                    if (_webSocketPrivate == null
                       || _webSocketPrivate?.ReadyState != WebSocketState.Open)
                    {
                        Disconnect();
                        return;
                    }

                    if (_subscribledSecutiries.Count > 0)
                    {
                        if (_webSocketPublic.Count == 0
                            || _webSocketPublic == null)
                        {
                            //Disconnect();
                            return;
                        }

                        WebSocket webSocketPublic = _webSocketPublic[0];

                        if (webSocketPublic == null
                            || webSocketPublic?.ReadyState != WebSocketState.Open)
                        {
                            Disconnect();
                            return;
                        }
                    }

                    if (ServerStatus != ServerConnectStatus.Connect)
                    {
                        ServerStatus = ServerConnectStatus.Connect;
                        ConnectEvent();
                    }
                }
            }
            catch (Exception ex)
            {
                SendLogMessage(ex.Message, LogMessageType.Error);
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
                    if (_webSocketPublic != null
                        && (_webSocketPrivate.ReadyState == WebSocketState.Open
                    || _webSocketPrivate.ReadyState == WebSocketState.Connecting))
                    {
                        _webSocketPrivate.Send("{\"event\":\"ping\", \"cid\":1277}");
                    }
                    else
                    {
                        Disconnect();
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

        #region  9  WebSocket security subscrible


        private RateGate _rateGateSubscribed = new RateGate(1, TimeSpan.FromMilliseconds(2500));

        public void Subscrible(Security security)
        {
            try
            {
                _rateGateSubscribed.WaitToProceed();

                CreateSubscribleMessageWebSocket(security);
                Thread.Sleep(100);
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

                for (int i = 0; i < _subscribledSecutiries.Count; i++)
                {
                    if (_subscribledSecutiries[i].Equals(security.Name))
                    {
                        return;
                    }
                }

                if (_webSocketPublic.Count == 0)
                {
                    WebSocket socket = CreateNewPublicSocket();

                    if (socket == null)
                    {
                        return;
                    }

                    DateTime timeEnd = DateTime.Now.AddSeconds(10);
                    while (socket.ReadyState != WebSocketState.Open)
                    {
                        Thread.Sleep(1000);

                        if (timeEnd < DateTime.Now)
                        {
                            break;
                        }
                    }

                    if (socket.ReadyState == WebSocketState.Open)
                    {
                        _webSocketPublic.Add(socket);
                    }
                }

                if (_webSocketPublic.Count == 0)
                {
                    return;
                }

                _subscribledSecutiries.Add(security.Name);

                WebSocket webSocketPublic = _webSocketPublic[_webSocketPublic.Count - 1];

                if (webSocketPublic.ReadyState == WebSocketState.Open
                    && _subscribledSecutiries.Count != 0
                    && _subscribledSecutiries.Count % 60 == 0)
                {
                    // creating a new socket
                    WebSocket newSocket = CreateNewPublicSocket();

                    DateTime timeEnd = DateTime.Now.AddSeconds(10);
                    while (newSocket.ReadyState != WebSocketState.Open)
                    {
                        Thread.Sleep(1000);

                        if (timeEnd < DateTime.Now)
                        {
                            break;
                        }
                    }

                    if (newSocket.ReadyState == WebSocketState.Open)
                    {
                        _webSocketPublic.Add(newSocket);
                        webSocketPublic = newSocket;
                    }
                }

                if (webSocketPublic != null)
                {

                    webSocketPublic.Send($"{{\"op\":\"sub\",\"ch\":\"depth:{security.Name}\"}}");
                    webSocketPublic.Send($"{{\"op\":\"sub\",\"ch\":\"trades:{security.Name}\"}}");
                    webSocketPublic.Send($"{{\"op\":\"req\",\"action\":\"depth-snapshot\",\"args\":{{\"symbol\":\"{security.Name}\"}}}}");

                }


            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }
        private void UnsubscribeFromAllWebSockets()
        {
            try
            {
                // Проверяем наличие публичных WebSocket-подключений
                if (_webSocketPublic != null && _webSocketPublic.Count != 0)
                {
                    for (int i = 0; i < _webSocketPublic.Count; i++)
                    {
                        WebSocket webSocketPublic = _webSocketPublic[i];

                        try
                        {
                            // Проверяем, открыт ли сокет
                            if (webSocketPublic != null && webSocketPublic.ReadyState == WebSocketState.Open)
                            {
                                // Проверяем наличие подписанных инструментов
                                if (_subscribledSecutiries != null)
                                {
                                    for (int i2 = 0; i2 < _subscribledSecutiries.Count; i2++)
                                    {
                                        string symbol = _subscribledSecutiries[i2];

                                        // Отписываемся от трейдов
                                        webSocketPublic.Send($"{{\"op\":\"unsub\",\"ch\":\"trades:{symbol}\"}}");

                                        // Отписываемся от стакана
                                        webSocketPublic.Send($"{{\"op\":\"unsub\",\"ch\":\"depth:{symbol}\"}}");
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            // Логируем ошибку отписки
                            SendLogMessage($"{ex.Message} {ex.StackTrace}", LogMessageType.Error);
                        }
                    }
                }
            }
            catch
            {
                // Игнорируем общие ошибки
            }

            // Обработка приватного WebSocket
            if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
            {
                try
                {
                    // Отписка от приватного канала заказов
                    _webSocketPrivate.Send("{\"op\":\"unsub\",\"ch\":\"order:cash\"}");
                }
                catch
                {
                    // Игнорируем ошибки
                }
            }
        }

        private void DeleteWebSocketConnection()
        {
            if (_webSocketPublic != null)
            {
                try
                {
                    for (int i = 0; i < _webSocketPublic.Count; i++)
                    {
                        WebSocket webSocketPublic = _webSocketPublic[i];

                        webSocketPublic.OnOpen -= WebSocketPublicNew_OnOpen;
                        webSocketPublic.OnClose -= WebSocketPublicNew_OnClose;
                        webSocketPublic.OnMessage -= WebSocketPublicNew_OnMessage;
                        webSocketPublic.OnError -= WebSocketPublicNew_OnError;

                        if (webSocketPublic.ReadyState == WebSocketState.Open)
                        {
                            webSocketPublic.CloseAsync();
                        }
                        webSocketPublic = null;
                    }
                }
                catch
                {
                    // ignore
                }

                _webSocketPublic.Clear();
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




        //private void UnsubscribeFromAllChannels(Security security)
        //{
        //    try
        //    {
        //        if (ServerStatus == ServerConnectStatus.Disconnect)
        //        {
        //            return;
        //        }

        //        for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
        //        {
        //            WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[i];

        //            if (webSocketPublicMarketDepths != null && webSocketPublicMarketDepths?.ReadyState == WebSocketState.Open)
        //            {
        //                //  { "op": "unsub", "id": "abc123", "ch":"trades:ASD/USDT" }
        //                string message = $"{{\"op\":\"unsub\",\"ch\":\"depth:{security.Name}\"}}";

        //                webSocketPublicMarketDepths.Send(message);
        //            }
        //        }

        //        for (int i = 0; i < _webSocketPublicTrades.Count; i++)
        //        {
        //            WebSocket webSocketPublicTrades = _webSocketPublicTrades[i];

        //            if (webSocketPublicTrades != null && webSocketPublicTrades?.ReadyState == WebSocketState.Open)
        //            {
        //                string message = $"{{\"op\":\"unsub\",\"ch\":\"trades:{security.Name}\"}}";

        //                webSocketPublicTrades.Send(message);
        //            }

        //            SendLogMessage("All subscriptions have been successfully removed", LogMessageType.System);
        //        }
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage("Error unsubscribing from channels:" + exception.ToString(), LogMessageType.Error);
        //    }
        //}

        #endregion

        #region  10 WebSocket parsing the messages

        public event Action<List<Security>> SecurityEvent;
        public event Action<News> NewsEvent;
        public event Action<MarketDepth> MarketDepthEvent;
        public event Action<Trade> NewTradesEvent;
        public event Action<Order> MyOrderEvent;
        public event Action<MyTrade> MyTradeEvent;
        public event Action<OptionMarketDataForConnector> AdditionalMarketDataEvent;


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



        // Обработка стакана: инициализация снапшотом и обновлениями
        private Dictionary<string, AscendexSpotDepthResponse> _depths = new Dictionary<string, AscendexSpotDepthResponse>();

        // Храним все активные MarketDepth по инструментам
        private List<MarketDepth> _allDepths = new List<MarketDepth>();
        private bool _snapshotInitialized = false;
        private long _lastSeqNum = -1;
        private string _currentSymbol = string.Empty;

        // Метод обработки снапшота стакана
        private void SnapshotDepth(string message)
        {
            // Десериализуем JSON-сообщение в структуру AscendexSpotDepthMessage
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
            for (int i = 0; i < snapshot.data.bids.Count; i++)
            {
                var level = snapshot.data.bids[i];
                newDepth.Bids.Add(new MarketDepthLevel
                {
                    Price = level[0].ToDecimal(),
                    Bid = level[1].ToDecimal()
                });
            }

            // Добавляем уровни ASK (продажи)
            for (int i = 0; i < snapshot.data.asks.Count; i++)
            {
                var level = snapshot.data.asks[i];
                newDepth.Asks.Add(new MarketDepthLevel
                {
                    Price = level[0].ToDecimal(),
                    Ask = level[1].ToDecimal()
                });
            }

            // Передаем стакан в систему
            MarketDepthEvent?.Invoke(newDepth);

            // Обновляем локальное хранилище стаканов
            var needDepth = _allDepths.Find(d => d.SecurityNameCode == newDepth.SecurityNameCode);
            if (needDepth != null)
            {
                _allDepths.Remove(needDepth);
            }
            _allDepths.Add(newDepth);
        }

        // Метод обновления стакана по дельте
        private void UpdateDepth(string json)
        {
            try
            {
                // Десериализуем входящее сообщение
                var update = JsonConvert.DeserializeObject<AscendexSpotDepthMessage>(json);
                if (update?.data == null /*|| update.symbol != _currentSymbol*/)
                    return;

                if (!_snapshotInitialized) return;

                // Проверка: если seqnum пропущен — нужно обновить снапшот
                if (_lastSeqNum != -1 && Convert.ToInt64(update.data.seqnum) != _lastSeqNum + 1)
                {
                    _snapshotInitialized = false;
                    _lastSeqNum = -1;
                  //  RequestSnapshot(_currentSymbol);
                    return;
                }

                _lastSeqNum = Convert.ToInt64(update.data.seqnum);

                // Находим соответствующий стакан
                var depth = _allDepths.Find(d => d.SecurityNameCode == update.symbol);
                if (depth == null)
                    return;

                // Применяем изменения
                ApplyLevels(update.data.bids, depth.Bids, isBid: true);
                ApplyLevels(update.data.asks, depth.Asks, isBid: false);

                // Обновляем время и передаём дальше
                depth.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(update.data.ts));
                MarketDepthEvent?.Invoke(depth.GetCopy());
            }
            catch (Exception ex)
            {
                SendLogMessage("Ошибка обновления стакана: " + ex.Message, LogMessageType.Error);
            }
        }

        // Метод подписки на обновление стакана
        //private void SubscribeDepth(string symbol)
        //{
        //    webSocketPublic.Send($"{{\"op\":\"sub\",\"ch\":\"depth:{symbol}\"}}");
        //}

        //// Метод запроса снапшота стакана
        //private void RequestSnapshot(string symbol)
        //{
        //    webSocketPublic.Send($"{{\"op\":\"req\",\"action\":\"depth-snapshot\",\"args\":{{\"symbol\":\"{symbol}\"}}}}");
        //}

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

        private void UpdateMyTrade(string message)
        {
            try
            {
                //WebSocketMessage<AscendexSpotMyTradeData> tradeMessage = JsonConvert.DeserializeObject<WebSocketMessage<AscendexSpotMyTradeData>>(message);

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
                orderId = order.NumberMarket,
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

            var body = new { symbol = security.Name };


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

        //public IRestResponse CreatePrivateQuery(string fullPath, object body = null, Method method = Method.GET)
        //{
        //    long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        //    string shortPath = fullPath.Substring(fullPath.LastIndexOf('/') + 1);
        //    string message = timestamp + "+" + shortPath;
        //    string signature = GenerateSignature(message, _secretKey);

        //    RestClient client = new RestClient(_baseUrl);
        //    RestRequest request = new RestRequest(fullPath, method);

        //    request.AddHeader("Content-Type", "application/json");
        //    request.AddHeader("x-auth-key", _publicKey);
        //    request.AddHeader("x-auth-timestamp", timestamp.ToString());
        //    request.AddHeader("x-auth-signature", signature);

        //    if (body != null && method != Method.GET)
        //    {
        //        string jsonBody = JsonConvert.SerializeObject(body);
        //        request.AddParameter("application/json", jsonBody, ParameterType.RequestBody);
        //    }

        //    return client.Execute(request);
        //}

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
                if (body != null && method != Method.GET)
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