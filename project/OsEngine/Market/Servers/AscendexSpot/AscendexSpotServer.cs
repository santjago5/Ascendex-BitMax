using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Newtonsoft.Json;
using OsEngine.Entity;
using OsEngine.Language;
using OsEngine.Logging;
using OsEngine.Market.Servers.AscendexSpot.Json;
using OsEngine.Market.Servers.Entity;
using RestSharp;
using Candle = OsEngine.Entity.Candle;
using CloseEventArgs = OsEngine.Entity.WebSocketOsEngine.CloseEventArgs;
using ErrorEventArgs = OsEngine.Entity.WebSocketOsEngine.ErrorEventArgs;
using MessageEventArgs = OsEngine.Entity.WebSocketOsEngine.MessageEventArgs;
using Method = RestSharp.Method;
using Order = OsEngine.Entity.Order;
using Security = OsEngine.Entity.Security;
using Side = OsEngine.Entity.Side;
using Trade = OsEngine.Entity.Trade;
using WebSocket = OsEngine.Entity.WebSocketOsEngine.WebSocket;
using WebSocketState = OsEngine.Entity.WebSocketOsEngine.WebSocketState;

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

            Thread threadForPrivateMessages = new Thread(PrivateMessageReader);
            threadForPrivateMessages.IsBackground = true;
            threadForPrivateMessages.Name = "PrivateMessageReaderAscendexSpot";
            threadForPrivateMessages.Start();

            Thread threadCheckAliveWebSocket = new Thread(CheckAliveWebSocket);
            threadCheckAliveWebSocket.IsBackground = true;
            threadCheckAliveWebSocket.Name = "CheckAliveWebSocket";
            threadCheckAliveWebSocket.Start();
        }

        public DateTime ServerTime { get; set; }

        private RateGate _rateGateConnect = new RateGate(1, TimeSpan.FromSeconds(5));

        public void Connect(WebProxy proxy = null)
        {
            try
            {
                LoadOrderTrackers();

                _publicKey = ((ServerParameterString)ServerParameters[0]).Value;
                _secretKey = ((ServerParameterPassword)ServerParameters[1]).Value;

                if (string.IsNullOrEmpty(_publicKey) || string.IsNullOrEmpty(_secretKey))
                {
                    SendLogMessage("Error:Invalid public or secret key.", LogMessageType.Error);
                    return;
                }

                _rateGateConnect.WaitToProceed();

                string _apiPath = "/api/pro/v2/assets";

                IRestResponse response = CreatePublicQuery(_apiPath, Method.GET);

                if (string.IsNullOrEmpty(response.Content))
                {
                    return;
                }

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotSecurityResponse result = JsonConvert.DeserializeObject<AscendexSpotSecurityResponse>(response.Content);

                    if (result != null && result.code == "0")
                    {
                        FIFOListWebSocketPublicMarketDepthsMessage = new ConcurrentQueue<string>();
                        FIFOListWebSocketPrivateMessage = new ConcurrentQueue<string>();
                        CreatePrivateWebSocketConnect();
                        CheckSocketsActivate();

                        SendLogMessage("Start AscendExSpot Connection", LogMessageType.System);
                    }
                    else
                    {
                        SendLogMessage("Status: Maintenance mode", LogMessageType.System);
                    }
                }
                else
                {
                    SendLogMessage($"No connection to AscendExSpot server. Code:{response.StatusCode}, Error:{response.Content}", LogMessageType.Error);
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

        private List<string> _subscribedSecutiries = new List<string>();

        private void CheckSocketsActivate()
        {
            try
            {
                lock (_socketActivateLocker)
                {
                    if (_webSocketPrivate == null || 
                        _webSocketPrivate?.ReadyState != WebSocketState.Open)
                    {
                        Disconnect();
                        return;
                    }

                    if (_subscribedSecutiries.Count > 0)
                    {
                        if (_webSocketPublicMarketDepths.Count == 0 ||
                            _webSocketPublicMarketDepths == null)
                        {
                            //Disconnect();
                            return;
                        }

                        WebSocket webSocketPublic = _webSocketPublicMarketDepths[0];

                        if (webSocketPublic == null || 
                            webSocketPublic?.ReadyState != WebSocketState.Open)
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
            catch (Exception exception)
            {
                SendLogMessage(exception.Message, LogMessageType.Error);
            }
        }

        public void Dispose()
        {
            try
            {
                UnsubscribeFromAllWebSockets();

                DeleteWebSocketConnection();
            }
            catch (Exception exception)
            {
                SendLogMessage("Dispose method error: " + exception.ToString(), LogMessageType.Error);
            }

            FIFOListWebSocketPublicMarketDepthsMessage = null;
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

        private string _accountCategory = "cash";

        #endregion

        #region 3 Securities

        private RateGate _rateGateSecurity = new RateGate(1, TimeSpan.FromMilliseconds(2100));

        public void GetSecurities()
        {
            try
            {
                _rateGateSecurity.WaitToProceed();

                string _apiPath = $"api/pro/v1/{_accountCategory}/products";

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
                            newSecurity.Lot = securityList.data[i].lotSize.ToDecimal();
                            newSecurity.State = SecurityStateType.Activ;
                            newSecurity.PriceStep = securityList.data[i].tickSize.ToString().ToDecimal();
                            newSecurity.Decimals = price.DecimalsCount() == 0 ? 1 : price.DecimalsCount();
                            newSecurity.PriceStepCost = newSecurity.PriceStep;
                            newSecurity.DecimalsVolume = Convert.ToInt32(securityList.data[i].qtyScale);
                            newSecurity.MinTradeAmount = securityList.data[i].minQty.ToString().ToDecimal();
                            newSecurity.MinTradeAmountType = MinTradeAmountType.Contract;
                            newSecurity.VolumeStep = newSecurity.DecimalsVolume.GetValueByDecimals();

                            securities.Add(newSecurity);
                        }

                        SecurityEvent?.Invoke(securities);
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
            try
            {
                string fullPath = $"/api/pro/v1/info";

                string prehashPath = "info";

                IRestResponse response = CreatePrivateQuery(fullPath, prehashPath, null, Method.GET/*, null*/);

                if (response == null || response.StatusCode != HttpStatusCode.OK)
                {
                    SendLogMessage($"Failed to get account group> {response.Content}", LogMessageType.Error);

                    return string.Empty;
                }

                ApiKeyInfoResponse responses = JsonConvert.DeserializeObject<ApiKeyInfoResponse>(response.Content);

                if (responses.code == "0" && responses.data != null)
                {
                    return responses.data.accountGroup;
                }
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }

            return string.Empty;
        }

        private string GetNameClass(string security)
        {
            if (security.EndsWith("USD")) return "USD";
            if (security.EndsWith("USDT")) return "USDT";
            if (security.EndsWith("BTC")) return "BTC";

            return "CurrencyPair";
        }

        #endregion

        #region 4 Portfolios

        private List<Portfolio> _portfolios = new List<Portfolio>();

        public event Action<List<Portfolio>> PortfolioEvent;

        private RateGate _rateGatePortfolio = new RateGate(1, TimeSpan.FromMilliseconds(2000));

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

                _portfolios.Clear(); // очищаем старые данные

                string accountGroup = GetAccountGroup();
                string fullPath = $"/{accountGroup}/api/pro/v1/{_accountCategory}/balance";
                string prehashPath = "balance";

                IRestResponse response = CreatePrivateQuery(fullPath, prehashPath, null, Method.GET/*, _myProxy*/);

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

            //return GetCandleHistory(security.NameFull, timeFrameBuilder.TimeFrameTimeSpan, true, countNeedToLoad, endTime);
            List<Candle> candles = GetCandleHistory(security.NameFull, timeFrameBuilder.TimeFrameTimeSpan, true, countNeedToLoad, endTime);

            if (candles == null || candles.Count == 0)
            {
                return null;
            }

            return candles;
        }

        //public List<Candle> GetCandleHistory(string nameSec, TimeSpan tf, bool isOsData, int countToLoad, DateTime timeEnd)
        //{


        //    int candlesLoaded = 0;

        //    if (periodEnd > DateTime.UtcNow)
        //    {
        //        periodEnd = DateTime.UtcNow;
        //    }
        //    while (candlesLoaded < countToLoad)
        //    {
        //        int candlesToLoad = Math.Min(limit, countToLoad - candlesLoaded);

        //        List<Candle> rangeCandles = CreateQueryCandles(nameSec, timeFrame, periodEnd, candlesToLoad);

        //        if (rangeCandles == null || rangeCandles.Count == 0)
        //        {
        //            break;
        //        }

        //        for (int i = 0; i < rangeCandles.Count; i++)
        //        {
        //            if (uniqueTimes.Add(rangeCandles[i].TimeStart))
        //            {
        //                allCandles.Add(rangeCandles[i]);
        //            }
        //        }

        //        candlesLoaded += rangeCandles.Count;

        //        periodEnd = rangeCandles[0].TimeStart;

        //        if (periodEnd <= timeEnd - TimeSpan.FromMinutes(tf.TotalMinutes * countToLoad))
        //        {
        //            break;
        //        }
        //    }

        //    for (int i = allCandles.Count - 1; i >= 0; i--)
        //    {
        //        if (allCandles[i].TimeStart > timeEnd)
        //        {
        //            allCandles.RemoveAt(i);
        //        }
        //    }

        //    allCandles.Sort((a, b) => a.TimeStart.CompareTo(b.TimeStart));

        //    return allCandles;
        //}

        public List<Candle> GetCandleHistory(string nameSec, TimeSpan tf, bool isOsData, int countToLoad, DateTime timeEnd)
        {
            string timeFrame = GetInterval(tf);

            int limit = 480;

            List<Candle> allCandles = new List<Candle>();

            HashSet<DateTime> uniqueTimes = new HashSet<DateTime>();

            int candlesLoaded = 0;

            DateTime periodEnd = timeEnd;

            DateTime periodStart = timeEnd.AddMinutes(-countToLoad * tf.TotalMinutes);

            while (candlesLoaded < countToLoad)
            {
                int candlesToLoad = Math.Min(limit, countToLoad - candlesLoaded);

                List<Candle> rangeCandles = CreateQueryCandles(nameSec, timeFrame, periodEnd, candlesToLoad);

                if (rangeCandles == null || rangeCandles.Count == 0)
                {
                    break;
                }

                for (int i = 0; i < rangeCandles.Count; i++)
                {
                    if (uniqueTimes.Add(rangeCandles[i].TimeStart))
                    {
                        allCandles.Add(rangeCandles[i]);
                    }
                }

                periodEnd = rangeCandles[0].TimeStart;

                if (periodEnd <= periodStart)
                {
                    break;
                }

                candlesLoaded += rangeCandles.Count;
            }

            for (int i = allCandles.Count - 1; i >= 0; i--)
            {
                if (allCandles[i].TimeStart < periodStart || allCandles[i].TimeStart > timeEnd)
                {
                    allCandles.RemoveAt(i);
                }
            }
            if (allCandles.Count == 0)
            {
                return null;
            }
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
                timeFrameMinutes == 240 |
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

        private RateGate _rateGateCandleHistory = new RateGate(1, TimeSpan.FromMilliseconds(2000));

        //private List<Candle> CreateQueryCandles(string symbol, string interval/*, DateTime startTime*/, DateTime endTime, int limit)
        //{
        //    _rateGateCandleHistory.WaitToProceed();

        //    try
        //    {
        //        // long startDate = TimeManager.GetTimeStampMilliSecondsToDateTime(startTime);
        //        long endDate = TimeManager.GetTimeStampMilliSecondsToDateTime(endTime);

        //        string _apiPath = $"/api/pro/v1/barhist?symbol={symbol}&interval={interval}&n={limit}&to={endDate}";

        //        IRestResponse response = CreatePublicQuery(_apiPath, Method.GET/*, _myProxy*/);

        //        if (response.StatusCode == HttpStatusCode.OK)
        //        {
        //            AscendexSpotCandleResponse json = JsonConvert.DeserializeObject<AscendexSpotCandleResponse>(response.Content);

        //            if (json == null || json.code != "0" || json.data == null || json.data.Count == 0)
        //            {
        //                SendLogMessage($"{json.code}, {json.data}, Data format error or response code != 0", LogMessageType.Error);
        //                return null;
        //                // return new List<Candle>();
        //            }

        //            List<AscendexSpotCandleData> candleList = new List<AscendexSpotCandleData>();

        //            for (int i = 0; i < json.data.Count; i++)
        //            {
        //                AscendexSpotCandleData candleData = json.data[i].data;

        //                if (string.IsNullOrEmpty(candleData.o) || string.IsNullOrEmpty(candleData.c)
        //                || string.IsNullOrEmpty(candleData.h) || string.IsNullOrEmpty(candleData.l)
        //                || string.IsNullOrEmpty(candleData.v))
        //                {
        //                    continue;
        //                }

        //                if ((candleData.o).ToDecimal() == 0 || (candleData.c).ToDecimal() == 0 ||
        //                     (candleData.h.ToDecimal() == 0 || (candleData.l).ToDecimal() == 0 ||
        //                     (candleData.v).ToDecimal() == 0))
        //                {
        //                    continue;
        //                }

        //                //Candle candle = new Candle();

        //                //candle.TimeStart = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(candleData.ts));
        //                //candle.Open = Convert.ToDecimal(candleData.o);
        //                //candle.Close = Convert.ToDecimal(candleData.c);
        //                //candle.High = Convert.ToDecimal(candleData.h);
        //                //candle.Low = Convert.ToDecimal(candleData.l);
        //                //candle.Volume = Convert.ToDecimal(candleData.v);

        //                //candleList.Add(candle);
        //                AscendexSpotCandleData candle = new AscendexSpotCandleData();
        //            }

        //            if (candleList.Count == 0)
        //            {
        //                return null;
        //            }

        //            return ConvertToCandles(candleList);
        //        }
        //        else
        //        {
        //            SendLogMessage($"Failed to query candles. Code: {response.StatusCode}, Error: {response.Content}", LogMessageType.Error);
        //        }
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage($"Request error: {exception.Message}", LogMessageType.Error);
        //    }

        //    return null;
        //}
        private List<Candle> CreateQueryCandles(string symbol, string interval, DateTime endTime, int limit)
        {
            _rateGateCandleHistory.WaitToProceed();

            try
            {
                long endDate = TimeManager.GetTimeStampMilliSecondsToDateTime(endTime);

                string _apiPath = $"/api/pro/v1/barhist?symbol={symbol}&interval={interval}&n={limit}&to={endDate}";

                IRestResponse response = CreatePublicQuery(_apiPath, Method.GET);

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCandleResponse json = JsonConvert.DeserializeObject<AscendexSpotCandleResponse>(response.Content);

                    if (json == null || json.code != "0" || json.data == null || json.data.Count == 0)
                    {
                        return null;
                    }

                    List<Candle> candles = new List<Candle>();

                    for (int i = 0; i < json.data.Count; i++)
                    {
                        AscendexSpotCandleData candleData = json.data[i].data;

                        if (string.IsNullOrEmpty(candleData.o) || string.IsNullOrEmpty(candleData.c) ||
                            string.IsNullOrEmpty(candleData.h) || string.IsNullOrEmpty(candleData.l) ||
                            string.IsNullOrEmpty(candleData.v))
                        {
                            continue;
                        }

                        if (candleData.o.ToDecimal() == 0 || candleData.c.ToDecimal() == 0 ||
                            candleData.h.ToDecimal() == 0 || candleData.l.ToDecimal() == 0 ||
                            candleData.v.ToDecimal() == 0)
                        {
                            continue;
                        }

                        Candle newCandle = new Candle();

                        newCandle.State = CandleState.Finished;
                        newCandle.TimeStart = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(candleData.ts));
                        newCandle.Open = candleData.o.ToDecimal();
                        newCandle.Close = candleData.c.ToDecimal();
                        newCandle.High = candleData.h.ToDecimal();
                        newCandle.Low = candleData.l.ToDecimal();
                        newCandle.Volume = candleData.v.ToDecimal();

                        candles.Add(newCandle);
                        SendLogMessage($"{symbol},{interval}", LogMessageType.System);
                    }

                    if (candles.Count == 0)
                    {
                        return null;
                    }

                    return candles;
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

        #endregion

        #region 6 WebSocket creation

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
                    else if (message.Contains("\"m\":\"error\""))
                    {
                        SendLogMessage($"Error  websocketDepth -{message}", LogMessageType.Error);
                        continue;
                    }

                    if (message.Contains("\"m\":\"depth-snapshot\""))
                    {
                        SnapshotDepth(message);
                    }
                    else if (message.Contains("\"m\":\"depth\""))
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

        //private void PublicMessageTradesReader()
        //{
        //    while (true)
        //    {
        //        try
        //        {
        //            if (ServerStatus == ServerConnectStatus.Disconnect)
        //            {
        //                Thread.Sleep(2000);
        //                continue;
        //            }

        //            if (FIFOListWebSocketPublicTradesMessage.IsEmpty)
        //            {
        //                Thread.Sleep(1);
        //                continue;
        //            }

        //            FIFOListWebSocketPublicTradesMessage.TryDequeue(out string message);

        //            if (message == null)
        //            {
        //                continue;
        //            }

        //            else if (message.Contains("\"m\":\"error\""))
        //            {
        //                SendLogMessage($"Error websocketTrades-{message}", LogMessageType.Error);
        //                continue;
        //            }
        //            if (message.Contains("\"m\":\"trades\""))
        //            {
        //                UpdateTrade(message);
        //            }
        //        }
        //        catch (Exception exception)
        //        {
        //            Thread.Sleep(5000);
        //            SendLogMessage(exception.ToString(), LogMessageType.Error);
        //        }
        //    }
        //}

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
                    else if (message.Contains("\"m\":\"error\""))
                    {
                        SendLogMessage($"Error webSocketPrivate {message}", LogMessageType.Error);
                        continue;
                    }

                    if (message.Contains("\"op\":\"auth\""))
                    {
                        SendLogMessage("WebSocket private opened", LogMessageType.System);

                        if (message.Contains("\"code\":0"))
                        {
                            SendLogMessage("Authorization to private channels", LogMessageType.System);
                        }
                        else
                        {
                            ServerStatus = ServerConnectStatus.Disconnect;
                            DisconnectEvent();
                            SendLogMessage($"WebSocket auth error {message}", LogMessageType.Error);
                        }

                        return;
                    }
                    else if (message.Contains("\"m\":\"order\""))
                    {
                        var orderMessage = JsonConvert.DeserializeObject<WebSocketMessage<AscendexSpotOrderData>>(message);

                        UpdateOrder(orderMessage);
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
        //
        //rivate ConcurrentQueue<string> FIFOListWebSocketPublicTradesMessage = new ConcurrentQueue<string>();

        //private List<WebSocket> _webSocketPublicTrades = new List<WebSocket>();
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

        private int _minReconnectIntervalSec = 8;
        private DateTime _lastMarketDepthsConnectTime = DateTime.MinValue;
        private DateTime _lastPublicTradesConnectTime = DateTime.MinValue;
        private DateTime _lastPrivateConnectTime = DateTime.MinValue;
        private readonly object _socketReconnectLock = new object();

        private void WaitUntilReconnectAvailable(ref DateTime lastConnectTime, int minIntervalSeconds, string socketName)
        {
            DateTime now = DateTime.UtcNow;

            double secondsSinceLastConnect = (now - lastConnectTime).TotalSeconds;

            if (secondsSinceLastConnect < minIntervalSeconds)
            {
                double waitTime = minIntervalSeconds - secondsSinceLastConnect;

                SendLogMessage($"[{socketName}] Задержка перед реконнектом: {waitTime:F2} сек.", LogMessageType.System);

                Thread.Sleep(TimeSpan.FromSeconds(waitTime));
            }

            //lastConnectTime = DateTime.UtcNow;
        }

        private int MaxWebSocketCount = 19; //максимум сокетов на ip (>20) после этого бан на 15 минут

        private WebSocket CreateNewPublicMarketDepthsSocket()
        {
            try
            {
                // Thread.Sleep(400);

                //lock (_socketReconnectLock)
                //{
                //    WaitUntilReconnectAvailable(ref _lastMarketDepthsConnectTime, _minReconnectIntervalSec, "MarketDepths");
                //}

                WebSocket webSocketPublicMarketDepthsNew = new WebSocket(_webSocketUrl);

                webSocketPublicMarketDepthsNew.EmitOnPing = false;
                webSocketPublicMarketDepthsNew.OnOpen += WebSocketPublicMarketDepthsNew_OnOpen;
                webSocketPublicMarketDepthsNew.OnClose += WebSocketPublicMarketDepthsNew_OnClose;
                webSocketPublicMarketDepthsNew.OnMessage += WebSocketPublicMarketDepthsNew_OnMessage;
                webSocketPublicMarketDepthsNew.OnError += WebSocketPublicMarketDepthsNew_OnError;
                webSocketPublicMarketDepthsNew.Connect().Wait();

                return webSocketPublicMarketDepthsNew;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return null;
            }
        }

        //private void CreatePublicWebSocketTradesConnect()
        //{
        //    try
        //    {
        //        if (_webSocketPublicTrades.Count > MaxWebSocketCount)
        //        {
        //            SendLogMessage("WebSocket  PublicTrades  limit exceeded: not creating new connection.", LogMessageType.Error);
        //            return;
        //        }
        //        if (FIFOListWebSocketPublicTradesMessage == null)
        //        {
        //            FIFOListWebSocketPublicTradesMessage = new ConcurrentQueue<string>();
        //        }

        //        _webSocketPublicTrades.Add(CreateNewPublicTradesSocket());
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage($"{exception.Message} {exception.StackTrace}", LogMessageType.Error);
        //    }
        //}

        //private WebSocket CreateNewPublicTradesSocket()
        //{
        //    try
        //    {
        //        // Thread.Sleep(1000);

        //        //lock (_socketReconnectLock)
        //        //{
        //        //    WaitUntilReconnectAvailable(ref _lastPublicTradesConnectTime, _minReconnectIntervalSec, "PublicTrades");
        //        //}

        //        WebSocket webSocketPublicTradesNew = new WebSocket(_webSocketUrl);

        //        webSocketPublicTradesNew.EmitOnPing = false;
        //        webSocketPublicTradesNew.OnOpen += WebSocketPublicTradesNew_OnOpen;
        //        webSocketPublicTradesNew.OnClose += WebSocketPublicTradesNew_OnClose;
        //        webSocketPublicTradesNew.OnMessage += WebSocketPublicTradesNew_OnMessage;
        //        webSocketPublicTradesNew.OnError += WebSocketPublicTradesNew_OnError;
        //        webSocketPublicTradesNew.Connect().Wait();
        //        _lastPublicTradesConnectTime = DateTime.UtcNow;
        //        return webSocketPublicTradesNew;
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //        return null;
        //    }
        //}

        //private void WebSocketPublicTradesNew_OnError(object sender, ErrorEventArgs e)
        //{
        //    try
        //    {
        //        if (e.Exception != null)
        //        {
        //            SendLogMessage($"AscendexSpot WebSocket Trades Error: {e.Exception}", LogMessageType.Error);
        //        }
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage("AscendexSpot Data socket Trades exception: " + exception.ToString(), LogMessageType.Error);
        //    }
        //}

        //private void WebSocketPublicTradesNew_OnMessage(object sender, MessageEventArgs e)
        //{
        //    try
        //    {
        //        if (ServerStatus == ServerConnectStatus.Disconnect ||
        //            e == null || string.IsNullOrEmpty(e.Data) ||
        //            FIFOListWebSocketPublicTradesMessage == null)
        //        {
        //            return;
        //        }
        //        if (e.IsText && e.Data.Contains("\"m\":\"connected\""))
        //        {
        //            if (e.Data.Contains("\"type\":\"unauth\""))
        //            {
        //                SendLogMessage("WebSocket Trades opened", LogMessageType.System);///system
        //            }
        //            return;
        //        }
        //        if (e.IsText && e.Data.Contains("\"m\":\"ping\""))
        //        {
        //            WebSocket socket = sender as WebSocket;

        //            if (socket != null && socket.ReadyState == WebSocketState.Open)
        //            {
        //                socket.Send("{\"op\":\"pong\"}");

        //            }

        //            return;
        //        }

        //        FIFOListWebSocketPublicTradesMessage.Enqueue(e.Data);
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}

        //private void WebSocketPublicTradesNew_OnClose(object sender, CloseEventArgs e)
        //{
        //    try
        //    {
        //        SendLogMessage($"{DateTime.Now:HH:mm:ss.fff}Socket closed PublicTrades. Reason: {e.Reason}", LogMessageType.System);
        //        Disconnect();

        //        SendLogMessage($"AscendexSpot Public Trades WebSocket closed by AscendexSpot. Code: {e.Code}", LogMessageType.Error);
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}

        //private void WebSocketPublicTradesNew_OnOpen(object sender, EventArgs e)
        //{
        //    try
        //    {
        //        CheckActivationSockets();

        //        SendLogMessage("WebSocket public Trades AscendexSpot open.", LogMessageType.System);///system
        //        SendLogMessage("Trades socket fully OPEN. Open sockets: " + CountOpenSockets(), LogMessageType.System);

        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}

        private void CreatePrivateWebSocketConnect()
        {
            try
            {
                // Thread.Sleep(1000);

                //lock (_socketReconnectLock)
                //{
                //    WaitUntilReconnectAvailable(ref _lastPrivateConnectTime, _minReconnectIntervalSec, "Private");
                //}

                if (_webSocketPrivate != null)
                {
                    return;
                }

                _webSocketPrivate = new WebSocket(_webSocketUrl);

                //if (_myProxy != null)
                //{
                //    _webSocketPrivate.SetProxy(_myProxy);
                //}

                _webSocketPrivate.EmitOnPing = false;
                _webSocketPrivate.OnOpen += _webSocketPrivate_OnOpen;
                _webSocketPrivate.OnClose += _webSocketPrivate_OnClose;
                _webSocketPrivate.OnMessage += _webSocketPrivate_OnMessage;
                _webSocketPrivate.OnError += _webSocketPrivate_OnError;

                _webSocketPrivate.Connect().Wait();
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
                            webSocketPublicMarketDepthsNew.CloseAsync().Wait();
                        }
                        webSocketPublicMarketDepthsNew.Dispose();
                        webSocketPublicMarketDepthsNew = null;

                        SendLogMessage($"[Reconnect][MarketDepths] Socket disconnected at {DateTime.Now:HH:mm:ss.fff}", LogMessageType.System);
                    }
                }
                catch
                {
                    // ignore
                }

                _webSocketPublicMarketDepths.Clear();
            }

            //if (_webSocketPublicTrades != null)
            //{
            //    try
            //    {
            //        for (int i = 0; i < _webSocketPublicTrades.Count; i++)
            //        {
            //            WebSocket webSocketPublicTradesNew = _webSocketPublicTrades[i];

            //            webSocketPublicTradesNew.OnOpen -= WebSocketPublicTradesNew_OnOpen;
            //            webSocketPublicTradesNew.OnClose -= WebSocketPublicTradesNew_OnClose;
            //            webSocketPublicTradesNew.OnMessage -= WebSocketPublicTradesNew_OnMessage;
            //            webSocketPublicTradesNew.OnError -= WebSocketPublicTradesNew_OnError;

            //            if (webSocketPublicTradesNew.ReadyState == WebSocketState.Open)
            //            {
            //                webSocketPublicTradesNew.CloseAsync().Wait();
            //                //_lastPublicTradesDisconnectTime = DateTime.UtcNow;

            //            }
            //            webSocketPublicTradesNew.Dispose();
            //            webSocketPublicTradesNew = null;
            //            SendLogMessage($"[Reconnect][Trades] Socket disconnected at {DateTime.Now:HH:mm:ss.fff}", LogMessageType.System);
            //        }
            //    }
            //    catch
            //    {
            //        // ignore
            //    }

            //    _webSocketPublicTrades.Clear();
            //}

            if (_webSocketPrivate != null)
            {
                try
                {
                    _webSocketPrivate.OnOpen -= _webSocketPrivate_OnOpen;
                    _webSocketPrivate.OnClose -= _webSocketPrivate_OnClose;
                    _webSocketPrivate.OnMessage -= _webSocketPrivate_OnMessage;
                    _webSocketPrivate.OnError -= _webSocketPrivate_OnError;

                    _webSocketPrivate.CloseAsync().Wait();
                    //_lastPrivateDisconnectTime = DateTime.UtcNow;

                    _webSocketPrivate.Dispose();

                    //  _privateOrderChannelSubscribed = false;
                }
                catch
                {
                    // ignore
                }
                _webSocketPrivate = null;
                SendLogMessage($"[Reconnect][Private] Socket disconnected at {DateTime.Now:HH:mm:ss.fff}", LogMessageType.System);
            }
        }

        #endregion

        private int CountOpenSockets()
        {
            int count = 0;

            if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
            {
                count++;
                SendLogMessage("Current OPEN WebSocket Private count: " + count, LogMessageType.System);
            }

            //for (int i = 0; i < _webSocketPublicTrades.Count; i++)
            //{
            //    if (_webSocketPublicTrades[i] != null && _webSocketPublicTrades[i].ReadyState == WebSocketState.Open)
            //    {
            //        count++;
            //        SendLogMessage("Current OPEN WebSocket Trades count: " + count, LogMessageType.System);///system
            //    }
            //}

            for (int i = 0; i < _webSocketPublicMarketDepths.Count; i++)
            {
                if (_webSocketPublicMarketDepths[i] != null && _webSocketPublicMarketDepths[i].ReadyState == WebSocketState.Open)
                {
                    count++;
                    SendLogMessage("Current OPEN WebSocket Depth count: " + count, LogMessageType.System);
                }
            }

            return count;
        }

        #region 7 WebSocket events

        private void WebSocketPublicMarketDepthsNew_OnOpen(object sender, EventArgs e)
        {
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    CheckActivationSockets();

                    SendLogMessage("AscendexSpot WebSocket MarketDepths connection open", LogMessageType.System);///system
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
                if (ServerStatus == ServerConnectStatus.Disconnect ||
                    e == null ||
                    string.IsNullOrEmpty(e.Data) ||
                    FIFOListWebSocketPublicMarketDepthsMessage == null)
                {
                    return;
                }

                if (e.IsText && e.Data.Contains("\"m\":\"connected\""))
                {
                    if (e.Data.Contains("\"type\":\"unauth\""))
                    {
                        SendLogMessage("WebSocket MarketDepth opened", LogMessageType.System);
                    }

                    return;
                }

                if (e.IsText && e.Data.Contains("\"m\":\"ping\""))
                {
                    WebSocket socket = sender as WebSocket;

                    if (socket != null && socket.ReadyState == WebSocketState.Open)
                    {
                        socket.Send("{\"op\":\"pong\"}");
                    }

                    return;
                }

                if (e.Data.Contains("\"m\":\"error\""))
                {
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
            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    return;
                }

                if (e.Exception != null)
                {
                    string message = e.Exception.ToString();

                    if (message.Contains("The remote party closed the WebSocket connection"))
                    {
                        // ignore
                    }
                    else
                    {
                        SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
                    }
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Data socket error" + exception.ToString(), LogMessageType.Error);
            }
        }

        private void _webSocketPrivate_OnOpen(object sender, EventArgs e)
        {
            Thread.Sleep(1000);

            try
            {
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    GenerateAuthenticate();

                    CheckActivationSockets();
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
                if (ServerStatus == ServerConnectStatus.Disconnect ||
                    e == null ||
                    string.IsNullOrEmpty(e.Data) ||
                    FIFOListWebSocketPrivateMessage == null)
                {
                    return;
                }

                if (e.IsText && e.Data.Contains("\"m\":\"ping\""))
                {
                    WebSocket socket = sender as WebSocket;
                    if (socket != null && socket.ReadyState == WebSocketState.Open)
                    {
                        socket.Send("{\"op\":\"pong\"}");
                    }
                    return;
                }

                if (e.IsText && e.Data.Contains("\"op\":\"auth\""))
                {
                    if (e.Data.Contains("\"code\":0"))
                    {
                        SendLogMessage("Authorization to private channels", LogMessageType.System);// system
                    }
                    else
                    {
                        ServerStatus = ServerConnectStatus.Disconnect;
                        DisconnectEvent();
                        SendLogMessage($"WebSocket private channel error {e.Data}", LogMessageType.Error);
                    }
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
                if (ServerStatus == ServerConnectStatus.Disconnect)
                {
                    return;
                }

                if (e.Exception != null)
                {
                    string message = e.Exception.ToString();

                    if (message.Contains("The remote party closed the WebSocket AscendexSpot connection"))
                    {
                        // ignore
                    }
                    else
                    {
                        SendLogMessage(e.Exception.ToString(), LogMessageType.Error);
                    }
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Data socket error" + exception.ToString(), LogMessageType.Error);
            }
        }

        private readonly object _socketActivateLocker = new object();

        private List<string> _subscribedSecurities = new List<string>();

        //private void CheckActivationSockets()
        //{
        //    lock (_socketActivateLocker)
        //    {
        //        try
        //        {
        //            if (_webSocketPrivate == null
        //               || _webSocketPrivate.ReadyState != WebSocketState.Open)
        //            {
        //                Disconnect();
        //                return;
        //            }

        //            if (_webSocketPublicMarketDepths.Count == 0)
        //            {
        //                Disconnect();
        //                return;
        //            }

        //            WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[0];

        //            if (webSocketPublicMarketDepths == null
        //                || webSocketPublicMarketDepths.ReadyState != WebSocketState.Open)
        //            {
        //                Disconnect();
        //                return;
        //            }

        //            if (_webSocketPublicTrades.Count == 0)
        //            {
        //                Disconnect();
        //                return;
        //            }

        //            WebSocket webSocketPublicTrades = _webSocketPublicTrades[0];

        //            if (webSocketPublicTrades == null
        //                || webSocketPublicTrades.ReadyState != WebSocketState.Open)
        //            {
        //                Disconnect();
        //                return;
        //            }

        //            if (ServerStatus != ServerConnectStatus.Connect)
        //            {
        //                ServerStatus = ServerConnectStatus.Connect;
        //                ConnectEvent();
        //            }

        //            SendLogMessage("All sockets activated.", LogMessageType.System);
        //        }
        //        catch (Exception exception)
        //        {
        //            SendLogMessage(exception.Message, LogMessageType.Error);
        //        }
        //    }
        //}

        private void CheckActivationSockets()
        {
            lock (_socketActivateLocker)
            {
                try
                {
                    int open = CountOpenSockets();
                    if (open > MaxWebSocketCount)
                    {
                        SendLogMessage("CheckActivation: Detected " + open + " sockets. Ascendex limit is 17!", LogMessageType.System);
                    }

                    if (_webSocketPublicMarketDepths.Count == 0)

                    {
                        SendLogMessage("CheckActivation: _webSocketPublicMarketDepths is EMPTY", LogMessageType.System);
                        Disconnect();
                        return;
                    }

                    WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[0];

                    if (webSocketPublicMarketDepths == null)
                    {
                        SendLogMessage($"CheckActivation: {webSocketPublicMarketDepths} is NULL", LogMessageType.System);
                        Disconnect();
                        return;
                    }
                    if (webSocketPublicMarketDepths.ReadyState != WebSocketState.Open)
                    {
                        SendLogMessage($"CheckActivation: {webSocketPublicMarketDepths} not OPEN: " + webSocketPublicMarketDepths.ReadyState, LogMessageType.System);
                        Disconnect();
                        return;
                    }

                    //if (_webSocketPublicTrades.Count == 0)
                    //{
                    //    SendLogMessage("CheckActivation: _webSocketPublicTrades is EMPTY", LogMessageType.System);
                    //    Disconnect();
                    //    return;
                    //}

                    //WebSocket webSocketPublicTrades = _webSocketPublicTrades[0];

                    //if (webSocketPublicTrades == null)
                    //{
                    //    SendLogMessage($"CheckActivation: {webSocketPublicTrades} is NULL", LogMessageType.System);
                    //    Disconnect();
                    //    return;
                    //}
                    //if (webSocketPublicTrades.ReadyState != WebSocketState.Open)
                    //{
                    //    SendLogMessage($"CheckActivation: {webSocketPublicTrades} not OPEN: " + webSocketPublicTrades.ReadyState, LogMessageType.System);
                    //    Disconnect();
                    //    return;
                    //}
                    if (_webSocketPrivate == null)
                    {
                        SendLogMessage("CheckActivation: _webSocketPrivate is NULL", LogMessageType.System);
                        Disconnect();
                        return;
                    }
                    if (_webSocketPrivate.ReadyState != WebSocketState.Open)
                    {
                        SendLogMessage("CheckActivation: _webSocketPrivate not OPEN: " + _webSocketPrivate.ReadyState, LogMessageType.System);
                        Disconnect();
                        return;
                    }

                    if (ServerStatus != ServerConnectStatus.Connect)
                    {
                        ServerStatus = ServerConnectStatus.Connect;
                        ConnectEvent();
                    }

                    SendLogMessage("All sockets activated.", LogMessageType.System);///system
                }
                catch (Exception exception)
                {
                    SendLogMessage("CheckActivation EXCEPTION: " + exception.Message, LogMessageType.Error);
                }
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
                            && webSocketPublicMarketDepths.ReadyState == WebSocketState.Open)
                        {
                            webSocketPublicMarketDepths.Send("{\"op\":\"ping\"}");
                        }
                        else
                        {
                            Disconnect();
                        }
                    }

                    //for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                    //{
                    //    WebSocket webSocketPublicTrades = _webSocketPublicTrades[i];
                    //    if (webSocketPublicTrades != null
                    //        && webSocketPublicTrades.ReadyState == WebSocketState.Open)
                    //    {
                    //        webSocketPublicTrades.Send("{\"op\":\"ping\"}");
                    //    }
                    //    else
                    //    {
                    //        Disconnect();
                    //    }

                    //}

                    if (_webSocketPrivate != null
                        && (_webSocketPrivate.ReadyState == WebSocketState.Open
                    || _webSocketPrivate.ReadyState == WebSocketState.Connecting))
                    {
                        _webSocketPrivate.Send("{\"op\":\"ping\"}");
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

        #region 9  WebSocket security subscribe

        private RateGate _rateGateSubscribed = new RateGate(1, TimeSpan.FromMilliseconds(790));

        public void Subscrible(Security security)//////ошибка в слове Subscribe
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

        private DateTime _lastSocketCreateTime = DateTime.MinValue;

        private bool _isPrivateSubscribed = false;

        private object _socketCreationLock = new object();

        private void CreateSubscribeMessageWebSocket(Security security)
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

                if (_webSocketPublicMarketDepths.Count == 0 /*|| _webSocketPublicTrades.Count == 0*/)
                {
                    return;
                }

                WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[_webSocketPublicMarketDepths.Count - 1];
                //  WebSocket webSocketPublicTrades = _webSocketPublicTrades[_webSocketPublicTrades.Count - 1];

                int MaxWebSocketCount = 18;      // Максимум сокетов на тип
                int MaxSubsPerSocket = 140;       // Подписок на один сокет

                int currentSocketInstrumentCount = _subscribedSecurities.Count % MaxSubsPerSocket;

                if (webSocketPublicMarketDepths.ReadyState == WebSocketState.Open
                   // &&   webSocketPublicTrades.ReadyState == WebSocketState.Open
                   && currentSocketInstrumentCount == 0)
                {
                    lock (_socketCreationLock)
                    {
                        if (_webSocketPublicMarketDepths.Count >= MaxWebSocketCount
                            //||_webSocketPublicTrades.Count >= MaxWebSocketCount
                            )
                        {
                            SendLogMessage("WebSocket connections limit exceeded. Subscription will be postponed.", LogMessageType.Error);
                            return;
                        }

                        if ((DateTime.Now - _lastSocketCreateTime).TotalSeconds < 10)
                        {
                            SendLogMessage("Слишком быстрое создание сокета. Подписка остановлена.", LogMessageType.Error);
                            return;
                        }

                        _lastSocketCreateTime = DateTime.Now;

                        //   _rateGateSubscribed.WaitToProceed();

                        SendLogMessage("Ждём перед созданием нового сокета...", LogMessageType.System);

                        WebSocket newSocketMarketDepths = CreateNewPublicMarketDepthsSocket();

                        //Thread.Sleep(2500);

                        DateTime timeEndMarketDepths = DateTime.Now.AddSeconds(15);
                        while (newSocketMarketDepths.ReadyState != WebSocketState.Open && DateTime.Now < timeEndMarketDepths)
                        {
                            Thread.Sleep(500);
                        }

                        if (newSocketMarketDepths.ReadyState == WebSocketState.Open)
                        {
                            _webSocketPublicMarketDepths.Add(newSocketMarketDepths);
                            webSocketPublicMarketDepths = newSocketMarketDepths;
                            SendLogMessage("Новый сокет для стаканов открыт. Всего сокетов: " + _webSocketPublicMarketDepths.Count, LogMessageType.System);
                        }

                        //  Thread.Sleep(2500);

                        //WebSocket newSocketTrades = CreateNewPublicTradesSocket();

                        //DateTime timeEndTrades = DateTime.Now.AddSeconds(15);
                        //while (newSocketTrades.ReadyState != WebSocketState.Open && DateTime.Now < timeEndTrades)
                        //{
                        //    Thread.Sleep(500);
                        //}

                        //if (newSocketTrades.ReadyState == WebSocketState.Open)
                        //{
                        //    _webSocketPublicTrades.Add(newSocketTrades);
                        //    webSocketPublicTrades = newSocketTrades;
                        //}
                    }
                }

                int socketIndexDepth = _webSocketPublicMarketDepths.IndexOf(webSocketPublicMarketDepths);
                // int socketIndexTrade = _webSocketPublicTrades.IndexOf(webSocketPublicTrades);
                int subCount = _subscribedSecurities.Count;

                if (webSocketPublicMarketDepths != null /*&& webSocketPublicTrades != null*/)
                {
                    _rateGateSubscribed.WaitToProceed();

                    webSocketPublicMarketDepths.Send($"{{\"op\":\"req\",\"action\":\"depth-snapshot\",\"args\":{{\"symbol\":\"{security.Name}\"}}}}");
                    webSocketPublicMarketDepths.Send($"{{\"op\":\"sub\",\"ch\":\"depth:{security.Name}\"}}");
                    webSocketPublicMarketDepths.Send($"{{\"op\":\"sub\",\"ch\":\"trades:{security.Name}\"}}");
                }

                if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open && !_isPrivateSubscribed)
                {
                    _rateGateSubscribed.WaitToProceed();
                    _webSocketPrivate.Send("{\"op\":\"sub\",\"ch\":\"order:cash\"}");
                    _isPrivateSubscribed = true;
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Ошибка подписки: " + exception.ToString(), LogMessageType.Error);
            }
        }

        //private void CreateSubscribeMessageWebSocket(Security security)
        //{
        //    try
        //    {
        //        if (ServerStatus == ServerConnectStatus.Disconnect)
        //        {
        //            return;
        //        }

        //        for (int i = 0; i < _subscribedSecurities.Count; i++)
        //        {
        //            if (_subscribedSecurities[i].Equals(security.Name))
        //            {
        //                return;
        //            }
        //        }

        //        _subscribedSecurities.Add(security.Name);

        //        if (_webSocketPublicMarketDepths.Count == 0 || _webSocketPublicTrades.Count == 0)
        //        {
        //            return;
        //        }

        //        WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[_webSocketPublicMarketDepths.Count - 1];
        //        WebSocket webSocketPublicTrades = _webSocketPublicTrades[_webSocketPublicTrades.Count - 1];

        //        int MaxWebSocketCount = 19;
        //        int MaxSubsPerSocket = 130;

        //        if (webSocketPublicMarketDepths.ReadyState == WebSocketState.Open
        //            && webSocketPublicTrades.ReadyState == WebSocketState.Open
        //            && _subscribedSecurities.Count != 0
        //            && _subscribedSecurities.Count % MaxSubsPerSocket == 0)
        //        {
        //            if (_webSocketPublicMarketDepths.Count >= MaxWebSocketCount || _webSocketPublicTrades.Count >= MaxWebSocketCount)
        //            {
        //                SendLogMessage("WebSocket connections limit exceeded. Subscription will be postponed.", LogMessageType.Error);
        //                return;
        //            }

        //            WebSocket newSocketMarketDepths = CreateNewPublicMarketDepthsSocket();
        //            WebSocket newSocketTrades = CreateNewPublicTradesSocket();

        //            DateTime timeEndMarketDepths = DateTime.Now.AddSeconds(20);
        //            while (newSocketMarketDepths.ReadyState != WebSocketState.Open)
        //            {
        //                Thread.Sleep(500);
        //                if (timeEndMarketDepths < DateTime.Now)
        //                {
        //                    break;
        //                }
        //            }

        //            if (newSocketMarketDepths.ReadyState == WebSocketState.Open)
        //            {
        //                _webSocketPublicMarketDepths.Add(newSocketMarketDepths);
        //                webSocketPublicMarketDepths = newSocketMarketDepths;
        //            }

        //            DateTime timeEndTrades = DateTime.Now.AddSeconds(20);
        //            while (newSocketTrades.ReadyState != WebSocketState.Open)
        //            {
        //                Thread.Sleep(500);
        //                if (timeEndTrades < DateTime.Now)
        //                {
        //                    break;
        //                }
        //            }

        //            if (newSocketTrades.ReadyState == WebSocketState.Open)
        //            {
        //                _webSocketPublicTrades.Add(newSocketTrades);
        //                webSocketPublicTrades = newSocketTrades;
        //            }
        //            Thread.Sleep(7000);
        //        }

        //        if (webSocketPublicMarketDepths != null && webSocketPublicTrades != null)
        //        {
        //            webSocketPublicMarketDepths.Send($"{{\"op\":\"req\",\"action\":\"depth-snapshot\",\"args\":{{\"symbol\":\"{security.Name}\"}}}}");
        //            Thread.Sleep(150);

        //            webSocketPublicMarketDepths.Send($"{{\"op\":\"sub\",\"ch\":\"depth:{security.Name}\"}}");
        //            Thread.Sleep(150);

        //            webSocketPublicTrades.Send($"{{\"op\":\"sub\",\"ch\":\"trades:{security.Name}\"}}");
        //            Thread.Sleep(150);
        //        }

        //        if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
        //        {
        //            _webSocketPrivate.Send("{\"op\":\"sub\",\"ch\":\"order:cash\"}");
        //        }
        //    }
        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}

        private void UnsubscribeFromAllWebSockets()
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

                    if (webSocketPublicMarketDepths != null && webSocketPublicMarketDepths.ReadyState == WebSocketState.Open)
                    {
                        try
                        {
                            if (_subscribedSecurities != null && _subscribedSecurities.Count > 0)
                            {
                                for (int j = 0; j < _subscribedSecurities.Count; j++)
                                {
                                    string symbol = _subscribedSecurities[j];

                                    webSocketPublicMarketDepths.Send($"{{\"op\":\"unsub\",\"ch\":\"depth:{symbol}\"}}");
                                }
                            }
                        }
                        catch (Exception exception)
                        {
                            SendLogMessage($"Unsubscribe error on public depth socket: {exception.Message} {exception.StackTrace}", LogMessageType.Error);
                        }
                    }
                }

                //for (int i = 0; i < _webSocketPublicTrades.Count; i++)
                //{
                //    WebSocket webSocketPublicTrades = _webSocketPublicTrades[i];

                //    if (webSocketPublicTrades != null && webSocketPublicTrades.ReadyState == WebSocketState.Open)
                //    {
                //        try
                //        {
                //            if (_subscribedSecurities != null && _subscribedSecurities.Count > 0)
                //            {
                //                for (int j = 0; j < _subscribedSecurities.Count; j++)
                //                {
                //                    string symbol = _subscribedSecurities[j];

                //                    webSocketPublicTrades.Send($"{{\"op\":\"unsub\",\"ch\":\"trades:{symbol}\"}}");
                //                }
                //            }

                //            //_tradeDictionary.Clear();
                //        }
                //        catch (Exception exception)
                //        {
                //            SendLogMessage($"Unsubscribe error on public trades socket: {exception.Message} {exception.StackTrace}", LogMessageType.Error);
                //        }
                //    }
                //}

                if (_webSocketPrivate != null && _webSocketPrivate.ReadyState == WebSocketState.Open)
                {
                    try
                    {
                        _webSocketPrivate.Send("{\"op\":\"unsub\",\"ch\":\"order:cash\"}");
                    }
                    catch (Exception exception)
                    {
                        SendLogMessage($"Unsubscribe error on private socket: {exception.Message} {exception.StackTrace}", LogMessageType.Error);
                    }
                }

                _subscribedSecurities.Clear();

                SendLogMessage("All subscriptions have been successfully removed", LogMessageType.System);
            }
            catch (Exception exception)
            {
                SendLogMessage($"General unsubscribe error: {exception.Message} {exception.StackTrace}", LogMessageType.Error);
            }
        }

        #endregion

        #region 10 WebSocket parsing the messages

        public event Action<List<Security>> SecurityEvent;

        public event Action<News> NewsEvent;

        public event Action<MarketDepth> MarketDepthEvent;

        public event Action<Trade> NewTradesEvent;

        public event Action<Order> MyOrderEvent;

        public event Action<MyTrade> MyTradeEvent;

        public event Action<OptionMarketDataForConnector> AdditionalMarketDataEvent;

        private List<MarketDepth> _allDepths = new List<MarketDepth>();

        private bool _snapshotInitialized = false;

        private long _lastSeqNum = -1;

        private DateTime _lastTimeMd = DateTime.MinValue;

        private void SnapshotDepth(string message)
        {
            try
            {
                AscendexSpotDepthMessage snapshot = JsonConvert.DeserializeObject<AscendexSpotDepthMessage>(message);

                if (snapshot == null || snapshot.data == null)
                    return;

                _lastSeqNum = Convert.ToInt64(snapshot.data.seqnum);
                _snapshotInitialized = true;

                MarketDepth newDepth = new MarketDepth();
                newDepth.SecurityNameCode = snapshot.symbol;
                newDepth.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(snapshot.data.ts));


                for (int i = 0; i < snapshot.data.bids.Count && i < 25; i++)
                {
                    var level = snapshot.data.bids[i];
                    newDepth.Bids.Add(new MarketDepthLevel
                    {
                        Price = level[0].ToDecimal(),
                        Bid = level[1].ToDecimal()
                    });
                }

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

                if (newDepth.Time <= _lastTimeMd)
                {
                    _lastTimeMd = _lastTimeMd.AddTicks(1);
                    newDepth.Time = _lastTimeMd;
                }
                else
                {
                    _lastTimeMd = newDepth.Time;
                }

                var needDepth = _allDepths.Find(d => d.SecurityNameCode == newDepth.SecurityNameCode);

                if (needDepth != null)
                {
                    _allDepths.Remove(needDepth);
                }
                _allDepths.Add(newDepth);

                if (newDepth.Bids.Count == 0 || newDepth.Asks.Count == 0)
                {
                    return;
                }

                MarketDepthEvent?.Invoke(newDepth.GetCopy());
            }
            catch (Exception error)
            {
                SendLogMessage(error.ToString(), LogMessageType.Error);
            }
        }

        private void UpdateDepth(string json)
        {
            try
            {
                var update = JsonConvert.DeserializeObject<AscendexSpotDepthMessage>(json);

                var depth = _allDepths.Find(d => d.SecurityNameCode == update.symbol);

                if (depth == null)
                    return;

                if (update?.data == null || update.symbol != depth.SecurityNameCode)
                    return;

                if (!_snapshotInitialized) return;

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

                ApplyLevels(update.data.bids, depth.Bids, isBid: true);
                ApplyLevels(update.data.asks, depth.Asks, isBid: false);

                depth.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(update.data.ts));

                depth.Bids.Sort((a, b) => b.Price.CompareTo(a.Price));

                List<MarketDepthLevel> topBids = new List<MarketDepthLevel>();

                for (int i = 0; i < depth.Bids.Count && i < 25; i++)
                {
                    topBids.Add(depth.Bids[i]);
                }
                depth.Bids = topBids;

                depth.Asks.Sort((a, b) => a.Price.CompareTo(b.Price));

                List<MarketDepthLevel> topAsks = new List<MarketDepthLevel>();

                for (int i = 0; i < depth.Asks.Count && i < 25; i++)
                {
                    topAsks.Add(depth.Asks[i]);
                }
                depth.Asks = topAsks;

                if (depth.Bids.Count == 0 || depth.Asks.Count == 0)
                {
                    return;
                }
                MarketDepthEvent?.Invoke(depth.GetCopy());
            }
            catch (Exception exception)
            {
                SendLogMessage("Depth of Market update error: " + exception.Message, LogMessageType.Error);
            }
        }

        private void RequestSnapshot(string symbol)
        {
            WebSocket webSocketPublicMarketDepths = _webSocketPublicMarketDepths[_webSocketPublicMarketDepths.Count - 1];

            if (webSocketPublicMarketDepths.ReadyState == WebSocketState.Open)
            {
                webSocketPublicMarketDepths.Send($"{{\"op\":\"req\",\"action\":\"depth-snapshot\",\"args\":{{\"symbol\":\"{symbol}\"}}}}");
            }
        }

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

        private void DeleteLevel(decimal price, Side side, MarketDepth marketDepth)
        {
            var levels = side == Side.Buy ? marketDepth.Bids : marketDepth.Asks;
            var level = levels.Find(l => l.Price == price);
            if (level != null)
                levels.Remove(level);
        }

        private void SortBids(List<MarketDepthLevel> levels)
        {
            levels.Sort((a, b) => b.Price.CompareTo(a.Price));
        }

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

        //private void UpdateMyTrade(string json)
        //{
        //    try
        //    {
        //        AscendexSpotQueryOrderResponse response = JsonConvert.DeserializeObject<AscendexSpotQueryOrderResponse>(json);

        //        if (response == null || response.code != "0" || response.data == null /*|| response.data.Count == 0*/)
        //        {
        //            SendLogMessage("UpdateMyTrade> Received empty or invalid json", LogMessageType.Error);
        //            return;
        //        }

        //        //for (int i = 0; i < response.data.Count; i++)
        //        //{
        //        //    AscendexSpotQueryOrderMessage item = response.data[i];

        //        MyTrade myTrade = new MyTrade();

        //        myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(response.data.lastExecTime));

        //        myTrade.SecurityNameCode = response.data.symbol;

        //        myTrade.Price = response.data.price.ToDecimal();

        //        myTrade.NumberTrade = response.data.seqNum;

        //        myTrade.NumberOrderParent = response.data.orderId;

        //        myTrade.Volume = response.data.orderQty.ToDecimal();

        //        myTrade.Side = (response.data.side == "Buy") ? Side.Buy : Side.Sell;

        //        //myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(item.lastExecTime));

        //        //myTrade.SecurityNameCode = item.symbol;

        //        //myTrade.Price = item.price.ToDecimal();

        //        //myTrade.NumberTrade = item.seqNum;

        //        //myTrade.NumberOrderParent = item.orderId;

        //        //myTrade.Volume = item.orderQty.ToDecimal();

        //        //myTrade.Side = (item.side == "Buy") ? Side.Buy : Side.Sell;

        //        MyTradeEvent?.Invoke(myTrade);

        //        SendLogMessage(myTrade.ToString(), LogMessageType.Trade);
        //        //}
        //    }

        //    catch (Exception exception)
        //    {
        //        SendLogMessage(exception.ToString(), LogMessageType.Error);
        //    }
        //}

        private void UpdateOrder(WebSocketMessage<AscendexSpotOrderData> json)
        {
            try
            {
                if (json == null || json.m != "order" || json.data == null)
                {
                    SendLogMessage("UpdateOrder> Received empty json", LogMessageType.Error);
                    return;
                }

                Order updateOrder = new Order();

                var data = json.data;
                if (data.orderType == "Market" && data.status == "New")
                {
                    return;
                }
                updateOrder.SecurityNameCode = data.symbol;
                updateOrder.SecurityClassCode = GetNameClass(data.symbol);

                updateOrder.State = GetOrderState(data.status);
                updateOrder.NumberMarket = data.orderId;
                updateOrder.NumberUser = GetUserOrderNumber(data.orderId);
                updateOrder.Side = data.sd == "Buy" ? Side.Buy : Side.Sell;
                updateOrder.TypeOrder = (data.orderType == "Limit") ? OrderPriceType.Limit : OrderPriceType.Market;
                updateOrder.Price = (data.price).ToDecimal();
                updateOrder.Volume = (data.q).ToDecimal();
                updateOrder.TimeCreate = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(data.t));
                updateOrder.ServerType = ServerType.AscendexSpot;
                updateOrder.PortfolioNumber = "AscendexSpotPortfolio";

                SendLogMessage($" Order send: status {updateOrder.State}, OrderId :{updateOrder.NumberMarket}, User:{updateOrder.NumberUser}  ", LogMessageType.Error);
                if (json.data.status == "PartiallyFilled" || json.data.status == "Filled")
                {
                    UpdateMyTrade(data);
                }

                UpdatePortfolioFromOrder(data);

                MyOrderEvent?.Invoke(updateOrder);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private void UpdateMyTrade(AscendexSpotOrderData data)
        {
            try
            {
                if (string.IsNullOrEmpty(data.quantity) || string.IsNullOrEmpty(data.ap))
                {
                    SendLogMessage("UpdateOrder> Trade skipped due to missing data (quantity  or price)", LogMessageType.Error);
                    return;
                }

                MyTrade myTrade = new MyTrade();

                myTrade.NumberOrderParent = data.orderId;
                myTrade.Side = data.sd == "Buy" ? Side.Buy : Side.Sell;
                myTrade.SecurityNameCode = data.symbol;
                myTrade.Price = data.price.ToDecimal();
                myTrade.Volume = data.quantity.ToDecimal();
                myTrade.NumberTrade = data.sn;
                myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(data.t));

                MyTradeEvent?.Invoke(myTrade);
            }
            catch (Exception exception)
            {
                SendLogMessage("UpdateMyTrade> Error: " + exception.Message, LogMessageType.Error);
            }
        }

        private void UpdatePortfolioFromOrder(AscendexSpotOrderData data)
        {
            try
            {
                if (data == null)
                {
                    return;
                }

                Portfolio portfolio = new Portfolio();
                portfolio.Number = "AscendexSpotPortfolio";
                portfolio.ValueBegin = 1;
                portfolio.ValueCurrent = 1;
                portfolio.ServerType = ServerType.AscendexSpot;

                string[] parts = data.symbol.Split('/');
                if (parts.Length == 2)
                {
                    string baseAsset = parts[0];
                    string quoteAsset = parts[1];

                    PositionOnBoard basePos = new PositionOnBoard();
                    basePos.PortfolioName = portfolio.Number;
                    basePos.SecurityNameCode = baseAsset;
                    basePos.ValueBegin = data.btb.ToDecimal();
                    basePos.ValueCurrent = data.bab.ToDecimal();
                    basePos.ValueBlocked = basePos.ValueBegin - basePos.ValueCurrent;

                    portfolio.SetNewPosition(basePos);

                    PositionOnBoard quotePos = new PositionOnBoard();
                    quotePos.PortfolioName = portfolio.Number;
                    quotePos.SecurityNameCode = quoteAsset;
                    quotePos.ValueBegin = data.qtb.ToDecimal();
                    quotePos.ValueCurrent = data.qab.ToDecimal();
                    quotePos.ValueBlocked = quotePos.ValueBegin - quotePos.ValueCurrent;

                    portfolio.SetNewPosition(quotePos);
                }

                _portfolios.Add(portfolio);

                PortfolioEvent?.Invoke(_portfolios);
            }
            catch (Exception exception)
            {
                SendLogMessage("UpdatePortfolio> Error: " + exception.Message, LogMessageType.Error);
            }
        }

        #endregion

        private Dictionary<int, string> _orderTrackerDict = new Dictionary<int, string>();

        private Dictionary<string, int> _marketToUserDict = new Dictionary<string, int>();

        private string GetMarketOrderId(int userOrderNumber)
        {
            if (_orderTrackerDict.Count == 0)
            {
                LoadOrderTrackers();
            }

            if (_orderTrackerDict.ContainsKey(userOrderNumber))
            {
                return _orderTrackerDict[userOrderNumber];
            }

            return null;
        }

        private int GetUserOrderNumber(string marketOrderId)
        {
            if (_marketToUserDict.Count == 0)
            {
                LoadOrderTrackers();
            }

            if (_marketToUserDict.ContainsKey(marketOrderId))
            {
                return _marketToUserDict[marketOrderId];
            }

            return 0;
        }

        private void LoadOrderTrackers()
        {
            try
            {// Загрузка MarketOrderId → NumberUser
                if (File.Exists("marketToUserDict.json"))
                {
                    string json = File.ReadAllText("marketToUserDict.json");
                    _marketToUserDict = JsonConvert.DeserializeObject<Dictionary<string, int>>(json);
                }// Загрузка NumberUser → MarketOrderId
                if (File.Exists("orderTrackerDict.json"))
                {
                    string json1 = File.ReadAllText("orderTrackerDict.json");
                    _orderTrackerDict = JsonConvert.DeserializeObject<Dictionary<int, string>>(json1);
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Error loading dictionary: " + exception.Message, LogMessageType.Error);
            }
        }

        private void SaveOrderTrackers()
        {
            try
            {
                if (_orderTrackerDict == null || _marketToUserDict == null)
                {
                    return;
                }

                string json1 = JsonConvert.SerializeObject(_orderTrackerDict, Formatting.Indented);
                File.WriteAllText("orderTrackerDict.json", json1);

                string json2 = JsonConvert.SerializeObject(_marketToUserDict, Formatting.Indented);
                File.WriteAllText("marketToUserDict.json", json2);
            }
            catch (Exception exception)
            {
                SendLogMessage("Error while saving : " + exception.Message, LogMessageType.Error);
            }
        }

        #region 11 Trade

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

                string fullPath = $"/{accountGroup}/api/pro/v1/{_accountCategory}/order";
                string prehashPath = "order";

                IRestResponse request = CreatePrivateQuery(fullPath, prehashPath, body, Method.POST);

                if (request == null || request.StatusCode != HttpStatusCode.OK)
                {
                    SendLogMessage("Deserialization resulted in null", LogMessageType.Error);
                    return;
                }

                AscendexSpotOrderResponse response = JsonConvert.DeserializeObject<AscendexSpotOrderResponse>(request.Content);

                if (response == null || response.code != "0" || response.data == null || response.data.info == null)// if (request.StatusCode == HttpStatusCode.OK && response.code != "0")
                {
                    order.State = OrderStateType.Fail;
                    //MyOrderEvent?.Invoke(order);
                    return;
                }

                if (response != null && response.code == "0" && response.data != null)
                {
                    order.NumberMarket = response.data.info.orderId;

                    if (order.NumberUser != 0)
                    {
                        if (!_orderTrackerDict.ContainsKey(order.NumberUser))
                        {
                            _orderTrackerDict.Add(order.NumberUser, response.data.info.orderId);
                        }

                        if (!_marketToUserDict.ContainsKey(response.data.info.orderId))
                        {
                            _marketToUserDict.Add(response.data.info.orderId, order.NumberUser);
                        }
                    }

                    SaveOrderTrackers();
                }
            }
            catch (Exception exception)
            {
                SendLogMessage("Order send exception " + exception.ToString(), LogMessageType.Error);
            }
        }

        private RateGate _rateGateCancelOrder = new RateGate(1, TimeSpan.FromMilliseconds(1000));

        public void CancelAllOrders()
        {
            try
            {
                _rateGateCancelOrder.WaitToProceed();

                string accountGroup = GetAccountGroup();

                string path = $"/{accountGroup}/api/pro/v1/{_accountCategory}/order/all";
                string prehashPath = "order/all";

                IRestResponse response = CreatePrivateQuery(path, prehashPath, null, Method.DELETE/*, _myProxy*/);

                if (response == null)
                {
                    return;
                }

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                    if (cancelResult != null && cancelResult.code == "0")//cancel-All
                    {
                        SendLogMessage($"All active orders cancelled", LogMessageType.Trade);
                        GetPortfolios();
                    }
                    else
                    {
                        SendLogMessage($"Error: code={cancelResult.code}", LogMessageType.Error);
                    }
                }
                else
                {
                    SendLogMessage($"Error Order canceled {response.StatusCode}", LogMessageType.Error);
                }

                GetPortfolios();
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

                string path = $"/{accountGroup}/api/pro/v1/{_accountCategory}/order";
                string prehashPath = "order";
                long time = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string body;

                if (order.TypeOrder == OrderPriceType.Limit)
                {
                    body = $"{{" +
                           $"\"orderId\": \"{order.NumberMarket.ToString()}\", " +
                           $"\"orderType\": \"{order.TypeOrder.ToString()}\", " +
                           $"\"symbol\": \"{order.SecurityNameCode}\", " +
                           $"\"time\": {time}, " +
                           $"\"orderNumberUser\": \"{order.NumberUser.ToString()}\"" +
                           $"}}";
                }
                else
                {
                    body = $"{{" +
                           $"\"orderId\": \"{order.NumberMarket.ToString()}\", " +
                           $"\"symbol\": \"{order.SecurityNameCode}\", " +
                           $"\"time\": {time}, " +
                           $"\"orderNumberUser\": \"{order.NumberUser.ToString()}\"" +
                           $"}}";
                }

                IRestResponse response = CreatePrivateQuery(path, prehashPath, body, Method.DELETE/*, _myProxy*/);

                if (response == null)
                {
                    return;
                }

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                    if (cancelResult != null && cancelResult.code == "0")
                    {
                        //if (cancelResult.data.status == "Ack")//cancel-Order
                        //{
                        Order cancelOrd = new Order();

                        cancelOrd.NumberMarket = cancelResult.data.info.orderId;
                        cancelOrd.NumberUser = GetUserOrderNumber(cancelResult.data.info.orderId);
                        cancelOrd.TimeCancel = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(cancelResult.data.info.timestamp));
                        cancelOrd.State = OrderStateType.Cancel;//GetOrderState(cancelResult.data.status);

                        SendLogMessage("The order has been cancelled . OrderId: " + cancelOrd.NumberMarket + "NumberUser:" + cancelOrd.NumberUser, LogMessageType.Trade);

                        GetOrderStatus(order);

                        // }
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

                //GetPortfolios();
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
                _rateGateCancelOrder.WaitToProceed();

                string accountGroup = GetAccountGroup();

                string path = $"/{accountGroup}/api/pro/v1/{_accountCategory}/order/all";
                string prehashPath = "order/all";

                string body = $"{{" +
                              $"\"symbol\": \"{security.Name}\"" +
                              $"}}";

                IRestResponse response = CreatePrivateQuery(path, prehashPath, body, Method.DELETE/*, _myProxy*/);

                if (response == null)
                {
                    return;
                }

                if (response.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotCancelOrderResponse cancelResult = JsonConvert.DeserializeObject<AscendexSpotCancelOrderResponse>(response.Content);

                    if (cancelResult != null && cancelResult.code == "0")
                    {
                        SendLogMessage($" Orders cancelled: {cancelResult.data.info.orderId} |Status: {cancelResult.data.status}", LogMessageType.Trade);
                    }
                    else
                    {
                        SendLogMessage($" Cancel error: {response.Content}", LogMessageType.Error);
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
            try
            {
                _rateGateOrder.WaitToProceed();

                List<Order> orders = new List<Order>();

                string accountGroup = GetAccountGroup();

                string path = $"/{accountGroup}/api/pro/v1/{_accountCategory}/order/open";
                string prehashPath = "order/open";

                IRestResponse response = CreatePrivateQuery(path, prehashPath, null, Method.GET/*, _myProxy*/);

                if (response == null)
                {
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

                            Order activeOrder = new Order();

                            activeOrder.TimeCallBack = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(order.lastExecTime));
                            activeOrder.ServerType = ServerType.AscendexSpot;
                            activeOrder.SecurityNameCode = order.symbol;
                            activeOrder.NumberMarket = order.orderId;
                            activeOrder.NumberUser = GetUserOrderNumber(order.orderId);
                            activeOrder.Side = order.side == "Buy" ? Side.Buy : Side.Sell;
                            activeOrder.State = GetOrderState(order.status);
                            activeOrder.TypeOrder = order.orderType == "Limit" ? OrderPriceType.Limit : OrderPriceType.Market;
                            activeOrder.Volume = (order.orderQty).ToDecimal();
                            activeOrder.Price = order.price.ToDecimal();
                            activeOrder.PortfolioNumber = "AscendexSpotPortfolio";

                            orders.Add(activeOrder);
                        }
                    }
                    else
                    {
                        SendLogMessage($" GetOrderStatus Error: {response.Content}", LogMessageType.Error);
                    }
                }
                else
                {
                    SendLogMessage($" HTTP Error:{response.Content}", LogMessageType.Error);
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
                    order.NumberMarket = GetMarketOrderId(order.NumberUser);

                    if (string.IsNullOrWhiteSpace(order.NumberMarket))
                    {
                        //order.NumberMarket = GetUserOrderNumber();
                        return;
                    }
                }

                Order orderOnMarket = null;

                //List<Order> ordersActive = GetAllOpenOrders();
                //if (ordersActive != null)
                //{
                //    for (int i = 0; i < ordersActive.Count; i++)
                //    {
                //        if (ordersActive[i].NumberMarket == order.NumberMarket)
                //        {
                //            orderOnMarket = ordersActive[i];
                //            break;
                //        }
                //    }
                //}

                //if (orderOnMarket == null)
                //{
                //    List<Order> ordersHistory = GetHistoryOrders();
                //    if (ordersHistory != null)
                //    {
                //        for (int i = 0; i < ordersHistory.Count; i++)
                //        {
                //            if (ordersHistory[i].NumberMarket == order.NumberMarket)
                //            {
                //                orderOnMarket = ordersHistory[i];
                //                break;
                //            }
                //        }
                //    }
                //}

                //if (orderOnMarket == null)
                //{

                orderOnMarket = GetOrderStatusById(order.NumberMarket);

                if (orderOnMarket == null || string.IsNullOrWhiteSpace(orderOnMarket.NumberMarket))//pfvt
                {
                    SendLogMessage($"GetOrderStatus > Order not found: {order.NumberMarket}", LogMessageType.Error);
                    return;
                }
                // }
                MyOrderEvent?.Invoke(orderOnMarket);
            }
            catch (Exception exception)
            {
                SendLogMessage("GetOrderStatus > Exception: " + exception.Message, LogMessageType.Error);
            }
        }

        public Order GetOrderStatusById(string NumberMarket)
        {
            try
            {
                _rateGateOrder.WaitToProceed();

                Order order = new Order();

                if (NumberMarket == null)
                {
                    SendLogMessage("GetOrderStatus> Order is null", LogMessageType.Error);
                    return new Order();
                }

                string accountGroup = GetAccountGroup();

                string path = $"/{accountGroup}/api/pro/v1/{_accountCategory}/order/status?orderId={NumberMarket}";
                string prehashPath = "order/status";

                IRestResponse request = CreatePrivateQuery(path, prehashPath, null, Method.GET);

                if (request == null)
                {
                    SendLogMessage("GetOrderStatus> Request returned null", LogMessageType.Error);
                    return new Order();
                }

                if (request.StatusCode == HttpStatusCode.OK)
                {
                    AscendexSpotQueryOrderResponse response =
     JsonConvert.DeserializeObject<AscendexSpotQueryOrderResponse>(request.Content);

                    if (response == null)
                    {
                        SendLogMessage($"Error status order: {response.code}, message: {request.Content}", LogMessageType.Error);
                        return new Order();
                    }

                    if (response != null && response.data != null && response.code == "0")
                    {
                        AscendexSpotQueryOrderMessage orderData = response.data;

                        order.SecurityNameCode = orderData.symbol;
                        order.NumberMarket = orderData.orderId;
                        order.NumberUser = GetUserOrderNumber(orderData.orderId);
                        order.Price = orderData.price.ToDecimal();
                        order.PortfolioNumber = "AscendexSpotPortfolio";
                        order.SecurityClassCode = GetNameClass(orderData.symbol);
                        order.Side = orderData.side == "Buy" ? Side.Buy : Side.Sell;
                        order.TypeOrder = orderData.orderType == "Limit" ? OrderPriceType.Limit : OrderPriceType.Market;
                        order.Volume = orderData.orderQty.ToDecimal();
                        order.TimeCallBack = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(orderData.lastExecTime));
                        order.State = GetOrderState(orderData.status);

                        if (orderData.status == "Filled" || orderData.status == "PartiallyFilled")
                        {
                            MyTrade myTrade = new MyTrade();

                            myTrade.Time = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(orderData.lastExecTime));
                            myTrade.SecurityNameCode = orderData.symbol;
                            myTrade.Price = orderData.price.ToDecimal();
                            myTrade.NumberTrade = orderData.seqNum;
                            myTrade.NumberOrderParent = orderData.orderId;
                            myTrade.Volume = orderData.orderQty.ToDecimal();
                            myTrade.Side = orderData.side == "Buy" ? Side.Buy : Side.Sell;

                            MyTradeEvent?.Invoke(myTrade);
                        }
                    }
                    else
                    {
                        SendLogMessage($"HTTP Error: {request.StatusCode}, content={request.Content}", LogMessageType.Error);
                    }

                    MyOrderEvent?.Invoke(order);
                }
                return order;
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
                return new Order();
            }
        }

        private RateGate _rateGateOrder = new RateGate(1, TimeSpan.FromMilliseconds(1000));

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

        //                    myTrade.Volume = myTrade.Volume .ToDecimal();

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
            try
            {
                _rateGateOrder.WaitToProceed();

                List<Order> orders = new List<Order>();

                string accountGroup = GetAccountGroup();

                string path = $"/{accountGroup}/api/pro/v1/{_accountCategory}/order/hist/current";
                string prehashPath = "order/hist/current";

                IRestResponse response = CreatePrivateQuery(path, prehashPath, null, Method.GET);

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
                            historyOrder.NumberUser = GetUserOrderNumber(order.orderId);
                            historyOrder.TimeCallBack = TimeManager.GetDateTimeFromTimeStamp(Convert.ToInt64(order.lastExecTime));
                            historyOrder.ServerType = ServerType.AscendexSpot;
                            historyOrder.SecurityNameCode = order.symbol;
                            historyOrder.Side = order.side == "Buy" ? Side.Buy : Side.Sell;
                            historyOrder.State = GetOrderState(order.status);
                            historyOrder.Volume = order.orderQty.ToDecimal();
                            historyOrder.Price = order.price.ToDecimal();
                            historyOrder.PortfolioNumber = "AscendexSpotPortfolio";
                            historyOrder.TypeOrder = order.orderType == "Limit" ? OrderPriceType.Limit : OrderPriceType.Market;

                            orders.Add(historyOrder);
                        }
                    }
                    else
                    {
                        SendLogMessage($"GetOrderStatus Error: code={result?.code}, {response.Content}", LogMessageType.Error);
                    }
                }
                else
                {
                    SendLogMessage($"HTTP Error:{response.Content}", LogMessageType.Error);
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
            if (orderStateResponse.StartsWith("New") || orderStateResponse.StartsWith("Ack") || orderStateResponse.StartsWith("ACCEPT"))// orderStateResponse.StartsWith("DONE")
            {
                return OrderStateType.Active;
            }
            else if (orderStateResponse.StartsWith("Filled") || orderStateResponse.StartsWith("Done") || orderStateResponse.StartsWith("DONE"))
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
            SendLogMessage(orderStateResponse, LogMessageType.Error);
            return OrderStateType.None;
        }

        public bool SubscribeNews()
        {
            return false;
        }

        #endregion

        #region 12 Queries

        private IRestResponse CreatePublicQuery(string path, Method method/*, IWebProxy proxy = null*/)
        {
            try
            {
                RestClient client = new RestClient(_baseUrl);

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

        private IRestResponse CreatePrivateQuery(string fullPath, string prehashPath, object body = null, Method method = Method.GET)
        {
            try
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                string message = $"{timestamp}+{prehashPath}";

                string signature = GenerateSignature(message, _secretKey);

                RestClient client = new RestClient(_baseUrl);
                RestRequest request = new RestRequest(fullPath, method);

                request.AddHeader("Content-Type", "application/json");
                request.AddHeader("x-auth-key", _publicKey);
                request.AddHeader("x-auth-timestamp", timestamp.ToString());
                request.AddHeader("x-auth-signature", signature);

                if (body != null)
                {
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

        private void GenerateAuthenticate()
        {
            try
            {
                long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                string payload = timestamp + "+stream";
                string signature = GenerateSignature(payload, _secretKey);
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

                SendLogMessage("Auth sent: " + authJson, LogMessageType.System);
            }
            catch (Exception exception)
            {
                SendLogMessage(exception.ToString(), LogMessageType.Error);
            }
        }

        private static string GenerateSignature(string message, string secret)
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

            string logLine = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") +
                             " [" + messageType.ToString() + "] " + message;

            try
            {
                string logFilePath = "AscendexSpot_log.txt";

                File.AppendAllText(logFilePath, logLine + Environment.NewLine);
            }
            catch (Exception exception) { }
        }

        #endregion
    }
}