//+------------------------------------------------------------------+
//| GoldflowExecutor.mq5  -  GoldXFlow Phase 16 MT5 executor EA      |
//|                                                                  |
//| Executes orders sent by the GoldXFlow Python engine and reports  |
//| every broker event back. Protocol: docs/EXECUTION_V1.md and      |
//| goldflow/execution/protocol.py. The EA never decides to trade:   |
//| it validates, de-duplicates, executes and reports.               |
//|                                                                  |
//| Setup (Windows MT5 terminal):                                    |
//|  1. Tools > Options > Expert Advisors: allow algorithmic trading |
//|     and add 127.0.0.1 to "Allow WebRequest for listed URL"       |
//|     (required by SocketConnect).                                 |
//|  2. Create the shared secret once:                               |
//|     python -m goldflow.cli bridge-secret --out                   |
//|       "%APPDATA%\MetaQuotes\Terminal\Common\Files\goldflow\      |
//|        bridge.secret"                                            |
//|     and set bridge.secret_file in the config to the same path.   |
//|  3. Attach to an XAUUSD chart of a DEMO account. REAL accounts   |
//|     are refused unless InpAllowReal = true (Phase 16 gate).      |
//|                                                                  |
//| Not compiled in the build environment (no MetaEditor there):     |
//| compile in MetaEditor (F7) and fix anything the compiler reports |
//| before use. OnInit self-tests HMAC-SHA256 against the vector     |
//| that `python -m goldflow.cli ea-vector` prints.                  |
//+------------------------------------------------------------------+
#property copyright "GoldXFlow"
#property version   "1.00"
#property description "GoldXFlow executor: authenticated, idempotent order execution for the Python engine"

#include <Trade\Trade.mqh>

input group "=== Bridge ==="
input string InpHost          = "127.0.0.1";                // Python engine host (loopback)
input ushort InpPort          = 47011;                      // Python engine port
input bool   InpUseFileQueue  = false;                      // Use the Common Files queue instead of TCP
input string InpQueueDir      = "goldflow";                 // Queue folder (Common Files)
input string InpSecretFile    = "goldflow\\bridge.secret";  // Shared secret file (Common Files)
input int    InpMaxSkewMs     = 6000;                       // Max clock difference accepted (ms)

input group "=== Execution ==="
input string InpSymbol        = "XAUUSD";                   // Broker symbol (must match the broker block)
input ulong  InpMagic         = 20261031;                   // Magic number (must match bridge.magic)
input bool   InpAllowReal     = false;                      // Allow a REAL account (only after the M7 gate)
input int    InpHistoryDays   = 30;                         // Days of history searched for duplicate keys
input int    InpTimerMs       = 200;                        // Polling period (ms)
input int    InpHeartbeatMs   = 1000;                       // Heartbeat period (ms)

//--- protocol constants
#define PROTO_VERSION 1
#define KEY_PREFIX    "gf:"
#define RC_INVALID_REQUEST 10013
#define RC_INVALID_VOLUME  10014
#define RC_INVALID_STOPS   10016
#define RC_TRADE_DISABLED  10017
#define RC_POSITION_CLOSED 10036

//--- self-test vector (python -m goldflow.cli ea-vector)
#define SELFTEST_KEY  "goldflow-test-key"
#define SELFTEST_TEXT "{\"v\":1,\"type\":\"HEARTBEAT\",\"seq\":1,\"ts\":0,\"session\":\"selftest\",\"sender\":\"py\"}"
#define SELFTEST_MAC  "878f5c9575a47c38ae4ddfb1b5a8fe24028f40cd0db2d20909cd028f1f04f1de"

CTrade   g_trade;
uchar    g_secret[];
int      g_sock        = INVALID_HANDLE;
string   g_rx          = "";
string   g_session     = "";
long     g_seqOut      = 0;
long     g_seqIn       = 0;
long     g_fileOut     = 0;
ulong    g_lastHb      = 0;
ulong    g_lastConnTry = 0;
uint     g_backoffMs   = 1000;
string   g_sentKeys[];

//+------------------------------------------------------------------+
//| time, hashing                                                     |
//+------------------------------------------------------------------+
long NowMs() { return (long)TimeGMT() * 1000; }

bool Sha256(const uchar &data[], uchar &out[])
  {
   uchar nokey[];
   return CryptEncode(CRYPT_HASH_SHA256, data, nokey, out) == 32;
  }

string ToHex(const uchar &b[])
  {
   string s = "";
   for(int i = 0; i < ArraySize(b); i++)
      s += StringFormat("%02x", b[i]);
   return s;
  }

int TextBytes(const string text, uchar &out[])
  {
   int n = StringToCharArray(text, out, 0, StringLen(text), CP_UTF8);
   ArrayResize(out, n);
   return n;
  }

string HmacHex(const uchar &keyIn[], const string text)
  {
   uchar key[];
   if(ArraySize(keyIn) > 64)
      Sha256(keyIn, key);
   else
      ArrayCopy(key, keyIn);
   int kn = ArraySize(key);
   ArrayResize(key, 64);
   for(int i = kn; i < 64; i++)
      key[i] = 0;
   uchar msg[];
   int len = TextBytes(text, msg);
   uchar inner[];
   ArrayResize(inner, 64 + len);
   for(int i = 0; i < 64; i++)
      inner[i] = (uchar)(key[i] ^ 0x36);
   if(len > 0)
      ArrayCopy(inner, msg, 64, 0, len);
   uchar ih[];
   if(!Sha256(inner, ih))
      return "";
   uchar outer[];
   ArrayResize(outer, 96);
   for(int i = 0; i < 64; i++)
      outer[i] = (uchar)(key[i] ^ 0x5c);
   ArrayCopy(outer, ih, 64, 0, 32);
   uchar oh[];
   if(!Sha256(outer, oh))
      return "";
   return ToHex(oh);
  }

//+------------------------------------------------------------------+
//| minimal JSON for flat objects (Python -> EA messages are flat)    |
//+------------------------------------------------------------------+
bool JsonGet(const string json, const string key, string &out)
  {
   string pat = "\"" + key + "\":";
   int p = StringFind(json, pat);
   if(p < 0)
      return false;
   p += StringLen(pat);
   int n = StringLen(json);
   if(p < n && StringGetCharacter(json, p) == '"')
     {
      string s = "";
      for(int i = p + 1; i < n; i++)
        {
         ushort c = StringGetCharacter(json, i);
         if(c == '\\' && i + 1 < n)
           {
            ushort e = StringGetCharacter(json, i + 1);
            if(e == 'n') s += "\n"; else s += ShortToString(e);
            i++;
            continue;
           }
         if(c == '"')
           {
            out = s;
            return true;
           }
         s += ShortToString(c);
        }
      return false;
     }
   int e = p;
   while(e < n)
     {
      ushort c = StringGetCharacter(json, e);
      if(c == ',' || c == '}')
         break;
      e++;
     }
   out = StringSubstr(json, p, e - p);
   StringTrimLeft(out);
   StringTrimRight(out);
   return StringLen(out) > 0;
  }

string Js(const string s)
  {
   string r = s;
   StringReplace(r, "\\", "\\\\");
   StringReplace(r, "\"", "\\\"");
   return "\"" + r + "\"";
  }

string Jd(const double v, const int digits = 8) { return DoubleToString(v, digits); }

//+------------------------------------------------------------------+
//| transport                                                         |
//+------------------------------------------------------------------+
void CloseSocket()
  {
   if(g_sock != INVALID_HANDLE)
      SocketClose(g_sock);
   g_sock = INVALID_HANDLE;
   g_rx = "";
   g_session = "";   // a new connection must be authenticated by a new HELLO
  }

void TryConnect()
  {
   if(InpUseFileQueue || g_sock != INVALID_HANDLE)
      return;
   ulong now = GetTickCount64();
   if(now - g_lastConnTry < g_backoffMs)
      return;
   g_lastConnTry = now;
   g_sock = SocketCreate();
   if(g_sock == INVALID_HANDLE)
     {
      PrintFormat("goldflow: SocketCreate failed, error %d", GetLastError());
      return;
     }
   if(!SocketConnect(g_sock, InpHost, InpPort, 1000))
     {
      PrintFormat("goldflow: connect %s:%d failed, error %d (is 127.0.0.1 in the allowed URL list?)", InpHost, InpPort, GetLastError());
      SocketClose(g_sock);
      g_sock = INVALID_HANDLE;
      g_backoffMs = (uint)MathMin(60000, g_backoffMs * 2);
      return;
     }
   g_backoffMs = 1000;
   PrintFormat("goldflow: connected to %s:%d", InpHost, InpPort);
  }

bool SendLine(const string line)
  {
   if(InpUseFileQueue)
     {
      g_fileOut++;
      string base = InpQueueDir + "\\to_py\\" + StringFormat("%015I64d", g_fileOut);
      int h = FileOpen(base + ".tmp", FILE_WRITE | FILE_TXT | FILE_ANSI | FILE_COMMON);
      if(h == INVALID_HANDLE)
         return false;
      FileWriteString(h, line);
      FileClose(h);
      return FileMove(base + ".tmp", FILE_COMMON, base + ".msg", FILE_COMMON | FILE_REWRITE);
     }
   if(g_sock == INVALID_HANDLE)
      return false;
   uchar buf[];
   int n = TextBytes(line, buf);
   if(SocketSend(g_sock, buf, n) != n)
     {
      CloseSocket();
      return false;
     }
   return true;
  }

bool SendMsg(const string type, const string body)
  {
   if(g_session == "" && type != "HELLO")
      return false;
   g_seqOut++;
   string json = "{\"v\":" + IntegerToString(PROTO_VERSION) + ",\"type\":" + Js(type) + ",\"seq\":" + IntegerToString(g_seqOut) +
                 ",\"ts\":" + IntegerToString(NowMs()) + ",\"session\":" + Js(g_session) + ",\"sender\":\"ea\"" +
                 (body == "" ? "" : "," + body) + "}";
   return SendLine(HmacHex(g_secret, json) + " " + json + "\n");
  }

void SortStrings(string &a[])
  {
   for(int i = 1; i < ArraySize(a); i++)
     {
      string v = a[i];
      int j = i - 1;
      while(j >= 0 && StringCompare(a[j], v) > 0)
        {
         a[j + 1] = a[j];
         j--;
        }
      a[j + 1] = v;
     }
  }

void ReadInbox(string &lines[])
  {
   ArrayResize(lines, 0);
   if(InpUseFileQueue)
     {
      string name;
      string names[];
      long sh = FileFindFirst(InpQueueDir + "\\to_ea\\*.msg", name, FILE_COMMON);
      if(sh != INVALID_HANDLE)
        {
         do
           {
            int k = ArraySize(names);
            ArrayResize(names, k + 1);
            names[k] = name;
           }
         while(FileFindNext(sh, name));
         FileFindClose(sh);
        }
      SortStrings(names);   // ArraySort is for numeric arrays only
      for(int i = 0; i < ArraySize(names); i++)
        {
         string path = InpQueueDir + "\\to_ea\\" + names[i];
         int h = FileOpen(path, FILE_READ | FILE_TXT | FILE_ANSI | FILE_COMMON);
         if(h == INVALID_HANDLE)
            continue;
         string s = FileReadString(h);
         FileClose(h);
         FileDelete(path, FILE_COMMON);
         int k = ArraySize(lines);
         ArrayResize(lines, k + 1);
         lines[k] = s;
        }
      return;
     }
   if(g_sock == INVALID_HANDLE)
      return;
   if(!SocketIsConnected(g_sock))
     {
      Print("goldflow: connection lost");
      CloseSocket();
      return;
     }
   uint avail = SocketIsReadable(g_sock);
   if(avail > 0)
     {
      uchar buf[];
      int r = SocketRead(g_sock, buf, avail, 50);
      if(r > 0)
         g_rx += CharArrayToString(buf, 0, r, CP_UTF8);
     }
   int p;
   while((p = StringFind(g_rx, "\n")) >= 0)
     {
      int k = ArraySize(lines);
      ArrayResize(lines, k + 1);
      lines[k] = StringSubstr(g_rx, 0, p);
      g_rx = StringSubstr(g_rx, p + 1);
     }
  }

//+------------------------------------------------------------------+
//| verification of a Python message                                  |
//+------------------------------------------------------------------+
bool Verify(const string line, string &json, string &type)
  {
   string ln = line;
   StringTrimRight(ln);
   int sp = StringFind(ln, " ");
   if(sp != 64)
      return false;
   string mac = StringSubstr(ln, 0, 64);
   json = StringSubstr(ln, 65);
   if(HmacHex(g_secret, json) != mac)
     {
      Print("goldflow: refused message with a bad MAC");
      return false;
     }
   string v, sender, seqs, tss, sess;
   if(!JsonGet(json, "v", v) || !JsonGet(json, "type", type) || !JsonGet(json, "seq", seqs) || !JsonGet(json, "ts", tss) ||
      !JsonGet(json, "session", sess) || !JsonGet(json, "sender", sender))
      return false;
   if((int)StringToInteger(v) != PROTO_VERSION || sender != "py")
      return false;
   long seq = StringToInteger(seqs);
   long ts = StringToInteger(tss);
   if(MathAbs((double)(ts - NowMs())) > InpMaxSkewMs)
     {
      Print("goldflow: refused message: clock skew");
      return false;
     }
   if(type == "HELLO")
     {
      g_session = sess;   // adopt the engine's session (authenticated and timely)
      g_seqIn = seq;
      return true;
     }
   if(g_session == "" || sess != g_session)
      return false;
   if(seq <= g_seqIn)
     {
      Print("goldflow: refused replayed message");
      return false;
     }
   g_seqIn = seq;
   return true;
  }

//+------------------------------------------------------------------+
//| broker helpers                                                    |
//+------------------------------------------------------------------+
string TradeModeName()
  {
   long m = AccountInfoInteger(ACCOUNT_TRADE_MODE);
   if(m == ACCOUNT_TRADE_MODE_DEMO) return "DEMO";
   if(m == ACCOUNT_TRADE_MODE_CONTEST) return "CONTEST";
   return "REAL";
  }

string MarginModeName()
  {
   long m = AccountInfoInteger(ACCOUNT_MARGIN_MODE);
   if(m == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING) return "HEDGING";
   if(m == ACCOUNT_MARGIN_MODE_RETAIL_NETTING) return "NETTING";
   return "EXCHANGE";
  }

string KeyFromComment(const string c)
  {
   if(StringFind(c, KEY_PREFIX) == 0)
      return StringSubstr(c, StringLen(KEY_PREFIX));
   return "";
  }

// key of a position from its opening order's comment (manual and SL/TP deals do not carry it)
string KeyOfPosition(const ulong posId)
  {
   if(!HistorySelectByPosition(posId))
      return "";
   for(int i = 0; i < HistoryDealsTotal(); i++)
     {
      ulong d = HistoryDealGetTicket(i);
      if(HistoryDealGetInteger(d, DEAL_ENTRY) == DEAL_ENTRY_IN && (ulong)HistoryDealGetInteger(d, DEAL_MAGIC) == InpMagic)
        {
         ulong ord = (ulong)HistoryDealGetInteger(d, DEAL_ORDER);
         string k = KeyFromComment(HistoryOrderGetString(ord, ORDER_COMMENT));
         if(k == "")
            k = KeyFromComment(HistoryDealGetString(d, DEAL_COMMENT));
         return k;
        }
     }
   return "";
  }

bool KeyExists(const string key)
  {
   string c = KEY_PREFIX + key;
   for(int i = 0; i < ArraySize(g_sentKeys); i++)
      if(g_sentKeys[i] == key)
         return true;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
     {
      ulong t = PositionGetTicket(i);
      if(t > 0 && PositionGetString(POSITION_COMMENT) == c)
         return true;
     }
   for(int i = OrdersTotal() - 1; i >= 0; i--)
     {
      ulong t = OrderGetTicket(i);
      if(t > 0 && OrderGetString(ORDER_COMMENT) == c)
         return true;
     }
   datetime from = TimeCurrent() - (datetime)(InpHistoryDays * 86400);
   if(HistorySelect(from, TimeCurrent() + 86400))
     {
      for(int i = HistoryOrdersTotal() - 1; i >= 0; i--)
        {
         ulong t = HistoryOrderGetTicket(i);
         if(HistoryOrderGetString(t, ORDER_COMMENT) == c)
            return true;
        }
     }
   return false;
  }

double SnapPrice(const double p)
  {
   double ts = SymbolInfoDouble(InpSymbol, SYMBOL_TRADE_TICK_SIZE);
   if(ts <= 0)
      return NormalizeDouble(p, (int)SymbolInfoInteger(InpSymbol, SYMBOL_DIGITS));
   return NormalizeDouble(MathRound(p / ts) * ts, (int)SymbolInfoInteger(InpSymbol, SYMBOL_DIGITS));
  }

bool VolumeValid(const double v)
  {
   double mn = SymbolInfoDouble(InpSymbol, SYMBOL_VOLUME_MIN);
   double mx = SymbolInfoDouble(InpSymbol, SYMBOL_VOLUME_MAX);
   double st = SymbolInfoDouble(InpSymbol, SYMBOL_VOLUME_STEP);
   if(v < mn - 1e-9 || v > mx + 1e-9 || st <= 0)
      return false;
   double k = v / st;
   return MathAbs(k - MathRound(k)) < 1e-6;
  }

//+------------------------------------------------------------------+
//| replies                                                           |
//+------------------------------------------------------------------+
void SendHello()
  {
   string b = "\"account\":" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) + ",\"trade_mode\":" + Js(TradeModeName()) +
              ",\"margin_mode\":" + Js(MarginModeName()) + ",\"server\":" + Js(AccountInfoString(ACCOUNT_SERVER)) +
              ",\"currency\":" + Js(AccountInfoString(ACCOUNT_CURRENCY)) + ",\"balance\":" + Jd(AccountInfoDouble(ACCOUNT_BALANCE), 2) +
              ",\"equity\":" + Jd(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ",\"build\":" + IntegerToString(TerminalInfoInteger(TERMINAL_BUILD));
   SendMsg("HELLO", b);
  }

void SendSymbolInfo(const string sym)
  {
   long tm = SymbolInfoInteger(sym, SYMBOL_TRADE_MODE);
   string mode = (tm == SYMBOL_TRADE_MODE_FULL ? "FULL" : tm == SYMBOL_TRADE_MODE_DISABLED ? "DISABLED" :
                  tm == SYMBOL_TRADE_MODE_CLOSEONLY ? "CLOSEONLY" : tm == SYMBOL_TRADE_MODE_LONGONLY ? "LONGONLY" : "SHORTONLY");
   string b = "\"symbol\":" + Js(sym) + ",\"digits\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_DIGITS)) +
              ",\"point\":" + Jd(SymbolInfoDouble(sym, SYMBOL_POINT), 10) + ",\"tick_size\":" + Jd(SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE), 10) +
              ",\"tick_value\":" + Jd(SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE), 10) +
              ",\"contract_size\":" + Jd(SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE), 6) +
              ",\"volume_min\":" + Jd(SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN), 6) + ",\"volume_max\":" + Jd(SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX), 6) +
              ",\"volume_step\":" + Jd(SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP), 6) +
              ",\"stops_level\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_TRADE_STOPS_LEVEL)) + ",\"trade_mode\":" + Js(mode);
   SendMsg("SYMBOL_INFO", b);
  }

void SendPositions()
  {
   string ps = "";
   for(int i = 0; i < PositionsTotal(); i++)
     {
      ulong t = PositionGetTicket(i);
      if(t == 0 || (ulong)PositionGetInteger(POSITION_MAGIC) != InpMagic || PositionGetString(POSITION_SYMBOL) != InpSymbol)
         continue;
      string key = KeyFromComment(PositionGetString(POSITION_COMMENT));
      string side = PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY ? "BUY" : "SELL";
      double vol = PositionGetDouble(POSITION_VOLUME), po = PositionGetDouble(POSITION_PRICE_OPEN);
      double sl = PositionGetDouble(POSITION_SL), tp = PositionGetDouble(POSITION_TP);
      if(key == "")
         key = KeyOfPosition((ulong)PositionGetInteger(POSITION_IDENTIFIER));
      if(ps != "")
         ps += ",";
      ps += "{\"ticket\":" + IntegerToString((long)t) + ",\"key\":" + Js(key) + ",\"side\":" + Js(side) + ",\"volume\":" + Jd(vol, 6) +
            ",\"price_open\":" + Jd(po, 6) + ",\"sl\":" + Jd(sl, 6) + ",\"tp\":" + Jd(tp, 6) + ",\"magic\":" + IntegerToString((long)InpMagic) + "}";
     }
   string os = "";
   for(int i = 0; i < OrdersTotal(); i++)
     {
      ulong t = OrderGetTicket(i);
      if(t == 0 || (ulong)OrderGetInteger(ORDER_MAGIC) != InpMagic)
         continue;
      if(os != "")
         os += ",";
      os += "{\"ticket\":" + IntegerToString((long)t) + ",\"key\":" + Js(KeyFromComment(OrderGetString(ORDER_COMMENT))) + "}";
     }
   SendMsg("POSITIONS", "\"positions\":[" + ps + "],\"orders\":[" + os + "],\"balance\":" + Jd(AccountInfoDouble(ACCOUNT_BALANCE), 2) +
           ",\"equity\":" + Jd(AccountInfoDouble(ACCOUNT_EQUITY), 2));
  }

void SendHeartbeat()
  {
   MqlTick tk;
   if(!SymbolInfoTick(InpSymbol, tk))
      return;
   string b = "\"bid\":" + Jd(tk.bid, 6) + ",\"ask\":" + Jd(tk.ask, 6) + ",\"spread_points\":" + IntegerToString(SymbolInfoInteger(InpSymbol, SYMBOL_SPREAD)) +
              ",\"balance\":" + Jd(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ",\"equity\":" + Jd(AccountInfoDouble(ACCOUNT_EQUITY), 2) +
              ",\"time_msc\":" + IntegerToString(tk.time_msc);
   SendMsg("HEARTBEAT", b);
  }

void Reject(const long ref, const string key, const int rc, const string why)
  {
   SendMsg("REJECT", "\"ref\":" + IntegerToString(ref) + ",\"key\":" + Js(key) + ",\"retcode\":" + IntegerToString(rc) + ",\"reason\":" + Js(why));
  }

//+------------------------------------------------------------------+
//| commands                                                          |
//+------------------------------------------------------------------+
void OnOrderNew(const string json, const long ref)
  {
   string key, sym, side, vols, sls, tps, devs;
   if(!JsonGet(json, "key", key) || !JsonGet(json, "symbol", sym) || !JsonGet(json, "side", side) || !JsonGet(json, "volume", vols) ||
      !JsonGet(json, "sl", sls) || !JsonGet(json, "tp", tps) || !JsonGet(json, "deviation", devs))
     {
      Reject(ref, key, RC_INVALID_REQUEST, "missing fields");
      return;
     }
   if(KeyExists(key))
     {
      SendMsg("ACK", "\"ref\":" + IntegerToString(ref) + ",\"key\":" + Js(key) + ",\"status\":\"DUPLICATE\"");
      return;
     }
   if(sym != InpSymbol || (side != "BUY" && side != "SELL"))
     {
      Reject(ref, key, RC_INVALID_REQUEST, "wrong symbol or side");
      return;
     }
   if(TradeModeName() == "REAL" && !InpAllowReal)
     {
      Reject(ref, key, RC_TRADE_DISABLED, "REAL account and InpAllowReal = false");
      return;
     }
   double vol = StringToDouble(vols), sl = SnapPrice(StringToDouble(sls)), tp = StringToDouble(tps) > 0 ? SnapPrice(StringToDouble(tps)) : 0.0;
   if(!VolumeValid(vol))
     {
      Reject(ref, key, RC_INVALID_VOLUME, "volume outside min/max/step");
      return;
     }
   MqlTick tk;
   if(!SymbolInfoTick(InpSymbol, tk))
     {
      Reject(ref, key, RC_INVALID_REQUEST, "no tick");
      return;
     }
   double px = side == "BUY" ? tk.ask : tk.bid, pt = SymbolInfoDouble(InpSymbol, SYMBOL_POINT);
   double minDist = (double)SymbolInfoInteger(InpSymbol, SYMBOL_TRADE_STOPS_LEVEL) * pt;
   int sgn = side == "BUY" ? 1 : -1;
   if(sl <= 0 || (px - sl) * sgn < minDist || (px - sl) * sgn <= 0 || (tp > 0 && ((tp - px) * sgn < minDist || (tp - px) * sgn <= 0)))
     {
      Reject(ref, key, RC_INVALID_STOPS, "stop/target on the wrong side or inside the stops level");
      return;
     }
   int k = ArraySize(g_sentKeys);
   ArrayResize(g_sentKeys, k + 1);
   g_sentKeys[k] = key;   // before sending: a retry arriving during OrderSend is a duplicate
   SendMsg("ACK", "\"ref\":" + IntegerToString(ref) + ",\"key\":" + Js(key) + ",\"status\":\"SENT\"");
   g_trade.SetExpertMagicNumber(InpMagic);
   g_trade.SetDeviationInPoints((ulong)StringToInteger(devs));
   g_trade.SetTypeFillingBySymbol(InpSymbol);
   bool ok = side == "BUY" ? g_trade.Buy(vol, InpSymbol, 0.0, sl, tp, KEY_PREFIX + key) : g_trade.Sell(vol, InpSymbol, 0.0, sl, tp, KEY_PREFIX + key);
   uint rc = g_trade.ResultRetcode();
   if(!ok)
      PrintFormat("goldflow: order %s failed: retcode %u %s", key, rc, g_trade.ResultRetcodeDescription());
   SendMsg("TXN", "\"key\":" + Js(key) + ",\"kind\":\"REQUEST\",\"position\":0,\"volume\":" + Jd(vol, 6) + ",\"price\":" + Jd(g_trade.ResultPrice(), 6) +
           ",\"retcode\":" + IntegerToString(rc) + ",\"reason\":" + Js(g_trade.ResultRetcodeDescription()) + ",\"time_msc\":" + IntegerToString(NowMs()));
  }

void OnOrderModify(const string json, const long ref)
  {
   string key, poss, sls, tps;
   if(!JsonGet(json, "key", key) || !JsonGet(json, "position", poss) || !JsonGet(json, "sl", sls) || !JsonGet(json, "tp", tps))
      return;
   ulong ticket = (ulong)StringToInteger(poss);
   if(!PositionSelectByTicket(ticket) || (ulong)PositionGetInteger(POSITION_MAGIC) != InpMagic)
     {
      Reject(ref, key, RC_POSITION_CLOSED, "position not found");
      return;
     }
   double sl = SnapPrice(StringToDouble(sls)), tp = StringToDouble(tps) > 0 ? SnapPrice(StringToDouble(tps)) : 0.0;
   if(!g_trade.PositionModify(ticket, sl, tp))
     {
      Reject(ref, key, (int)g_trade.ResultRetcode(), g_trade.ResultRetcodeDescription());
      return;
     }
   SendMsg("TXN", "\"key\":" + Js(key) + ",\"kind\":\"MODIFY\",\"position\":" + IntegerToString((long)ticket) + ",\"volume\":0,\"price\":0,\"sl\":" +
           Js(DoubleToString(sl, _Digits)) + ",\"tp\":" + Js(DoubleToString(tp, _Digits)) + ",\"time_msc\":" + IntegerToString(NowMs()));
  }

void OnOrderClose(const string json, const long ref)
  {
   string key, poss, vols;
   if(!JsonGet(json, "key", key) || !JsonGet(json, "position", poss) || !JsonGet(json, "volume", vols))
      return;
   ulong ticket = (ulong)StringToInteger(poss);
   double vol = StringToDouble(vols);
   if(!PositionSelectByTicket(ticket) || (ulong)PositionGetInteger(POSITION_MAGIC) != InpMagic)
     {
      Reject(ref, key, RC_POSITION_CLOSED, "position not found");
      return;
     }
   bool ok = (vol > 0 && vol < PositionGetDouble(POSITION_VOLUME)) ? g_trade.PositionClosePartial(ticket, vol) : g_trade.PositionClose(ticket);
   if(!ok)
      Reject(ref, key, (int)g_trade.ResultRetcode(), g_trade.ResultRetcodeDescription());
   // the closing deal is reported by OnTradeTransaction
  }

void FlattenAll(const string reason)
  {
   PrintFormat("goldflow: FLATTEN_ALL (%s)", reason);
   for(int i = PositionsTotal() - 1; i >= 0; i--)
     {
      ulong t = PositionGetTicket(i);
      if(t > 0 && (ulong)PositionGetInteger(POSITION_MAGIC) == InpMagic)
         if(!g_trade.PositionClose(t))
            PrintFormat("goldflow: close %I64u failed: %u", t, g_trade.ResultRetcode());
     }
   for(int i = OrdersTotal() - 1; i >= 0; i--)
     {
      ulong t = OrderGetTicket(i);
      if(t > 0 && (ulong)OrderGetInteger(ORDER_MAGIC) == InpMagic)
         g_trade.OrderDelete(t);
     }
   SendPositions();
  }

void Dispatch(const string line)
  {
   string json, type;
   if(!Verify(line, json, type))
      return;
   string seqs;
   JsonGet(json, "seq", seqs);
   long ref = StringToInteger(seqs);
   if(type == "HELLO")
      SendHello();
   else if(type == "SYMBOL_INFO_REQ")
     {
      string sym;
      JsonGet(json, "symbol", sym);
      SendSymbolInfo(sym);
     }
   else if(type == "POSITIONS_REQ")
      SendPositions();
   else if(type == "HEARTBEAT")
      SendHeartbeat();
   else if(type == "ORDER_NEW")
      OnOrderNew(json, ref);
   else if(type == "ORDER_MODIFY")
      OnOrderModify(json, ref);
   else if(type == "ORDER_CLOSE")
      OnOrderClose(json, ref);
   else if(type == "FLATTEN_ALL")
     {
      string why;
      JsonGet(json, "reason", why);
      FlattenAll(why);
     }
  }

//+------------------------------------------------------------------+
//| event handlers                                                    |
//+------------------------------------------------------------------+
int OnInit()
  {
   uchar tk[];
   TextBytes(SELFTEST_KEY, tk);
   if(HmacHex(tk, SELFTEST_TEXT) != SELFTEST_MAC)
     {
      Print("goldflow: HMAC-SHA256 self-test FAILED; refusing to run");
      return INIT_FAILED;
     }
   int h = FileOpen(InpSecretFile, FILE_READ | FILE_TXT | FILE_ANSI | FILE_COMMON);
   if(h == INVALID_HANDLE)
     {
      PrintFormat("goldflow: cannot open the secret file Common\\Files\\%s (error %d)", InpSecretFile, GetLastError());
      return INIT_FAILED;
     }
   string s = FileReadString(h);
   FileClose(h);
   StringTrimLeft(s);
   StringTrimRight(s);
   if(TextBytes(s, g_secret) < 32)
     {
      Print("goldflow: secret shorter than 32 bytes");
      return INIT_FAILED;
     }
   if(TradeModeName() == "REAL" && !InpAllowReal)
     {
      Print("goldflow: REAL account and InpAllowReal = false: refusing to run (Phase 16 deployment gate)");
      return INIT_FAILED;
     }
   if(!SymbolSelect(InpSymbol, true))
     {
      PrintFormat("goldflow: symbol %s not available", InpSymbol);
      return INIT_FAILED;
     }
   g_fileOut = NowMs();   // file names keep increasing across restarts
   g_trade.SetExpertMagicNumber(InpMagic);
   g_trade.SetAsyncMode(false);
   EventSetMillisecondTimer(InpTimerMs);
   PrintFormat("goldflow: executor ready (%s, magic %I64u, %s account)", InpSymbol, InpMagic, TradeModeName());
   return INIT_SUCCEEDED;
  }

void OnDeinit(const int reason)
  {
   EventKillTimer();
   CloseSocket();
  }

void OnTimer()
  {
   TryConnect();
   string lines[];
   ReadInbox(lines);
   for(int i = 0; i < ArraySize(lines); i++)
      Dispatch(lines[i]);
   ulong now = GetTickCount64();
   if(g_session != "" && now - g_lastHb >= (ulong)InpHeartbeatMs)
     {
      g_lastHb = now;
      SendHeartbeat();
     }
  }

void OnTick() { }

void OnTradeTransaction(const MqlTradeTransaction &trans, const MqlTradeRequest &request, const MqlTradeResult &result)
  {
   if(trans.type != TRADE_TRANSACTION_DEAL_ADD || trans.symbol != InpSymbol)
      return;
   if(!HistoryDealSelect(trans.deal))
      return;
   long entry = HistoryDealGetInteger(trans.deal, DEAL_ENTRY);
   long dtype = HistoryDealGetInteger(trans.deal, DEAL_TYPE);
   long reason = HistoryDealGetInteger(trans.deal, DEAL_REASON);
   ulong posId = (ulong)HistoryDealGetInteger(trans.deal, DEAL_POSITION_ID);
   ulong order = (ulong)HistoryDealGetInteger(trans.deal, DEAL_ORDER);
   double vol = HistoryDealGetDouble(trans.deal, DEAL_VOLUME), price = HistoryDealGetDouble(trans.deal, DEAL_PRICE);
   double profit = HistoryDealGetDouble(trans.deal, DEAL_PROFIT) + HistoryDealGetDouble(trans.deal, DEAL_SWAP) + HistoryDealGetDouble(trans.deal, DEAL_COMMISSION);
   long tmsc = HistoryDealGetInteger(trans.deal, DEAL_TIME_MSC);
   string key = KeyOfPosition(posId);   // after this call the history selection is the position's
   if(key == "")
      return;                           // not one of ours
   string e = entry == DEAL_ENTRY_IN ? "IN" : (entry == DEAL_ENTRY_OUT || entry == DEAL_ENTRY_OUT_BY) ? "OUT" : "INOUT";
   string r = reason == DEAL_REASON_SL ? "SL" : reason == DEAL_REASON_TP ? "TP" : reason == DEAL_REASON_SO ? "SO" : reason == DEAL_REASON_EXPERT ? "EXPERT" :
              (reason == DEAL_REASON_CLIENT || reason == DEAL_REASON_MOBILE || reason == DEAL_REASON_WEB) ? "CLIENT" : "OTHER";
   string b = "\"key\":" + Js(key) + ",\"kind\":\"DEAL\",\"order\":" + IntegerToString((long)order) + ",\"deal\":" + IntegerToString((long)trans.deal) +
              ",\"position\":" + IntegerToString((long)posId) + ",\"entry\":" + Js(e) + ",\"side\":" + Js(dtype == DEAL_TYPE_BUY ? "BUY" : "SELL") +
              ",\"volume\":" + Jd(vol, 6) + ",\"price\":" + Jd(price, 6) + ",\"reason\":" + Js(r) + ",\"profit\":" + Jd(profit, 2) +
              ",\"time_msc\":" + IntegerToString(tmsc);
   SendMsg("TXN", b);
  }
//+------------------------------------------------------------------+
