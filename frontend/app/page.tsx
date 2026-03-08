"use client";

import { useEffect, useState, useRef } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

interface MarketData { time: string; price: number; }

export default function Dashboard() {
  // Application State
  const [isClient, setIsClient] = useState(false); // Prevents SSR Fetching
  const [chartData, setChartData] = useState<MarketData[]>([]);
  const [currentPrice, setCurrentPrice] = useState<number | null>(null);
  const [anomalyStatus, setAnomalyStatus] = useState<boolean>(false);
  const [isWaiting, setIsWaiting] = useState<boolean>(true);
  const [activeSymbol, setActiveSymbol] = useState<string>("BTCUSDT");

  // Time Travel State
  const [isTimeTravel, setIsTimeTravel] = useState<boolean>(false);
  const [maxVersion, setMaxVersion] = useState<number>(0);
  const [selectedVersion, setSelectedVersion] = useState<number>(0);

  // ⚠️ IMPORTANT: Ensure this matches your Public Codespace URL
  const API_BASE = "https://crispy-winner-x5vq5qxv99jw39x7g-8000.app.github.dev";

  // Tell Next.js we are officially in the browser
  useEffect(() => {
    setIsClient(true);
  }, []);

  // ✅ DOUBLE-SHIELDED FETCH WRAPPER
  const safeFetchJSON = async (url: string) => {
    try {
      const response = await fetch(url, { cache: 'no-store' });
      if (!response.ok) return null;
      
      const text = await response.text();
      if (!text || text.trim() === "" || text.startsWith("<")) return null;
      
      try {
        return JSON.parse(text);
      } catch (parseError) {
        return null; // Absorb JSON.parse crashes completely
      }
    } catch (networkError) {
      return null; // Absorb network drops completely
    }
  };

  // Reset chart when switching coins
  useEffect(() => {
    setChartData([]);
    setIsWaiting(true);
  }, [activeSymbol]);

  // Fetch Latest Version for the Slider
  useEffect(() => {
    if (!isClient) return; // Block SSR

    const fetchMaxVersion = async () => {
      try {
        const data = await safeFetchJSON(`${API_BASE}/api/v1/market/versions`);
        if (data && typeof data.latest_version === 'number') {
          setMaxVersion(data.latest_version);
          if (!isTimeTravel) setSelectedVersion(data.latest_version);
        }
      } catch (e) { /* Silent fail */ }
    };
    
    // Explicitly catch unhandled interval rejections
    const interval = setInterval(() => { fetchMaxVersion().catch(() => {}); }, 10000);
    fetchMaxVersion().catch(() => {});
    
    return () => clearInterval(interval);
  }, [isClient, isTimeTravel, API_BASE]);

  // Main Data Fetcher
  useEffect(() => {
    if (!isClient) return; // Block SSR

    const fetchData = async () => {
      try {
        if (isTimeTravel) {
          const data = await safeFetchJSON(`${API_BASE}/api/v1/market/timetravel/${activeSymbol}/${selectedVersion}`);
          if (data && data.status === "success" && data.data) {
            setChartData(data.data);
            if (data.data.length > 0) {
              setCurrentPrice(data.data[data.data.length - 1].price);
            }
            setAnomalyStatus(false);
            setIsWaiting(false);
          }
        } else {
          const data = await safeFetchJSON(`${API_BASE}/api/v1/market/status/${activeSymbol}`);
          if (!data || data.status === "waiting") {
            setIsWaiting(true);
            return;
          }

          setIsWaiting(false);
          if (data.metrics && data.metrics.close) {
            const newTime = new Date(data.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            const newPrice = data.metrics.close;
            
            setCurrentPrice(newPrice);
            setAnomalyStatus(data.ai_analysis?.anomaly_detected || false);

            setChartData((prev) => {
              if (prev.length > 0 && prev[prev.length - 1].time === newTime) return prev;
              const updated = [...prev, { time: newTime, price: newPrice }];
              return updated.length > 20 ? updated.slice(1) : updated;
            });
          }
        }
      } catch (e) { /* Silent fail */ }
    };

    if (!isTimeTravel) {
      // Explicitly catch unhandled interval rejections
      const interval = setInterval(() => { fetchData().catch(() => {}); }, 5000);
      fetchData().catch(() => {}); 
      return () => clearInterval(interval);
    } else {
      fetchData().catch(() => {});
    }
  }, [isClient, activeSymbol, isTimeTravel, selectedVersion, API_BASE]);

  // Prevent server from attempting to render the complex UI
  if (!isClient) {
    return <div className="min-h-screen bg-slate-950 flex items-center justify-center text-slate-500 font-mono">Initializing Nexus Stream App Environment...</div>;
  }

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100 p-4 md:p-8 font-sans">
      <div className="max-w-6xl mx-auto space-y-6">
        
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center border-b border-slate-800 pb-6 gap-4">
          <div>
            <h1 className="text-3xl font-extrabold tracking-tight text-white italic">NEXUS STREAM AI</h1>
            <p className="text-sm text-slate-400 font-mono tracking-tighter">MEDALLION LAKEHOUSE PIPELINE v1.0</p>
          </div>

          <div className="flex space-x-2 bg-slate-900 p-2 rounded-xl border border-slate-700">
            {["BTCUSDT", "ETHUSDT", "SOLUSDT"].map((sym) => (
              <button key={sym} onClick={() => setActiveSymbol(sym)}
                className={`px-4 py-2 rounded-lg font-bold text-sm transition-all ${
                  activeSymbol === sym ? 'bg-emerald-600 text-white' : 'text-slate-400 hover:bg-slate-800'
                }`}
              >
                {sym.replace("USDT", "")}
              </button>
            ))}
          </div>

          <div className={`px-6 py-4 rounded-xl border shadow-2xl transition-colors ${isTimeTravel ? 'bg-indigo-950 border-indigo-700' : 'bg-slate-900 border-slate-700'}`}>
            <div className="text-[10px] text-slate-500 uppercase font-black tracking-[0.2em] mb-1">
              {isTimeTravel ? "HISTORICAL" : "LIVE"} {activeSymbol.replace("USDT", " / USDT")}
            </div>
            <div className={`text-3xl font-mono font-bold ${isTimeTravel ? 'text-indigo-400' : 'text-emerald-400'}`}>
              ${currentPrice ? currentPrice.toLocaleString(undefined, {minimumFractionDigits: 2}) : "---.--"}
            </div>
          </div>
        </div>

        <div className="bg-slate-900/80 p-5 rounded-xl border border-slate-800 flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center space-x-4">
            <button 
              onClick={() => setIsTimeTravel(!isTimeTravel)}
              className={`px-4 py-2 rounded-lg font-bold text-sm border ${
                isTimeTravel ? 'bg-indigo-600 border-indigo-500 text-white' : 'bg-slate-800 border-slate-700 text-slate-300'
              }`}
            >
              {isTimeTravel ? "⏱️ Disable Time Travel" : "⏳ Enable Time Travel"}
            </button>
            {isTimeTravel && <span className="text-indigo-300 font-mono text-sm">Querying Delta Version: {selectedVersion}</span>}
          </div>
          
          {isTimeTravel && maxVersion > 0 && (
            <input type="range" min="0" max={maxVersion} value={selectedVersion} onChange={(e) => setSelectedVersion(parseInt(e.target.value))}
              className="w-full md:w-1/2 h-2 bg-slate-700 rounded-lg appearance-none cursor-pointer accent-indigo-500"
            />
          )}
        </div>

        {anomalyStatus && !isWaiting && !isTimeTravel && (
          <div className="bg-red-900/30 border-2 border-red-600 text-red-100 p-5 rounded-xl flex items-center shadow-[0_0_40px_rgba(220,38,38,0.2)] animate-pulse">
            <span className="text-2xl mr-4">🚨</span>
            <div>
              <p className="font-black text-lg uppercase tracking-tight">AI Anomaly Detected</p>
              <p className="text-sm opacity-90 font-medium italic">Unusual volatility detected by Isolation Forest model in current window.</p>
            </div>
          </div>
        )}

        <div className="bg-slate-900/50 rounded-2xl border border-slate-800 p-6 shadow-inner relative overflow-hidden" style={{ height: '500px' }}>
          {isWaiting ? (
            <div className="absolute inset-0 flex flex-col items-center justify-center space-y-6">
              <div className={`w-14 h-14 border-4 rounded-full animate-spin ${isTimeTravel ? 'border-indigo-500/10 border-t-indigo-500' : 'border-emerald-500/10 border-t-emerald-500'}`}></div>
              <div className="text-center">
                <p className="text-slate-300 font-bold tracking-wide uppercase text-sm">Querying Lakehouse...</p>
              </div>
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis dataKey="time" stroke="#475569" tick={{fill: '#64748b', fontSize: 11, fontWeight: 600}} tickMargin={12} />
                <YAxis domain={['auto', 'auto']} stroke="#475569" tick={{fill: '#64748b', fontSize: 11, fontWeight: 600}} tickFormatter={(val) => `$${val.toLocaleString()}`} width={80} />
                <Tooltip contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px', fontSize: '12px' }} itemStyle={{ color: isTimeTravel ? '#818cf8' : '#10b981', fontWeight: 'bold' }} />
                <Line type="monotone" dataKey="price" stroke={isTimeTravel ? '#818cf8' : '#10b981'} strokeWidth={4} dot={{ r: 3, fill: isTimeTravel ? '#818cf8' : '#10b981', strokeWidth: 0 }} activeDot={{ r: 6, stroke: isTimeTravel ? '#4f46e5' : '#059669', strokeWidth: 2 }} isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </main>
  );
}