"use client";

import { useEffect, useState } from 'react';
import { 
  LineChart, 
  Line, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer 
} from 'recharts';

interface MarketData {
  time: string;
  price: number;
}

export default function Dashboard() {
  const [chartData, setChartData] = useState<MarketData[]>([]);
  const [currentPrice, setCurrentPrice] = useState<number | null>(null);
  const [anomalyStatus, setAnomalyStatus] = useState<boolean>(false);
  const [isWaiting, setIsWaiting] = useState<boolean>(true);

  useEffect(() => {
    const fetchMarketData = async () => {
      try {
        // ✅ CRITICAL FIX: Ensure https:// is present and use your unique Codespace URL
        // Make sure Port 8000 is set to PUBLIC in the 'Ports' tab
        const API_URL = "https://crispy-winner-x5vq5qxv99jw39x7g-8000.app.github.dev/api/v1/market/status";
        
        const response = await fetch(API_URL, { cache: 'no-store' });

        // 1. Read as raw text first
        const rawText = await response.text();
        
        // 2. PRE-PARSE VALIDATION: 
        // If the response is empty, too short, or looks like HTML (starts with <), skip it.
        if (!rawText || rawText.length < 2 || rawText.trim().startsWith("<!")) {
          console.log("Nexus Stream: Received invalid response, Spark still warming up...");
          setIsWaiting(true);
          return;
        }

        // 3. SAFE PARSING
        let data;
        try {
          data = JSON.parse(rawText);
        } catch (parseError) {
          console.error("Nexus Stream: JSON Parse failed. Content was:", rawText);
          return;
        }

        // 4. Handle 'Waiting' State from FastAPI
        if (data.status === "waiting") {
          setIsWaiting(true);
          return;
        }

        setIsWaiting(false);

        if (data.metrics && data.metrics.close) {
          const newTime = new Date(data.timestamp).toLocaleTimeString([], { 
            hour: '2-digit', 
            minute: '2-digit' 
          });

          const newPrice = data.metrics.close;
          setCurrentPrice(newPrice);
          setAnomalyStatus(data.ai_analysis?.anomaly_detected || false);

          setChartData((prev) => {
            // Avoid duplicate points for the same minute
            if (prev.length > 0 && prev[prev.length - 1].time === newTime) {
              return prev;
            }
            // Maintain a sliding window of the last 20 data points
            const updated = [...prev, { time: newTime, price: newPrice }];
            return updated.length > 20 ? updated.slice(1) : updated;
          });
        }
      } catch (error) {
        // Silently handle network/CORS errors while initializing
        console.log("Nexus Stream: Connection failed. Check Port 8000 visibility.");
      }
    };

    // Poll every 5 seconds
    const interval = setInterval(fetchMarketData, 5000);
    fetchMarketData(); 
    
    return () => clearInterval(interval);
  }, []);

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100 p-4 md:p-8 font-sans">
      <div className="max-w-6xl mx-auto space-y-6">
        
        {/* Header Section */}
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center border-b border-slate-800 pb-6 gap-4">
          <div>
            <h1 className="text-3xl font-extrabold tracking-tight text-white italic">NEXUS STREAM AI</h1>
            <p className="text-sm text-slate-400 font-mono tracking-tighter">MEDALLION LAKEHOUSE PIPELINE v1.0</p>
          </div>
          <div className="bg-slate-900 px-6 py-4 rounded-xl border border-slate-700 shadow-2xl">
            <div className="text-[10px] text-slate-500 uppercase font-black tracking-[0.2em] mb-1">LIVE BTC / USDT</div>
            <div className="text-3xl font-mono text-emerald-400 font-bold">
              ${currentPrice ? currentPrice.toLocaleString(undefined, {minimumFractionDigits: 2}) : "---.--"}
            </div>
          </div>
        </div>

        {/* AI Anomaly Alert Banner */}
        {anomalyStatus && !isWaiting && (
          <div className="bg-red-900/30 border-2 border-red-600 text-red-100 p-5 rounded-xl flex items-center shadow-[0_0_40px_rgba(220,38,38,0.2)] animate-pulse transition-all">
            <span className="text-2xl mr-4">🚨</span>
            <div>
              <p className="font-black text-lg uppercase tracking-tight">AI Anomaly Detected</p>
              <p className="text-sm opacity-90 font-medium italic">Unusual volatility detected by Isolation Forest model in current window.</p>
            </div>
          </div>
        )}

        {/* Main Chart Display */}
        <div className="bg-slate-900/50 rounded-2xl border border-slate-800 p-6 shadow-inner relative overflow-hidden" style={{ height: '500px' }}>
          {isWaiting ? (
            <div className="absolute inset-0 flex flex-col items-center justify-center space-y-6">
              <div className="w-14 h-14 border-4 border-emerald-500/10 border-t-emerald-500 rounded-full animate-spin"></div>
              <div className="text-center">
                <p className="text-slate-300 font-bold tracking-wide uppercase text-sm">Synchronizing Medallion Layers</p>
                <p className="text-slate-500 text-xs mt-1 italic">Awaiting Spark Watermark for first Gold window (Approx 60s)...</p>
              </div>
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" vertical={false} />
                <XAxis 
                  dataKey="time" 
                  stroke="#475569" 
                  tick={{fill: '#64748b', fontSize: 11, fontWeight: 600}} 
                  tickMargin={12}
                />
                <YAxis 
                  domain={['auto', 'auto']} 
                  stroke="#475569" 
                  tick={{fill: '#64748b', fontSize: 11, fontWeight: 600}}
                  tickFormatter={(val) => `$${val.toLocaleString()}`}
                  width={80}
                />
                <Tooltip 
                  contentStyle={{ backgroundColor: '#0f172a', border: '1px solid #334155', borderRadius: '12px', fontSize: '12px' }}
                  itemStyle={{ color: '#10b981', fontWeight: 'bold' }}
                />
                <Line 
                  type="monotone" 
                  dataKey="price" 
                  stroke="#10b981" 
                  strokeWidth={4}
                  dot={{ r: 3, fill: '#10b981', strokeWidth: 0 }}
                  activeDot={{ r: 6, stroke: '#059669', strokeWidth: 2 }}
                  isAnimationActive={false} 
                />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Real-Time Metrics Footer */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
           {[
             { label: "Pipeline Status", value: "Streaming / Active", color: "text-emerald-500" },
             { label: "Storage Engine", value: "Delta Lake / Parquet", color: "text-slate-300" },
             { label: "Orchestration", value: "Spark Structured Streaming", color: "text-slate-300" },
             { label: "Infrastructure", value: "Kafka (KRaft) + Docker", color: "text-slate-300" }
           ].map((item, idx) => (
             <div key={idx} className="bg-slate-900/80 p-4 rounded-xl border border-slate-800 flex flex-col justify-center">
                <p className="text-slate-500 text-[10px] uppercase font-black tracking-widest">{item.label}</p>
                <p className={`${item.color} font-mono text-xs mt-1 font-bold`}>{item.value}</p>
             </div>
           ))}
        </div>
      </div>
    </main>
  );
}