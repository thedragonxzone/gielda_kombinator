#!/usr/bin/env python3
"""
Multi-Ticker (Bitget) + NASDAQ (TradingView widget) Trading Assistant
- Panel boczny PO LEWEJ, wykresy PO PRAWEJ
- Bitget: lightweight-charts z danymi z Bitget Futures + strefy/setupy z pliku JSON
- Wybór Tickerów w QComboBox + zapamiętywanie wyboru w config.json
"""
import json
import os
import sys
import time
import requests
from pathlib import Path
from typing import Dict, List
import websocket
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QSplitter, QLabel, QListWidget, QListWidgetItem,
    QTextEdit, QFrame, QPushButton, QCheckBox, QComboBox
)
from PyQt6.QtCore import (
    Qt, QThread, pyqtSignal, QObject, QTimer, QUrl
)
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebEngineCore import QWebEnginePage, QWebEngineProfile, QWebEngineSettings
from PyQt6.QtGui import QFont, QTextCursor, QIcon
from PyQt6.QtMultimedia import QSoundEffect
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Mapowanie interwału UI na parametr Bitget REST API
BITGET_INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "1h": "1H", "1d": "1D"
}

# CONFIG: Lista wspieranych tickerów
AVAILABLE_TICKERS = {
    "NVDAUSDT": "NVDA",
    "SNDKUSDT": "SNDK",
    "TSLAUSDT": "TSLA",
    "BTCUSDT": "BTC",
    "SOLUSDT": "SOL"
}

class BitgetFuturesClient(QThread):
    historical_data_ready = pyqtSignal(str, str, str)
    realtime_update_ready = pyqtSignal(str, str, str)
    change_interval_signal = pyqtSignal(str, str)
    change_ticker_signal = pyqtSignal(str, str)

    def __init__(self, default_ticker="NVDAUSDT"):
        super().__init__()
        self.running = True
        self.active_ticker = default_ticker
        self.monitored_assets = {
            "ASSET": {"ticker": self.active_ticker, "interval": "1m", "last_ts": 0}
        }
        self.base_url = "https://api.bitget.com/api/v2/mix/market/candles"
        self.ws_url = "wss://ws.bitget.com/v2/ws/public"
        self.ws = None
        self.change_interval_signal.connect(self._change_ws_subscription)
        self.change_ticker_signal.connect(self._change_active_ticker)

    def fetch_candles(self, symbol: str, granularity: str, limit: int = 1000, end_time: int = None) -> List[list]:
        params = {
            "symbol": symbol,
            "productType": "usdt-futures",
            "granularity": granularity,
            "limit": limit
        }
        if end_time:
            params["endTime"] = str(end_time)
        try:
            res = requests.get(self.base_url, params=params, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if data.get("code") == "00000" and "data" in data:
                    return data["data"]
                print(f"[PYTHON ERROR] Bitget API error dla {symbol}: {res.text}")
        except Exception as e:
            print(f"[PYTHON EXCEPTION] Błąd pobierania {symbol}: {e}")
        return []

    def load_historical(self, asset_id: str, ui_interval: str, ticker_name: str):
        bitget_granularity = BITGET_INTERVAL_MAP.get(ui_interval, "1m")
        self.monitored_assets[asset_id]["interval"] = ui_interval
        symbol = self.monitored_assets[asset_id]["ticker"]
        all_candles = []
        last_end_time = None
        target_cycles = 3
        print(f"[PYTHON] Rozpoczynam pobieranie ~3000 świec (REST) dla {symbol}...")
        for cycle in range(target_cycles):
            candles = self.fetch_candles(symbol, bitget_granularity, limit=1000, end_time=last_end_time)
            if not candles:
                break
            all_candles.extend(candles)
            timestamps = [int(c[0]) for c in candles]
            if not timestamps:
                break
            oldest_ts = min(timestamps)
            last_end_time = oldest_ts - 1
            if len(candles) < 1000:
                break
            time.sleep(0.1)
        
        if all_candles:
            parsed_history = []
            seen_times = set()
            for c in all_candles:
                raw_ts = int(c[0])
                clean_ts = int(raw_ts // 1000) if raw_ts > 10000000000 else int(raw_ts)
                if clean_ts in seen_times:
                    continue
                seen_times.add(clean_ts)
                parsed_history.append({
                    "time": clean_ts,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5])
                })
            parsed_history.sort(key=lambda x: x["time"])
            self.monitored_assets[asset_id]["last_ts"] = parsed_history[-1]["time"]
            json_str = json.dumps(parsed_history)
            print(f"[PYTHON] Pomyślnie załadowano {len(parsed_history)} świec do wykresu dla {symbol}.")
            self.historical_data_ready.emit(asset_id, json_str, ticker_name)

    def _get_channel_name(self, granularity: str) -> str:
        return f"candle{granularity}"

    def _subscribe(self, asset_id: str):
        info = self.monitored_assets[asset_id]
        bitget_granularity = BITGET_INTERVAL_MAP.get(info["interval"], "1m")
        channel = self._get_channel_name(bitget_granularity)
        ticker = info["ticker"]
        msg = {
            "op": "subscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": channel, "instId": ticker}]
        }
        try:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self.ws.send(json.dumps(msg))
                print(f"[WS] Subskrypcja: {ticker} / {channel}")
        except Exception as e:
            print(f"[WS ERROR] Błąd wysyłania subskrypcji: {e}")

    def _unsubscribe(self, asset_id: str):
        info = self.monitored_assets[asset_id]
        bitget_granularity = BITGET_INTERVAL_MAP.get(info["interval"], "1m")
        channel = self._get_channel_name(bitget_granularity)
        ticker = info["ticker"]
        msg = {
            "op": "unsubscribe",
            "args": [{"instType": "USDT-FUTURES", "channel": channel, "instId": ticker}]
        }
        try:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self.ws.send(json.dumps(msg))
                print(f"[WS] Anulowanie subskrypcji: {ticker} / {channel}")
        except Exception as e:
            print(f"[WS ERROR] Błąd wysyłania unsubscribe: {e}")

    def _change_ws_subscription(self, asset_id: str, new_interval: str):
        if asset_id not in self.monitored_assets:
            return
        self._unsubscribe(asset_id)
        self.monitored_assets[asset_id]["interval"] = new_interval
        self.monitored_assets[asset_id]["last_ts"] = 0
        time.sleep(0.2)
        self._subscribe(asset_id)

    def _change_active_ticker(self, asset_id: str, new_ticker: str):
        if asset_id not in self.monitored_assets:
            return
        self._unsubscribe(asset_id)
        self.monitored_assets[asset_id]["ticker"] = new_ticker
        self.monitored_assets[asset_id]["last_ts"] = 0
        self.active_ticker = new_ticker
        time.sleep(0.2)
        self._subscribe(asset_id)

    def _on_open(self, ws):
        print("[WS] Połączenie z Bitget WebSocket otwarte.")
        for asset_id in self.monitored_assets.keys():
            self._subscribe(asset_id)

    def _on_ping(self, ws, data):
        try:
            ws.send("pong", opcode=websocket.ABNF.OPCODE_PONG)
        except Exception as e:
            print(f"[WS ERROR] Nie udało się odpowiedzieć pong: {e}")

    def _on_error(self, ws, error):
        print(f"[WS ERROR] {error}")

    def _on_close(self, ws, close_status_code, close_msg):
        print(f"[WS] Połączenie zamknięte. Code: {close_status_code}, Msg: {close_msg}")

    def _on_message(self, ws, message):
        if message == "pong":
            return
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        
        if "event" in data and data["event"] in ("subscribe", "unsubscribe"):
            print(f"[WS] Potwierdzenie: {data.get('event')} dla {data.get('arg')}")
            return
        
        if "data" not in data or not data["data"]:
            return
        
        inst_id = data.get("arg", {}).get("instId")
        asset_id = None
        for aid, info in self.monitored_assets.items():
            if info["ticker"] == inst_id:
                asset_id = aid
                break
        
        if not asset_id:
            return
        
        for candle_arr in data["data"]:
            if len(candle_arr) < 6:
                continue
            try:
                raw_ts = int(candle_arr[0])
                clean_ts = int(raw_ts // 1000) if raw_ts > 10000000000 else int(raw_ts)
                last_recorded = self.monitored_assets[asset_id].get("last_ts", 0)
                if clean_ts >= last_recorded:
                    bar = {
                        "time": clean_ts,
                        "open": float(candle_arr[1]),
                        "high": float(candle_arr[2]),
                        "low": float(candle_arr[3]),
                        "close": float(candle_arr[4]),
                        "volume": float(candle_arr[5])
                    }
                    self.monitored_assets[asset_id]["last_ts"] = clean_ts
                    self.realtime_update_ready.emit(
                        asset_id,
                        json.dumps(bar),
                        self.monitored_assets[asset_id]["ticker"]
                    )
            except (ValueError, IndexError) as e:
                print(f"[WS PARSE ERROR] {e} dla danych: {candle_arr}")

    def run(self):
        print("[WS] Start wątku WebSocket...")
        while self.running:
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_ping=self._on_ping,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
                if self.running:
                    print("[WS] Utrata połączenia. Ponowna próba za 5 sekund...")
                    time.sleep(5)
            except Exception as e:
                print(f"[WS FATAL] Błąd w głównej pętli: {e}")
                if self.running:
                    time.sleep(5)

    def stop(self):
        self.running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith('.json'):
            self.callback()

class SchemaWatcher(QObject):
    file_changed = pyqtSignal()

    def __init__(self, watch_dir: str):
        super().__init__()
        self.observer = Observer()
        self.handler = FileChangeHandler(self.file_changed.emit)
        self.observer.schedule(self.handler, path=watch_dir, recursive=False)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()

class WebEnginePageCustom(QWebEnginePage):
    def __init__(self, parent, asset_id):
        super().__init__(parent)
        self.parent_win = parent
        self.asset_id = asset_id

    def javaScriptConsoleMessage(self, level, message, lineNumber, sourceID):
        if "DEBUG_JS" in message or "Error" in message or "TypeError" in message:
            print(f"[JS CONSOLE] {self.asset_id}: {message}")
        if "INTERVAL_SWITCH:" in message:
            new_interval = message.replace("INTERVAL_SWITCH:", "").strip()
            self.parent_win.handle_interval_change(self.asset_id, new_interval)

class MainWindow(QMainWindow):
    BASE_TITLE = "Bitget Conditioner"

    def __init__(self):
        super().__init__()
        self.resize(1600, 950)
        self.base_dir = Path(__file__).resolve().parent
        self.config_path = self.base_dir / "config.json"
        
        self.alerts_enabled = False
        QTimer.singleShot(5000, self.enable_alerts)
        
        self.current_ticker = self.load_saved_ticker()
        self.current_setup = None
        self.chart_interval = "1m"
        self.chart_ready = False
        self.schema_data = None
        
        self.alert_sound_path = self.base_dir / "alert.wav"
        self.sound_effect = QSoundEffect()
        if self.alert_sound_path.exists():
            self.sound_effect.setSource(QUrl.fromLocalFile(str(self.alert_sound_path)))
            self.sound_effect.setVolume(1.0)
        else:
            print(f"⚠️ [WARNING] Brak pliku dźwiękowego: {self.alert_sound_path}")
        
        self.last_alert_state = False
        self.reload_timer = QTimer()
        self.reload_timer.setSingleShot(True)
        self.reload_timer.timeout.connect(self.reset_and_load_schema)
        
        self.init_ui()
        
        self.bitget_client = BitgetFuturesClient(default_ticker=self.current_ticker)
        self.bitget_client.historical_data_ready.connect(self.on_historical_data)
        self.bitget_client.realtime_update_ready.connect(self.on_realtime_update)
        self.bitget_client.start()
        
        sciezka_ikony = self.base_dir / "gielda_kombinator.svg"
        if sciezka_ikony.exists():
            self.setWindowIcon(QIcon(str(sciezka_ikony)))
            
        self.file_watcher = SchemaWatcher(str(self.base_dir))
        self.file_watcher.file_changed.connect(self.on_file_changed)

    def enable_alerts(self):
        self.alerts_enabled = True
        print("[INFO] Alerty dźwiękowe zostały aktywowane.")

    def get_schema_path_for_ticker(self, ticker_symbol: str) -> Path:
        short_name = AVAILABLE_TICKERS.get(ticker_symbol, "NVDA")
        if short_name == "NVDA":
            path = self.base_dir / "NVDA_BITGET.json"
            return path
        return self.base_dir / f"{short_name}_BITGET.json"

    def load_saved_ticker(self) -> str:
        if self.config_path.exists():
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                ticker = cfg.get("last_ticker")
                if ticker in AVAILABLE_TICKERS:
                    return ticker
            except Exception as e:
                print(f"[CONFIG ERROR] Błąd wczytywania config.json: {e}")
        return "NVDAUSDT"

    def save_current_ticker(self):
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump({"last_ticker": self.current_ticker}, f, indent=2)
        except Exception as e:
            print(f"[CONFIG ERROR] Nie można zapisać config.json: {e}")

    def init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QHBoxLayout(main_widget)
        main_layout.setContentsMargins(5, 5, 5, 5)
        
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.setStyleSheet("QSplitter::handle { background-color: #313244; }")
        
        left_panel = QFrame()
        left_panel.setStyleSheet("background-color: #1e1e2e; border-right: 1px solid #313244;")
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(10, 10, 10, 10)
        
        left_layout.addWidget(QLabel("Wybór Tickeru (Bitget):", styleSheet="color: #bac2de; font-weight: bold;"))
        self.ticker_combo = QComboBox()
        self.ticker_combo.setStyleSheet("""
            QComboBox {
                background-color: #181825; color: #cdd6f4; border: 1px solid #313244;
                border-radius: 4px; padding: 5px; font-weight: bold;
            }
            QComboBox::drop-down { border: none; }
            QComboBox QAbstractItemView {
                background-color: #181825; color: #cdd6f4; selection-background-color: #313244;
            }
        """)
        for sym, short in AVAILABLE_TICKERS.items():
            self.ticker_combo.addItem(f"{sym} ({short})", sym)
        
        idx = self.ticker_combo.findData(self.current_ticker)
        if idx >= 0:
            self.ticker_combo.setCurrentIndex(idx)
        self.ticker_combo.currentIndexChanged.connect(self.on_ticker_changed)
        left_layout.addWidget(self.ticker_combo)
        
        self.sound_checkbox = QCheckBox("🔊 Dźwięk powiadomień Entry")
        self.sound_checkbox.setChecked(False)
        self.sound_checkbox.setStyleSheet("color: #a6e3a1; font-weight: bold; margin-top: 5px; margin-bottom: 5px;")
        left_layout.addWidget(self.sound_checkbox)
        
        self.macro_label = QLabel("Sentyment: N/A")
        self.macro_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        self.macro_label.setStyleSheet("color: #cdd6f4;")
        left_layout.addWidget(self.macro_label)
        
        left_layout.addWidget(QLabel("Kontekst Rynkowy (NASDAQ):", styleSheet="color: #bac2de; font-weight: bold;"))
        self.market_context_box = QTextEdit()
        self.market_context_box.setReadOnly(True)
        self.market_context_box.setMaximumHeight(120)
        self.market_context_box.setStyleSheet(
            "background-color: #181825; color: #a6adc8; border: 1px solid #313244; "
            "border-radius: 4px; font-size: 11px;"
        )
        left_layout.addWidget(self.market_context_box)
        
        left_inner_splitter = QSplitter(Qt.Orientation.Vertical)
        left_inner_splitter.setStyleSheet("QSplitter::handle { background-color: #313244; }")
        
        lists_widget = QWidget()
        lists_layout = QVBoxLayout(lists_widget)
        lists_layout.setContentsMargins(0, 0, 0, 0)
        lists_layout.addWidget(QLabel("Aktywne Poziomy i Analizy:", styleSheet="color: #bac2de; font-weight: bold;"))
        
        self.ranges_list = QListWidget()
        self.ranges_list.setStyleSheet(
            "background-color: #181825; color: #cdd6f4; border: 1px solid #313244; border-radius: 4px;"
        )
        self.ranges_list.itemChanged.connect(self.on_range_visibility_changed)
        self.ranges_list.itemClicked.connect(self.on_range_selected)
        lists_layout.addWidget(self.ranges_list)
        
        self.setups_label = QLabel(f"Dostępne Strategie {AVAILABLE_TICKERS.get(self.current_ticker)} (Setups):")
        self.setups_label.setStyleSheet("color: #bac2de; font-weight: bold;")
        lists_layout.addWidget(self.setups_label)
        
        self.setups_list = QListWidget()
        self.setups_list.setStyleSheet(
            "background-color: #181825; color: #cdd6f4; border: 1px solid #313244; border-radius: 4px;"
        )
        self.setups_list.itemChanged.connect(self.on_setup_visibility_changed)
        self.setups_list.itemClicked.connect(self.on_setup_selected)
        lists_layout.addWidget(self.setups_list)
        
        details_widget = QWidget()
        details_layout = QVBoxLayout(details_widget)
        details_layout.setContentsMargins(0, 0, 0, 0)
        details_layout.addWidget(QLabel("Komentarz i Wytyczne Strategii:", styleSheet="color: #bac2de; font-weight: bold;"))
        
        self.details_box = QTextEdit()
        self.details_box.setReadOnly(True)
        self.details_box.setStyleSheet(
            "background-color: #11111b; color: #a6adc8; border: 1px solid #313244; "
            "border-radius: 4px; font-family: monospace;"
        )
        details_layout.addWidget(self.details_box)
        
        left_inner_splitter.addWidget(lists_widget)
        left_inner_splitter.addWidget(details_widget)
        left_inner_splitter.setSizes([400, 300])
        left_layout.addWidget(left_inner_splitter, stretch=1)
        
        chart_splitter = QSplitter(Qt.Orientation.Vertical)
        chart_splitter.setStyleSheet("background-color: #11111b; QSplitter::handle { background-color: #313244; }")
        
        self.chart_container = QWidget()
        chart_lay = QVBoxLayout(self.chart_container)
        chart_lay.setContentsMargins(0, 0, 0, 0)
        chart_lay.setSpacing(0)
        
        self.chart_label = QLabel(f"{self.current_ticker} (Bitget Futures) - Interwał: {self.chart_interval}")
        self.chart_label.setStyleSheet("background-color: #11111b; color: #a6adc8; padding: 6px; font-weight: bold;")
        self.chart_label.setFixedHeight(28)
        
        self.main_chart_view = QWebEngineView()
        self.main_page = WebEnginePageCustom(self, "ASSET")
        self.main_chart_view.setPage(self.main_page)
        
        chart_lay.addWidget(self.chart_label)
        chart_lay.addWidget(self.main_chart_view, stretch=1)
        
        self.nasdaq_container = QWidget()
        nasdaq_lay = QVBoxLayout(self.nasdaq_container)
        nasdaq_lay.setContentsMargins(0, 0, 0, 0)
        nasdaq_lay.setSpacing(0)
        
        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(0, 0, 0, 0)
        self.nasdaq_label = QLabel("NASDAQ-100 (TradingView CFD) - Podgląd rynku")
        self.nasdaq_label.setStyleSheet("background-color: #11111b; color: #a6adc8; padding: 6px; font-weight: bold;")
        self.nasdaq_label.setFixedHeight(28)
        header_layout.addWidget(self.nasdaq_label, stretch=1)
        
        self.tv_toggle_btn = QPushButton("Włącz TradingView")
        self.tv_toggle_btn.setFixedHeight(28)
        self.tv_toggle_btn.setCursor(Qt.CursorShape.PointingHandCursor)
        self.tv_toggle_btn.setStyleSheet("""
            QPushButton {
                background-color: #a6e3a1; color: #11111b; border: none; padding: 4px 12px;
                font-weight: bold; border-radius: 4px; margin-right: 5px;
            }
            QPushButton:hover { background-color: #94e2d5; }
            QPushButton:pressed { background-color: #89dceb; }
        """)
        self.tv_toggle_btn.clicked.connect(self.toggle_tradingview)
        header_layout.addWidget(self.tv_toggle_btn)
        nasdaq_lay.addLayout(header_layout)
        
        self.nasdaq_chart_container = QWidget()
        self.nasdaq_chart_container.setStyleSheet("background-color: #000000;")
        self.nasdaq_chart_lay = QVBoxLayout(self.nasdaq_chart_container)
        self.nasdaq_chart_lay.setContentsMargins(0, 0, 0, 0)
        
        self.tv_placeholder = QLabel("TradingView jest wyłączony.\nKliknij przycisk powyżej, aby załadować wykres (oszczędzanie CPU).")
        self.tv_placeholder.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.tv_placeholder.setStyleSheet("color: #6c7086; font-size: 14px; background-color: #11111b;")
        self.nasdaq_chart_lay.addWidget(self.tv_placeholder)
        nasdaq_lay.addWidget(self.nasdaq_chart_container, stretch=1)
        
        self.nasdaq_chart_view = None
        self.tv_is_active = False
        
        chart_splitter.addWidget(self.chart_container)
        chart_splitter.addWidget(self.nasdaq_container)
        chart_splitter.setCollapsible(0, False)
        chart_splitter.setCollapsible(1, False)
        chart_splitter.setSizes([450, 450])
        
        main_splitter.addWidget(left_panel)
        main_splitter.addWidget(chart_splitter)
        main_splitter.setSizes([420, 1180])
        main_layout.addWidget(main_splitter, stretch=1)
        
        chart_html_path = self.base_dir / "chart.html"
        self.main_chart_view.setUrl(QUrl.fromLocalFile(str(chart_html_path)))
        self.main_chart_view.loadFinished.connect(self.on_chart_load_finished)
        
        self.update_window_title()

    def update_window_title(self):
        short_name = AVAILABLE_TICKERS.get(self.current_ticker, self.current_ticker)
        self.setWindowTitle(f"[{short_name}] - {self.BASE_TITLE}")

    def generate_tradingview_html(self) -> str:
        baseUrl = "https://s.tradingview.com/widgetembed/"
        queryParams = {
            "symbol": "IG:NASDAQ",
            "interval": "1",
            "theme": "dark",
            "style": "1",
            "timezone": "Europe/Warsaw",
            "locale": "pl",
            "hideideas": "1",
            "backgroundColor": "#000000",
            "studies": "VWAP@tv-basicstudies",
            "disabled_features": '["show_right_widgets_panel_by_default","right_toolbar","widget_logo","hide_top_toolbar","hide_side_toolbar","volume_force_overlay","create_volume_indicator_by_default","display_studies_logo_in_legend","use_localstorage"]'
        }
        from urllib.parse import urlencode
        full_url = f"{baseUrl}?{urlencode(queryParams)}"
        return f"""
        <!DOCTYPE html>
        <html style="margin: 0; padding: 0; width: 100%; height: 100%; overflow: hidden;">
        <head>
        <meta charset="utf-8">
        <style>
        html, body {{ margin: 0; padding: 0; width: 100%; height: 100%; background-color: #000000; overflow: hidden; }}
        iframe {{ width: 100%; height: 100%; border: none; display: block; }}
        </style>
        </head>
        <body>
        <iframe src="{full_url}" scrolling="no" allowtransparency="true"></iframe>
        </body>
        </html>
        """

    def toggle_tradingview(self):
        if self.tv_is_active:
            if self.nasdaq_chart_view:
                self.nasdaq_chart_lay.removeWidget(self.nasdaq_chart_view)
                self.nasdaq_chart_view.stop()
                self.nasdaq_chart_view.deleteLater()
                self.nasdaq_chart_view = None
            self.tv_placeholder.show()
            self.tv_is_active = False
            self.tv_toggle_btn.setText("Włącz TradingView")
            self.tv_toggle_btn.setStyleSheet("""
                QPushButton {
                    background-color: #a6e3a1; color: #11111b; border: none; padding: 4px 12px;
                    font-weight: bold; border-radius: 4px; margin-right: 5px;
                }
                QPushButton:hover { background-color: #94e2d5; }
            """)
        else:
            self.tv_placeholder.hide()
            self.nasdaq_chart_view = QWebEngineView()
            
            # Odchudzenie ustawień przeglądarki dla widżetu NASDAQ
            settings = self.nasdaq_chart_view.settings()
            settings.setAttribute(QWebEngineSettings.WebAttribute.ScrollAnimatorEnabled, False)
            settings.setAttribute(QWebEngineSettings.WebAttribute.LocalStorageEnabled, False)
            settings.setAttribute(QWebEngineSettings.WebAttribute.JavascriptCanAccessClipboard, False)
            settings.setAttribute(QWebEngineSettings.WebAttribute.PluginsEnabled, False)
            
            self.nasdaq_chart_lay.addWidget(self.nasdaq_chart_view)
            
            # ZAPIS DO PLIKU I setUrl (omija sandboxing setHtml)
            nasdaq_html_content = self.generate_tradingview_html()
            nasdaq_html_path = self.base_dir / "nasdaq_tv.html"
            try:
                with open(nasdaq_html_path, "w", encoding="utf-8") as f:
                    f.write(nasdaq_html_content)
                self.nasdaq_chart_view.setUrl(QUrl.fromLocalFile(str(nasdaq_html_path)))
            except Exception as e:
                print(f"[ERROR] Nie udało się zapisać tymczasowego HTML dla TV: {e}")
                # Fallback
                self.nasdaq_chart_view.setHtml(nasdaq_html_content, QUrl("https://s.tradingview.com"))

            self.tv_is_active = True
            self.tv_toggle_btn.setText("Wyłącz TV")
            self.tv_toggle_btn.setStyleSheet("""
                QPushButton {
                    background-color: #f38ba8; color: #11111b; border: none; padding: 4px 12px;
                    font-weight: bold; border-radius: 4px; margin-right: 5px;
                }
                QPushButton:hover { background-color: #eba0ac; }
            """)

    def on_ticker_changed(self, index: int):
        new_ticker = self.ticker_combo.itemData(index)
        if not new_ticker or new_ticker == self.current_ticker:
            return
        print(f"[UI] Zmiana wybranego waloru: {self.current_ticker} -> {new_ticker}")
        self.current_ticker = new_ticker
        self.save_current_ticker()
        short_name = AVAILABLE_TICKERS.get(new_ticker, new_ticker)
        self.setups_label.setText(f"Dostępne Strategie {short_name} (Setups):")
        self.chart_label.setText(f"{self.current_ticker} (Bitget Futures) - Interwał: {self.chart_interval}")
        self.update_window_title()
        
        self.bitget_client.change_ticker_signal.emit("ASSET", self.current_ticker)
        
        if self.chart_ready:
            self.bitget_client.load_historical("ASSET", self.chart_interval, short_name)
            self.reset_and_load_schema()

    def handle_interval_change(self, asset_id: str, new_interval: str):
        self.chart_interval = new_interval
        self.chart_label.setText(f"{self.current_ticker} (Bitget Futures) - Interwał: {new_interval}")
        self.bitget_client.change_interval_signal.emit("ASSET", new_interval)
        self.bitget_client.load_historical("ASSET", new_interval, AVAILABLE_TICKERS.get(self.current_ticker, "ASSET"))

    def on_historical_data(self, asset_id: str, data_json: str, ticker_name: str):
        if self.chart_ready:
            js_code = f"""
            if (typeof window.loadHistoricalData === 'function') {{
                window.loadHistoricalData(`{data_json}`);
            }} else {{
                console.log('DEBUG_JS: loadHistoricalData jeszcze nie jest gotowe.');
            }}
            """
            self.main_chart_view.page().runJavaScript(js_code)

    def on_realtime_update(self, asset_id: str, bar_json: str, ticker_name: str):
        if self.chart_ready:
            js_code = f"""
            if (typeof window.updateRealTimeBar === 'function') {{
                window.updateRealTimeBar(`{bar_json}`);
            }} else {{
                console.log('DEBUG_JS: updateRealTimeBar jeszcze nie jest gotowe.');
            }}
            """
            self.main_chart_view.page().runJavaScript(js_code)
            
            try:
                bar = json.loads(bar_json)
                current_price = bar.get("close")
                if current_price is not None:
                    self.check_execution_zones(current_price)
            except Exception as e:
                print(f"[ERROR] Błąd sprawdzania warunków stref: {e}")

    def check_execution_zones(self, current_price: float):
        if not self.alerts_enabled:
            return
        
        in_any_entry_zone = False
        active_setup_name = ""
        
        for i in range(self.setups_list.count()):
            item = self.setups_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                setup = item.data(Qt.ItemDataRole.UserRole)
                if not setup:
                    continue
                entry_zone = setup.get("execution", {}).get("entry_zone", [])
                if len(entry_zone) == 2:
                    bottom, top = float(entry_zone[0]), float(entry_zone[1])
                    if bottom > top:
                        bottom, top = top, bottom
                    if bottom <= current_price <= top:
                        in_any_entry_zone = True
                        active_setup_name = setup.get("name", "Setup")
                        break
        
        short_name = AVAILABLE_TICKERS.get(self.current_ticker, self.current_ticker)
        if in_any_entry_zone:
            self.setWindowTitle(f"🚨 [ENTRY ALERT: {active_setup_name} | {current_price:.2f}] - [{short_name}] {self.BASE_TITLE}")
        else:
            self.update_window_title()
            
        if in_any_entry_zone and not self.last_alert_state:
            if self.sound_checkbox.isChecked() and self.alert_sound_path.exists():
                self.sound_effect.play()
            QApplication.alert(self, 0)
            
        self.last_alert_state = in_any_entry_zone

    def reset_and_load_schema(self):
        print(f"[WATCHDOG/RESET] Przeładowanie schematu JSON dla {self.current_ticker}...")
        self.current_setup = None
        self.schema_data = None
        self.macro_label.setText("Sentyment: N/A | F&G: N/A")
        self.market_context_box.clear()
        self.details_box.clear()
        
        if self.chart_ready:
            self.main_chart_view.page().runJavaScript("if(window.hideRangeLines){window.hideRangeLines();}")
            self.main_chart_view.page().runJavaScript("if(window.hideSetupLines){window.hideSetupLines();}")
        
        self.ranges_list.blockSignals(True)
        self.ranges_list.clear()
        self.ranges_list.blockSignals(False)
        
        self.setups_list.blockSignals(True)
        self.setups_list.clear()
        self.setups_list.blockSignals(False)
        
        self.load_schema()

    def load_schema(self):
        schema_path = self.get_schema_path_for_ticker(self.current_ticker)
        if not schema_path.exists():
            print(f"⚠️ [BŁĄD] Nie znaleziono pliku JSON pod ścieżką: {schema_path}")
            return
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                self.schema_data = json.load(f)
            
            env = self.schema_data.get("macro_environment", {})
            self.macro_label.setText(
                f"Sentyment: {env.get('market_sentiment','N/A').upper()} | F&G: {env.get('fear_and_greed_index','N/A')}"
            )
            self.update_market_context()
            self.update_ranges_list()
            self.update_setups_list()
        except Exception as e:
            print(f"Error loading schema: {e}")

    def on_file_changed(self):
        self.reload_timer.start(500)

    def update_market_context(self):
        nasdaq_data = self.schema_data.get("assets", {}).get("NASDAQ", {})
        if not nasdaq_data:
            self.market_context_box.setPlainText("Brak danych o NASDAQ w JSON.")
            self.market_context_box.moveCursor(QTextCursor.MoveOperation.Start)
            return
        
        text = f"Sentiment: {nasdaq_data.get('sentiment', 'N/A')}\n"
        text += f"Kluczowy poziom (Gatekeeper): {nasdaq_data.get('key_gatekeeper_level', 'N/A')}\n"
        analyses = nasdaq_data.get("analyses", [])
        if analyses:
            text += f"\n📌 {analyses[0]['name']}:\n{analyses[0]['description'][:120]}...\n"
        
        self.market_context_box.setPlainText(text)
        self.market_context_box.moveCursor(QTextCursor.MoveOperation.Start)

    def update_ranges_list(self):
        self.ranges_list.blockSignals(True)
        checked_ids = [
            self.ranges_list.item(i).data(Qt.ItemDataRole.UserRole)["id"]
            for i in range(self.ranges_list.count())
            if self.ranges_list.item(i).checkState() == Qt.CheckState.Checked
        ]
        self.ranges_list.clear()
        
        nasdaq_data = self.schema_data.get("assets", {}).get("NASDAQ", {})
        nasdaq_analyses = nasdaq_data.get("analyses", [])
        if nasdaq_analyses:
            for a in nasdaq_analyses:
                item = QListWidgetItem(f"[NASDAQ] {a['name']}")
                item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
                item.setCheckState(Qt.CheckState.Checked)
                item.setData(Qt.ItemDataRole.UserRole, {"type": "nasdaq_analysis", "data": a})
                self.ranges_list.addItem(item)
        
        asset_short = AVAILABLE_TICKERS.get(self.current_ticker, "NVDA")
        asset_ranges = self.schema_data.get("assets", {}).get(asset_short, {}).get("price_ranges", [])
        for r in asset_ranges:
            item = QListWidgetItem(r['name'])
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            if not checked_ids or r["id"] in checked_ids:
                item.setCheckState(Qt.CheckState.Checked)
            else:
                item.setCheckState(Qt.CheckState.Unchecked)
            item.setData(Qt.ItemDataRole.UserRole, {"type": "asset_range", "data": r})
            self.ranges_list.addItem(item)
            
        self.ranges_list.blockSignals(False)
        self.redraw_ranges()

    def update_setups_list(self):
        self.setups_list.blockSignals(True)
        checked_ids = [
            self.setups_list.item(i).data(Qt.ItemDataRole.UserRole)["id"]
            for i in range(self.setups_list.count())
            if self.setups_list.item(i).checkState() == Qt.CheckState.Checked
        ]
        current_row = self.setups_list.currentRow()
        self.setups_list.clear()
        
        asset_short = AVAILABLE_TICKERS.get(self.current_ticker, "NVDA")
        setups = self.schema_data.get("assets", {}).get(asset_short, {}).get("setups", [])
        for s in setups:
            item = QListWidgetItem(f"{s['name']} ({s['bias']})")
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            if not checked_ids or s["id"] in checked_ids:
                item.setCheckState(Qt.CheckState.Checked)
            else:
                item.setCheckState(Qt.CheckState.Unchecked)
            item.setData(Qt.ItemDataRole.UserRole, s)
            self.setups_list.addItem(item)
            
        if 0 <= current_row < self.setups_list.count():
            self.setups_list.setCurrentRow(current_row)
            
        self.setups_list.blockSignals(False)
        self.redraw_setups()

    def on_range_visibility_changed(self, item):
        self.redraw_ranges()

    def on_range_selected(self, item):
        role_data = item.data(Qt.ItemDataRole.UserRole)
        if not role_data:
            return
        item_type = role_data.get("type")
        data = role_data.get("data")
        if item_type == "nasdaq_analysis":
            self.display_nasdaq_analysis_details(data)
        elif item_type == "asset_range":
            self.display_range_details(data)

    def on_setup_visibility_changed(self, item):
        self.redraw_setups()

    def on_setup_selected(self, item):
        setup = item.data(Qt.ItemDataRole.UserRole)
        self.current_setup = setup
        self.display_setup_details(setup)

    def redraw_ranges(self):
        if not self.chart_ready:
            return
        self.main_chart_view.page().runJavaScript("if(window.hideRangeLines){window.hideRangeLines();}")
        for i in range(self.ranges_list.count()):
            item = self.ranges_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                role_data = item.data(Qt.ItemDataRole.UserRole)
                if role_data and role_data.get("type") == "asset_range":
                    r = role_data.get("data")
                    zone_info = [
                        {
                            "top": r["resistance_zone"][1],
                            "bottom": r["resistance_zone"][0],
                            "borderColor": r.get("color", "#f38ba8"),
                            "label": f"OPÓR: {r['name']}"
                        },
                        {
                            "top": r["support_zone"][1],
                            "bottom": r["support_zone"][0],
                            "borderColor": r.get("color", "#a6e3a1"),
                            "label": f"WSPARCIE: {r['name']}"
                        }
                    ]
                    json_str = json.dumps(zone_info)
                    js_command = f"""
                    if(window.showRangeLines){{
                        window.showRangeLines(`{json_str}`);
                    }}
                    """
                    self.main_chart_view.page().runJavaScript(js_command)

    def redraw_setups(self):
        if not self.chart_ready:
            return
        self.main_chart_view.page().runJavaScript("if(window.hideSetupLines){window.hideSetupLines();}")
        lines_to_draw = []
        for i in range(self.setups_list.count()):
            item = self.setups_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                setup = item.data(Qt.ItemDataRole.UserRole)
                if not setup:
                    continue
                bias = setup.get("bias", "LONG").upper()
                border_color = "rgba(255, 255, 10, 0.8)"
                label_prefix = "LONG" if bias == "LONG" else "SHORT"
                entry_prefix = "🟢 LONG" if bias == "LONG" else "🔴 SHORT"
                name = setup.get("name", "")
                execution = setup.get("execution", {})
                
                entry_zone = execution.get("entry_zone", [])
                if len(entry_zone) == 2:
                    t, b = float(entry_zone[1]), float(entry_zone[0])
                    lines_to_draw.append({
                        "top": t,
                        "bottom": b,
                        "isRange": t != b,
                        "borderColor": border_color,
                        "label": f"{entry_prefix} ({name}) Entry"
                    })
                
                sl_zone = execution.get("stop_loss_zone", [])
                if len(sl_zone) == 2:
                    t, b = float(sl_zone[1]), float(sl_zone[0])
                    lines_to_draw.append({
                        "top": t,
                        "bottom": b,
                        "isRange": t != b,
                        "borderColor": "rgba(239, 83, 80, 0.8)",
                        "label": f"{label_prefix} SL"
                    })
                
                tp_zones = execution.get("take_profit_zones", [])
                for idx, tp in enumerate(tp_zones):
                    if len(tp) == 2:
                        t, b = float(tp[1]), float(tp[0])
                        lines_to_draw.append({
                            "top": t,
                            "bottom": b,
                            "isRange": t != b,
                            "borderColor": "rgba(38, 166, 154, 0.8)",
                            "label": f"{label_prefix} TP{idx+1}"
                        })
        
        if lines_to_draw:
            json_str = json.dumps(lines_to_draw)
            js_command = f"""
            if(window.showSetupLines){{
                window.showSetupLines(`{json_str}`);
            }}
            """
            self.main_chart_view.page().runJavaScript(js_command)

    def display_nasdaq_analysis_details(self, a: dict):
        text = f"=== ANALIZA NASDAQ: {a['name']} ===\n"
        text += f"{a.get('description', 'Brak opisu.')}\n"
        self.details_box.setPlainText(text)
        self.details_box.moveCursor(QTextCursor.MoveOperation.Start)

    def display_range_details(self, r: dict):
        text = f"=== POZIOM: {r['name']} ===\n"
        text += f"Ram czasowy (Timeframe): {r.get('timeframe', 'N/A')}\n"
        text += f"Strefa Wsparcia (Support): {r['support_zone']}\n"
        text += f"Strefa Oporu (Resistance): {r['resistance_zone']}\n"
        text += f"Opis i Kontekst:\n{r.get('description', 'Brak opisu.')}\n"
        self.details_box.setPlainText(text)
        self.details_box.moveCursor(QTextCursor.MoveOperation.Start)

    def display_setup_details(self, s: dict):
        cond = s.get("conditions", {})
        short_name = AVAILABLE_TICKERS.get(self.current_ticker, "ASSET")
        text = f"=== STRATEGIA: {s['name']} ===\n"
        text += f"Kierunek (Bias): {s['bias']}\n"
        text += f"{short_name} Trigger Zone: {cond.get('trigger_zone')}\n"
        text += f"Trigger: {cond.get('trigger_condition')}\n"
        text += f"📊 Warunek konfirmacji NASDAQ:\n"
        text += f"  Cena: {cond.get('nasdaq_confirmation_price')}\n"
        text += f"  Warunek: {cond.get('nasdaq_confirmation_condition')}\n"
        text += f"Wytyczne wykonania pozycji:\n"
        text += f" - Entry: {s['execution']['entry_zone']}\n"
        text += f" - Stop Loss: {s['execution']['stop_loss_zone']}\n"
        text += f" - Targety TP: {s['execution']['take_profit_zones']}\n"
        text += f"Komentarz teoretyczny:\n{s.get('commentary','')}\n"
        self.details_box.setPlainText(text)
        self.details_box.moveCursor(QTextCursor.MoveOperation.Start)

    def on_chart_load_finished(self, ok):
        if ok:
            self.chart_ready = True
            short_name = AVAILABLE_TICKERS.get(self.current_ticker, "NVDA")
            QTimer.singleShot(1500, lambda: self.bitget_client.load_historical("ASSET", self.chart_interval, short_name))
            self.load_schema()

    def check_gpu_status(self):
        script = """
        (function() {
            var canvas = document.createElement('canvas');
            var gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
            if (!gl) return 'NO_WEBGL';
            var debugInfo = gl.getExtension('WEBGL_debug_renderer_info');
            return debugInfo ? gl.getParameter(debugInfo.UNMASKED_RENDERER_WEBGL) : 'WEBGL_NO_DEBUG';
        })();
        """
        if self.nasdaq_chart_view:
            self.nasdaq_chart_view.page().runJavaScript(script, lambda res: print(f"[GPU CHECK] Renderer: {res}"))

    def closeEvent(self, event):
        self.bitget_client.stop()
        self.file_watcher.stop()
        event.accept()

if __name__ == "__main__":
    os.environ["QTWEBENGINE_REMOTE_DEBUGGING"] = "9222"
    
    # POPRAWIONE FLAGI: 
    # 1. Dodano spacje na końcu każdego ciągu, aby uniknąć błędu konkatenacji ("--disable-vulkan--disable...")
    # 2. Zmieniono --use-gl-desktop na --use-gl=desktop
    # 3. Dodano --allow-file-access-from-files, aby lokalny plik HTML mógł ładować zewnętrzne iframe/WebSockety
    os.environ["LIBVA_DRIVER_NAME"] = "i965"
    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = (
        "--enable-gpu-rasterization "
        "--enable-zero-copy "
        "--disable-vulkan "
        "--disable-gpu-driver-bug-workarounds "
        "--allow-file-access-from-files "
        "--disable-web-security "                  # Wyłącza blokady CORS/Origin
        "--allow-running-insecure-content "         # Pozwala na ładowanie zewnętrznych skryptów
        "--no-sandbox "                             # Omija piaskownicę Chromium dla lokalnych plików
    )
    
    app = QApplication(sys.argv)
    QWebEngineProfile.defaultProfile().setHttpCacheType(QWebEngineProfile.HttpCacheType.DiskHttpCache)
    window = MainWindow()
    window.showMaximized()
    sys.exit(app.exec())