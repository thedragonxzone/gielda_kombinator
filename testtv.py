#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from urllib.parse import urlencode

from PyQt6.QtCore import QUrl
from PyQt6.QtWidgets import QApplication, QMainWindow
from PyQt6.QtWebEngineCore import QWebEngineProfile, QWebEngineSettings
from PyQt6.QtWebEngineWidgets import QWebEngineView


def generate_tradingview_html() -> str:
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
        "disabled_features": '["show_right_widgets_panel_by_default","right_toolbar","widget_logo","hide_top_toolbar","hide_side_toolbar","volume_force_overlay","create_volume_indicator_by_default","display_studies_logo_in_legend","use_localstorage"]',
    }
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


class TradingViewWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("TradingView - NASDAQ-100")
        self.resize(1200, 800)

        self.base_dir = Path(__file__).resolve().parent

        # Tworzenie widoku WebEngine dla TradingView
        self.web_view = QWebEngineView()

        # Optymalizacja i odchudzenie ustawień
        settings = self.web_view.settings()
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.ScrollAnimatorEnabled, False
        )
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.LocalStorageEnabled, False
        )
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.JavascriptCanAccessClipboard, False
        )
        settings.setAttribute(
            QWebEngineSettings.WebAttribute.PluginsEnabled, False
        )

        self.setCentralWidget(self.web_view)

        # Generowanie pliku tymczasowego i ładowanie
        html_content = generate_tradingview_html()
        html_path = self.base_dir / "nasdaq_tv.html"

        try:
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            self.web_view.setUrl(QUrl.fromLocalFile(str(html_path)))
        except Exception as e:
            print(f"[ERROR] Błąd zapisu pliku HTML: {e}")
            self.web_view.setHtml(
                html_content, QUrl("https://s.tradingview.com")
            )


if __name__ == "__main__":
    os.environ["QTWEBENGINE_REMOTE_DEBUGGING"] = "6222"

    # Flagi pod Linux / Chromium eliminujące problemy ze sterownikami GPU
    if "LIBVA_DRIVER_NAME" in os.environ:
        del os.environ["LIBVA_DRIVER_NAME"]

    os.environ["QTWEBENGINE_CHROMIUM_FLAGS"] = (
        "--enable-gpu-rasterization "
        "--disable-accelerated-video-decode "
        "--allow-file-access-from-files "
        "--disable-web-security "
        "--allow-running-insecure-content "
        "--no-sandbox "
    )

    app = QApplication(sys.argv)
    QWebEngineProfile.defaultProfile().setHttpCacheType(
        QWebEngineProfile.HttpCacheType.DiskHttpCache
    )

    window = TradingViewWindow()
    window.show()

    sys.exit(app.exec())