"""Flask dashboard with Google OAuth and REST API for the paper-trading bot."""

import json
import logging
import os
from datetime import datetime
from functools import wraps
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import yaml
from authlib.integrations.flask_client import OAuth
from flask import (
    Flask, abort, jsonify, redirect, render_template, request,
    session, url_for,
)

from paper_trader.db import PaperTradingDB

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")

db: Optional[PaperTradingDB] = None
scheduler_ref = None  # set by run_dashboard.py


def load_config(path: str = "config/paper_trading.yaml") -> Dict:
    with open(path) as f:
        return yaml.safe_load(f)


def create_app(config: Optional[Dict] = None) -> Flask:
    global db

    if config is None:
        config = load_config()

    app = Flask(
        __name__,
        template_folder=os.path.join(os.path.dirname(__file__), "templates"),
    )
    app.secret_key = config["dashboard"].get("secret_key", "dev-secret-change-me")
    app.config["PAPER_TRADING"] = config

    db_path = config.get("database", {}).get("path", "data/paper_trading.db")
    db = PaperTradingDB(db_path)

    auth_cfg = config.get("auth", {})
    client_id = auth_cfg.get("google_client_id") or os.environ.get("GOOGLE_CLIENT_ID", "")
    client_secret = auth_cfg.get("google_client_secret") or os.environ.get("GOOGLE_CLIENT_SECRET", "")
    auth_enabled = bool(client_id and client_secret)
    app.config["AUTH_ENABLED"] = auth_enabled
    app.config["ALLOWED_EMAILS"] = set(auth_cfg.get("allowed_emails", []))

    if auth_enabled:
        oauth = OAuth(app)
        oauth.register(
            name="google",
            client_id=client_id,
            client_secret=client_secret,
            server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
            client_kwargs={"scope": "openid email profile"},
        )
        app.config["OAUTH"] = oauth

    _register_routes(app)
    return app


def _login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        app = kwargs.get("app") or request.app if hasattr(request, "app") else None
        from flask import current_app
        if not current_app.config.get("AUTH_ENABLED"):
            return f(*args, **kwargs)
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


def _register_routes(app: Flask):

    # ── auth routes ───────────────────────────────────────────────────

    @app.route("/login")
    def login():
        if not app.config["AUTH_ENABLED"]:
            return redirect(url_for("dashboard_page"))
        return render_template("login.html")

    @app.route("/auth/login")
    def auth_login():
        if not app.config["AUTH_ENABLED"]:
            return redirect(url_for("dashboard_page"))
        oauth = app.config["OAUTH"]
        redirect_uri = url_for("auth_callback", _external=True)
        return oauth.google.authorize_redirect(redirect_uri)

    @app.route("/auth/callback")
    def auth_callback():
        if not app.config["AUTH_ENABLED"]:
            return redirect(url_for("dashboard_page"))
        oauth = app.config["OAUTH"]
        token = oauth.google.authorize_access_token()
        userinfo = token.get("userinfo", {})
        email = userinfo.get("email", "")
        allowed = app.config.get("ALLOWED_EMAILS", set())

        if allowed and email not in allowed:
            logger.warning(f"Unauthorized login attempt: {email}")
            return render_template("login.html", error="Access denied. Your account is not authorized."), 403

        session["user"] = {
            "email": email,
            "name": userinfo.get("name", email),
            "picture": userinfo.get("picture", ""),
        }
        return redirect(url_for("dashboard_page"))

    @app.route("/logout")
    def logout():
        session.pop("user", None)
        return redirect(url_for("login"))

    # ── dashboard page ────────────────────────────────────────────────

    @app.route("/")
    @_login_required
    def dashboard_page():
        user = session.get("user", {"name": "Developer", "email": ""})
        auth_enabled = app.config.get("AUTH_ENABLED", False)
        return render_template("dashboard.html", user=user, auth_enabled=auth_enabled)

    # ── API routes ────────────────────────────────────────────────────

    @app.route("/api/bots")
    @_login_required
    def api_bots():
        date = request.args.get("date")
        if date:
            bots = db.get_bots_for_date(date)
        else:
            today = datetime.now(ET).strftime("%Y-%m-%d")
            bots = db.get_bots_for_date(today)
            active = db.get_active_bots()
            active_ids = {b["id"] for b in active}
            for b in bots:
                if b["id"] not in active_ids:
                    continue
            bot_map = {b["id"]: b for b in bots}
            for a in active:
                if a["id"] not in bot_map:
                    bots.append(a)

        for b in bots:
            trade = db.get_trade_by_bot(b["id"])
            b["trade"] = trade
        return jsonify(bots)

    @app.route("/api/trades")
    @_login_required
    def api_trades():
        limit = request.args.get("limit", 500, type=int)
        trades = db.get_all_trades(limit)
        return jsonify(trades)

    @app.route("/api/daily-pnl")
    @_login_required
    def api_daily_pnl():
        totals = db.get_daily_pnl()
        by_symbol = db.get_daily_pnl_by_symbol()
        return jsonify({"totals": totals, "by_symbol": by_symbol})

    @app.route("/api/summary")
    @_login_required
    def api_summary():
        today = datetime.now(ET).strftime("%Y-%m-%d")
        overall = db.get_summary()
        today_summary = db.get_summary(today)
        open_trades = db.get_open_trades()
        return jsonify({
            "overall": overall,
            "today": today_summary,
            "open_positions": open_trades,
        })

    @app.route("/api/bots/<int:bot_id>/cancel", methods=["POST"])
    @_login_required
    def api_cancel_bot(bot_id):
        bot = db.get_bot(bot_id)
        if not bot:
            abort(404)
        if bot["status"] in ("completed", "no_signal", "cancelled"):
            return jsonify({"error": "Bot already finished"}), 400

        db.request_cancel(bot_id)

        trade = db.get_trade_by_bot(bot_id)
        if trade and trade["status"] == "open":
            from paper_trader.bot import TradingBot
            db.close_trade(
                trade["id"],
                spot_exit=trade["spot_entry"],
                exit_premium=trade["entry_premium"],
                exit_reason="cancelled",
            )

        db.update_bot(bot_id, status="cancelled", stage="cancelled")
        logger.info(f"Bot {bot_id} ({bot['symbol']}) cancelled via dashboard")
        return jsonify({"ok": True})

    @app.route("/api/scheduler/status")
    @_login_required
    def api_scheduler_status():
        if not scheduler_ref:
            return jsonify({"running": False, "ready": False, "paused": True})
        return jsonify({
            "running": scheduler_ref.running,
            "ready": scheduler_ref.ready,
            "paused": scheduler_ref.paused,
        })

    @app.route("/api/scheduler/start", methods=["POST"])
    @_login_required
    def api_scheduler_start():
        if not scheduler_ref:
            return jsonify({"error": "Scheduler not configured. Set POLYGON_API_KEY and restart."}), 400
        if not scheduler_ref.ready:
            return jsonify({"error": "Scheduler thread not running. Restart the application."}), 500
        scheduler_ref.resume()
        logger.info("Scheduler started via dashboard")
        return jsonify({"ok": True, "running": True})

    @app.route("/api/scheduler/stop", methods=["POST"])
    @_login_required
    def api_scheduler_stop():
        if not scheduler_ref:
            return jsonify({"error": "Scheduler not configured"}), 400
        scheduler_ref.pause()
        logger.info("Scheduler stopped via dashboard")
        return jsonify({"ok": True, "running": False})
