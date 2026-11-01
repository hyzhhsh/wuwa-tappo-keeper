#!/usr/bin/env python3
"""WuwaTappoKeeper：无人值守的 Wuwa Tappo 自动上报与开箱工具。

登录后持续上报点击，按服务器时间自动开宝箱，并在每次开箱成功后发送飞书卡片。
登录支持两种方式：游戏 UID + 游戏内站内信验证码，或库洛通行证手机号 + 短信验证码。
凭据和飞书 webhook URL 保存在脚本同目录的 credentials.json 中。
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import secrets
import signal
import string
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, urlencode

try:
    import requests
    import websocket
except ImportError as exc:  # pragma: no cover
    raise SystemExit("缺少依赖，请运行：pip install -r requirements.txt") from exc


# ============================== 用户可调常量 ==============================

# Wuwa Tappo 后端 API 根地址，HTTP 请求与 socket.io 连接共用；
# 仅在服务端更换域名时需要修改。
API_HOST = "https://api-mc-wuwatappo.kurogames-global.com"
# 登录凭据与飞书 webhook 的本地保存路径，默认是脚本同目录的 credentials.json。
CREDENTIALS_FILE = Path(__file__).resolve().with_name("credentials.json")

# 官方 Wuwa Tappo 客户端内嵌的库洛 Web SDK 配置。
# client_id / client_secret 是客户端公开值，不是用户凭据。
# SDK 接口根地址，用于手机号发码、登录和换取 access_token。
KURO_SDK_HOST = "https://sdkapi.kurogame.com/sdkcom"
# SDK 公开客户端 ID，登录和换 token 时作为 client_id 提交。
KURO_SDK_CLIENT_ID = "vvkewnskrxxwfo0yi61cy24l"
# SDK 公开客户端密钥，仅换取 access_token 时使用。
KURO_SDK_CLIENT_SECRET = "g9ej0i1jf3y68wchb0ncm266"
# 渠道 ID，随每个 SDK 请求提交。
KURO_SDK_CHANNEL_ID = "201"
# 以下参数抓取自 2026-11-01 的官方客户端 1.0.3（SDK 1.2.3w）。
# 项目 ID，随每个 SDK 请求提交。
KURO_SDK_PROJECT_ID = "G152"
# 产品 ID，随每个 SDK 请求提交。
KURO_SDK_PRODUCT_ID = "A1476"
# 平台标识，随每个 SDK 请求提交。
KURO_SDK_PLATFORM = "h5"
# SDK 版本号，同时作为 version 与 sdkVersion 参数提交。
KURO_SDK_VERSION = "1.2.3w"
# 请求头 Kr-Ver 的版本号，需与官方客户端保持一致。
KURO_SDK_KR_VER = "1.9.0"
# 请求头 Origin，模拟官方 Tauri 客户端。
KURO_SDK_ORIGIN = "http://tauri.localhost"
# 请求头 Referer，模拟官方 Tauri 客户端。
KURO_SDK_REFERER = "http://tauri.localhost/"
# 请求头 Accept-Language。
KURO_SDK_ACCEPT_LANGUAGE = "zh-Hans,zh-CN,en,en-GB,en-US"
# 请求头 User-Agent，模拟官方客户端内嵌浏览器。
KURO_SDK_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0"
)

# 每次上报随机增加的点击数下限，与 MAX 组成闭区间；须 ≥1 且 ≤ MAX。
REPORT_CLICK_MIN = 20
# 每次上报随机增加的点击数上限，必须 ≥ MIN。
REPORT_CLICK_MAX = 60
# 两次上报的基准间隔（秒），实际间隔还会额外随机加 0–2 秒。
REPORT_INTERVAL_SEC = 30

# 刷新 /me 并检查宝箱是否就绪的间隔（秒）。
CHEST_CHECK_INTERVAL_SEC = 30
# 待上报点击数的缓冲上限，超出部分丢弃，避免长时间断线后单次上报过多。
MAX_PENDING_CLICKS = 200
# 连续开启左右两个宝箱之间的等待时间（秒）。
OPEN_DELAY_SEC = 3
# 开启一个宝箱所需的点击数。
CHEST_CLICK_COST = 1000

# 飞书自定义机器人 webhook 在 --login 时收集并写入 credentials.json；
# 留空表示不发送通知。
# 飞书开箱通知卡片的标题。
WEBHOOK_TITLE = "Wuwa Tappo · 开箱成功"
# 发送飞书 webhook 请求的超时时间（秒）。
FEISHU_TIMEOUT_SEC = 10
# 奖励类型到 CDN 目录的映射；接口只返回文件名，不返回完整 URL。
REWARD_IMAGE_DIRS = {
    "character_skin": "character",  # 角色皮肤
    "emote": "emote",  # 表情
    "accessory": "accessory",  # 饰品
}


# ============================== 协议与固定配置 ==============================

# Socket.IO 握手路径，拼接在 API_HOST 之后。
SOCKET_PATH = "/socket.io/"
# 日志输出格式（时间、级别、消息）。
LOG_FORMAT = "%(asctime)s %(levelname)-5s %(message)s"
# 日志时间格式。
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 普通 HTTP 请求（含 SDK 登录）的超时时间（秒）。
REQUEST_TIMEOUT_SEC = 15
# WebSocket 建连及 Engine.IO/Socket.IO 握手的超时时间（秒）。
SOCKET_CONNECT_TIMEOUT_SEC = 15
# 发送事件后等待服务端 ack 的超时时间（秒）。
SOCKET_ACK_TIMEOUT_SEC = 8
# Engine.IO 心跳间隔（秒）。服务端不会主动发送 ping，客户端必须定期发送 "2"，
# 否则连接会被服务端判定为超时并断开。
SOCKET_PING_INTERVAL_SEC = 20
# socket 断线重连指数退避的最大间隔（秒）。
RECONNECT_MAX_SEC = 60

# 服务器时间同步（同时刷新资源 CDN 地址）的间隔（秒）。
SERVER_TIME_SYNC_SEC = 300
# 可采纳的服务器时间偏移上限（毫秒，即 24 小时），超出则忽略本次结果。
MAX_SERVER_TIME_SKEW_MS = 24 * 3600 * 1000
# 检查 token 是否需要续期的间隔（秒）。
RENEW_CHECK_SEC = 600
# token 续期失败后的重试间隔（秒）。
RENEW_RETRY_SEC = 120
# 续期成功后假定的 token 有效期（秒，即 7 天）。
TOKEN_TTL_SEC = 7 * 24 * 3600
# 剩余有效期低于该阈值（秒，即 48 小时）时触发续期。
RENEW_THRESHOLD_SEC = 48 * 3600
# 使用 --once/--cycles 时，连续跳过上报达到该次数即停止运行。
MAX_SKIPPED_REPORTS = 5

# 点击上报被限流的业务码，保留缓冲稍后重试。
RATE_LIMIT_CODE = 80013
# 服务端判定客户端点击数异常的业务码，清空缓冲并退避 5 分钟。
CLIENT_CLICK_ANOMALY_CODE = 70003
# 登录验证码相关业务码（发码过于频繁或验证码错误）。
LOGIN_CODE_ERROR_CODE = 10031
# SDK 要求极验验证的业务码。
KURO_SDK_GEETEST_CODE = 41000
# 登录态失效、需要重新登录的业务码集合。
AUTH_ERROR_CODES = {10003, 90002}
# 宝箱位置标识，顺序即开箱优先级。
CHEST_PLACES = ("left", "right")


# ============================== 日志初始化 ==============================

# Windows 控制台默认编码可能无法输出中文，统一改成 UTF-8。
for console_stream in (sys.stdout, sys.stderr):
    try:
        console_stream.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

log = logging.getLogger("wuwa")


def setup_logging(debug: bool = False) -> None:
    """初始化控制台日志；不写文件，由外部决定是否重定向。"""
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    root = logging.getLogger()
    if debug:
        root.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)


# ============================== 通用工具 ==============================

def to_int(value: Any, default: int = 0) -> int:
    """把任意值转成整数，转换失败时返回默认值。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def mask_token(token: Optional[str]) -> str:
    """日志中展示 token 时只保留头尾，避免泄露完整凭据。"""
    value = str(token or "")
    if len(value) <= 10:
        return "***"
    return f"{value[:6]}...{value[-4:]}"


def mask_phone(phone: str) -> str:
    """日志中展示手机号时隐藏中间四位。"""
    value = str(phone or "")
    if len(value) < 7:
        return "***"
    return f"{value[:3]}****{value[-4:]}"


_DEVICE_ID_ALPHABET = string.digits + string.ascii_uppercase + string.ascii_lowercase


def generate_device_id() -> str:
    """生成库洛 SDK 使用的 32 位随机设备号。"""
    characters: list[str] = []
    for _ in range(32):
        characters.append(secrets.choice(_DEVICE_ID_ALPHABET))
    return "".join(characters)


def format_remaining(cred: dict) -> str:
    """把 token 剩余有效期格式化成便于阅读的中文文本。"""
    remaining = remaining_sec(cred)
    if remaining is None:
        return "未知"
    if remaining <= 0:
        return "已过期"
    hours = int(remaining // 3600)
    return f"{hours} 小时"


def normalize_expire_ms(value: Any) -> Optional[int]:
    """把秒级/毫秒级时间戳或 ISO 时间字符串统一转成毫秒时间戳。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    # 接口在不同部署下可能返回秒级或毫秒级时间戳。
    try:
        number = float(text)
    except ValueError:
        number = None
    if number is not None and number > 0:
        if number < 1e12:
            return int(number * 1000)
        return int(number)

    # 也兼容 ISO 8601 字符串。
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except ValueError:
        return None


def remaining_sec(cred: dict) -> Optional[float]:
    """返回 token 剩余秒数；凭据里没有有效过期时间时返回 None。"""
    expire_ms = normalize_expire_ms(cred.get("expire_time"))
    if expire_ms is None:
        return None
    return expire_ms / 1000 - time.time()


# ============================== 凭据读写 ==============================

def save_credentials(cred: dict) -> bool:
    """原子写入凭据：先写同目录临时文件并 fsync，再用 os.replace 覆盖。

    凭据包含 token，不能直接截断重写，否则进程中途退出会留下损坏文件。
    """
    payload = json.dumps(cred, ensure_ascii=False, indent=2) + "\n"
    tmp_path = CREDENTIALS_FILE.with_name(CREDENTIALS_FILE.name + ".tmp")
    try:
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, CREDENTIALS_FILE)
    except OSError as exc:
        log.warning("无法保存 %s：%s", CREDENTIALS_FILE.name, exc)
        try:
            tmp_path.unlink()
        except OSError:
            pass
        return False

    try:
        os.chmod(CREDENTIALS_FILE, 0o600)
    except OSError:
        pass
    return True


def load_credentials() -> Optional[dict]:
    """读取 credentials.json；文件不存在、无法解析或缺少 token 时返回 None。"""
    if not CREDENTIALS_FILE.exists():
        return None

    try:
        cred = json.loads(CREDENTIALS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("无法读取 %s：%s", CREDENTIALS_FILE.name, exc)
        return None

    if not isinstance(cred, dict) or not cred.get("token"):
        return None
    return cred


# ============================== 异常与 HTTP 客户端 ==============================

class ApiError(Exception):
    """业务接口错误；保留响应码、request_id 和 HTTP 状态供上层判断。"""

    def __init__(self, code: Any, message: str, request_id: Any = None, status: Any = None):
        if message:
            super().__init__(message)
        else:
            super().__init__(f"接口错误 {code}")
        self.code = to_int(code, -1)
        self.message = message or ""
        self.request_id = request_id
        self.status = status


class NetworkError(Exception):
    """网络层错误；与业务错误分开，方便决定是否直接结束进程。"""


class ApiClient:
    """ Wuwa Tappo 业务接口客户端，统一处理鉴权和错误码。"""

    def __init__(self, host: str):
        self.host = host.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "wuwa-tappo-tool/1.0",
        })
        self.token: Optional[str] = None

    def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        token: Optional[str] = None,
        timeout: int = REQUEST_TIMEOUT_SEC,
        ok_codes: tuple[int, ...] = (0,),
    ) -> Any:
        """发送请求并返回 data 或 result 字段。

        网络异常统一转成 NetworkError；HTTP >= 400、非 JSON 响应、
        业务码不在 ok_codes 中都转成 ApiError。HTTP 状态先于业务码判断。
        """
        if path.startswith("http"):
            url = path
        else:
            url = f"{self.host}/{path.lstrip('/')}"

        headers: dict[str, str] = {}
        if token is None:
            effective_token = self.token
        else:
            effective_token = token
        if effective_token:
            headers["Authorization"] = f"Bearer {effective_token}"

        try:
            response = self.session.request(
                method,
                url,
                json=body,
                headers=headers,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise NetworkError(f"{method} {url} 请求失败：{exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiError(
                -1,
                f"接口返回了非 JSON 内容（HTTP {response.status_code}）：{response.text[:200]}",
                status=response.status_code,
            ) from exc

        if response.status_code >= 400:
            message = payload.get("message")
            if not message:
                message = f"HTTP {response.status_code}"
            raise ApiError(
                payload.get("code", -1),
                message,
                payload.get("request_id"),
                response.status_code,
            )

        code = to_int(payload.get("code"), -1)
        if code not in ok_codes:
            raise ApiError(
                code,
                payload.get("message") or "",
                payload.get("request_id"),
                response.status_code,
            )

        if "data" in payload:
            return payload["data"]
        if "result" in payload:
            return payload["result"]
        return None


# ============================== 库洛 SDK 与登录 ==============================

class KuroSdkError(Exception):
    """库洛 Web SDK 返回的错误。"""

    def __init__(
        self,
        code: Any,
        message: str,
        *,
        status: Any = None,
        request_id: Any = None,
    ):
        if message:
            super().__init__(message)
        else:
            super().__init__(f"库洛 SDK 错误 {code}")
        self.code = to_int(code, -1)
        self.message = message or ""
        self.status = status
        self.request_id = request_id

    def describe(self) -> str:
        """拼接带 request_id 的错误描述，便于反馈服务端问题。"""
        text = f"{self.code}: {self.message}"
        if self.request_id:
            text += f" [request_id={self.request_id}]"
        return text


class GeetestRequired(KuroSdkError):
    """服务端要求极验滑块验证；脚本无法自动完成。"""


def _sanitize_sdk_body(body: dict) -> dict:
    """复制 SDK 请求体，并对手机号、验证码和 client_secret 脱敏。"""
    safe_body = {}
    for key, value in body.items():
        if key == "phone":
            safe_body[key] = mask_phone(str(value))
        elif key in {"code", "client_secret"}:
            safe_body[key] = "***"
        else:
            safe_body[key] = value
    return safe_body


class KuroSdkClient:
    """库洛 Web SDK 登录客户端，负责发码、短信登录和换取 access_token。"""

    def __init__(self, device_id: str):
        self.device_id = device_id
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Kr-Ver": KURO_SDK_KR_VER,
            "Accept-Language": KURO_SDK_ACCEPT_LANGUAGE,
            "Origin": KURO_SDK_ORIGIN,
            "Referer": KURO_SDK_REFERER,
            "User-Agent": KURO_SDK_USER_AGENT,
        })

    def _post(self, path: str, data: dict) -> dict:
        body = {
            "redirect_uri": "1",
            "__e__": "1",
            "pack_mark": "1",
            "projectId": KURO_SDK_PROJECT_ID,
            "productId": KURO_SDK_PRODUCT_ID,
            "platform": KURO_SDK_PLATFORM,
            "channelId": KURO_SDK_CHANNEL_ID,
            "deviceNum": self.device_id,
            "version": KURO_SDK_VERSION,
            "sdkVersion": KURO_SDK_VERSION,
            **data,
        }
        url = f"{KURO_SDK_HOST}{path}"
        log.debug("SDK 请求 POST %s，请求体=%s", path, _sanitize_sdk_body(body))

        try:
            response = self.session.post(url, data=body, timeout=REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            raise NetworkError(f"POST {url} 请求失败：{exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise KuroSdkError(
                -1,
                f"接口返回了非 JSON 内容（HTTP {response.status_code}）：{response.text[:200]}",
                status=response.status_code,
            ) from exc

        code = to_int(payload.get("code"), -1)
        message = str(payload.get("msg") or payload.get("message") or "")
        request_id = payload.get("request_id") or payload.get("trace_id")
        log.debug(
            "SDK 响应 %s：HTTP=%s code=%s msg=%s request_id=%s",
            path,
            response.status_code,
            code,
            message,
            request_id,
        )

        if code == KURO_SDK_GEETEST_CODE:
            if message:
                geetest_message = message
            else:
                geetest_message = "需要极验验证"
            raise GeetestRequired(
                code,
                geetest_message,
                status=response.status_code,
                request_id=request_id,
            )

        if code != 0:
            if message:
                error_message = message
            else:
                error_message = f"HTTP {response.status_code}"
            raise KuroSdkError(
                code,
                error_message,
                status=response.status_code,
                request_id=request_id,
            )

        result = payload.get("data")
        if isinstance(result, dict):
            return result
        return {}

    def send_code(self, phone: str) -> dict:
        return self._post("/v2/login/getPhoneCode.lg", {"phone": phone})

    def login(self, phone: str, code: str) -> dict:
        return self._post(
            "/v2/login/phoneCode.lg",
            {
                "phone": phone,
                "code": code,
                "response_type": "code",
                "client_id": KURO_SDK_CLIENT_ID,
            },
        )

    def exchange_token(self, auth_code: str) -> str:
        data = self._post(
            "/v2/auth/getToken.lg",
            {
                "code": auth_code,
                "client_id": KURO_SDK_CLIENT_ID,
                "client_secret": KURO_SDK_CLIENT_SECRET,
                "grant_type": "authorization_code",
            },
        )
        return str(data.get("access_token") or "").strip()


def prompt_uid(preferred: Optional[str] = None) -> str:
    """读取并校验游戏 UID；命令行已提供时直接使用。"""
    uid = str(preferred or "").strip()
    if not uid:
        uid = input("游戏 UID（9-10 位数字）：").strip()
    if not re.fullmatch(r"\d{9,10}", uid):
        raise SystemExit(f"UID 无效：{uid!r}（应为 9-10 位数字）")
    return uid


def uid_login_flow(
    api: ApiClient,
    preferred_uid: Optional[str] = None,
    *,
    ask_webhook: bool = False,
    existing_feishu_webhook: str = "",
    existing_device_id: str = "",
) -> tuple[dict, dict]:
    """UID + 游戏内站内信验证码登录。

    发码失败不会立刻退出：限流、接口拒绝或网络错误时，仍允许用户从
    官方客户端/游戏内站内信取得验证码后继续登录。
    """
    if not sys.stdin.isatty():
        raise SystemExit(
            "登录需要交互式终端。请先在终端运行一次 `python wuwa_tappo.py --login`。"
        )

    uid = prompt_uid(preferred_uid)
    log.info("正在为 UID %s 申请登录验证码（发送到游戏内站内信）......", uid)

    code_request_sent = False
    try:
        api.request("POST", "api/auth/uid-send-code", body={"playerId": int(uid)}, token="")
        code_request_sent = True
    except ApiError as exc:
        log.warning("申请登录验证码失败（%s）：%s", exc.code, exc.message)
        if exc.code == LOGIN_CODE_ERROR_CODE:
            print(
                "登录验证码申请过于频繁（10031），通常约 15 分钟后恢复。\n"
                "可以打开官方 Wuwa Tappo 客户端或游戏内站内信，在其中申请 6 位验证码，\n"
                "然后回到这里输入。限流前申请的验证码可能已经失效。"
            )
        elif exc.code == 10007 or exc.status == 403:
            print(
                "服务端拒绝了 UID 发码请求（10007/403 Access forbidden）。\n"
                "你仍然可以在这里输入游戏内站内信收到的 6 位验证码；\n"
                "如果仍然登录失败，请改用 `--login --login-method phone`。"
            )
        else:
            print(
                f"无法自动申请登录验证码（{exc.code}: {exc.message}）。\n"
                "你可以从游戏内站内信获取 6 位验证码，然后在这里输入。"
            )
    except NetworkError as exc:
        log.warning("申请登录验证码时发生网络错误：%s", exc)
        print(
            "无法连接鸣潮登录服务。\n"
            "你可以从游戏内站内信获取 6 位验证码，然后在这里输入。"
        )

    if code_request_sent:
        log.info("登录验证码已发送到 UID %s 的游戏内站内信。", uid)

    code = input("请输入游戏内站内信中的 6 位登录验证码（直接回车放弃）：").strip()
    if not code:
        raise SystemExit("未输入登录验证码")
    if not re.fullmatch(r"\d{6}", code):
        raise SystemExit("登录验证码格式错误（应为 6 位数字）")

    try:
        data = api.request(
            "POST",
            "api/auth/uid-login",
            body={"playerId": int(uid), "code": int(code)},
            token="",
            ok_codes=(0, 200),
        )
    except ApiError as exc:
        if exc.code == LOGIN_CODE_ERROR_CODE:
            raise SystemExit("登录失败（10031）：UID 或验证码错误。") from exc
        if exc.code == 10007 or exc.status == 403:
            raise SystemExit(
                "服务端拒绝了 UID 登录（10007/403 Access forbidden）。"
                "请改用 `--login --login-method phone`。"
            ) from exc
        raise

    if not data:
        data = {}
    token = str(data.get("token") or "").strip()
    if not token:
        raise SystemExit("登录响应中没有 token")

    cred = {
        "token": token,
        "expire_time": normalize_expire_ms(data.get("expire_time")),
        "uid": uid,
        "cuid": data.get("cuid"),
        "nickname": data.get("nickname"),
        "saved_at": int(time.time() * 1000),
        "feishu_webhook": existing_feishu_webhook,
        "login_method": "uid",
    }
    if existing_device_id:
        cred["kuro_device_id"] = existing_device_id

    api.token = token or None
    me = api.request("GET", "api/frontend/user/me")
    if not me:
        me = {}
    cred["cuid"] = me.get("cuid") or cred["cuid"]
    cred["nickname"] = me.get("nickname") or cred["nickname"]

    if ask_webhook:
        cred["feishu_webhook"] = input(
            "飞书机器人 webhook URL（直接回车表示关闭通知）："
        ).strip()

    save_credentials(cred)
    log.info(
        "登录成功：%s（cuid=%s）；凭据已保存到 %s",
        cred["nickname"],
        cred["cuid"],
        CREDENTIALS_FILE.name,
    )
    if cred["feishu_webhook"]:
        log.info("飞书通知：已启用")
    else:
        log.info("飞书通知：已关闭（未配置 webhook）")
    return cred, me


def prompt_login_method(default: str = "uid") -> str:
    """交互式选择登录方式。"""
    if default not in {"uid", "phone"}:
        default = "uid"

    print("登录方式：")
    print("  1) UID + 游戏内站内信验证码")
    print("  2) 库洛通行证手机号 + 短信验证码")
    if default == "uid":
        default_choice = "1"
    else:
        default_choice = "2"

    choice = input(f"请选择 [1/2]（默认 {default_choice}）：").strip().lower()
    if not choice:
        return default
    if choice in {"1", "uid"}:
        return "uid"
    if choice in {"2", "phone", "sms"}:
        return "phone"
    raise SystemExit(f"未知的登录方式：{choice!r}")


def phone_login_flow(
    api: ApiClient,
    *,
    ask_webhook: bool = False,
    existing_feishu_webhook: str = "",
    existing_device_id: str = "",
) -> tuple[dict, dict]:
    """库洛通行证手机号 + 短信验证码登录。

    手机号只存在于内存中，不会写入 credentials.json；仅复用随机设备号。
    """
    if not sys.stdin.isatty():
        raise SystemExit(
            "登录需要交互式终端。请先在终端运行一次 `python wuwa_tappo.py --login`。"
        )

    phone = input("库洛通行证手机号（中国大陆 11 位，可带 +86）：").strip()
    phone = phone.replace(" ", "").replace("-", "")
    if phone.startswith("+86"):
        phone = phone[3:]
    elif phone.startswith("86") and len(phone) == 13:
        phone = phone[2:]
    if not re.fullmatch(r"1[3-9]\d{9}", phone):
        raise SystemExit(f"手机号无效：{phone!r}（应为 11 位数字）")

    device_id = generate_device_id()
    if existing_device_id and re.fullmatch(r"[0-9A-Za-z]{32}", existing_device_id):
        device_id = existing_device_id
    sdk = KuroSdkClient(device_id)

    log.info("正在为 %s 申请短信登录验证码......", mask_phone(phone))
    sms_sent = False
    try:
        sdk.send_code(phone)
        sms_sent = True
    except GeetestRequired:
        log.warning("发送短信验证码需要滑块验证。")
        print(
            "库洛要求先完成滑块验证才能发送短信验证码。\n"
            "请打开官方 Wuwa Tappo 客户端，用同一手机号申请验证码，\n"
            "然后回到这里输入收到的验证码。"
        )
    except KuroSdkError as exc:
        log.warning("发送短信验证码失败（%s）", exc.describe())
        print(
            f"无法自动发送短信验证码（{exc.describe()}）。\n"
            "你可以在官方客户端申请验证码，然后回到这里输入。"
        )
        if exc.code == 10:
            print(
                "code 10 是服务端的通用拒绝（请求参数或风控）。\n"
                "反馈问题时请保留上面的 request_id。"
            )
    except NetworkError as exc:
        log.warning("发送短信验证码时发生网络错误：%s", exc)
        print(
            "无法连接库洛登录服务。\n"
            "你可以在官方客户端申请验证码，然后回到这里输入。"
        )

    if sms_sent:
        log.info("短信验证码已发送到 %s。", mask_phone(phone))

    code = input("请输入短信中的 4 位验证码（直接回车放弃）：").strip()
    if not code:
        raise SystemExit("未输入短信验证码")
    if not re.fullmatch(r"\d{4,6}", code):
        raise SystemExit("短信验证码格式错误（应为 4-6 位数字）")

    try:
        sdk_data = sdk.login(phone, code)
    except GeetestRequired as exc:
        raise SystemExit(
            "提交短信验证码需要滑块验证，本脚本无法自动完成。\n"
            "请改用 UID 登录（`--login --login-method uid`），或在官方客户端完成登录。"
        ) from exc
    except KuroSdkError as exc:
        raise SystemExit(f"短信登录失败（{exc.describe()}）") from exc
    except NetworkError as exc:
        raise SystemExit(f"短信登录网络错误：{exc}") from exc

    auth_code = str(sdk_data.get("code") or "").strip()
    cuid = str(sdk_data.get("cuid") or "").strip()
    sdk_openid = str(sdk_data.get("id") or "").strip()
    username = str(sdk_data.get("username") or "").strip()
    if not auth_code or not cuid or not sdk_openid:
        raise SystemExit("库洛 SDK 登录响应缺少 code/cuid/id")

    try:
        access_token = sdk.exchange_token(auth_code)
    except GeetestRequired as exc:
        raise SystemExit(
            "换取 token 时库洛要求滑块验证，本脚本无法自动完成。\n"
            "请改用 UID 登录，或在官方客户端完成登录。"
        ) from exc
    except KuroSdkError as exc:
        raise SystemExit(f"库洛 token 换取失败（{exc.describe()}）") from exc
    except NetworkError as exc:
        raise SystemExit(f"库洛 token 换取网络错误：{exc}") from exc
    if not access_token:
        raise SystemExit("库洛 SDK 没有返回 access_token")

    log.info("短信登录已通过，正在查询鸣潮角色......")
    try:
        query = api.request(
            "POST",
            "api/auth/player/query",
            body={"cuid": cuid, "is_domestic": 1},
            token="",
        )
    except ApiError as exc:
        raise SystemExit(f"角色查询失败（{exc.code}）：{exc.message}") from exc

    if not query:
        query = {}
    if not isinstance(query, dict):
        raise SystemExit("角色查询返回了异常响应")

    items = query.get("items") or []
    if not isinstance(items, list) or not items:
        raise SystemExit("该库洛账号下没有找到鸣潮角色")
    if isinstance(items[0], dict):
        role = items[0]
    else:
        role = {}

    player_id = to_int(role.get("playerId"))
    server_id = str(role.get("serverId") or "0")
    if player_id <= 0:
        raise SystemExit("角色查询返回了无效的 playerId")
    channel_id = to_int(query.get("channelId"))

    player_name = role.get("playerName") or "?"
    if len(items) > 1:
        log.info(
            "账号下有 %s 个角色，使用第一个：%s（UID %s，服务器 %s）",
            len(items),
            player_name,
            player_id,
            server_id,
        )
    else:
        log.info(
            "使用角色 %s（UID %s，服务器 %s）",
            player_name,
            player_id,
            server_id,
        )

    try:
        data = api.request(
            "POST",
            "api/auth/login",
            body={
                "cuid": cuid,
                "puid": sdk_openid,
                "channelId": channel_id,
                "userid": sdk_openid,
                "cname": username,
                "accessToken": access_token,
                "source": "sdk",
                "playerId": player_id,
                "serverId": server_id,
                "is_domestic": 1,
            },
            token="",
            ok_codes=(0, 200),
        )
    except ApiError as exc:
        raise SystemExit(f"Tappo 登录失败（{exc.code}）：{exc.message}") from exc

    if not data:
        data = {}
    if not isinstance(data, dict):
        raise SystemExit("Tappo 登录返回了异常响应")

    token = str(data.get("token") or "").strip()
    if not token:
        raise SystemExit("Tappo 登录响应中没有 token")

    cred = {
        "token": token,
        "expire_time": normalize_expire_ms(data.get("expire_time")),
        "uid": str(player_id),
        "cuid": cuid,
        "nickname": role.get("playerName") or username,
        "saved_at": int(time.time() * 1000),
        "feishu_webhook": existing_feishu_webhook,
        "login_method": "phone",
        "kuro_device_id": device_id,
    }

    api.token = token or None
    me = api.request("GET", "api/frontend/user/me")
    if not isinstance(me, dict):
        me = {}
    cred["cuid"] = me.get("cuid") or cred["cuid"]
    cred["nickname"] = me.get("nickname") or cred["nickname"]

    if ask_webhook:
        cred["feishu_webhook"] = input(
            "飞书机器人 webhook URL（直接回车表示关闭通知）："
        ).strip()

    save_credentials(cred)
    log.info(
        "登录成功：%s（cuid=%s）；凭据已保存到 %s",
        cred["nickname"],
        cred["cuid"],
        CREDENTIALS_FILE.name,
    )
    if cred["feishu_webhook"]:
        log.info("飞书通知：已启用")
    else:
        log.info("飞书通知：已关闭（未配置 webhook）")
    return cred, me


def login_flow(
    api: ApiClient,
    preferred_uid: Optional[str] = None,
    *,
    ask_webhook: bool = False,
    existing_feishu_webhook: str = "",
    method: Optional[str] = None,
    existing_device_id: str = "",
) -> tuple[dict, dict]:
    """根据参数或交互选择，分发到 UID 登录或手机号登录。"""
    if not sys.stdin.isatty():
        raise SystemExit(
            "登录需要交互式终端。请先在终端运行一次 `python wuwa_tappo.py --login`。"
        )
    if not method:
        method = prompt_login_method()

    if method == "phone":
        return phone_login_flow(
            api,
            ask_webhook=ask_webhook,
            existing_feishu_webhook=existing_feishu_webhook,
            existing_device_id=existing_device_id,
        )
    if method != "uid":
        raise SystemExit(f"未知的登录方式：{method!r}")
    return uid_login_flow(
        api,
        preferred_uid,
        ask_webhook=ask_webhook,
        existing_feishu_webhook=existing_feishu_webhook,
        existing_device_id=existing_device_id,
    )


def renew_token(api: ApiClient, cred: dict) -> None:
    """调用续期接口并本地顺延过期时间。"""
    # 只要响应里没有错误码就视为续期成功；不同部署返回的 data 结构不一致，
    # 可能是 true、null 或对象。
    api.request("GET", "api/frontend/user/renew-token")
    base = max(to_int(cred.get("expire_time")), int(time.time() * 1000))
    cred["expire_time"] = base + TOKEN_TTL_SEC * 1000
    save_credentials(cred)
    log.info("Token 已续期，剩余有效期：%s", format_remaining(cred))


def validate_credentials(api: ApiClient, cred: dict) -> dict:
    """校验本地 token；剩余有效期不足阈值时尝试续期。"""
    api.token = cred.get("token") or None
    me = api.request("GET", "api/frontend/user/me")

    remaining = remaining_sec(cred)
    if remaining is None or remaining < RENEW_THRESHOLD_SEC:
        try:
            renew_token(api, cred)
        except NetworkError as exc:
            log.warning("Token 续期网络错误：%s", exc)
    return me


def bootstrap(api: ApiClient, args: argparse.Namespace) -> tuple[dict, dict]:
    """启动阶段获取可用凭据：优先复用本地凭据，失效时进入登录流程。"""
    if args.login:
        cred = None
    else:
        cred = load_credentials()

    if cred:
        try:
            me = validate_credentials(api, cred)
            return cred, me
        except ApiError as exc:
            if exc.code not in AUTH_ERROR_CODES:
                raise
            log.warning("本地凭据已失效（%s）：%s", exc.code, exc)
            if not sys.stdin.isatty():
                raise SystemExit(
                    "本地凭据已失效。请在终端运行 `python wuwa_tappo.py --login`。"
                ) from exc
        except NetworkError as exc:
            raise SystemExit(f"校验凭据时发生网络错误：{exc}") from exc

    if cred:
        existing = cred
    else:
        existing = {}

    requested_method = getattr(args, "login_method", None)
    method = requested_method
    if not args.login and not method:
        saved_method = str(existing.get("login_method") or "")
        if saved_method:
            method = saved_method
        else:
            method = None

    return login_flow(
        api,
        preferred_uid=existing.get("uid"),
        ask_webhook=True,
        existing_feishu_webhook=str(existing.get("feishu_webhook") or ""),
        method=method,
        existing_device_id=str(existing.get("kuro_device_id") or ""),
    )


# ============================== 飞书卡片 ==============================

PLACE_LABELS = {"left": "左箱", "right": "右箱"}


def place_label(place: str) -> str:
    """把 left/right 映射为卡片与日志中的中文位置名。"""
    return PLACE_LABELS.get(place, place)


ASSET_TYPE_LABELS = {
    "character_skin": "角色皮肤",
    "emote": "表情",
    "accessory": "饰品",
}
# rarity_type -> (中文标签, 文本标签颜色, 页眉模板色)
RARITY_STYLES = {
    "common": ("普通", "green", "green"),
    "uncommon": ("优良", "blue", "blue"),
    "rare": ("稀有", "violet", "violet"),
    "epic": ("史诗", "orange", "orange"),
    "legendary": ("传说", "red", "red"),
}


def rarity_style(value: Any) -> tuple[str, str, str]:
    """返回稀有度对应的中文标签、标签颜色和页眉颜色。"""
    raw = str(value or "").strip()
    if not raw:
        return "未知", "neutral", "grey"
    return RARITY_STYLES.get(raw.lower(), (raw, "neutral", "grey"))


def display_asset_name(value: Any) -> str:
    """去掉资源文件扩展名，作为卡片中的奖励名称。"""
    name = str(value or "").strip()
    if not name:
        return "未知奖励"
    return Path(name).stem


def format_countdown(ms: int) -> str:
    """把毫秒倒计时格式化成“约 X 分 Y 秒后”这样的中文描述。"""
    seconds = max(0, to_int(ms) // 1000)
    if seconds < 60:
        return "不到 1 分钟"

    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"约 {minutes} 分 {seconds} 秒后"

    hours, minutes = divmod(minutes, 60)
    return f"约 {hours} 小时 {minutes} 分后"


def chest_summary(place: str, state: str) -> str:
    return f"⏰ 下次开箱　**{place_label(place)}** · {state}"


def next_chest_summary(chests: list[dict]) -> Optional[tuple[str, str]]:
    """返回下一次开箱的位置和状态：优先已就绪的箱子，否则取最快就绪的。"""
    valid_chests = []
    for chest in chests:
        if to_int(chest.get("readyAt")) > 0:
            valid_chests.append(chest)

    if not valid_chests:
        return None

    for chest in valid_chests:
        if chest.get("ready"):
            return str(chest.get("place")), "已就绪"

    # 使用严格小于，保持和原 min() 一样的选择顺序：并列时取靠前的箱子。
    soonest = valid_chests[0]
    for chest in valid_chests[1:]:
        if to_int(chest.get("readyInMs")) < to_int(soonest.get("readyInMs")):
            soonest = chest
    return str(soonest.get("place")), format_countdown(to_int(soonest.get("readyInMs")))


def stat_column(label: str, value: str) -> dict:
    """构造飞书卡片中的一列统计数据。"""
    return {
        "tag": "column",
        "width": "weighted",
        "weight": 1,
        "elements": [{
            "tag": "markdown",
            "content": f"<font color='grey'>{label}</font>\n**{value}**",
        }],
    }


def build_open_card(
    place: str,
    data: dict,
    remaining: int,
    *,
    image_url: Optional[str] = None,
    next_chest: Optional[tuple[str, str]] = None,
    now: Optional[datetime] = None,
) -> dict:
    """构造开箱成功的飞书卡片。"""
    if now is None:
        now = datetime.now()

    asset_type = str(data.get("asset_type") or "").strip()
    type_label = ASSET_TYPE_LABELS.get(asset_type, asset_type or "未知")
    rarity_label, tag_color, template = rarity_style(data.get("rarity_type"))
    place_text = place_label(place)

    elements: list[dict] = [
        {
            "tag": "markdown",
            "content": (
                f"**{type_label}**\n"
                f"<font color='grey'>{display_asset_name(data.get('asset_name'))}</font>"
            ),
        },
        {"tag": "hr"},
        {
            "tag": "column_set",
            "columns": [
                stat_column("位置", place_text),
                stat_column("消耗点击", f"{to_int(data.get('click_consume')):,}"),
                stat_column("剩余可用", f"{to_int(remaining):,}"),
                stat_column("开箱时间", now.strftime("%m-%d %H:%M")),
            ],
        },
    ]

    if next_chest:
        elements.append({
            "tag": "markdown",
            "content": chest_summary(next_chest[0], next_chest[1]),
        })

    if image_url:
        elements.append({
            "tag": "button",
            "text": {"tag": "plain_text", "content": "查看奖励原图"},
            "type": "default",
            "behaviors": [{"type": "open_url", "default_url": image_url}],
        })
    else:
        elements.append({
            "tag": "markdown",
            "content": "<font color='grey'>原图暂不可用</font>",
        })

    return {
        "schema": "2.0",
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"tag": "plain_text", "content": WEBHOOK_TITLE},
            "text_tag_list": [{
                "tag": "text_tag",
                "text": {"tag": "plain_text", "content": rarity_label},
                "color": tag_color,
            }],
        },
        "body": {"elements": elements},
    }


def build_test_card(*, now: Optional[datetime] = None) -> dict:
    """构造一张飞书测试卡片，用于验证 webhook。"""
    if now is None:
        now = datetime.now()

    return {
        "schema": "2.0",
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "Wuwa Tappo · 通知测试"},
        },
        "body": {
            "elements": [
                {
                    "tag": "markdown",
                    "content": "飞书自定义机器人已配置成功。\n开箱结果将以卡片消息推送到本群。",
                },
                {"tag": "hr"},
                {
                    "tag": "markdown",
                    "content": f"<font color='grey'>发送时间</font>\n**{now:%Y-%m-%d %H:%M:%S}**",
                },
            ],
        },
    }


def mask_webhook(url: str) -> str:
    """日志中只暴露 webhook token 的末四位。"""
    text = str(url or "").rstrip("/")
    parts = text.rsplit("/", 1)
    token = parts[-1]
    if len(token) > 4:
        return f"***{token[-4:]}"
    return "***"


def send_feishu_card(webhook_url: str, card: dict) -> bool:
    """发送飞书卡片；失败只记录日志，不向上抛异常。"""
    if not webhook_url:
        return False

    try:
        response = requests.post(
            webhook_url,
            json={"msg_type": "interactive", "card": card},
            timeout=FEISHU_TIMEOUT_SEC,
        )
    except requests.RequestException as exc:
        text = str(webhook_url).rstrip("/")
        parts = text.rsplit("/", 1)
        token = parts[-1]
        message = str(exc).replace(webhook_url, mask_webhook(webhook_url))
        if token:
            message = message.replace(token, mask_webhook(webhook_url))
        log.warning("飞书 webhook 请求失败：%s", message)
        return False

    if not response.ok:
        log.warning(
            "飞书 webhook 返回失败（HTTP %s）：%s",
            response.status_code,
            response.text[:200],
        )
        return False

    try:
        body = response.json()
    except ValueError:
        body = {}

    code = body.get("code", body.get("StatusCode"))
    if code is not None and to_int(code, -1) != 0:
        log.warning(
            "飞书 webhook 拒绝消息（code=%s）：%s",
            code,
            body.get("msg") or body.get("StatusMessage") or "",
        )
        return False

    log.info("飞书卡片已发送（HTTP %s）", response.status_code)
    return True


# ============================== Socket.IO 客户端 ==============================

def describe_close_frame(frame: Any) -> str:
    """解析服务端 close 帧的前两字节关闭码，用于日志定位。"""
    data = getattr(frame, "data", b"")
    if isinstance(data, (bytes, bytearray)) and len(data) >= 2:
        code = int.from_bytes(data[:2], "big")
        reason = bytes(data[2:]).decode("utf-8", "replace")
        return f"服务端关闭连接（code={code}，reason={reason!r}）"
    return "服务端关闭连接"


class SocketIOClient:
    """最小可用的 Engine.IO 3 / Socket.IO 4 WebSocket 客户端。

    服务端协议的细节都集中在读线程、心跳线程和 ack 等待表中，
    不依赖额外的 socketio 库。
    """

    def __init__(self, host: str, token: str):
        self.host = host.rstrip("/")
        self.token = token
        self.ws: Optional[websocket.WebSocket] = None
        self.reader: Optional[threading.Thread] = None
        self.heartbeat: Optional[threading.Thread] = None
        self.connected = False
        self.close_reason: Optional[str] = None
        self.forced_disconnect = False
        self._closing = False
        self._stop = threading.Event()
        self._ack_seq = 0
        self._acks: dict[int, dict[str, Any]] = {}
        self._ack_lock = threading.Lock()
        self._send_lock = threading.Lock()

    def connect(self) -> "SocketIOClient":
        """建立 WebSocket 连接并完成 Socket.IO 命名空间握手。"""
        if self.host.startswith("https://"):
            base = "wss://" + self.host[len("https://"):]
        elif self.host.startswith("http://"):
            base = "ws://" + self.host[len("http://"):]
        else:
            base = self.host
        query = urlencode({"EIO": "3", "transport": "websocket", "token": self.token})
        url = f"{base}{SOCKET_PATH}?{query}"

        try:
            self.ws = websocket.create_connection(
                url,
                timeout=SOCKET_CONNECT_TIMEOUT_SEC,
                enable_multithread=True,
                # 服务端会拒绝带 Origin 头的浏览器式握手（403）；
                # 官方 Node 客户端不带 Origin，所以这里也禁用。
                suppress_origin=True,
            )
            self.ws.settimeout(SOCKET_CONNECT_TIMEOUT_SEC)

            open_packet = self._recv(SOCKET_CONNECT_TIMEOUT_SEC)
            if not open_packet or not open_packet.startswith("0"):
                raise RuntimeError(f"收到异常的 Engine.IO open 包：{open_packet!r}")

            self._send("40")
            deadline = time.time() + SOCKET_CONNECT_TIMEOUT_SEC
            namespace_ready = False
            while time.time() < deadline:
                packet = self._recv(max(0.1, deadline - time.time()))
                if packet.startswith("40"):
                    namespace_ready = True
                    break
                if packet.startswith("2"):
                    self._send("3")
            if not namespace_ready:
                raise TimeoutError("Socket.IO 命名空间连接超时")

            self.connected = True
            self.reader = threading.Thread(
                target=self._reader_loop,
                name="wuwa-socket-reader",
                daemon=True,
            )
            self.reader.start()
            self.heartbeat = threading.Thread(
                target=self._heartbeat_loop,
                name="wuwa-socket-heartbeat",
                daemon=True,
            )
            self.heartbeat.start()
            return self
        except Exception:
            self.connected = False
            if self.ws is not None:
                try:
                    self.ws.close()
                except Exception:
                    pass
                self.ws = None
            raise

    def _recv(self, timeout: Optional[float] = None) -> str:
        ws = self.ws
        if ws is None:
            raise ConnectionError("socket 已关闭")
        if timeout is not None:
            ws.settimeout(max(0.1, timeout))
        data = ws.recv()
        if isinstance(data, bytes):
            data = data.decode("utf-8", "replace")
        return data

    def _recv_frame(self, timeout: float) -> tuple[int, Any]:
        ws = self.ws
        if ws is None:
            raise ConnectionError("socket 已关闭")
        ws.settimeout(max(0.1, timeout))
        return ws.recv_data_frame(control_frame=True)

    def _handle_event(self, packet: str) -> bool:
        """处理 42 事件包；返回 True 表示读线程应该停止。"""
        try:
            args = json.loads(packet[2:])
        except json.JSONDecodeError:
            log.debug("无法解析 socket 事件：%s", packet[:200])
            return False

        if not isinstance(args, list) or not args:
            return False

        name = args[0]
        if len(args) > 1:
            payload = args[1]
        else:
            payload = None

        if name == "fd":
            code = -1
            if isinstance(payload, dict):
                code = to_int(payload.get("c"), -1)
            self.forced_disconnect = True
            self.close_reason = f"服务端强制断开连接（fd code={code}）"
            self.connected = False
            log.warning("服务端强制断开连接（fd code=%s）", code)
            return True

        log.debug("Socket 事件 %s：%s", name, json.dumps(payload, ensure_ascii=False)[:200])
        return False

    def _send(self, packet: str) -> None:
        ws = self.ws
        if ws is None:
            raise ConnectionError("socket 已关闭")
        with self._send_lock:
            ws.send(packet)

    def _heartbeat_loop(self) -> None:
        """服务端不主动 ping，客户端必须定期发送 "2" 维持连接。"""
        while not self._stop.wait(SOCKET_PING_INTERVAL_SEC):
            if self._closing or not self.connected:
                return
            try:
                self._send("2")
                log.debug("已发送 Engine.IO ping")
            except Exception as exc:
                if not self._closing:
                    self.close_reason = f"心跳发送失败（{exc}）"
                return

    def _reader_loop(self) -> None:
        """唯一读线程：处理心跳、关闭、ack 和业务事件。

        读超时属于正常空闲，继续等待；只有连接真正断开或服务端要求
        断开时才退出循环。
        """
        opcodes = websocket.ABNF
        try:
            while not self._closing:
                try:
                    opcode, frame = self._recv_frame(120)
                except (websocket.WebSocketTimeoutException, TimeoutError):
                    # 服务端不会主动发送 Engine.IO ping，读超时是正常情况；
                    # 保持连接由心跳线程负责。
                    continue
                except Exception as exc:
                    if not self._closing:
                        self.close_reason = f"连接丢失（{exc}）"
                    break

                if opcode == opcodes.OPCODE_CLOSE:
                    self.close_reason = describe_close_frame(frame)
                    break
                if opcode not in (opcodes.OPCODE_TEXT, opcodes.OPCODE_BINARY):
                    continue

                packet = frame.data
                if isinstance(packet, bytes):
                    packet = packet.decode("utf-8", "replace")
                if not packet:
                    continue

                if packet.startswith("2"):
                    try:
                        self._send("3")
                    except Exception as exc:
                        self.close_reason = f"pong 发送失败（{exc}）"
                        break
                    continue

                if packet.startswith("43"):
                    self._handle_ack(packet)
                    continue

                if packet.startswith("41"):
                    self.close_reason = "服务端关闭了 Socket.IO 命名空间"
                    break

                if packet.startswith("42"):
                    if self._handle_event(packet):
                        break
                    continue
        finally:
            self.connected = False
            if not self._closing and self.close_reason:
                log.warning("Socket 已断开：%s", self.close_reason)

    def _handle_ack(self, packet: str) -> None:
        """解析 43 ack 包并唤醒对应的等待线程。

        ack 包格式为 "43<ack_id><json>"，ack_id 可有多个数字位。
        """
        rest = packet[2:]
        index = 0
        while index < len(rest) and rest[index].isdigit():
            index += 1
        if index == 0:
            return

        ack_id = int(rest[:index])
        try:
            args = json.loads(rest[index:])
        except json.JSONDecodeError:
            args = None

        if isinstance(args, list) and args:
            result = args[0]
        else:
            result = args

        with self._ack_lock:
            waiter = self._acks.get(ack_id)
        if waiter is not None:
            waiter["result"] = result
            waiter["event"].set()

    def emit_with_ack(self, event: str, data: Any, timeout: int = SOCKET_ACK_TIMEOUT_SEC) -> Any:
        """发送事件并等待 ack。

        发送前把等待项挂到 _acks；读线程收到 43 后回填结果并唤醒。
        超时或发送失败时必须移除等待项，避免内存泄漏。
        """
        if not self.connected or self.ws is None:
            raise ConnectionError("socket 未连接")

        with self._ack_lock:
            self._ack_seq += 1
            ack_id = self._ack_seq
            waiter = {"event": threading.Event(), "result": None}
            self._acks[ack_id] = waiter

        packet = f"42{ack_id}" + json.dumps([event, data], separators=(",", ":"))
        try:
            self._send(packet)
        except Exception:
            with self._ack_lock:
                self._acks.pop(ack_id, None)
            raise

        if not waiter["event"].wait(timeout):
            with self._ack_lock:
                self._acks.pop(ack_id, None)
            raise TimeoutError(f"{event} 的 ack 在 {timeout} 秒后超时")

        with self._ack_lock:
            completed = self._acks.pop(ack_id, None)
        if completed:
            return completed["result"]
        return None

    def close(self) -> None:
        """幂等关闭：停止心跳、关闭 socket、等待线程退出并唤醒所有等待者。"""
        self._closing = True
        self._stop.set()

        try:
            if self.connected:
                self._send("41")
        except Exception:
            pass

        ws = self.ws
        try:
            if ws is not None:
                ws.close()
        except Exception:
            pass

        current = threading.current_thread()
        for thread in (self.reader, self.heartbeat):
            if thread is not None and thread is not current:
                thread.join(2)

        self.connected = False
        self.ws = None

        with self._ack_lock:
            for waiter in self._acks.values():
                waiter["result"] = None
                waiter["event"].set()
            self._acks.clear()


# ============================== 机器人主循环 ==============================

class Bot:
    """运行期状态与调度：上报点击、刷新账号、开箱并发送飞书通知。"""

    def __init__(self, api: ApiClient, cred: dict, me: dict, args: argparse.Namespace):
        self.api = api
        self.cred = cred
        if me:
            self.me = me
        else:
            self.me = {}
        self.dry_run = bool(args.dry_run)
        self.no_open = bool(args.no_open)
        self.webhook_url = str(cred.get("feishu_webhook") or "").strip()
        self.stop = False

        self.sock: Optional[SocketIOClient] = None
        self.reported_total: Optional[int] = to_int(self.me.get("total_click_count"))
        self.pending = 0
        self.server_offset_ms = 0
        self.assets_host = ""
        self.chests: list[dict] = []

        self.next_connect = 0.0
        self.reconnect_delay = 5
        self.ack_failures = 0
        self.report_backoff_until = 0.0
        self.next_report = 0.0
        self.next_me = time.time() + CHEST_CHECK_INTERVAL_SEC
        self.next_time_sync = 0.0
        self.next_renew = time.time() + RENEW_CHECK_SEC
        self.cycles_done = 0
        if args.once:
            self.max_cycles = 1
        else:
            self.max_cycles = max(0, args.cycles)
        self.skip_streak = 0
        self.exit_code = 0
        self.time_synced = False

    def server_now_ms(self) -> int:
        """返回按服务器时间校正后的毫秒时间戳。"""
        return int(time.time() * 1000) + self.server_offset_ms

    def sync_time(self) -> bool:
        """同步服务器时间偏移，并顺带更新资源 CDN 地址。

        宝箱是否就绪依赖服务器时间；只有偏移不超过 24 小时才采纳，
        否则保留上一次结果并记录警告。
        """
        try:
            data = self.api.request("GET", "api/common/activity-config")
        except (ApiError, NetworkError) as exc:
            log.warning("服务器时间同步失败：%s", exc)
            return self.time_synced

        if not data:
            data = {}
        if not isinstance(data, dict):
            data = {}

        server_time = to_int(data.get("server_time"))
        now_ms = int(time.time() * 1000)
        if server_time > 0:
            offset = server_time - now_ms
            if abs(offset) <= MAX_SERVER_TIME_SKEW_MS:
                self.server_offset_ms = offset
                self.time_synced = True
                log.debug("服务器时间已同步（偏移 %sms）", offset)
            else:
                log.warning(
                    "忽略不合理的 server_time=%s（偏移 %sms）",
                    server_time,
                    offset,
                )

        host = str(data.get("assets_host") or "").strip().rstrip("/")
        if host.startswith("http"):
            self.assets_host = host
        return self.time_synced

    def reward_image_url(self, data: dict) -> Optional[str]:
        """按资源类型拼接 CDN 原图地址。"""
        name = str(data.get("asset_name") or "").strip()
        if not name or not self.assets_host:
            return None

        asset_type = str(data.get("asset_type") or "").strip()
        directory = REWARD_IMAGE_DIRS.get(asset_type, asset_type)
        if not directory:
            return None

        return (
            f"{self.assets_host}/"
            f"{quote(directory, safe='')}/"
            f"{quote(name, safe='')}"
        )

    def log_account(self) -> None:
        """输出账号、点击余额、token 和宝箱状态摘要。"""
        total = to_int(self.me.get("total_click_count"))
        consumed = to_int(self.me.get("total_click_consume"))
        log.info(
            "账号：%s（cuid=%s）",
            self.me.get("nickname") or self.cred.get("nickname"),
            self.me.get("cuid") or self.cred.get("cuid"),
        )
        log.info(
            "点击：累计=%s 已消耗=%s 可用=%s",
            total,
            consumed,
            max(0, total - consumed),
        )
        log.info(
            "Token：%s（剩余 %s）",
            mask_token(self.cred.get("token")),
            format_remaining(self.cred),
        )

        for chest in self.chests:
            if chest["readyAt"] <= 0:
                state = "未知"
            elif chest["ready"]:
                state = "已就绪"
            else:
                state = f"{chest['readyInMs'] // 1000} 秒后"
            log.info("%s：%s", place_label(chest["place"]), state)

    def compute_chests(self, me: dict) -> list[dict]:
        """根据 /me 返回的宝箱时间戳计算左右宝箱状态。"""
        now = self.server_now_ms()
        states = []
        for place in CHEST_PLACES:
            if me:
                ready_at = to_int(me.get(f"{place}_treasure_chest"))
            else:
                ready_at = 0

            # 时间戳缺失或为 0 表示“未知”，不能当成“已就绪”，
            # 否则会向服务端发送无效的开箱请求。
            known = ready_at > 0
            if known:
                remaining = max(0, ready_at - now)
            else:
                remaining = 0
            states.append({
                "place": place,
                "readyAt": ready_at,
                "readyInMs": remaining,
                "ready": known and remaining == 0,
            })
        return states

    def apply_me(self, me: dict) -> None:
        """用新的 /me 数据更新账号状态，并处理服务端点击数异常增加。"""
        if me:
            self.me = me
        else:
            self.me = {}

        server_total = to_int(self.me.get("total_click_count"))
        if self.reported_total is None:
            self.reported_total = server_total
        elif server_total > self.reported_total:
            if self.pending > 0:
                log.warning(
                    "服务端累计点击异常增加；已清空 %s 个缓冲点击",
                    self.pending,
                )
            self.reported_total = server_total
            self.pending = 0

        self.chests = self.compute_chests(self.me)

    def refresh_me(self) -> None:
        """刷新 /me，然后检查是否可以开箱。"""
        try:
            me = self.api.request("GET", "api/frontend/user/me")
        except ApiError as exc:
            if exc.code in AUTH_ERROR_CODES:
                self.handle_auth_error(exc)
            else:
                log.warning("GET /me 失败：%s", exc)
            return
        except NetworkError as exc:
            log.warning("GET /me 网络错误：%s", exc)
            return

        if not me:
            me = {}
        self.apply_me(me)

        chest_pairs = []
        for chest in self.chests:
            chest_pairs.append((place_label(chest["place"]), chest["readyInMs"]))
        log.debug(
            "账号状态：累计=%s 已消耗=%s 宝箱=%s",
            to_int(me.get("total_click_count")),
            to_int(me.get("total_click_consume")),
            chest_pairs,
        )

        self.maybe_open_chests(me)

    def maybe_open_chests(self, me: dict) -> None:
        """检查并开启已就绪的宝箱。

        左右两个箱子共用一个循环：开箱后立刻刷新 /me 覆盖 current，
        如果另一侧也已就绪，间隔 3 秒后再开一次；刷新失败则结束本次开箱。
        """
        if self.no_open or not self.time_synced:
            return

        if not me:
            current = {}
        else:
            current = me

        for attempt in range(len(CHEST_PLACES)):
            total = to_int(current.get("total_click_count"))
            consumed = to_int(current.get("total_click_consume"))
            available = max(0, total - consumed)
            if available < CHEST_CLICK_COST:
                return

            now = self.server_now_ms()
            place = None
            for candidate in CHEST_PLACES:
                ready_at = to_int(current.get(f"{candidate}_treasure_chest"))
                if 0 < ready_at <= now:
                    place = candidate
                    break
            if place is None:
                return
            label = place_label(place)

            if self.dry_run:
                log.info(
                    "试运行：本可开启%s（可用=%s，click_num=%s）",
                    label,
                    available,
                    total,
                )
                return

            try:
                log.info(
                    "正在开启%s（可用=%s，click_num=%s）......",
                    label,
                    available,
                    total,
                )
                data = self.api.request(
                    "POST",
                    "api/frontend/chests/open",
                    body={"place": place, "click_num": total},
                )
            except ApiError as exc:
                if exc.code in AUTH_ERROR_CODES:
                    self.handle_auth_error(exc)
                else:
                    log.warning("开启%s失败：%s", label, exc)
                return
            except NetworkError as exc:
                log.warning("开启%s网络错误：%s", label, exc)
                return

            if not data:
                data = {}
            log.debug("开箱响应：%s", json.dumps(data, ensure_ascii=False))

            click_consume = to_int(data.get("click_consume"))
            remaining = max(0, available - click_consume)
            image_url = self.reward_image_url(data)
            if image_url:
                image_text = image_url
            else:
                image_text = "-"
            log.info(
                "开箱成功：位置=%s 奖励=%s 类型=%s 稀有度=%s 消耗=%s 图片=%s",
                label,
                data.get("asset_name"),
                data.get("asset_type"),
                data.get("rarity_type"),
                click_consume,
                image_text,
            )
            if click_consume <= 0:
                log.warning("开箱响应中没有 click_consume：%s", data)

            next_chest = None
            refreshed = False
            try:
                current = self.api.request("GET", "api/frontend/user/me")
                self.apply_me(current)
                refreshed = True
                next_chest = next_chest_summary(self.chests)
            except ApiError as exc:
                if exc.code in AUTH_ERROR_CODES:
                    self.handle_auth_error(exc)
                else:
                    log.warning("开箱后刷新 /me 失败：%s", exc)
            except NetworkError as exc:
                log.warning("开箱后刷新 /me 失败：%s", exc)

            if click_consume > 0 or data.get("asset_name"):
                card = build_open_card(
                    place,
                    data,
                    remaining,
                    image_url=image_url,
                    next_chest=next_chest,
                )
                if self.webhook_url:
                    try:
                        threading.Thread(
                            target=send_feishu_card,
                            args=(self.webhook_url, card),
                            name="wuwa-feishu",
                            daemon=True,
                        ).start()
                    except Exception as exc:
                        log.warning("无法启动飞书发送线程：%s", exc)

            if attempt == 0:
                time.sleep(OPEN_DELAY_SEC)

            if not refreshed:
                return

    def report_cycle(self) -> str:
        """执行一次点击上报。

        先把随机点击累加到缓冲（上限 200）。试运行、socket 离线、
        尚未拿到初始 /me 时分别返回 "dry" 或 "skipped"；
        只有 ack 成功才推进 reported_total 并清空缓冲。
        """
        delta = random.randint(REPORT_CLICK_MIN, REPORT_CLICK_MAX)
        before = self.pending
        self.pending = min(MAX_PENDING_CLICKS, before + delta)
        if before + delta > MAX_PENDING_CLICKS:
            log.warning("缓冲点击数达到上限（%s），丢弃多余部分", MAX_PENDING_CLICKS)

        if self.dry_run:
            if self.reported_total is None:
                reported_total = 0
            else:
                reported_total = self.reported_total
            total = reported_total + self.pending
            consumed = to_int(self.me.get("total_click_consume"))
            available = max(0, total - consumed)
            log.info(
                "试运行：本应上报 累计=%s 本次=%s 可用=%s",
                total,
                self.pending,
                available,
            )
            self.pending = 0
            return "dry"

        if self.sock is None or not self.sock.connected:
            log.debug("socket 未连接，缓冲 %s 个点击", self.pending)
            return "skipped"

        if self.reported_total is None:
            log.warning("尚未拿到初始 /me，本次跳过上报")
            return "skipped"

        report_total = self.reported_total + self.pending
        try:
            ack_payload = self.sock.emit_with_ack(
                "cr",
                {"num": report_total},
                SOCKET_ACK_TIMEOUT_SEC,
            )
        except Exception as exc:
            self.ack_failures += 1
            log.warning(
                "点击上报 ack 失败（%s）；保留 %s 个缓冲点击",
                exc,
                self.pending,
            )
            if self.ack_failures >= 3 and self.sock is not None:
                log.warning("连续 ack 失败，正在重建 socket 连接")
                try:
                    self.sock.close()
                except Exception:
                    pass
                self.sock = None
                self.next_connect = time.time() + 3
                self.ack_failures = 0
            return "sent"

        self.ack_failures = 0
        code = -1
        if isinstance(ack_payload, dict):
            code = to_int(ack_payload.get("c"), -1)

        if code == 0:
            sent = self.pending
            self.reported_total = report_total
            self.pending = 0
            consumed = to_int(self.me.get("total_click_consume"))
            available = max(0, self.reported_total - consumed)
            log.info(
                "点击上报成功：累计=%s 本次=%s 已消耗=%s 可用=%s",
                report_total,
                sent,
                consumed,
                available,
            )
            self.next_me = 0.0
        elif code == RATE_LIMIT_CODE:
            log.warning(
                "点击上报被限流（80013）；保留 %s 个缓冲点击",
                self.pending,
            )
        elif code == CLIENT_CLICK_ANOMALY_CODE:
            log.error(
                "服务端判定客户端点击数异常（70003）；清空缓冲并退避 5 分钟"
            )
            self.pending = 0
            self.reported_total = None
            self.report_backoff_until = time.time() + 300
            self.next_me = 0.0
        elif code in AUTH_ERROR_CODES:
            self.handle_auth_error(ApiError(code, "登录已过期"))
        else:
            log.warning("点击上报被拒绝：%r", ack_payload)
        return "sent"

    def ensure_socket(self) -> None:
        """维护 socket 连接：按需关闭旧连接、退避重连。"""
        now = time.time()

        if self.sock is not None and not self.sock.connected:
            if self.sock.close_reason:
                reason = self.sock.close_reason
            else:
                reason = "连接已丢失"

            # fd=90002 表示账号已被另一个 socket 占用，
            # 需要等服务端会话超时后再重试。
            if self.sock.forced_disconnect:
                delay = 65
            else:
                delay = 3

            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
            self.next_connect = max(self.next_connect, now + delay)
            log.info("将在 %s 秒后重连（%s）", delay, reason)

        if self.sock is not None and self.sock.connected:
            return
        if now < self.next_connect:
            return

        try:
            log.info("正在连接 socket......")
            self.sock = SocketIOClient(API_HOST, self.cred["token"]).connect()
            self.reconnect_delay = 5
            self.next_connect = 0.0
            log.info("socket 已连接")
        except Exception as exc:
            self.sock = None
            self.next_connect = now + self.reconnect_delay + random.randint(0, 1)
            log.warning(
                "socket 连接失败：%s（将在 %s 秒后重试）",
                exc,
                self.reconnect_delay,
            )
            self.reconnect_delay = min(self.reconnect_delay * 2, RECONNECT_MAX_SEC)

    def maybe_renew(self) -> None:
        """按间隔检查 token；剩余不足 48 小时时续期。"""
        remaining = remaining_sec(self.cred)
        if remaining is not None and remaining > RENEW_THRESHOLD_SEC:
            self.next_renew = time.time() + RENEW_CHECK_SEC
            return

        try:
            renew_token(self.api, self.cred)
        except ApiError as exc:
            if exc.code in AUTH_ERROR_CODES:
                self.handle_auth_error(exc)
                self.next_renew = time.time() + RENEW_CHECK_SEC
            else:
                log.warning("Token 续期失败：%s", exc)
                self.next_renew = time.time() + RENEW_RETRY_SEC
            return
        except NetworkError as exc:
            log.warning("Token 续期网络错误：%s", exc)
            self.next_renew = time.time() + RENEW_RETRY_SEC
            return

        self.next_renew = time.time() + RENEW_CHECK_SEC

    def handle_auth_error(self, exc: ApiError) -> None:
        """处理认证失效：关闭 socket、重新登录并重置运行状态。"""
        log.error("认证已失效（%s）：%s", exc.code, exc.message)
        if self.sock is not None:
            self.sock.close()
            self.sock = None

        if not sys.stdin.isatty():
            raise SystemExit(
                "凭据已失效。请在终端运行 `python wuwa_tappo.py --login`。"
            )

        log.info("开始重新登录......")
        self.cred, me = login_flow(
            self.api,
            preferred_uid=self.cred.get("uid"),
            existing_feishu_webhook=self.webhook_url,
            method=str(self.cred.get("login_method") or "") or None,
            existing_device_id=str(self.cred.get("kuro_device_id") or ""),
        )
        self.webhook_url = str(self.cred.get("feishu_webhook") or "").strip()
        if me:
            self.me = me
        else:
            self.me = {}
        self.reported_total = to_int(self.me.get("total_click_count"))
        self.pending = 0
        self.chests = self.compute_chests(self.me)
        self.next_me = 0.0
        self.next_connect = 0.0
        self.reconnect_delay = 5

    def flush_pending(self) -> None:
        """退出前尝试补报缓冲区里的点击。"""
        if self.dry_run:
            return
        if self.sock is None or not self.sock.connected:
            return
        if self.reported_total is None:
            return
        if self.pending <= 0:
            return

        report_total = self.reported_total + self.pending
        try:
            log.info(
                "退出前正在补报 %s 个缓冲点击（累计=%s）......",
                self.pending,
                report_total,
            )
            ack_payload = self.sock.emit_with_ack("cr", {"num": report_total}, 3)
            code = -1
            if isinstance(ack_payload, dict):
                code = to_int(ack_payload.get("c"), -1)
            if code == 0:
                self.reported_total = report_total
                self.pending = 0
                log.info("缓冲点击已补报")
            else:
                log.warning("最终补报被拒绝：%r", ack_payload)
        except Exception as exc:
            log.warning("最终点击补报失败：%s", exc)

    def shutdown(self) -> None:
        """退出清理：补报缓冲点击、关闭 socket、保存凭据。"""
        self.flush_pending()
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        save_credentials(self.cred)

    def run(self) -> None:
        """主调度循环。

        每轮只取一次 now，依次处理时间同步、token 续期、/me 刷新、
        socket 重连和点击上报。next_report 在完成上报后才锚定，
        避免开箱耗时把下次上报的间隔压缩到服务端限流窗口内。
        """
        log.info(
            "开始运行（每次上报 %s-%s 点击，间隔=%s 秒，dry_run=%s，开箱=%s）",
            REPORT_CLICK_MIN,
            REPORT_CLICK_MAX,
            REPORT_INTERVAL_SEC,
            self.dry_run,
            not self.no_open,
        )

        self.sync_time()
        if self.time_synced:
            self.next_time_sync = time.time() + SERVER_TIME_SYNC_SEC
        else:
            self.next_time_sync = time.time() + 30
        self.chests = self.compute_chests(self.me)
        self.log_account()

        if self.webhook_url:
            log.info("飞书通知：已启用")
        else:
            log.warning(
                "飞书通知：已关闭（%s 中没有 feishu_webhook；可运行 --login 设置）",
                CREDENTIALS_FILE.name,
            )

        if self.time_synced:
            self.maybe_open_chests(self.me)
        else:
            log.warning("跳过启动时的开箱检查：服务器时间尚未同步")
        self.ensure_socket()

        while not self.stop:
            now = time.time()

            if now >= self.next_time_sync:
                self.sync_time()
                # 首次同步成功前用更短的间隔快速重试。
                if self.time_synced:
                    self.next_time_sync = time.time() + SERVER_TIME_SYNC_SEC
                else:
                    self.next_time_sync = time.time() + 30

            if now >= self.next_renew:
                self.maybe_renew()

            if now >= self.next_me:
                self.refresh_me()
                self.next_me = time.time() + CHEST_CHECK_INTERVAL_SEC

            self.ensure_socket()

            if now >= self.next_report and now >= self.report_backoff_until:
                status = self.report_cycle()
                # 上报完成后再锚定下一次时间：refresh_me() 中的开箱可能睡眠，
                # 如果复用循环开头的 now，会缩短间隔并触发 80013 限流。
                self.next_report = (
                    time.time() + REPORT_INTERVAL_SEC + random.randint(0, 2)
                )

                if status == "skipped":
                    self.skip_streak += 1
                    if self.max_cycles and self.skip_streak >= MAX_SKIPPED_REPORTS:
                        log.error(
                            "连续 %s 次上报被跳过，停止运行",
                            self.skip_streak,
                        )
                        self.exit_code = 1
                        break
                else:
                    self.skip_streak = 0
                    self.cycles_done += 1
                    if self.max_cycles and self.cycles_done >= self.max_cycles:
                        log.info("已达到周期上限（%s），停止运行", self.max_cycles)
                        break

            if self.stop:
                break
            time.sleep(1)


# ============================== 命令行参数与入口 ==============================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="WuwaTappoKeeper：上报 Wuwa Tappo 点击、自动开箱并发送飞书卡片。",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "示例：\n"
            "  python wuwa_tappo.py                 # 正常运行\n"
            "  python wuwa_tappo.py --once --dry-run\n"
            "  python wuwa_tappo.py --once --no-open\n"
            "  python wuwa_tappo.py --login         # 重新登录并设置飞书 webhook\n"
            "  python wuwa_tappo.py --login --login-method phone\n"
            "  python wuwa_tappo.py --test-webhook  # 使用 credentials.json 中的 webhook 发送测试卡片\n"
        ),
    )
    parser.add_argument("--login", action="store_true", help="强制重新登录后退出")
    parser.add_argument(
        "--login-method",
        choices=("uid", "phone"),
        default=None,
        help="登录方式：uid = 游戏内站内信，phone = 库洛通行证短信（用于 --login 或凭据失效时）",
    )
    parser.add_argument("--once", action="store_true", help="只运行一个上报周期后退出")
    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="运行 N 个上报周期后退出（0 表示无限）",
    )
    parser.add_argument("--dry-run", action="store_true", help="不上报、不开箱、不发送卡片")
    parser.add_argument("--no-open", action="store_true", help="正常上报，但不开箱")
    parser.add_argument(
        "--test-webhook",
        action="store_true",
        help="发送一张飞书测试卡片后退出",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="输出协议层调试日志（ping/pong、服务端事件）到控制台",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    setup_logging(args.debug)

    if not (1 <= REPORT_CLICK_MIN <= REPORT_CLICK_MAX):
        raise SystemExit("REPORT_CLICK_MIN/REPORT_CLICK_MAX 配置无效")

    if args.test_webhook:
        cred = load_credentials()
        webhook_url = ""
        if cred is not None:
            webhook_url = str(cred.get("feishu_webhook") or "").strip()

        if not webhook_url:
            log.error(
                "%s 中没有 feishu_webhook；请运行 `python wuwa_tappo.py --login` 设置",
                CREDENTIALS_FILE.name,
            )
            return 1

        if send_feishu_card(webhook_url, build_test_card()):
            return 0
        return 1

    api = ApiClient(API_HOST)
    cred, me = bootstrap(api, args)

    if args.login:
        log.info("登录完成，凭据已保存，正在退出。")
        return 0

    bot = Bot(api, cred, me, args)

    def handle_signal(signum: int, _frame: Any) -> None:
        log.info("收到信号 %s，正在停止......", signum)
        bot.stop = True

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    try:
        bot.run()
    finally:
        bot.shutdown()

    return bot.exit_code


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyboardInterrupt, EOFError):
        log.warning("已中断")
        raise SystemExit(130)
    except (ApiError, NetworkError) as exc:
        log.error("致命错误：%s", exc)
        raise SystemExit(1)
