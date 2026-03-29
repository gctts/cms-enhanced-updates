"""
CMS Panso 搜索增强模块
功能：TG 机器人 + 企业微信 搜索 Panso 资源
触发方式：消息结尾带 ？ 或 ?
"""
import builtins
import base64
import concurrent.futures
import copy
import glob
import hashlib
import html
import io
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
from urllib.parse import urlparse

logger = logging.getLogger("cms_enhance")

try:
    with open("/tmp/cms_usercustomize_loaded.txt", "w", encoding="utf-8") as _f:
        _f.write(f"loaded {time.time()}\n")
except Exception:
    pass


def _log(msg, level="info"):
    """输出日志"""
    print(f"[🐳404🐳] {msg}", flush=True)


# ============================================================
# PansoClient
# ============================================================
class PansoClient:
    def __init__(self):
        self.base_url = os.environ.get("PANSO_URL", "").rstrip("/")
        self.username = os.environ.get("PANSO_USERNAME", "")
        self.password = os.environ.get("PANSO_PASSWORD", "")
        self._token = None
        self._token_time = 0
        self._lock = threading.Lock()

    @property
    def available(self):
        return bool(self.base_url)

    def _login(self):
        import requests
        for path in ["/api/auth/login", "/api/login"]:
            try:
                resp = requests.post(
                    f"{self.base_url}{path}",
                    json={"username": self.username, "password": self.password},
                    timeout=10,
                )
                if resp.status_code == 404:
                    continue
                data = resp.json()
                token = (data.get("token") or
                         (data.get("data", {}) or {}).get("token") or
                         data.get("access_token"))
                if token:
                    self._token = token
                    self._token_time = time.time()

                    return True
            except Exception:
                continue
        pass
        return False

    def _ensure_token(self):
        with self._lock:
            if self._token and (time.time() - self._token_time) < 3600:
                return True
            if self.username and self.password:
                return self._login()
            self._token = ""
            return True

    def search(self, keyword, cloud_types=None):
        if not self.available:
            return {"merged_by_type": {}, "total": 0}
        if not self._ensure_token():
            return {"merged_by_type": {}, "total": 0}

        import requests
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        body = {"kw": keyword, "cloud_types": cloud_types or list(ALLOWED_CLOUD_TYPES)}

        try:
            resp = requests.post(f"{self.base_url}/api/search", json=body, headers=headers, timeout=30)
            if resp.status_code == 401:
                self._token = None
                if self._ensure_token():
                    headers["Authorization"] = f"Bearer {self._token}"
                    resp = requests.post(f"{self.base_url}/api/search", json=body, headers=headers, timeout=30)

            data = resp.json()
            if "data" in data and isinstance(data["data"], dict):
                inner = data["data"]
                if "merged_by_type" not in inner:
                    inner["merged_by_type"] = {}
                return inner
            if "merged_by_type" in data:
                return data
            return {"merged_by_type": {}, "total": 0}
        except Exception as e:
            pass
            return {"merged_by_type": {}, "total": 0}


panso_client = PansoClient()


class GyingClient:
    HASH_PATTERN = re.compile(r"/gying/([a-f0-9]{64})")

    def __init__(self, auth_client):
        self.auth_client = auth_client
        self.base_url = auth_client.base_url
        self.hash = os.environ.get("GYING_HASH", "").strip().lower()
        self.username = os.environ.get("GYING_USERNAME", "").strip()
        self._resolved_hash = self.hash

    @property
    def available(self):
        return bool(self.base_url and (self.hash or self.username))

    def _headers(self):
        headers = {"Content-Type": "application/json"}
        if self.auth_client._token:
            headers["Authorization"] = f"Bearer {self.auth_client._token}"
        return headers

    def _request(self, method, path, json_data=None, allow_redirects=True):
        import requests

        if not self.auth_client._ensure_token():
            return None

        url = f"{self.base_url}{path}"
        headers = self._headers()
        for _ in range(2):
            try:
                resp = requests.request(
                    method,
                    url,
                    json=json_data,
                    headers=headers,
                    timeout=20,
                    allow_redirects=allow_redirects,
                )
                if resp.status_code != 401:
                    return resp
                self.auth_client._token = None
                if not self.auth_client._ensure_token():
                    return None
                headers = self._headers()
            except Exception:
                return None
        return None

    def _post_action(self, action, extra=None):
        hash_value = self._resolve_hash()
        if not hash_value:
            return {"success": False, "message": "未找到观影账号配置", "data": {}}

        payload = {"action": action}
        if extra:
            payload.update(extra)

        resp = self._request("POST", f"/gying/{hash_value}", payload)
        if resp is None:
            return {"success": False, "message": f"{action} 请求失败", "data": {}}

        try:
            data = resp.json()
        except Exception:
            return {"success": False, "message": f"{action} 响应解析失败", "data": {}}

        if isinstance(data, dict):
            data.setdefault("success", True)
            if not isinstance(data.get("data"), dict):
                data["data"] = {}
            return data
        return {"success": False, "message": f"{action} 响应格式异常", "data": {}}

    def _resolve_hash(self):
        if self._resolved_hash:
            return self._resolved_hash
        if not self.username:
            return ""

        resp = self._request("GET", f"/gying/{self.username}")
        if resp is None:
            return ""

        candidates = [resp.url, resp.headers.get("Location", "")]
        for candidate in candidates:
            match = self.HASH_PATTERN.search(candidate or "")
            if match:
                self._resolved_hash = match.group(1)
                return self._resolved_hash

        try:
            data = resp.json()
        except Exception:
            data = {}

        hash_value = (
            data.get("hash")
            or (data.get("data") or {}).get("hash")
            or (data.get("data") or {}).get("id")
        )
        if isinstance(hash_value, str) and re.fullmatch(r"[a-f0-9]{64}", hash_value):
            self._resolved_hash = hash_value
            return self._resolved_hash
        return ""

    def get_status(self):
        return self._post_action("get_status")

    def get_config(self):
        return self._post_action("get_config")

    def search(self, keyword, max_results=20):
        empty = {"success": False, "message": "观影未配置", "data": {"results": []}}
        if not self.available:
            return empty
        data = self._post_action("test_search", {"keyword": keyword, "max_results": max_results})
        data.setdefault("data", {})
        data["data"].setdefault("results", [])
        return data


class GyingDirectClient:
    DEFAULT_BASE_URL = "https://www.gying.net"
    CHALLENGE_PATTERN = re.compile(r"const json=(\{.*?\});const jss=", re.S)
    SEARCH_PATTERN = re.compile(r"_obj\.search=(\{.*?\});", re.S)
    CURL_STATUS_MARKER = "__CMS_CURL_STATUS__:"

    def __init__(self, proxy_client):
        self.proxy_client = proxy_client
        self.base_url = os.environ.get("GYING_BASE_URL", "").strip().rstrip("/")
        self.username = (os.environ.get("GYING_DIRECT_USERNAME", "").strip()
                         or os.environ.get("GYING_USERNAME", "").strip())
        self.password = os.environ.get("GYING_PASSWORD", "").strip()
        self._resolved_base_url = ""
        self._resolved_username = ""
        self._session = None
        self._session_login_at = 0
        self._persisted_session_loaded = False
        self._lock = threading.RLock()

    @property
    def available(self):
        if not self.password:
            return False
        if self.base_url and self.username:
            return True
        return self.proxy_client.available

    def _create_session(self):
        import requests
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/json,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        return session

    def _proxy_data(self, action):
        if not self.proxy_client.available:
            return {}
        if action == "config":
            data = self.proxy_client.get_config()
        else:
            data = self.proxy_client.get_status()
        if not data.get("success"):
            return {}
        return data.get("data") or {}

    def _get_base_url(self):
        if self.base_url:
            return self.base_url
        if self._resolved_base_url:
            return self._resolved_base_url
        data = self._proxy_data("config")
        base_url = (data.get("base_url") or "").strip().rstrip("/")
        self._resolved_base_url = base_url or self.DEFAULT_BASE_URL
        return self._resolved_base_url

    def _get_username(self):
        if self.username:
            return self.username
        if self._resolved_username:
            return self._resolved_username
        data = self._proxy_data("status")
        self._resolved_username = (data.get("username") or "").strip()
        return self._resolved_username

    def _load_persisted_session(self):
        if self._persisted_session_loaded:
            return bool(self._session)
        self._persisted_session_loaded = True

        payload = _cms_config_get_json(GYING_SESSION_CONFIG_KEY) or {}
        cookies = payload.get("cookies") or {}
        if not isinstance(cookies, dict) or not cookies:
            return False

        try:
            import requests

            session = self._create_session()
            session.cookies = requests.utils.cookiejar_from_dict(cookies, cookiejar=None, overwrite=True)
            self._session = session
            self._session_login_at = int(payload.get("saved_at") or 0) or int(time.time())
            base_url = str(payload.get("base_url") or "").strip().rstrip("/")
            username = str(payload.get("username") or "").strip()
            if base_url:
                self._resolved_base_url = base_url
            if username:
                self._resolved_username = username
            return True
        except Exception:
            self._session = None
            self._session_login_at = 0
            return False

    def _persist_session(self):
        if not self._session:
            return False

        try:
            import requests

            cookies = requests.utils.dict_from_cookiejar(self._session.cookies)
        except Exception:
            return False

        if not cookies:
            return False

        now_ts = int(time.time())
        payload = {
            "cookies": cookies,
            "saved_at": now_ts,
            "updated_at": now_ts,
            "base_url": self._get_base_url(),
            "username": self._get_username(),
        }
        return _cms_config_set_json(GYING_SESSION_CONFIG_KEY, payload)

    def _is_bot_challenge_page(self, text):
        return bool(text and "正在确认你是不是机器人" in text and self.CHALLENGE_PATTERN.search(text))

    def _is_login_shell(self, text):
        if not text:
            return False
        return (
            "_BT.PC.HTML('login')" in text
            or '_BT.PC.HTML("login")' in text
            or "_BT.PC.HTML('nologin')" in text
            or '_BT.PC.HTML("nologin")' in text
            or "未登录，访问受限" in text
        )

    def _solve_bot_challenge(self, session, request_url, text):
        match = self.CHALLENGE_PATTERN.search(text or "")
        if not match:
            raise RuntimeError("未找到机器人验证数据")

        challenge = json.loads(match.group(1))
        challenge_id = challenge.get("id", "")
        targets = [str(item).lower() for item in (challenge.get("challenge") or [])]
        diff = int(challenge.get("diff") or 0)
        salt = str(challenge.get("salt") or "")
        if not challenge_id or not targets or diff <= 0 or not salt:
            raise RuntimeError("机器人验证数据无效")

        remain = {target: idx for idx, target in enumerate(targets)}
        nonces = [0] * len(targets)
        for nonce in range(diff + 1):
            digest = hashlib.sha256(f"{nonce}{salt}".encode()).hexdigest()
            idx = remain.pop(digest, None)
            if idx is not None:
                nonces[idx] = nonce
                if not remain:
                    break

        if remain:
            raise RuntimeError("机器人验证求解失败")

        form = [("action", "verify"), ("id", challenge_id)]
        form.extend(("nonce[]", str(nonce)) for nonce in nonces)
        resp = session.post(request_url, data=form, timeout=20)
        try:
            data = resp.json()
        except Exception:
            raise RuntimeError("机器人验证响应解析失败")
        if not data.get("success"):
            raise RuntimeError(data.get("msg") or "机器人验证失败")

    def _request_with_challenge_retry(self, session, method, request_url, data=None, headers=None):
        import requests
        for attempt in range(2):
            try:
                resp = session.request(
                    method,
                    request_url,
                    data=data,
                    headers=headers,
                    timeout=20,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                raise RuntimeError(str(exc))

            text = resp.text
            if self._is_bot_challenge_page(text):
                if attempt >= 1:
                    raise RuntimeError("重试后仍然进入机器人验证页")
                self._solve_bot_challenge(session, request_url, text)
                continue
            return resp, text
        raise RuntimeError("请求重试次数已耗尽")

    def _login(self, force=False):
        with self._lock:
            if not force and self._session and (time.time() - self._session_login_at) < GYING_SESSION_TTL_SECONDS:
                return True
            if not force and not self._session:
                self._load_persisted_session()
                if self._session and (time.time() - self._session_login_at) < GYING_SESSION_TTL_SECONDS:
                    return True

            username = self._get_username()
            password = self.password
            base_url = self._get_base_url()
            if not username or not password or not base_url:
                return False

            session = self._create_session()
            login_page_url = f"{base_url}/user/login/"
            login_api_url = f"{base_url}/user/login"
            warmup_url = f"{base_url}/mv/wkMn"

            try:
                resp, _ = self._request_with_challenge_retry(session, "GET", login_page_url)
                if resp.status_code != 200:
                    return False

                resp, text = self._request_with_challenge_retry(session, "POST", login_api_url, {
                    "code": "",
                    "siteid": "1",
                    "dosubmit": "1",
                    "cookietime": "10506240",
                    "username": username,
                    "password": password,
                })
                data = json.loads(text or "{}")
                code = data.get("code")
                try:
                    code_value = int(code)
                except Exception:
                    code_value = -1
                if code_value != 200:
                    return False

                try:
                    self._request_with_challenge_retry(session, "GET", warmup_url)
                except Exception:
                    pass
            except Exception:
                return False

            self._session = session
            self._session_login_at = time.time()
            self._persisted_session_loaded = True
            self._persist_session()
            return True

    def _authed_request(self, method, request_url, data=None, headers=None):
        with self._lock:
            if not self._login():
                return None, ""
            for attempt in range(2):
                session = self._session
                try:
                    resp, text = self._request_with_challenge_retry(
                        session,
                        method,
                        request_url,
                        data=data,
                        headers=headers,
                    )
                except Exception:
                    return None, ""
                if resp.status_code == 403 or self._is_login_shell(text):
                    if attempt >= 1 or not self._login(force=True):
                        return resp, text
                    continue
                return resp, text
            return None, ""

    def _prime_verified_cookie(self, session, request_url, headers=None):
        import requests

        try:
            resp = session.request(
                "GET",
                request_url,
                headers=headers,
                timeout=20,
                allow_redirects=True,
            )
        except requests.RequestException:
            return False

        text = resp.text or ""
        if not self._is_bot_challenge_page(text):
            return True

        try:
            self._solve_bot_challenge(session, request_url, text)
            self._persist_session()
            return True
        except Exception:
            return False

    def _session_cookie_header(self, session):
        try:
            import requests

            cookies = requests.utils.dict_from_cookiejar(session.cookies)
        except Exception:
            cookies = {}
        parts = []
        for key, value in (cookies or {}).items():
            key = str(key or "").strip()
            value = str(value or "").strip()
            if key:
                parts.append(f"{key}={value}")
        return "; ".join(parts)

    def _curl_request(self, request_url, headers=None):
        session = self._session
        if session is None:
            return None, ""

        skip_headers = {
            "accept-encoding",
            "connection",
            "content-length",
            "cookie",
            "host",
        }
        header_map = {}
        for key, value in (session.headers or {}).items():
            normalized_key = str(key or "").strip()
            if not normalized_key or normalized_key.lower() in skip_headers:
                continue
            header_map[normalized_key] = str(value)
        for key, value in (headers or {}).items():
            normalized_key = str(key or "").strip()
            if not normalized_key or normalized_key.lower() in skip_headers:
                continue
            header_map[normalized_key] = str(value)

        cookie_header = self._session_cookie_header(session)
        command = [
            "curl",
            "-sS",
            "--max-time",
            "20",
            "--compressed",
            "-L",
            request_url,
            "-w",
            f"\n{self.CURL_STATUS_MARKER}%{{http_code}}",
        ]
        for key, value in header_map.items():
            if key and value:
                command.extend(["-H", f"{key}: {value}"])
        if cookie_header:
            command.extend(["-H", f"Cookie: {cookie_header}"])

        try:
            completed = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except Exception:
            return None, ""

        if completed.returncode != 0:
            return None, ""

        output = completed.stdout or ""
        marker_index = output.rfind(self.CURL_STATUS_MARKER)
        if marker_index < 0:
            return None, output

        body = output[:marker_index]
        status_text = output[marker_index + len(self.CURL_STATUS_MARKER):].strip()
        try:
            status_code = int(status_text or 0)
        except Exception:
            status_code = 0
        return status_code, body

    def _authed_get_text(self, request_url, headers=None, prime_challenge=False, prefer_curl=False):
        with self._lock:
            if not self._login():
                return None, ""

            if prefer_curl:
                if prime_challenge:
                    self._prime_verified_cookie(self._session, request_url, headers=headers)
                status_code, text = self._curl_request(request_url, headers=headers)
                if status_code:
                    if status_code == 403 and self._login(force=True):
                        if prime_challenge:
                            self._prime_verified_cookie(self._session, request_url, headers=headers)
                        status_code, text = self._curl_request(request_url, headers=headers)
                    if status_code:
                        return status_code, text

            resp, text = self._authed_request("GET", request_url, headers=headers)
            if resp is None:
                return None, text
            return resp.status_code, text

    def _search_url(self, keyword):
        from urllib.parse import quote
        return f"{self._get_base_url()}/s/1---1/{quote(keyword)}"

    def _detail_url(self, resource_type, resource_id):
        return f"{self._get_base_url()}/res/downurl/{resource_type}/{resource_id}"

    def _extract_password_from_url(self, link_url):
        if "?pwd=" in link_url:
            match = re.search(r"[?&]pwd=([a-zA-Z0-9]+)", link_url)
            if match:
                return match.group(1)
        if "?password=" in link_url:
            match = re.search(r"[?&]password=([a-zA-Z0-9]+)", link_url)
            if match:
                return match.group(1)
        return ""

    def _title_with_year(self, search_data, index):
        titles = ((search_data.get("l") or {}).get("title") or [])
        years = ((search_data.get("l") or {}).get("year") or [])
        if index >= len(titles):
            return ""
        title = str(titles[index] or "").strip()
        year = years[index] if index < len(years) else ""
        if title and year:
            return f"{title}（{year}）"
        return title

    def _fetch_detail(self, resource_type, resource_id):
        detail_headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": f"{self._get_base_url()}/",
            "X-Requested-With": "XMLHttpRequest",
        }
        status_code, text = self._authed_get_text(
            self._detail_url(resource_type, resource_id),
            headers=detail_headers,
            prefer_curl=True,
        )
        if status_code != 200 or not text:
            return None
        try:
            data = json.loads(text)
        except Exception:
            return None
        if (data or {}).get("code") == 403:
            return None
        return data or {}

    def search(self, keyword, max_results=20, allowed_cloud_types=None):
        empty = {"success": False, "message": "观影直连未配置", "merged_by_type": {}, "total": 0, "raw_results": []}
        if not self.available:
            return empty

        allowed_types = {
            str(item or "").strip().lower()
            for item in (allowed_cloud_types or [])
            if str(item or "").strip()
        }
        if not allowed_types:
            allowed_types = None

        search_url = self._search_url(keyword)
        status_code, text = self._authed_get_text(search_url, prime_challenge=True, prefer_curl=True)
        if status_code is None:
            return {"success": False, "message": "观影直连请求失败", "merged_by_type": {}, "total": 0, "raw_results": []}
        if status_code != 200 or not text:
            return {"success": False, "message": f"观影直连返回 HTTP {status_code}", "merged_by_type": {}, "total": 0, "raw_results": []}

        match = self.SEARCH_PATTERN.search(text)
        if not match:
            return {"success": False, "message": "观影直连未找到搜索结果数据", "merged_by_type": {}, "total": 0, "raw_results": []}

        try:
            search_data = json.loads(match.group(1))
        except Exception:
            return {"success": False, "message": "观影直连搜索数据解析失败", "merged_by_type": {}, "total": 0, "raw_results": []}

        inner = search_data.get("l") or {}
        ids = inner.get("i") or []
        types = inner.get("d") or []
        titles = inner.get("title") or []

        merged = {}
        seen = set()
        raw_results = []
        candidate_indexes = []
        for idx, resource_id in enumerate(ids):
            if idx >= len(types) or idx >= len(titles):
                continue
            title = str(titles[idx] or "").strip()
            if not _is_precise_source_title_match(keyword, title):
                continue
            candidate_indexes.append(idx)
            if len(candidate_indexes) >= max_results:
                break

        for idx in candidate_indexes:
            resource_id = ids[idx]
            resource_type = types[idx]
            media_title = self._title_with_year(search_data, idx)
            detail = self._fetch_detail(resource_type, resource_id)
            if not detail:
                continue

            panlist = detail.get("panlist") or {}
            urls = panlist.get("url") or []
            names = panlist.get("name") or []
            passwords = panlist.get("p") or []
            tnames = panlist.get("tname") or []
            users = panlist.get("user") or []
            times = panlist.get("time") or []

            links = []
            for link_idx, raw_url in enumerate(urls):
                url = _sanitize_share_url(raw_url)
                if not url:
                    continue

                link_type = tnames[link_idx] if link_idx < len(tnames) else ""
                cloud_type = _detect_cloud_type(link_type, url)
                if cloud_type == "other":
                    continue
                if allowed_types and cloud_type not in allowed_types:
                    continue

                password = str(passwords[link_idx] or "").strip() if link_idx < len(passwords) else ""
                if not password:
                    password = self._extract_password_from_url(url)

                resource_name = str(names[link_idx] or "").strip() if link_idx < len(names) else ""
                note = resource_name or media_title or "未知资源"
                dedupe_key = (cloud_type, url, password)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)

                item = {
                    "title": media_title or note,
                    "note": note,
                    "resource_name": resource_name,
                    "url": url,
                    "password": password,
                    "source": "gying-direct",
                }
                if link_idx < len(users) and users[link_idx]:
                    item["user"] = users[link_idx]
                if link_idx < len(times) and times[link_idx]:
                    item["time"] = times[link_idx]
                if link_type:
                    item["pan_name"] = link_type

                merged.setdefault(cloud_type, []).append(item)
                links.append({
                    "type": link_type or cloud_type,
                    "url": url,
                    "password": password,
                    "name": resource_name,
                })

            if links:
                raw_results.append({
                    "title": media_title or "未知资源",
                    "links": links,
                })
            if allowed_types and all(len(merged.get(cloud_type, [])) >= max_results for cloud_type in allowed_types):
                break

        total = sum(len(v) for v in merged.values())
        return {
            "success": True,
            "message": "success",
            "merged_by_type": merged,
            "total": total,
            "raw_results": raw_results,
        }


gying_client = GyingClient(panso_client)
gying_direct_client = GyingDirectClient(gying_client)
_state = {"tg_bot": None, "wechat_instance": None}
_search_cache = {}
_search_cache_lock = threading.RLock()
_sub_sync_local = threading.local()
_sub_sync_patched = False
_event_service_patch_started = False
_tg_bot_patch_started = False
_inject_patch_lock = threading.Lock()
WX_LIST_SUBSCRIPTIONS_MENU_TITLE = "当前订阅"
WX_LIST_SUBSCRIPTIONS_COMMAND = "/current_subscriptions"
LEGACY_LIST_SUBSCRIPTIONS_COMMAND = "/cms_current_subscriptions"
WX_DOUBAN_HOT_MOVIE_MENU_TITLE = "豆瓣热门电影"
WX_DOUBAN_HOT_TV_MENU_TITLE = "豆瓣热门电视剧"
WX_DOUBAN_HOT_MOVIE_COMMAND = "/wx_douban_hot_movie"
WX_DOUBAN_HOT_TV_COMMAND = "/wx_douban_hot_tv"
GYING_SESSION_CONFIG_KEY = "cms_enhance_gying_cookie"
GYING_SESSION_TTL_SECONDS = 24 * 3600
WX_INTERACTIVE_CONTEXT_TTL_SECONDS = 5 * 60
TG_INTERACTIVE_CONTEXT_TTL_SECONDS = 5 * 60
WX_HDHIVE_CANDIDATE_PAGE_SIZE = 8
WX_HDHIVE_RESULT_PAGE_SIZE = 8
TG_SUBSCRIBE_CARD_PAGE_SIZE = 8
SEARCH_CACHE_TTL_SECONDS = 90
SEARCH_EMPTY_CACHE_TTL_SECONDS = 30
TMDB_SEARCH_CACHE_TTL_SECONDS = 5 * 60
TMDB_DETAIL_CACHE_TTL_SECONDS = 30 * 60
HDHIVE_DETAIL_CACHE_TTL_SECONDS = 60
DOUBAN_HOT_CACHE_TTL_SECONDS = 10 * 60
DOUBAN_HOT_DAILY_CACHE_TTL_SECONDS = 36 * 3600
DOUBAN_HOT_DISK_CACHE_PATH = "/tmp/cms_douban_hot_cache.json"
DOUBAN_HOT_REFRESH_HOUR = 0
DOUBAN_HOT_REFRESH_MINUTE = 5
DOUBAN_DEFAULT_LOC_ID = "108288"
DOUBAN_HOT_FETCH_COUNT = 20
DOUBAN_HOT_DISPLAY_COUNT = 20
DOUBAN_REQUEST_TIMEOUT_SECONDS = 20
WX_COMMAND_ACK_GRACE_SECONDS = 0.8
AUTO_UPDATE_DEFAULT_MANIFEST_URL = "https://raw.githubusercontent.com/gctts/cms-updates/main/manifest.json"
AUTO_UPDATE_STATE_PATH = "/tmp/cms_usercustomize_auto_update_state.json"
AUTO_UPDATE_REQUEST_TIMEOUT_SECONDS = 15
AUTO_UPDATE_START_DELAY_SECONDS = 3
AUTO_UPDATE_RESTART_DELAY_SECONDS = 2
AUTO_UPDATE_ATTEMPT_COOLDOWN_SECONDS = 10 * 60

CLOUD_TYPE_NAMES = {
    "115": "115网盘",
    "123": "123网盘",
    "magnet": "磁力链接",
    "quark": "夸克网盘",
    "baidu": "百度网盘",
    "aliyun": "阿里云盘",
    "uc": "UC网盘",
    "xunlei": "迅雷网盘",
    "tianyi": "天翼云盘",
    "mobile": "移动云盘",
}
ALLOWED_CLOUD_TYPES = {"115", "123", "magnet"}
WX_RESULT_CLOUD_TYPE_ORDER = ["115", "123", "magnet"]
SUB_SYNC_CHANNELS = {
    "hdhive": "__cms_virtual_hdhive__",
    "gying": "__cms_virtual_gying__",
    "panso": "__cms_virtual_panso__",
}
SUB_SYNC_SOURCE_NAMES = {"hdhive": "影巢", "gying": "观影", "panso": "盘搜"}
SUB_SYNC_CHANNEL_TO_SOURCE = {v: k for k, v in SUB_SYNC_CHANNELS.items()}

_CMS_DB_CANDIDATE_PATHS = [
    "/config/cms-online.db",
    "/cms/data/config/cms-online.db",
    "/cms/config/cms-online.db",
    "/data/config/cms-online.db",
]
_cms_db_cache = {"path": None, "loaded": False}
_cms_submedia_table_cache = {"name": None, "loaded": False}
_cms_proxy_cache = {"proxy": None, "loaded": False}
_cms_config_lock = threading.Lock()
_cms_submedia_lock = threading.Lock()
_cms_reconcile_state = {"last_run": 0.0, "running": False}
_cms_reconcile_lock = threading.Lock()
_recent_command_cache = {}
_recent_command_lock = threading.Lock()
_douban_hot_disk_cache_loaded = False
_douban_hot_disk_cache_day = ""
_douban_hot_disk_cache_lock = threading.RLock()
_douban_hot_refresh_state = {"started": False, "refreshing": False}
_douban_hot_refresh_lock = threading.Lock()
_wx_douban_hot_request_state = {}
_wx_douban_hot_request_lock = threading.Lock()
_auto_update_runtime_state = {"started": False, "checking": False, "restarting": False}
_auto_update_runtime_lock = threading.Lock()


def _build_search_cache_key(namespace, *parts):
    normalized = [str(namespace or "").strip().lower()]
    for part in parts:
        if isinstance(part, (list, tuple, set)):
            values = []
            for item in part:
                text = str(item or "").strip().lower()
                if text and text not in values:
                    values.append(text)
            normalized.append(tuple(sorted(values)))
        else:
            normalized.append(str(part or "").strip().lower())
    return tuple(normalized)


def _env_flag(name, default=False):
    raw_value = os.environ.get(name)
    if raw_value is None:
        return bool(default)
    text = str(raw_value).strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "true", "yes", "y", "on"}


def _safe_int(value, default=0):
    try:
        return int(str(value).strip())
    except Exception:
        return int(default)


def _load_json_file(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return copy.deepcopy(default)


def _write_json_file(path, payload):
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)
    temp_path = os.path.join(directory, f".{os.path.basename(path)}.{os.getpid()}.tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, path)


def _get_file_sha256(path):
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def _get_auto_update_target_path():
    configured = str(os.environ.get("CMS_AUTO_UPDATE_TARGET_PATH") or "").strip()
    target_path = configured or str(globals().get("__file__") or "").strip()
    if not target_path:
        return ""
    return os.path.abspath(target_path)


def _get_auto_update_target_name():
    configured = str(os.environ.get("CMS_AUTO_UPDATE_TARGET_NAME") or "").strip()
    if configured:
        return configured
    target_path = _get_auto_update_target_path()
    return os.path.basename(target_path) if target_path else ""


def _load_auto_update_state():
    payload = _load_json_file(AUTO_UPDATE_STATE_PATH, default={})
    return payload if isinstance(payload, dict) else {}


def _save_auto_update_state(**updates):
    payload = _load_auto_update_state()
    payload.update(updates)
    payload["updated_at"] = int(time.time())
    try:
        _write_json_file(AUTO_UPDATE_STATE_PATH, payload)
    except Exception:
        pass
    return payload


def _normalize_auto_update_manifest(manifest):
    if not isinstance(manifest, dict):
        return {}
    normalized = dict(manifest)
    channel = str(os.environ.get("CMS_AUTO_UPDATE_CHANNEL") or "stable").strip().lower()
    channels = manifest.get("channels")
    if isinstance(channels, dict):
        channel_payload = channels.get(channel) or channels.get("stable")
        if isinstance(channel_payload, dict):
            merged = dict(manifest)
            merged.update(channel_payload)
            normalized = merged
    targets = normalized.get("targets")
    if isinstance(targets, dict):
        target_name = _get_auto_update_target_name()
        target_payload = targets.get(target_name)
        if not isinstance(target_payload, dict) and target_name:
            target_payload = targets.get(os.path.basename(target_name))
        if isinstance(target_payload, dict):
            merged = dict(normalized)
            merged.update(target_payload)
            normalized = merged
    return normalized


def _fetch_auto_update_manifest():
    manifest_url = str(os.environ.get("CMS_AUTO_UPDATE_MANIFEST_URL") or AUTO_UPDATE_DEFAULT_MANIFEST_URL).strip()
    if not manifest_url:
        return {}
    import requests

    timeout = max(5, _safe_int(os.environ.get("CMS_AUTO_UPDATE_TIMEOUT"), AUTO_UPDATE_REQUEST_TIMEOUT_SECONDS))
    resp = requests.get(
        manifest_url,
        timeout=timeout,
        headers={"Cache-Control": "no-cache", "Pragma": "no-cache"},
    )
    resp.raise_for_status()
    payload = resp.json()
    normalized = _normalize_auto_update_manifest(payload)
    normalized["manifest_url"] = manifest_url
    return normalized


def _download_auto_update_file(download_url, target_path, expected_sha256="", expected_size=0):
    import requests

    target_dir = os.path.dirname(target_path) or "."
    os.makedirs(target_dir, exist_ok=True)
    timeout = max(5, _safe_int(os.environ.get("CMS_AUTO_UPDATE_TIMEOUT"), AUTO_UPDATE_REQUEST_TIMEOUT_SECONDS))
    temp_path = os.path.join(
        target_dir,
        f".{os.path.basename(target_path)}.{os.getpid()}.{int(time.time())}.download",
    )
    sha256 = hashlib.sha256()
    size = 0
    try:
        with requests.get(download_url, timeout=timeout, stream=True) as resp:
            resp.raise_for_status()
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if not chunk:
                        continue
                    size += len(chunk)
                    sha256.update(chunk)
                    f.write(chunk)
        actual_sha256 = sha256.hexdigest()
        expected_sha256 = str(expected_sha256 or "").strip().lower()
        expected_size = int(expected_size or 0)
        if expected_size > 0 and size != expected_size:
            raise RuntimeError(f"更新文件大小不匹配，本地={size} 远端={expected_size}")
        if expected_sha256 and actual_sha256 != expected_sha256:
            raise RuntimeError(f"更新文件校验失败，本地={actual_sha256} 远端={expected_sha256}")
        return temp_path, actual_sha256, size
    except Exception:
        try:
            os.remove(temp_path)
        except Exception:
            pass
        raise


def _request_auto_update_restart(reason):
    mode = str(os.environ.get("CMS_AUTO_UPDATE_RESTART_MODE") or "auto").strip().lower()
    if mode not in {"auto", "process", "pid1", "none"}:
        mode = "auto"
    if mode == "none":
        _log(f"自动更新已完成，但当前配置禁止自动重启：{reason}")
        return False

    with _auto_update_runtime_lock:
        if _auto_update_runtime_state.get("restarting"):
            return True
        _auto_update_runtime_state["restarting"] = True

    delay_seconds = max(1, _safe_int(os.environ.get("CMS_AUTO_UPDATE_RESTART_DELAY"), AUTO_UPDATE_RESTART_DELAY_SECONDS))

    def _restart_worker():
        time.sleep(delay_seconds)
        _log(f"检测到新版本，准备重启容器加载更新：{reason}")
        if mode in {"auto", "pid1"} and os.getpid() != 1:
            try:
                os.kill(1, signal.SIGTERM)
                time.sleep(3)
            except Exception:
                pass
        os._exit(95)

    threading.Thread(target=_restart_worker, daemon=True).start()
    return True


def _check_auto_update_once():
    target_path = _get_auto_update_target_path()
    if not target_path:
        return {"status": "skipped", "message": "未找到当前运行文件，无法检查更新"}
    if not target_path.endswith((".py", ".so")):
        _log(f"自动更新已跳过，当前挂载文件不是可更新目标：{target_path}")
        return {"status": "skipped", "message": f"当前文件不支持自动更新：{os.path.basename(target_path)}"}
    if not os.path.isfile(target_path):
        _log(f"自动更新已跳过，未找到目标文件：{target_path}")
        return {"status": "skipped", "message": f"未找到目标文件：{target_path}"}

    current_sha256 = _get_file_sha256(target_path)
    current_size = os.path.getsize(target_path)
    state = _load_auto_update_state()
    now = int(time.time())

    manifest = _fetch_auto_update_manifest()
    remote_sha256 = str(manifest.get("sha256") or "").strip().lower()
    remote_version = str(manifest.get("version") or "").strip()
    download_url = str(manifest.get("download_url") or "").strip()
    remote_size = int(manifest.get("size") or 0)
    remote_name = str(manifest.get("name") or _get_auto_update_target_name()).strip()

    if not remote_sha256 or not download_url:
        raise RuntimeError("远端 manifest 缺少 sha256 或 download_url")

    if remote_sha256 == current_sha256:
        _save_auto_update_state(
            last_result="up_to_date",
            last_checked_at=now,
            last_version=remote_version,
            last_remote_sha256=remote_sha256,
            current_sha256=current_sha256,
            current_size=current_size,
            target_path=target_path,
            target_name=remote_name,
        )
        version_text = remote_version or remote_sha256[:12]
        _log(f"自动更新检查完成，当前已是最新版本：{version_text}")
        return {"status": "up_to_date", "message": f"当前已是最新版本：{version_text}", "version": version_text}

    cooldown_seconds = max(
        60,
        _safe_int(os.environ.get("CMS_AUTO_UPDATE_ATTEMPT_COOLDOWN"), AUTO_UPDATE_ATTEMPT_COOLDOWN_SECONDS),
    )
    last_attempt_sha = str(state.get("last_attempt_sha256") or "").strip().lower()
    last_attempt_at = _safe_int(state.get("last_attempt_at"), 0)
    last_attempt_target_name = str(
        state.get("last_attempt_target_name")
        or state.get("target_name")
        or ""
    ).strip()
    if (
        last_attempt_target_name == remote_name
        and last_attempt_sha == remote_sha256
        and (now - last_attempt_at) < cooldown_seconds
    ):
        version_text = remote_version or remote_sha256[:12]
        _log(f"自动更新检查到新版本，但冷却期内不重复拉取：{version_text}")
        return {
            "status": "cooldown",
            "message": f"检测到新版本 {version_text}，但冷却期内不重复下载",
            "version": version_text,
        }

    _save_auto_update_state(
        last_result="downloading",
        last_checked_at=now,
        last_attempt_at=now,
        last_attempt_sha256=remote_sha256,
        last_attempt_target_name=remote_name,
        last_version=remote_version,
        target_path=target_path,
        target_name=remote_name,
    )
    temp_path, downloaded_sha256, downloaded_size = _download_auto_update_file(
        download_url=download_url,
        target_path=target_path,
        expected_sha256=remote_sha256,
        expected_size=remote_size,
    )
    os.replace(temp_path, target_path)
    _save_auto_update_state(
        last_result="applied",
        last_checked_at=now,
        last_applied_at=int(time.time()),
        last_applied_sha256=downloaded_sha256,
        last_version=remote_version,
        last_remote_sha256=remote_sha256,
        current_sha256=downloaded_sha256,
        current_size=downloaded_size,
        target_path=target_path,
        target_name=remote_name,
    )
    _log(
        "自动更新已应用新版本："
        f"{remote_version or remote_sha256[:12]} "
        f"({current_size} -> {downloaded_size} bytes)"
    )
    version_text = remote_version or remote_sha256[:12]
    restart_requested = _request_auto_update_restart(version_text)
    if restart_requested:
        message = f"已下载更新 {version_text}，容器即将重启加载新版本"
    else:
        message = f"已下载更新 {version_text}，但当前配置未自动重启"
    return {
        "status": "applied",
        "message": message,
        "version": version_text,
        "restart_requested": restart_requested,
    }


def _begin_auto_update_check():
    with _auto_update_runtime_lock:
        if _auto_update_runtime_state.get("checking"):
            return False
        _auto_update_runtime_state["checking"] = True
        return True


def _finish_auto_update_check():
    with _auto_update_runtime_lock:
        _auto_update_runtime_state["checking"] = False


def _start_auto_update_scheduler():
    default_enabled = bool(str(os.environ.get("CMS_AUTO_UPDATE_MANIFEST_URL") or "").strip())
    if not _env_flag("CMS_AUTO_UPDATE_ENABLED", default=default_enabled):
        return
    with _auto_update_runtime_lock:
        if _auto_update_runtime_state.get("started"):
            return
        _auto_update_runtime_state["started"] = True

    delay_seconds = max(0, _safe_int(os.environ.get("CMS_AUTO_UPDATE_START_DELAY"), AUTO_UPDATE_START_DELAY_SECONDS))

    def _worker():
        if delay_seconds:
            time.sleep(delay_seconds)
        if not _begin_auto_update_check():
            return
        try:
            _check_auto_update_once()
        except Exception as exc:
            _save_auto_update_state(
                last_result="error",
                last_checked_at=int(time.time()),
                last_error=str(exc),
                target_path=_get_auto_update_target_path(),
            )
            _log(f"自动更新检查失败：{exc}")
        finally:
            _finish_auto_update_check()

    threading.Thread(target=_worker, daemon=True, name="cms-auto-update").start()


def _get_cached_search_result(cache_key):
    now = time.time()
    with _search_cache_lock:
        entry = _search_cache.get(cache_key)
        if not isinstance(entry, dict) or not entry.get("__cms_ttl__"):
            return None
        if float(entry.get("expires_at", 0)) <= now:
            _search_cache.pop(cache_key, None)
            return None
        return copy.deepcopy(entry.get("value"))


def _set_cached_search_result(cache_key, value, ttl=SEARCH_CACHE_TTL_SECONDS):
    with _search_cache_lock:
        _search_cache[cache_key] = {
            "__cms_ttl__": True,
            "expires_at": time.time() + max(1, int(ttl or 1)),
            "value": copy.deepcopy(value),
        }
    return value


def _douban_hot_cache_day():
    return time.strftime("%Y-%m-%d", time.localtime())


def _douban_hot_rows_cache_key(hot_type, limit):
    return _build_search_cache_key(
        "douban_hot_rows",
        str(hot_type or "").strip().lower(),
        f"limit{int(limit or DOUBAN_HOT_DISPLAY_COUNT)}",
        f"fetch{int(DOUBAN_HOT_FETCH_COUNT or 0)}",
    )


def _load_douban_hot_disk_cache():
    global _douban_hot_disk_cache_day, _douban_hot_disk_cache_loaded
    if _douban_hot_disk_cache_loaded:
        return
    with _douban_hot_disk_cache_lock:
        if _douban_hot_disk_cache_loaded:
            return
        try:
            with open(DOUBAN_HOT_DISK_CACHE_PATH, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            payload = {}
        _douban_hot_disk_cache_day = str((payload or {}).get("day") or "").strip()

        entries = payload.get("entries") if isinstance(payload, dict) else {}
        if isinstance(entries, dict):
            now = time.time()
            with _search_cache_lock:
                for _, item in entries.items():
                    if not isinstance(item, dict):
                        continue
                    cache_key = item.get("cache_key")
                    rows = item.get("rows")
                    expires_at = float(item.get("expires_at") or 0)
                    if not isinstance(cache_key, list) or not isinstance(rows, list) or expires_at <= now:
                        continue
                    _search_cache[tuple(cache_key)] = {
                        "__cms_ttl__": True,
                        "expires_at": expires_at,
                        "value": copy.deepcopy(rows),
                    }
        _douban_hot_disk_cache_loaded = True


def _persist_douban_hot_disk_cache():
    global _douban_hot_disk_cache_day
    with _douban_hot_disk_cache_lock:
        entries = {}
        now = time.time()
        cache_day = _douban_hot_cache_day()
        with _search_cache_lock:
            for cache_key, entry in list(_search_cache.items()):
                if not isinstance(cache_key, tuple) or not cache_key:
                    continue
                if cache_key[0] != "douban_hot_rows":
                    continue
                if not isinstance(entry, dict) or not entry.get("__cms_ttl__"):
                    continue
                expires_at = float(entry.get("expires_at") or 0)
                if expires_at <= now:
                    continue
                entries["|".join(str(part) for part in cache_key)] = {
                    "cache_key": list(cache_key),
                    "expires_at": expires_at,
                    "rows": copy.deepcopy(entry.get("value") or []),
                }
        payload = {
            "updated_at": now,
            "day": cache_day,
            "entries": entries,
        }
        try:
            with open(DOUBAN_HOT_DISK_CACHE_PATH, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            _douban_hot_disk_cache_day = cache_day
        except Exception:
            pass


def _get_douban_hot_cached_rows(hot_type, limit=DOUBAN_HOT_DISPLAY_COUNT):
    _load_douban_hot_disk_cache()
    cached = _get_cached_search_result(_douban_hot_rows_cache_key(hot_type, limit))
    if isinstance(cached, list) and not cached:
        return None
    return cached


def _set_douban_hot_cached_rows(hot_type, limit, rows, ttl=DOUBAN_HOT_DAILY_CACHE_TTL_SECONDS):
    rows = list(rows or [])
    if not rows:
        return []
    cache_key = _douban_hot_rows_cache_key(hot_type, limit)
    value = _set_cached_search_result(cache_key, rows, ttl)
    _persist_douban_hot_disk_cache()
    return value


def _begin_wx_douban_hot_request(hot_type, stale_after=30):
    hot_type = str(hot_type or "").strip().lower() or "unknown"
    now = time.time()
    with _wx_douban_hot_request_lock:
        state = dict(_wx_douban_hot_request_state.get(hot_type) or {})
        started_at = float(state.get("started_at") or 0)
        if state.get("running") and started_at and (now - started_at) < max(1, int(stale_after or 30)):
            state["updated_at"] = now
            _wx_douban_hot_request_state[hot_type] = state
            return False
        _wx_douban_hot_request_state[hot_type] = {
            "running": True,
            "started_at": now,
            "updated_at": now,
            "notice_at": float(state.get("notice_at") or 0),
        }
        return True


def _mark_wx_douban_hot_notice(hot_type):
    hot_type = str(hot_type or "").strip().lower() or "unknown"
    now = time.time()
    with _wx_douban_hot_request_lock:
        state = dict(_wx_douban_hot_request_state.get(hot_type) or {})
        state["notice_at"] = now
        state["updated_at"] = now
        _wx_douban_hot_request_state[hot_type] = state


def _should_notify_wx_douban_hot_pending(hot_type, min_interval=1.5):
    hot_type = str(hot_type or "").strip().lower() or "unknown"
    now = time.time()
    with _wx_douban_hot_request_lock:
        state = dict(_wx_douban_hot_request_state.get(hot_type) or {})
        last_notice_at = float(state.get("notice_at") or 0)
        if last_notice_at and (now - last_notice_at) < float(min_interval or 0):
            return False
        state["notice_at"] = now
        state["updated_at"] = now
        _wx_douban_hot_request_state[hot_type] = state
        return True


def _finish_wx_douban_hot_request(hot_type):
    hot_type = str(hot_type or "").strip().lower() or "unknown"
    with _wx_douban_hot_request_lock:
        state = dict(_wx_douban_hot_request_state.get(hot_type) or {})
        if not state:
            return
        state["running"] = False
        state["updated_at"] = time.time()
        _wx_douban_hot_request_state[hot_type] = state


def _refresh_douban_hot_cache(force=False):
    _load_douban_hot_disk_cache()
    with _douban_hot_refresh_lock:
        if _douban_hot_refresh_state.get("refreshing"):
            return
        _douban_hot_refresh_state["refreshing"] = True

    try:
        today = _douban_hot_cache_day()
        for hot_type in ("movie", "tv"):
            cache_key = _douban_hot_rows_cache_key(hot_type, DOUBAN_HOT_DISPLAY_COUNT)
            cached = _get_cached_search_result(cache_key)
            if not force and cached and _douban_hot_disk_cache_day == today:
                continue
            rows = _build_douban_hot_subscription_rows(
                hot_type,
                limit=DOUBAN_HOT_DISPLAY_COUNT,
                use_cache=False,
            )
            if rows:
                _set_douban_hot_cached_rows(
                    hot_type,
                    DOUBAN_HOT_DISPLAY_COUNT,
                    rows,
                    ttl=DOUBAN_HOT_DAILY_CACHE_TTL_SECONDS,
                )
    except Exception as e:
        pass
    finally:
        with _douban_hot_refresh_lock:
            _douban_hot_refresh_state["refreshing"] = False


def _start_douban_hot_refresh_scheduler():
    if _douban_hot_refresh_state.get("started"):
        return
    with _douban_hot_refresh_lock:
        if _douban_hot_refresh_state.get("started"):
            return
        _douban_hot_refresh_state["started"] = True

    def _worker():
        time.sleep(15)
        _refresh_douban_hot_cache(force=False)
        while True:
            now = time.localtime()
            target_ts = time.mktime((
                now.tm_year,
                now.tm_mon,
                now.tm_mday,
                DOUBAN_HOT_REFRESH_HOUR,
                DOUBAN_HOT_REFRESH_MINUTE,
                0,
                now.tm_wday,
                now.tm_yday,
                now.tm_isdst,
            ))
            current_ts = time.time()
            if target_ts <= current_ts:
                target_ts += 24 * 3600
            sleep_seconds = max(60, int(target_ts - current_ts))
            time.sleep(sleep_seconds)
            _refresh_douban_hot_cache(force=True)

    threading.Thread(target=_worker, daemon=True).start()


def _get_cms_db_path():
    if _cms_db_cache["loaded"]:
        return _cms_db_cache["path"]

    for db_path in _CMS_DB_CANDIDATE_PATHS:
        if os.path.exists(db_path):
            _cms_db_cache["path"] = db_path
            _cms_db_cache["loaded"] = True
            return db_path

    _cms_db_cache["loaded"] = True
    return None


def _cms_config_get_json(key):
    db_path = _get_cms_db_path()
    if not db_path or not key:
        return None

    try:
        import sqlite3

        with _cms_config_lock:
            conn = sqlite3.connect(db_path, timeout=10)
            row = conn.execute("SELECT config_json FROM cms_config WHERE key=?", (key,)).fetchone()
            conn.close()
        if not row or not row[0]:
            return None
        return json.loads(row[0])
    except Exception:
        return None


def _normalize_tmdb_api_domain(domain):
    domain = str(domain or "").strip().rstrip("/")
    if not domain:
        return ""
    if not domain.endswith("/3"):
        domain += "/3"
    return domain


def _tmdb_iso_country_name(code):
    mapping = {
        "CN": "中国大陆",
        "HK": "中国香港",
        "TW": "中国台湾",
        "US": "美国",
        "GB": "英国",
        "JP": "日本",
        "KR": "韩国",
        "FR": "法国",
        "DE": "德国",
        "IT": "意大利",
        "ES": "西班牙",
        "TH": "泰国",
        "IN": "印度",
    }
    code = str(code or "").strip().upper()
    return mapping.get(code, code)


def _tmdb_iso_language_name(code):
    mapping = {
        "zh": "汉语普通话",
        "cn": "汉语普通话",
        "en": "英语",
        "ja": "日语",
        "ko": "韩语",
        "fr": "法语",
        "de": "德语",
        "es": "西班牙语",
        "th": "泰语",
        "hi": "印地语",
    }
    code = str(code or "").strip().lower()
    return mapping.get(code, code)


def _tmdb_unique_texts(values, limit=None):
    results = []
    for value in values or []:
        text = re.sub(r"\s+", " ", str(value or "").strip())
        if not text or text in results:
            continue
        results.append(text)
        if limit and len(results) >= int(limit):
            break
    return results


def _tmdb_person_jobs(person):
    jobs = []
    if isinstance(person, dict):
        if person.get("job"):
            jobs.append(str(person.get("job") or "").strip())
        for item in person.get("jobs") or []:
            if isinstance(item, dict) and item.get("job"):
                jobs.append(str(item.get("job") or "").strip())
    return _tmdb_unique_texts(jobs)


def _tmdb_collect_crew_names(crew_items, keywords, limit=5):
    names = []
    keywords = [str(item or "").strip().lower() for item in (keywords or []) if str(item or "").strip()]
    for person in crew_items or []:
        if not isinstance(person, dict):
            continue
        jobs = [job.lower() for job in _tmdb_person_jobs(person)]
        if not jobs:
            continue
        matched = False
        for job in jobs:
            if any(keyword in job for keyword in keywords):
                matched = True
                break
        if matched:
            names.append(person.get("name"))
    return _tmdb_unique_texts(names, limit=limit)


def _tmdb_top_cast_names(cast_items, limit=5):
    normalized = []
    for idx, item in enumerate(cast_items or []):
        if not isinstance(item, dict):
            continue
        order = item.get("order")
        if order is None:
            order = idx
        total_episode_count = item.get("total_episode_count")
        try:
            total_episode_count = int(total_episode_count or 0)
        except Exception:
            total_episode_count = 0
        normalized.append((int(order), -total_episode_count, item.get("name")))
    normalized.sort(key=lambda item: (item[0], item[1]))
    return _tmdb_unique_texts([item[2] for item in normalized], limit=limit)


def _truncate_overview(text, limit=120):
    text = re.sub(r"\s+", " ", str(text or "").strip())
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(0, int(limit) - 1)].rstrip() + "…"


def _get_tmdb_config():
    config = _cms_config_get_json("tmdb") or {}
    if not config:
        core_config = _cms_config_get_json("core") or {}
        config = core_config.get("tmdb") or {}
    return {
        "api_domain": _normalize_tmdb_api_domain(config.get("TMDB_API_DOMAIN") or ""),
        "img_domain": str(config.get("TMDB_IMG_DOMAIN") or "").strip().rstrip("/"),
        "api_key": str(config.get("TMDB_API_KEY") or "").strip(),
        "language": str(config.get("TMDB_LANGUAGE") or "").strip() or "zh-CN",
    }


def _build_tmdb_image_url(path, size="w500"):
    path = str(path or "").strip()
    if not path:
        return ""
    if re.match(r"^https?://", path, re.I):
        return path

    config = _get_tmdb_config()
    img_domain = config.get("img_domain") or ""
    if not img_domain:
        return ""
    if not path.startswith("/"):
        path = "/" + path
    return f"{img_domain}/t/p/{size}{path}"


def _tmdb_request_json(path, params=None):
    config = _get_tmdb_config()
    api_domain = config.get("api_domain") or ""
    api_key = config.get("api_key") or ""
    if not api_domain or not api_key:
        return None

    import requests

    query = {"api_key": api_key}
    language = config.get("language") or ""
    if language:
        query["language"] = language
    for key, value in (params or {}).items():
        if value is None or value == "":
            continue
        query[key] = value

    proxy = os.environ.get("HDHIVE_PROXY", "") or _get_cms_proxy()
    proxies = {"http": proxy, "https": proxy} if proxy else None

    try:
        resp = requests.get(
            f"{api_domain}/{str(path or '').lstrip('/')}",
            params=query,
            timeout=15,
            proxies=proxies,
        )
        if resp.status_code != 200:
            return None
        return resp.json()
    except Exception:
        return None


def _fetch_tmdb_detail(tmdb_id, media_type):
    tmdb_id = str(tmdb_id or "").strip()
    media_type = str(media_type or "").strip().lower()
    if not tmdb_id or media_type not in {"movie", "tv"}:
        return None

    cache_key = _build_search_cache_key("tmdb_detail", media_type, tmdb_id)
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    append_value = "credits,external_ids" if media_type == "movie" else "aggregate_credits,external_ids"
    payload = _tmdb_request_json(f"{media_type}/{tmdb_id}", {"append_to_response": append_value})
    if not isinstance(payload, dict):
        return _set_cached_search_result(cache_key, None, SEARCH_EMPTY_CACHE_TTL_SECONDS)

    title = (
        payload.get("title")
        or payload.get("name")
        or payload.get("original_title")
        or payload.get("original_name")
        or ""
    )
    date_text = payload.get("release_date") or payload.get("first_air_date") or ""
    year = str(date_text).split("-")[0].strip() if date_text else ""
    genres = _tmdb_unique_texts([
        item.get("name")
        for item in (payload.get("genres") or [])
        if isinstance(item, dict)
    ], limit=4)
    countries = _tmdb_unique_texts([
        item.get("name")
        for item in (payload.get("production_countries") or [])
        if isinstance(item, dict) and item.get("name")
    ], limit=3)
    if not countries:
        countries = _tmdb_unique_texts([
            _tmdb_iso_country_name(code)
            for code in (payload.get("origin_country") or [])
        ], limit=3)
    languages = _tmdb_unique_texts([
        item.get("name")
        for item in (payload.get("spoken_languages") or [])
        if isinstance(item, dict) and item.get("name")
    ], limit=3)
    if not languages:
        languages = _tmdb_unique_texts([
            _tmdb_iso_language_name(code)
            for code in (payload.get("languages") or [])
        ], limit=3)
    if not languages:
        languages = _tmdb_unique_texts([
            _tmdb_iso_language_name(payload.get("original_language"))
        ], limit=1)

    cast_items = []
    crew_items = []
    if media_type == "tv":
        cast_items = ((payload.get("aggregate_credits") or {}).get("cast") or [])
        crew_items = ((payload.get("aggregate_credits") or {}).get("crew") or [])
    else:
        cast_items = ((payload.get("credits") or {}).get("cast") or [])
        crew_items = ((payload.get("credits") or {}).get("crew") or [])

    directors = _tmdb_collect_crew_names(crew_items, ["director"], limit=4)
    writers = _tmdb_collect_crew_names(
        crew_items,
        ["writer", "screenplay", "story", "teleplay", "series composition"],
        limit=5,
    )
    creators = _tmdb_unique_texts([
        item.get("name")
        for item in (payload.get("created_by") or [])
        if isinstance(item, dict) and item.get("name")
    ], limit=4)
    cast = _tmdb_top_cast_names(cast_items, limit=5)

    runtime_minutes = None
    if media_type == "movie":
        try:
            runtime_minutes = int(payload.get("runtime") or 0) or None
        except Exception:
            runtime_minutes = None
    else:
        try:
            runtime_minutes = int(((payload.get("episode_run_time") or [0])[0]) or 0) or None
        except Exception:
            runtime_minutes = None

    imdb_id = str(payload.get("imdb_id") or "").strip()
    if not imdb_id:
        imdb_id = str(((payload.get("external_ids") or {}).get("imdb_id")) or "").strip()

    detail = {
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "title": title,
        "year": year,
        "vote_average": payload.get("vote_average") or 0,
        "genres": genres,
        "overview": _truncate_overview(payload.get("overview"), limit=120),
        "countries": countries,
        "languages": languages,
        "date": str(date_text or "").strip(),
        "directors": directors,
        "writers": writers,
        "creators": creators,
        "cast": cast,
        "episode_count": int(payload.get("number_of_episodes") or 0) or None,
        "season_count": int(payload.get("number_of_seasons") or 0) or None,
        "runtime_minutes": runtime_minutes,
        "imdb_id": imdb_id,
        "status": str(payload.get("status") or "").strip(),
        "poster_path": str(payload.get("poster_path") or "").strip(),
        "backdrop_path": str(payload.get("backdrop_path") or "").strip(),
        "poster_url": _build_tmdb_image_url(payload.get("poster_path"), size="w500"),
        "backdrop_url": _build_tmdb_image_url(payload.get("backdrop_path"), size="original"),
    }
    return _set_cached_search_result(cache_key, detail, TMDB_DETAIL_CACHE_TTL_SECONDS)


def _format_tmdb_rating(value):
    try:
        value = float(value)
    except Exception:
        value = 0
    return f"{value:.1f}" if value > 0 else "暂无评分"


def _format_subscribe_detail_lines(detail, media_type, fallback_year=""):
    if not isinstance(detail, dict):
        detail = {}

    media_type = str(media_type or detail.get("media_type") or "").strip().lower()
    type_name = "电视剧" if media_type == "tv" else "电影"
    year = str(detail.get("year") or fallback_year or "").strip()
    genres = " / ".join(detail.get("genres") or [])
    details = [f"类型：{type_name}" + (f"｜{genres}" if genres else "")]
    if year:
        details.append(f"年份：{year}")

    directors = " / ".join(detail.get("directors") or [])
    if directors:
        details.append(f"导演：{directors}")

    writers = " / ".join(detail.get("writers") or [])
    if writers:
        details.append(f"编剧：{writers}")
    elif media_type == "tv":
        creators = " / ".join(detail.get("creators") or [])
        if creators:
            details.append(f"主创：{creators}")

    cast = " / ".join(detail.get("cast") or [])
    if cast:
        details.append(f"主演：{cast}")

    countries = " / ".join(detail.get("countries") or [])
    if countries:
        details.append(f"地区：{countries}")

    languages = " / ".join(detail.get("languages") or [])
    if languages:
        details.append(f"语言：{languages}")

    date_text = str(detail.get("date") or "").strip()
    if date_text:
        details.append(f"{'首播' if media_type == 'tv' else '上映'}：{date_text}")

    if media_type == "tv":
        episode_count = detail.get("episode_count")
        if episode_count:
            details.append(f"集数：{episode_count}")
        runtime_minutes = detail.get("runtime_minutes")
        if runtime_minutes:
            details.append(f"单集：{runtime_minutes}分钟")
    else:
        runtime_minutes = detail.get("runtime_minutes")
        if runtime_minutes:
            details.append(f"片长：{runtime_minutes}分钟")

    tmdb_id = str(detail.get("tmdb_id") or "").strip()
    tmdb_url = _build_tmdb_url(media_type, tmdb_id)
    if tmdb_url:
        details.append(f"TMDB：{tmdb_url}")

    overview = str(detail.get("overview") or "").strip()
    if overview:
        details.append(f"简介：{overview}")

    return details


def _get_cms_submedia_table():
    if _cms_submedia_table_cache["loaded"]:
        return _cms_submedia_table_cache["name"]

    db_path = _get_cms_db_path()
    if not db_path:
        _cms_submedia_table_cache["loaded"] = True
        return None

    try:
        import sqlite3

        with _cms_submedia_lock:
            conn = sqlite3.connect(db_path, timeout=10)
            row = conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name IN ('sub_media1', 'sub_media')
                ORDER BY CASE name WHEN 'sub_media1' THEN 0 ELSE 1 END
                LIMIT 1
                """
            ).fetchone()
            conn.close()

        _cms_submedia_table_cache["name"] = row[0] if row else None
        _cms_submedia_table_cache["loaded"] = True
        return _cms_submedia_table_cache["name"]
    except Exception:
        _cms_submedia_table_cache["loaded"] = True
        return None


def _cms_config_set_json(key, value):
    db_path = _get_cms_db_path()
    if not db_path or not key:
        return False

    try:
        import sqlite3
        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        payload = json.dumps(value or {}, ensure_ascii=False)

        with _cms_config_lock:
            conn = sqlite3.connect(db_path, timeout=10)
            row = conn.execute("SELECT id FROM cms_config WHERE key=?", (key,)).fetchone()
            if row:
                conn.execute(
                    "UPDATE cms_config SET config_json=?, last_update_time=? WHERE key=?",
                    (payload, now, key),
                )
            else:
                conn.execute(
                    "INSERT INTO cms_config (key, config_json, create_time, last_update_time) VALUES (?, ?, ?, ?)",
                    (key, payload, now, now),
                )
            conn.commit()
            conn.close()
        return True
    except Exception:
        return False


def _cms_list_submedia_rows_db(status=None):
    db_path = _get_cms_db_path()
    table = _get_cms_submedia_table()
    if not db_path or not table:
        return []

    try:
        import sqlite3

        sql = f"SELECT * FROM {table}"
        params = []
        if status is not None:
            sql += " WHERE status=?"
            params.append(int(status))
        sql += " ORDER BY id DESC"

        with _cms_submedia_lock:
            conn = sqlite3.connect(db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
            conn.close()
        return rows
    except Exception:
        return []


def _cms_find_submedia_rows_db(tmdb_id=None, media_type=None, title=None, year=None, status=None):
    db_path = _get_cms_db_path()
    table = _get_cms_submedia_table()
    if not db_path or not table:
        return []

    clauses = []
    params = []

    if tmdb_id:
        clauses.append("trim(ifnull(tmdb_id, ''))=?")
        params.append(str(tmdb_id).strip())
    if media_type:
        clauses.append("trim(ifnull(type, ''))=?")
        params.append(str(media_type).strip())
    if title:
        clauses.append("trim(ifnull(title, ''))=?")
        params.append(str(title).strip())
    if year:
        clauses.append("trim(ifnull(year, ''))=?")
        params.append(str(year).strip())
    if status is not None:
        clauses.append("status=?")
        params.append(int(status))

    sql = f"SELECT * FROM {table}"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC"

    try:
        import sqlite3

        with _cms_submedia_lock:
            conn = sqlite3.connect(db_path, timeout=10)
            conn.row_factory = sqlite3.Row
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
            conn.close()
        return rows
    except Exception:
        return []


def _cms_update_submedia_status_db(ids, status):
    ids = [int(item) for item in (ids or []) if str(item).strip()]
    if not ids:
        return False, "缺少ID"

    db_path = _get_cms_db_path()
    table = _get_cms_submedia_table()
    if not db_path or not table:
        return False, "未找到CMS订阅表"

    try:
        import sqlite3

        now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        placeholders = ",".join(["?"] * len(ids))
        params = [int(status), now] + ids
        sql = f"UPDATE {table} SET status=?, last_update_time=? WHERE id IN ({placeholders})"

        with _cms_submedia_lock:
            conn = sqlite3.connect(db_path, timeout=10)
            conn.execute(sql, params)
            conn.commit()
            conn.close()
        return True, ""
    except Exception as e:
        return False, str(e)


_TMDB_PATH_PATTERN = re.compile(r"\[tmdb=(\d+)\]", re.I)
_SEASON_EPISODE_PATTERN = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})")


def _extract_tmdb_id_from_path(path):
    path = str(path or "").strip()
    if not path:
        return ""
    match = _TMDB_PATH_PATTERN.search(path)
    return match.group(1) if match else ""


def _parse_season_episode_from_text(text):
    match = _SEASON_EPISODE_PATTERN.search(str(text or ""))
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))


def _media_root_candidates():
    return [path for path in ["/Media/Cloud-Media-SynC", "/Media"] if os.path.exists(path)]


def _find_media_files_by_tmdb_id(tmdb_id):
    tmdb_id = str(tmdb_id or "").strip()
    if not tmdb_id:
        return []

    token = f"[tmdb={tmdb_id}]".replace("[", "[[]").replace("]", "[]]")
    exts = [".strm", ".mp4", ".mkv", ".avi", ".ts", ".m2ts", ".iso"]
    matched = []
    seen = set()

    for root in _media_root_candidates():
        for ext in exts:
            pattern = os.path.join(root, "**", f"*{token}*", "**", f"*{ext}")
            for path in glob.glob(pattern, recursive=True):
                if not os.path.isfile(path):
                    continue
                normalized = os.path.normpath(path)
                if normalized in seen:
                    continue
                seen.add(normalized)
                matched.append(normalized)
    return matched


def _expected_episode_map(item):
    season_info = str((item or {}).get("season_info") or "").strip()
    expected = {}

    if season_info:
        try:
            for season in json.loads(season_info):
                season_number = int((season or {}).get("season_number") or 0)
                episode_count = int((season or {}).get("episode_count") or 0)
                if season_number > 0 and episode_count > 0:
                    expected[season_number] = episode_count
        except Exception:
            pass

    if not expected:
        max_season = int((item or {}).get("max_season") or 0)
        max_episodes = int((item or {}).get("max_season_episodes") or 0)
        if max_season > 0 and max_episodes > 0:
            expected[max_season] = max_episodes
    return expected


def _actual_episode_map(paths):
    seasons = {}
    for path in paths or []:
        season_number, episode_number = _parse_season_episode_from_text(os.path.basename(path))
        if season_number is None or episode_number is None:
            continue
        seasons.setdefault(season_number, set()).add(episode_number)
    return seasons


def _is_submedia_complete(item):
    item = item or {}
    tmdb_id = str(item.get("tmdb_id") or "").strip()
    media_type = str(item.get("type") or "").strip()
    if not tmdb_id or not media_type:
        return False

    media_files = _find_media_files_by_tmdb_id(tmdb_id)
    if not media_files:
        return False

    if media_type == "movie":
        return True

    if media_type != "tv":
        return False

    expected = _expected_episode_map(item)
    if not expected:
        return False

    actual = _actual_episode_map(media_files)
    for season_number, episode_count in expected.items():
        if len(actual.get(season_number, set())) < episode_count:
            return False
    return True


def _reconcile_submedia_completion(items=None):
    return 0


def _reconcile_submedia_completion_if_due(force=False, min_interval=30):
    return 0


def _reconcile_submedia_completion_later(delay=12):
    return None


def _reconcile_submedia_completion_async(min_interval=30):
    return None


def _reconcile_submedia_completion_wait(timeout=2.5, force=False, min_interval=30):
    return True


def _handle_emby_webhook(raw_body):
    try:
        payload = json.loads(raw_body or b"{}")
    except Exception:
        return

    event_name = str(payload.get("Event") or "").strip().lower()
    item = payload.get("Item") or {}
    media_type = str(item.get("Type") or "").strip().lower()
    if event_name != "library.new" or media_type not in {"episode", "movie"}:
        return

    tmdb_id = _extract_tmdb_id_from_path(item.get("Path", ""))
    if tmdb_id:
        rows = _cms_find_submedia_rows_db(tmdb_id=tmdb_id, media_type="tv" if media_type == "episode" else "movie", status=1)
    else:
        rows = _cms_find_submedia_rows_db(
            title=item.get("SeriesName") if media_type == "episode" else item.get("Name"),
            year=item.get("ProductionYear"),
            media_type="tv" if media_type == "episode" else "movie",
            status=1,
        )

    if rows:
        _reconcile_submedia_completion(rows)


def _get_cms_proxy():
    """从 CMS 数据库读取代理配置"""
    if _cms_proxy_cache["loaded"]:
        return _cms_proxy_cache["proxy"]
    try:
        config = _cms_config_get_json("core") or {}
        proxy = config.get("proxy", {}).get("CMS_PROXY", "")
        if proxy:
            _cms_proxy_cache["proxy"] = proxy
            _cms_proxy_cache["loaded"] = True
            return proxy
    except Exception:
        pass
    _cms_proxy_cache["loaded"] = True
    return None


# ============================================================
# HDHive Client
# ============================================================
class HDHiveClient:
    BASE_URL = "https://hdhive.com/api/open"

    def __init__(self):
        self.api_key = os.environ.get("HDHIVE_API_KEY", "")

    @property
    def available(self):
        return bool(self.api_key)

    def _headers(self):
        return {
            "X-API-Key": self.api_key,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }

    def _proxies(self):
        proxy = os.environ.get("HDHIVE_PROXY", "") or _get_cms_proxy()
        return {"http": proxy, "https": proxy} if proxy else None

    def _get(self, path, params=None):
        import requests
        url = f"{self.BASE_URL}{path}"
        try:
            resp = requests.get(url, params=params, headers=self._headers(),
                                proxies=self._proxies(), timeout=15)
            return resp.json()
        except Exception as e:
            return {"success": False}

    def _post(self, path, json_data=None):
        import requests
        try:
            return requests.post(f"{self.BASE_URL}{path}", json=json_data or {},
                                 headers=self._headers(), proxies=self._proxies(), timeout=15).json()
        except Exception:
            return {"success": False}

    def search(self, media_type, tmdb_id):
        """搜索资源列表"""
        return self._get(f"/resources/{media_type}/{tmdb_id}")

    def unlock(self, slug):
        """解锁资源获取链接"""
        return self._post("/resources/unlock", {"slug": slug})


hdhive_client = HDHiveClient()


def _has_panso():
    return panso_client.available


def _has_hdhive():
    return hdhive_client.available


def _has_gying():
    return gying_direct_client.available or gying_client.available


def _has_any_search_source():
    return _has_panso() or _has_hdhive() or _has_gying()


def _normalize_tmdb_title(value):
    """归一化标题，便于判断电影/剧集是否与搜索词精确匹配"""
    value = (value or "").strip().lower()
    return re.sub(r"[\s\-_·:：,.，。!?！？'\"《》()（）\[\]]+", "", value)


def _compact_text(text):
    return re.sub(r"\s+", " ", (text or "")).strip()


def _build_tmdb_url(media_type, tmdb_id):
    media_type = str(media_type or "").strip().lower()
    tmdb_id = str(tmdb_id or "").strip()
    if media_type not in {"movie", "tv"} or not tmdb_id:
        return ""
    return f"https://www.themoviedb.org/{media_type}/{tmdb_id}"


def _title_match_variants(title):
    text = _compact_text(title)
    if not text:
        return []

    variants = []

    def add(value):
        value = _compact_text(value)
        if value and value not in variants:
            variants.append(value)

    add(text)
    add(text.strip("《》"))
    stripped = _strip_leading_meta_tags(text)
    add(stripped)
    add(stripped.strip("《》"))
    add(re.sub(r"\s*[\(\[（【]?\d{4}[\)\]）】]?\s*$", "", text))
    add(re.sub(r"\s*[\(\[（【]?\d{4}[\)\]）】]?\s*$", "", text.strip("《》")))
    add(re.sub(r"\s*[\(\[（【]?\d{4}[\)\]）】]?\s*$", "", stripped))
    add(re.sub(r"\s*[\(\[（【]?\d{4}[\)\]）】]?\s*$", "", stripped.strip("《》")))
    return variants


def _strip_leading_meta_tags(text):
    text = _compact_text(text)
    for _ in range(3):
        new_text = re.sub(r"^[\[【(（][^\]】)）]{1,20}[\]】)）]\s*", "", text)
        new_text = re.sub(r"^[^\w\u4e00-\u9fff《》]+", "", new_text)
        if new_text == text:
            break
        text = _compact_text(new_text)
    text = re.sub(r"^[^\w\u4e00-\u9fff《》]+", "", text)
    return text


def _is_precise_source_title_match(keyword, title):
    keyword_text = _compact_text(keyword)
    title_text = _compact_text(title)
    if not keyword_text or not title_text:
        return False

    normalized_keyword = _normalize_tmdb_title(keyword_text)
    if not normalized_keyword:
        return False

    for candidate in _title_match_variants(title_text):
        if _normalize_tmdb_title(candidate) == normalized_keyword:
            return True

    prepared = _strip_leading_meta_tags(title_text)
    pattern = re.compile(
        rf"^{re.escape(keyword_text)}(?:$|[\s\-_/|:：,，.。!！?？\(\[（【\)）\]】》]|s\d|e\d|ep\d|第|全\d)",
        re.I,
    )
    return bool(pattern.search(prepared))


def _iter_source_match_titles(item):
    if not isinstance(item, dict):
        return []

    titles = []

    def add(value):
        value = _compact_text(value)
        if value and value not in titles:
            titles.append(value)

    media = item.get("media")
    if isinstance(media, dict):
        for key in ["title", "name", "original_title", "original_name"]:
            add(media.get(key))

    for key in ["media_title", "title", "name", "note", "resource_name", "work_title"]:
        add(item.get(key))

    return titles


def _item_matches_precise_keyword(item, keyword):
    return any(_is_precise_source_title_match(keyword, title) for title in _iter_source_match_titles(item))


def _filter_merged_by_keyword(merged_by_type, keyword):
    filtered = {}
    for cloud_type, items in (merged_by_type or {}).items():
        matched_items = []
        for item in items or []:
            if _item_matches_precise_keyword(item, keyword):
                matched_items.append(item)
        if matched_items:
            filtered[cloud_type] = matched_items
    return filtered


def _prioritize_merged_by_keyword(merged_by_type, keyword):
    prioritized = {}
    for cloud_type, items in (merged_by_type or {}).items():
        exact_items = []
        fuzzy_items = []
        for item in items or []:
            if _item_matches_precise_keyword(item, keyword):
                exact_items.append(item)
            else:
                fuzzy_items.append(item)
        combined = exact_items + fuzzy_items
        if combined:
            prioritized[cloud_type] = combined
    return prioritized


def _prioritize_source_results_by_keyword(results, keyword):
    exact_items = []
    fuzzy_items = []
    for item in results or []:
        if _item_matches_precise_keyword(item, keyword):
            exact_items.append(item)
        else:
            fuzzy_items.append(item)
    return exact_items + fuzzy_items


def _sanitize_share_url(url):
    """清洗分享链接，去掉尾部拼接的标题/访问码说明"""
    url = (url or "").strip().replace("\r", " ").replace("\n", " ")
    if not url:
        return ""
    match = re.search(r"https?://\S+", url)
    if match:
        url = match.group(0)
    return url.rstrip("#")


def _search_tmdb_candidates(keyword, strict=False, collect_all=False, page_size=3):
    """通过 CMS 的 TMDB API 搜索候选媒体，尽量同时覆盖电影和电视剧"""
    keyword = _compact_text(keyword)
    if not keyword:
        return []

    cache_key = _build_search_cache_key(
        "tmdb_candidates",
        keyword,
        "strict" if strict else "fallback",
        "all" if collect_all else "filtered",
        f"size{int(page_size or 3)}",
    )
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    import requests
    normalized_keyword = _normalize_tmdb_title(keyword)

    def _candidate_title(item):
        return (
            item.get("title")
            or item.get("name")
            or item.get("original_title")
            or item.get("original_name")
            or ""
        )

    def _candidate_year(item):
        date_text = (
            item.get("release_date")
            or item.get("first_air_date")
            or item.get("air_date")
            or ""
        )
        year = str(date_text).split("-")[0].strip()
        return year if year and year != "None" else ""

    # CMS 内部端口可能是 9527 或 9528
    for port in [9527, 9528]:
        try:
            exact_candidates = []
            fallback_candidates = []
            all_candidates = []
            for media_type, path in [
                ("movie", "search_movie"),
                ("tv", "search_tv"),
            ]:
                resp = requests.get(
                    f"http://127.0.0.1:{port}/api/tmdb/{path}",
                    params={"keyword": keyword, "page": 1, "page_size": max(1, int(page_size or 3))},
                    timeout=10,
                )
                data = resp.json()
                results = data.get("data", {}).get("results", [])

                exact_matches = []
                fallback_matches = []
                for item in results:
                    tmdb_id = item.get("id")
                    if not tmdb_id:
                        continue
                    candidate = {
                        "media_type": media_type,
                        "tmdb_id": tmdb_id,
                        "title": _candidate_title(item),
                        "original_title": item.get("original_title") or item.get("original_name") or "",
                        "year": _candidate_year(item),
                        "vote_average": item.get("vote_average") or 0,
                        "genre_ids": list(item.get("genre_ids") or []),
                        "poster_path": item.get("poster_path") or "",
                        "backdrop_path": item.get("backdrop_path") or "",
                    }
                    if collect_all:
                        all_candidates.append(candidate)
                        continue
                    if _normalize_tmdb_title(candidate["title"]) == normalized_keyword:
                        exact_matches.append(candidate)
                    elif not fallback_matches:
                        fallback_matches.append(candidate)

                if collect_all:
                    continue
                if exact_matches:
                    exact_candidates.extend(exact_matches)
                else:
                    fallback_candidates.extend(fallback_matches)

            candidates = all_candidates if collect_all else (
                exact_candidates if strict else (exact_candidates or fallback_candidates)
            )
            if candidates:
                seen = set()
                deduped = []
                for candidate in candidates:
                    key = (candidate["media_type"], candidate["tmdb_id"])
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(candidate)
                return _set_cached_search_result(cache_key, deduped, TMDB_SEARCH_CACHE_TTL_SECONDS)
        except requests.exceptions.ConnectionError:
            continue
        except Exception:
            return _set_cached_search_result(cache_key, [], SEARCH_EMPTY_CACHE_TTL_SECONDS)
    return _set_cached_search_result(cache_key, [], SEARCH_EMPTY_CACHE_TTL_SECONDS)


def _search_tmdb(keyword):
    """通过 CMS 的 TMDB API 搜索，返回首个候选 (media_type, tmdb_id) 或 None"""
    candidates = _search_tmdb_candidates(keyword)
    if not candidates:
        return None
    first = candidates[0]
    return first["media_type"], first["tmdb_id"]


def _search_hdhive(keyword):
    """通过 HDHive 搜索：TMDB查ID → 获取资源列表 → 按pan_type过滤 → 解锁获取链接"""
    keyword = _compact_text(keyword)
    if not keyword or not hdhive_client.available:
        return {}

    cache_key = _build_search_cache_key("hdhive_detail", keyword)
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    resources = _search_hdhive_raw(keyword)
    if not resources:
        return _set_cached_search_result(cache_key, {}, SEARCH_EMPTY_CACHE_TTL_SECONDS)

    merged = {}
    for res in resources:
        cloud_type = res.get("pan_type", "")
        if cloud_type not in {"115", "123"}:
            continue

        slug = res.get("slug", "")
        title = res.get("title", "") or "HDHive资源"
        remark = res.get("remark", "")
        unlock_points = res.get("unlock_points") or 0
        is_unlocked = res.get("is_unlocked", False)
        tag = "🆓" if (is_unlocked or unlock_points == 0) else f"💰{unlock_points}"
        size = res.get("size", "")
        resolution = res.get("resolution", "")
        media_type = res.get("media_type", "")
        media_label = res.get("media_label", "")
        year = res.get("year", "")
        note_parts = [title]
        if remark:
            note_parts.append(remark)
        elif resolution and size:
            note_parts.append(f"{resolution} {size}")

        merged.setdefault(cloud_type, [])

        if is_unlocked or unlock_points == 0:
            unlock_data = hdhive_client.unlock(slug)
            if unlock_data.get("success"):
                link_data = unlock_data.get("data", {})
                full_url = _sanitize_share_url(link_data.get("full_url", "") or link_data.get("url", ""))
                access_code = link_data.get("access_code", "")
                if full_url:
                    merged[cloud_type].append({
                        "url": full_url,
                        "password": access_code,
                        "note": f"{tag} {' | '.join(note_parts)}",
                        "title": title,
                        "remark": remark,
                        "source": "hdhive",
                        "size": size,
                        "resolution": resolution,
                        "media_type": media_type,
                        "media_label": media_label,
                        "year": year,
                        "unlock_points": unlock_points,
                        "is_unlocked": True,
                        "is_official": res.get("is_official", False),
                    })
                    continue

        merged[cloud_type].append({
            "note": f"{tag} {' | '.join(note_parts)}",
            "title": title,
            "remark": remark,
            "slug": slug,
            "source": "hdhive",
            "size": size,
            "resolution": resolution,
            "media_type": media_type,
            "media_label": media_label,
            "year": year,
            "unlock_points": unlock_points,
            "is_unlocked": is_unlocked,
            "is_official": res.get("is_official", False),
        })

    return _set_cached_search_result(cache_key, merged, HDHIVE_DETAIL_CACHE_TTL_SECONDS)


def _hdhive_candidate_media_label(candidate):
    candidate = candidate or {}
    media_type = str(candidate.get("media_type") or "").strip().lower()
    try:
        genre_ids = {
            int(item)
            for item in (candidate.get("genre_ids") or [])
            if str(item).strip()
        }
    except Exception:
        genre_ids = set()

    if 16 in genre_ids:
        return "动漫"
    if media_type == "tv":
        return "电视剧"
    if media_type == "movie":
        return "电影"
    return ""


def _hdhive_item_meta_text(item):
    item = item or {}
    media_label = str(item.get("media_label") or "").strip()
    year = str(item.get("year") or "").strip()
    parts = []
    if media_label:
        parts.append(media_label)
    if year:
        parts.append(year)
    return "｜".join(parts)


def _is_free_hdhive_item(item):
    item = item or {}
    if _normalize_result_source_key(item.get("source")) != "hdhive":
        return False

    try:
        unlock_points = int(item.get("unlock_points") or 0)
    except Exception:
        unlock_points = 0

    if str(item.get("url") or "").strip() and not str(item.get("slug") or "").strip():
        return True
    return bool(item.get("is_unlocked")) or unlock_points == 0


def _tg_resource_meta_text(item):
    item = item or {}
    parts = []

    media_label = _compact_text(item.get("media_label"))
    year = _compact_text(item.get("year"))
    resolution = _compact_text(item.get("resolution"))
    size = _compact_text(item.get("size"))

    if media_label:
        parts.append(media_label)
    if year:
        parts.append(year)
    if resolution:
        parts.append(resolution)
    if size:
        parts.append(size)
    return "｜".join(parts)


def _tg_resource_note_text(item):
    item = item or {}
    if _normalize_result_source_key(item.get("source")) == "hdhive":
        badges = []
        title = _compact_text(item.get("title"))
        remark = _compact_text(item.get("remark"))

        if item.get("is_official"):
            badges.append("【官组】")

        if not _is_free_hdhive_item(item):
            try:
                unlock_points = int(item.get("unlock_points") or 0)
            except Exception:
                unlock_points = 0
            if unlock_points > 0:
                badges.append(f"【{unlock_points}💰】")

        body_parts = []
        if title:
            body_parts.append(title)
        if remark:
            body_parts.append(remark)

        badge_text = "".join(badges)
        body_text = " | ".join(body_parts)
        note_text = " ".join(part for part in [badge_text, body_text] if part).strip()
        if note_text:
            return note_text

    note = _compact_text(item.get("note") or item.get("title") or "未知")
    if _is_free_hdhive_item(item):
        note = re.sub(r"^(?:🆓|FREE)\s*", "", note, flags=re.I).strip()
    return note or "未知"


def _resolve_search_item_meta(title="", fallback_keyword=""):
    title = _compact_text(title)
    fallback_keyword = _compact_text(fallback_keyword)
    cache_key = _build_search_cache_key("search_item_meta", title or fallback_keyword or "")
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    candidates_to_try = []
    for raw_value in [fallback_keyword, title]:
        raw_value = _compact_text(raw_value)
        if raw_value and raw_value not in candidates_to_try:
            candidates_to_try.append(raw_value)

    for raw_value in candidates_to_try:
        keyword, _, target_year, prefer_animation = _parse_subscribe_keyword(raw_value)
        if not keyword:
            continue

        ranked = _resolve_subscribe_candidates(
            keyword,
            target_year=target_year,
            prefer_animation=prefer_animation,
        )
        normalized_keyword = _normalize_tmdb_title(keyword)
        exact_matches = [
            item for item in ranked
            if _normalize_tmdb_title(item.get("title", "")) == normalized_keyword
        ]
        if not exact_matches:
            continue

        candidate = exact_matches[0]
        meta = {
            "media_type": str(candidate.get("media_type") or "").strip().lower(),
            "media_label": _hdhive_candidate_media_label(candidate),
            "year": str(candidate.get("year") or "").strip(),
        }
        return _set_cached_search_result(cache_key, meta, TMDB_SEARCH_CACHE_TTL_SECONDS)

    return _set_cached_search_result(cache_key, {}, SEARCH_EMPTY_CACHE_TTL_SECONDS)


def _decorate_link_items_with_meta(merged_by_type, fallback_keyword=""):
    decorated = {}
    for cloud_type, items in (merged_by_type or {}).items():
        decorated_items = []
        for item in items or []:
            normalized = dict(item or {})
            meta = _resolve_search_item_meta(
                normalized.get("title") or normalized.get("note") or "",
                fallback_keyword=fallback_keyword,
            )
            if meta:
                normalized.update(meta)
            decorated_items.append(normalized)
        decorated[cloud_type] = decorated_items
    return decorated


def _decorate_hdhive_item_for_web(item):
    decorated = dict(item or {})
    meta_text = _hdhive_item_meta_text(decorated)
    title = str(decorated.get("title") or "").strip()
    decorated["title_has_meta"] = False
    if meta_text and title and meta_text not in title:
        decorated["title"] = f"{meta_text}｜{title}"
        decorated["title_has_meta"] = True
    return decorated


def _parse_hdhive_size_bytes(value):
    try:
        text = str(value or "").strip().upper()
        if text.endswith("TB"):
            return float(text[:-2]) * 1024 * 1024
        if text.endswith("GB"):
            return float(text[:-2]) * 1024
        if text.endswith("MB"):
            return float(text[:-2])
        if text.endswith("KB"):
            return float(text[:-2]) / 1024
    except Exception:
        pass
    return 0


def _collect_hdhive_raw_resources(tmdb_candidates, cache_key=None):
    if cache_key is not None:
        cached = _get_cached_search_result(cache_key)
        if cached is not None:
            return cached

    candidates = list(tmdb_candidates or [])
    if not candidates or not hdhive_client.available:
        if cache_key is None:
            return []
        return _set_cached_search_result(cache_key, [], SEARCH_EMPTY_CACHE_TTL_SECONDS)

    pan_type_map = {"115": "115", "123": "123"}
    result = []
    seen_slugs = set()

    for candidate in candidates:
        media_type = str(candidate.get("media_type") or "").strip().lower()
        tmdb_id = str(candidate.get("tmdb_id") or "").strip()
        if media_type not in {"movie", "tv"} or not tmdb_id:
            continue

        media_label = _hdhive_candidate_media_label(candidate)
        year = str(candidate.get("year") or "").strip()
        data = hdhive_client.search(media_type, tmdb_id)
        resources = data.get("data", [])
        if not isinstance(resources, list):
            continue

        for res in resources:
            cloud_type = pan_type_map.get(res.get("pan_type", ""))
            if not cloud_type:
                continue

            slug = str(res.get("slug") or "").strip()
            if slug and slug in seen_slugs:
                continue
            if slug:
                seen_slugs.add(slug)

            result.append({
                "slug": slug,
                "title": res.get("title", "") or "未知资源",
                "remark": res.get("remark", ""),
                "pan_type": cloud_type,
                "size": res.get("share_size", ""),
                "resolution": ", ".join(res.get("video_resolution", []) or []),
                "source": ", ".join(res.get("source", []) or []),
                "is_official": res.get("is_official", False),
                "unlock_points": res.get("unlock_points") or 0,
                "is_unlocked": res.get("is_unlocked", False),
                "uploader": (res.get("user") or {}).get("nickname", ""),
                "media_type": media_type,
                "media_label": media_label,
                "year": year,
            })

    result.sort(
        key=lambda item: (
            0 if item["unlock_points"] == 0 or item["is_unlocked"] else 1,
            -_parse_hdhive_size_bytes(item["size"]),
        )
    )

    if cache_key is None:
        return result

    ttl = SEARCH_CACHE_TTL_SECONDS if result else SEARCH_EMPTY_CACHE_TTL_SECONDS
    return _set_cached_search_result(cache_key, result, ttl)


def _search_hdhive_raw(keyword):
    """HDHive 搜索：返回原始资源列表（不解锁），用于企微展示"""
    keyword = _compact_text(keyword)
    if not keyword or not hdhive_client.available:
        return []

    cache_key = _build_search_cache_key("hdhive_raw", keyword)
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    tmdb_candidates = _search_tmdb_candidates(keyword, strict=True)
    if not tmdb_candidates:
        return _set_cached_search_result(cache_key, [], SEARCH_EMPTY_CACHE_TTL_SECONDS)
    return _collect_hdhive_raw_resources(tmdb_candidates, cache_key=cache_key)


def _search_hdhive_raw_by_candidate(candidate):
    candidate = dict(candidate or {})
    media_type = str(candidate.get("media_type") or "").strip().lower()
    tmdb_id = str(candidate.get("tmdb_id") or "").strip()
    if media_type not in {"movie", "tv"} or not tmdb_id:
        return []
    cache_key = _build_search_cache_key("hdhive_raw_selected", media_type, tmdb_id)
    return _collect_hdhive_raw_resources([candidate], cache_key=cache_key)


def _group_hdhive_raw_results(resources):
    merged = {}
    for res in resources or []:
        cloud_type = res.get("pan_type", "")
        if cloud_type not in {"115", "123"}:
            continue

        title = res.get("title", "") or "HDHive资源"
        remark = res.get("remark", "")
        unlock_points = res.get("unlock_points") or 0
        is_unlocked = res.get("is_unlocked", False)
        size = res.get("size", "")
        resolution = res.get("resolution", "")
        media_type = res.get("media_type", "")
        media_label = res.get("media_label", "")
        year = res.get("year", "")
        tag = "🆓" if (is_unlocked or unlock_points == 0) else f"💰{unlock_points}"

        note_parts = [title]
        if remark:
            note_parts.append(remark)
        elif resolution and size:
            note_parts.append(f"{resolution} {size}")

        merged.setdefault(cloud_type, []).append({
            "title": title,
            "remark": remark,
            "note": f"{tag} {' | '.join(note_parts)}",
            "slug": res.get("slug", ""),
            "source": "hdhive",
            "size": size,
            "resolution": resolution,
            "media_type": media_type,
            "media_label": media_label,
            "year": year,
            "unlock_points": unlock_points,
            "is_unlocked": is_unlocked,
            "is_official": res.get("is_official", False),
        })
    return merged


def _search_hdhive_preview(keyword):
    keyword = _compact_text(keyword)
    if not keyword:
        return {}

    cache_key = _build_search_cache_key("hdhive_preview", keyword)
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    merged = _group_hdhive_raw_results(_search_hdhive_raw(keyword))
    return _set_cached_search_result(cache_key, merged, SEARCH_CACHE_TTL_SECONDS)


def _format_hdhive_wx(keyword, resources):
    """企微格式化 HDHive 资源列表
    免费资源直接解锁显示链接，付费资源用数字解锁"""
    if not resources:
        return f"🔍 {keyword}（影巢）\n\n暂无搜索结果"

    def _compact_text(text):
        return re.sub(r"\s+", " ", (text or "")).strip()

    def _resource_name(title, remark):
        text = _compact_text(remark)
        if not text:
            return ""

        for prefix in (title, keyword):
            prefix = _compact_text(prefix)
            if prefix and text.startswith(prefix):
                text = text[len(prefix):].strip()

        text = text.replace("][", " / ")
        text = text.replace("【", "").replace("】", "")
        text = text.replace("[", "").replace("]", "")
        text = re.sub(r"\s*/\s*", " / ", text)
        text = text.strip(" -_/|")
        if not text:
            return ""
        if _normalize_tmdb_title(text) in {"", _normalize_tmdb_title(title), _normalize_tmdb_title(keyword)}:
            return ""
        return text[:36].rstrip()

    free_count = sum(1 for res in resources if res["is_unlocked"] or res["unlock_points"] == 0)
    paid_count = len(resources) - free_count
    lines = [f"🔍 {keyword}（影巢）", f"共 {len(resources)} 个资源｜免费 {free_count}｜付费 {paid_count}\n"]
    has_paid = False
    for i, res in enumerate(resources):
        title = _compact_text(res["title"])
        pan_name = CLOUD_TYPE_NAMES.get(res["pan_type"], res["pan_type"])
        points = res["unlock_points"]
        unlocked = res["is_unlocked"]
        is_free = unlocked or points == 0
        resource_name = _resource_name(title, res.get("remark", ""))
        meta_text = _hdhive_item_meta_text(res)
        resolution = _compact_text(res.get("resolution", ""))
        size = _compact_text(res.get("size", ""))
        indent = " " * (len(str(i + 1)) + 2)

        # 状态标记
        tag = "【免费】" if is_free else f"【💰{points}积分】"
        if res.get("is_official"):
            tag = f"【官组】{tag}"

        lines.append(f"{i+1}. {tag}{pan_name}")

        if meta_text:
            lines.append(f"{indent}{meta_text}")

        line2_parts = []
        if resolution:
            line2_parts.append(resolution)
        if size:
            line2_parts.append(size)
        if line2_parts:
            lines.append(f"{indent}{'｜'.join(line2_parts)}")

        if resource_name:
            lines.append(f"{indent}{resource_name}")

        # 免费资源直接解锁显示链接
        if is_free:
            try:
                unlock_data = hdhive_client.unlock(res["slug"])
                if unlock_data.get("success"):
                    link_data = unlock_data.get("data", {})
                    url = _sanitize_share_url(link_data.get("full_url", "") or link_data.get("url", ""))
                    if url:
                        lines.append(f"{indent}🔗 {url}")
            except Exception:
                pass
        else:
            has_paid = True

        lines.append("")

    if has_paid:
        lines.append("💡 回复数字解锁对应资源，如 1")
    elif free_count:
        lines.append("💡 复制链接发送即可转存")
    return "\n".join(lines)


def _unlock_hdhive_resource(index):
    """解锁指定序号的 HDHive 资源，返回消息文本"""
    cache = _search_cache.get("wx_hdhive")
    if not cache:
        return "没有搜索记录，请先搜索"

    items = list(cache.get("items") or [])
    if index < 1 or index > len(items):
        return f"序号无效，请输入 1-{len(items)}"

    item = items[index - 1]
    if str(item.get("kind") or "").strip() != "hdhive" or not item.get("is_paid"):
        return f"ℹ️ {index} 号资源已直接显示链接，无需解锁"

    slug = str(item.get("slug") or "").strip()
    title = str(item.get("display_title") or item.get("title_text") or "未知资源").strip() or "未知资源"
    if not slug:
        return "❌ 该资源缺少解锁标识"

    unlock_data = hdhive_client.unlock(slug)

    if not unlock_data.get("success"):
        msg = unlock_data.get("message", "解锁失败")
        return f"❌ 解锁失败: {msg}"

    link_data = unlock_data.get("data", {})
    full_url = _sanitize_share_url(link_data.get("full_url", "") or link_data.get("url", ""))

    if not full_url:
        return f"❌ 解锁成功但未获取到链接"

    item["direct_url"] = full_url
    cache["updated_at"] = time.time()
    _search_cache["wx_hdhive"] = cache

    lines = [f"✅ {title}"]
    lines.append(f"🔗 {full_url}")
    lines.append("\n💡 复制链接发送即可转存")
    return "\n".join(lines)


def _detect_cloud_type(link_type="", url=""):
    type_text = str(link_type or "").strip().lower()
    url_text = str(url or "").strip().lower()
    host = ""
    try:
        host = (urlparse(url_text).netloc or "").lower()
    except Exception:
        host = ""

    if "magnet" in type_text or "磁力" in type_text:
        return "magnet"
    if "115" in type_text or "115cdn" in type_text:
        return "115"
    if "123" in type_text or "123pan" in type_text or "123865" in type_text:
        return "123"
    if "alipan" in type_text or "aliyun" in type_text:
        return "aliyun"
    if "quark" in type_text:
        return "quark"
    if "baidu" in type_text or "pan.baidu" in type_text or "百度" in type_text:
        return "baidu"
    if "uc" in type_text:
        return "uc"
    if "xunlei" in type_text or "迅雷" in type_text:
        return "xunlei"
    if "tianyi" in type_text or "189" in type_text or "天翼" in type_text:
        return "tianyi"

    if url_text.startswith("magnet:"):
        return "magnet"
    if "pan.baidu.com" in host or host.endswith(".baidu.com"):
        return "baidu"
    if "123pan" in host or "123865" in host or "123684" in host:
        return "123"
    if "115" in host:
        return "115"
    if "alipan" in host or "aliyun" in host:
        return "aliyun"
    if "quark" in host:
        return "quark"
    if host.startswith("drive.uc.cn") or host.endswith(".uc.cn") or host == "uc.cn":
        return "uc"
    if "xunlei" in host:
        return "xunlei"
    if "cloud.189.cn" in host or host.endswith(".189.cn"):
        return "tianyi"

    text = f"{host} {url_text.split('?', 1)[0]}".lower()
    if "magnet" in text or "磁力" in text:
        return "magnet"
    if "pan.baidu" in text or "baidu" in text:
        return "baidu"
    if "123pan" in text or "123865" in text or "123684" in text:
        return "123"
    if "115" in text or "115cdn" in text:
        return "115"
    if "alipan" in text or "aliyun" in text:
        return "aliyun"
    if "quark" in text:
        return "quark"
    if "uc" in text:
        return "uc"
    if "xunlei" in text or "迅雷" in text:
        return "xunlei"
    if "tianyi" in text or "189" in text or "天翼" in text:
        return "tianyi"
    return "other"


def _merge_result_maps(target, extra):
    for cloud_type, items in (extra or {}).items():
        bucket = target.setdefault(cloud_type, [])
        seen = {
            (
                item.get("url", ""),
                item.get("slug", ""),
                item.get("note", ""),
                item.get("title", ""),
            )
            for item in bucket
        }
        for item in items:
            key = (
                item.get("url", ""),
                item.get("slug", ""),
                item.get("note", ""),
                item.get("title", ""),
            )
            if key in seen:
                continue
            seen.add(key)
            bucket.append(item)


def _normalize_result_source_key(source_key):
    value = str(source_key or "").strip().lower().replace("_", "-")
    if value.startswith("hdhive"):
        return "hdhive"
    if value.startswith("gying"):
        return "gying"
    if value.startswith("panso"):
        return "panso"
    return value


def _source_display_name(source_key):
    return {
        "hdhive": "影巢",
        "gying": "观影",
        "panso": "盘搜",
    }.get(_normalize_result_source_key(source_key), str(source_key or "").strip())


def _merged_result_source_label(merged_by_type):
    present = set()
    for items in (merged_by_type or {}).values():
        for item in items or []:
            source_key = _normalize_result_source_key((item or {}).get("source"))
            if source_key:
                present.add(source_key)

    labels = []
    for source_key in ["hdhive", "gying", "panso"]:
        if source_key in present:
            labels.append(_source_display_name(source_key))
    return "+".join(labels)


def _group_link_results(results, source):
    merged = {}
    seen = set()

    for result in results or []:
        title = (result.get("title") or "未知资源").strip()
        links = result.get("links", [])
        if not isinstance(links, list):
            continue

        for link in links:
            url = _sanitize_share_url(link.get("url", ""))
            if not url:
                continue

            password = (link.get("password") or "").strip()
            cloud_type = _detect_cloud_type(link.get("type", ""), url)
            key = (cloud_type, url, password, title)
            if key in seen:
                continue
            seen.add(key)

            merged.setdefault(cloud_type, []).append({
                "title": title,
                "note": title,
                "url": url,
                "password": password,
                "source": source,
            })

    return merged


def _search_panso(keyword, cloud_types=None):
    keyword = _compact_text(keyword)
    if not keyword or not panso_client.available:
        return {"merged_by_type": {}, "total": 0}

    normalized_cloud_types = []
    for item in (cloud_types or list(ALLOWED_CLOUD_TYPES)):
        text = str(item or "").strip().lower()
        if text and text not in normalized_cloud_types:
            normalized_cloud_types.append(text)

    cache_key = _build_search_cache_key("panso", keyword, normalized_cloud_types)
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    requested_cloud_types = normalized_cloud_types or list(ALLOWED_CLOUD_TYPES)
    data = panso_client.search(keyword, requested_cloud_types)
    prioritized = _prioritize_merged_by_keyword(data.get("merged_by_type", {}), keyword)
    decorated = _decorate_link_items_with_meta(prioritized, fallback_keyword=keyword)
    result = dict(data or {})
    result["merged_by_type"] = decorated
    result["total"] = sum(len(v) for v in decorated.values())
    ttl = SEARCH_CACHE_TTL_SECONDS if result["total"] else SEARCH_EMPTY_CACHE_TTL_SECONDS
    return _set_cached_search_result(cache_key, result, ttl)


def _search_gying(keyword, max_results=20):
    keyword = _compact_text(keyword)
    if not keyword or not _has_gying():
        return {"success": False, "message": "观影未配置", "merged_by_type": {}, "total": 0}

    cache_key = _build_search_cache_key("gying", keyword, max_results)
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    proxy_result = None
    proxy_error_message = ""
    if gying_client.available:
        data = gying_client.search(keyword, max_results=max_results)
        if data.get("success"):
            results = ((data.get("data") or {}).get("results") or [])
            if not isinstance(results, list):
                results = []
            results = _prioritize_source_results_by_keyword(results, keyword)

            merged = _group_link_results(results, "gying")
            merged = _decorate_link_items_with_meta(merged, fallback_keyword=keyword)
            proxy_result = {
                "success": True,
                "message": data.get("message", "success"),
                "merged_by_type": merged,
                "total": sum(len(v) for v in merged.values()),
                "raw_results": results,
            }
        else:
            proxy_error_message = data.get("message", "观影搜索失败")

    direct_data = None
    need_direct_115 = gying_direct_client.available and (
        proxy_result is None or bool((proxy_result.get("merged_by_type") or {}).get("115"))
    )
    if need_direct_115:
        direct_limit = max_results
        if proxy_result is not None:
            direct_limit = min(max_results, max(1, len((proxy_result.get("merged_by_type") or {}).get("115") or [])))
        direct_data = gying_direct_client.search(
            keyword,
            max_results=direct_limit,
            allowed_cloud_types=["115"],
        )
        direct_merged = _prioritize_merged_by_keyword(
            (direct_data or {}).get("merged_by_type", {}),
            keyword,
        )
        direct_data["merged_by_type"] = _decorate_link_items_with_meta(
            direct_merged,
            fallback_keyword=keyword,
        )
        direct_115 = (direct_data.get("merged_by_type") or {}).get("115") or []
        if direct_data.get("success") and direct_115:
            if proxy_result is None:
                result = direct_data
            else:
                merged = dict(proxy_result.get("merged_by_type") or {})
                merged["115"] = direct_115
                result = dict(proxy_result)
                result["merged_by_type"] = merged
                result["total"] = sum(len(v) for v in merged.values())
            return _set_cached_search_result(cache_key, result, SEARCH_CACHE_TTL_SECONDS)
        if proxy_result is None and direct_data.get("success"):
            return _set_cached_search_result(cache_key, direct_data, SEARCH_EMPTY_CACHE_TTL_SECONDS)

    if proxy_result is not None:
        ttl = SEARCH_CACHE_TTL_SECONDS if proxy_result["total"] else SEARCH_EMPTY_CACHE_TTL_SECONDS
        return _set_cached_search_result(cache_key, proxy_result, ttl)

    if gying_direct_client.available:
        if direct_data is None:
            direct_data = gying_direct_client.search(keyword, max_results=max_results, allowed_cloud_types=["115"])
            direct_merged = _prioritize_merged_by_keyword(
                (direct_data or {}).get("merged_by_type", {}),
                keyword,
            )
            direct_data["merged_by_type"] = _decorate_link_items_with_meta(
                direct_merged,
                fallback_keyword=keyword,
            )
        ttl = SEARCH_CACHE_TTL_SECONDS if direct_data.get("total", 0) else SEARCH_EMPTY_CACHE_TTL_SECONDS
        return _set_cached_search_result(cache_key, direct_data, ttl)

    result = {
        "success": False,
        "message": proxy_error_message or "观影直连失败，且代理未配置",
        "merged_by_type": {},
        "total": 0,
    }
    return _set_cached_search_result(cache_key, result, SEARCH_EMPTY_CACHE_TTL_SECONDS)


def _combined_search(keyword, cloud_types=None):
    """合并搜索 PanSou + HDHive + Gying"""
    merged = {}

    if _has_panso():
        panso_data = _search_panso(keyword, cloud_types)
        _merge_result_maps(merged, panso_data.get("merged_by_type", {}))

    if _has_hdhive():
        _merge_result_maps(merged, _search_hdhive(keyword))

    if _has_gying():
        gying_data = _search_gying(keyword)
        _merge_result_maps(merged, gying_data.get("merged_by_type", {}))

    if cloud_types:
        allow_set = set(cloud_types)
        merged = {k: v for k, v in merged.items() if k in allow_set}

    total = sum(len(v) for v in merged.values())
    return {"merged_by_type": merged, "total": total}


def _selected_candidate_search_keywords(candidate, fallback_keyword=""):
    candidate = candidate or {}
    title = _compact_text(candidate.get("title"))
    original_title = _compact_text(candidate.get("original_title"))
    year = str(candidate.get("year") or "").strip()

    keywords = []
    seen = set()

    def add(value):
        for item in _subscription_keyword_variants(value):
            normalized = _normalize_tmdb_title(item)
            if item and normalized and normalized not in seen:
                seen.add(normalized)
                keywords.append(item)

    add(title)
    if title and year:
        add(f"{title} {year}")

    if original_title and _normalize_tmdb_title(original_title) != _normalize_tmdb_title(title):
        add(original_title)
        if year:
            add(f"{original_title} {year}")

    add(fallback_keyword)
    return keywords


def _search_source_with_keywords(source_key, keywords, cloud_types=None):
    source_key = str(source_key or "").strip().lower()
    keyword_list = [item for item in (keywords or []) if _compact_text(item)]
    if not keyword_list:
        return {}

    allowed_cloud_types = []
    for cloud_type in (cloud_types or WX_RESULT_CLOUD_TYPE_ORDER):
        value = str(cloud_type or "").strip().lower()
        if value and value not in allowed_cloud_types:
            allowed_cloud_types.append(value)

    first_keyword = keyword_list[0]
    for keyword in keyword_list:
        if source_key == "gying":
            data = _search_gying(keyword)
        elif source_key == "panso":
            data = _search_panso(keyword, allowed_cloud_types)
        else:
            data = {}

        merged = (data or {}).get("merged_by_type", {})
        normalized_merged = {}
        total = 0
        for cloud_type in allowed_cloud_types:
            source_items = list((merged or {}).get(cloud_type) or [])
            if not source_items:
                continue
            normalized_items = []
            for item in source_items:
                normalized_item = dict(item or {})
                original_source = normalized_item.get("source")
                if original_source:
                    normalized_item["source_detail"] = original_source
                normalized_item["source"] = source_key
                normalized_item["cloud_type"] = cloud_type
                normalized_items.append(normalized_item)
            if normalized_items:
                normalized_merged[cloud_type] = normalized_items
                total += len(normalized_items)
        if total > 0:
            if keyword != first_keyword:
                _log(f"微信候选搜源回退命中 {_source_display_name(source_key)}: {first_keyword} -> {keyword} ({total})")
            return normalized_merged
    return {}


def _search_wx_selected_candidate_results(candidate, fallback_keyword=""):
    display_keyword = _hdhive_candidate_display_keyword(candidate, fallback_keyword=fallback_keyword)
    hdhive_merged = {}
    external_merged = {}
    hdhive_resources = []

    if _has_hdhive():
        hdhive_resources = [
            item for item in (_search_hdhive_raw_by_candidate(candidate) or [])
            if str(item.get("pan_type") or "").strip() in {"115", "123"}
        ]
        _merge_result_maps(hdhive_merged, _group_hdhive_raw_results(hdhive_resources) or {})

    keywords = _selected_candidate_search_keywords(candidate, fallback_keyword=display_keyword)
    tasks = []
    if _has_gying():
        tasks.append(("gying", lambda values=list(keywords): _search_source_with_keywords("gying", values)))
    if _has_panso():
        tasks.append(("panso", lambda values=list(keywords): _search_source_with_keywords("panso", values)))

    if tasks:
        executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=len(tasks),
            thread_name_prefix="cms-wx-src",
        )
        futures = {}
        try:
            for source_key, func in tasks:
                futures[source_key] = executor.submit(func)

            concurrent.futures.wait(list(futures.values()), timeout=20)

            for source_key, _ in tasks:
                future = futures[source_key]
                if not future.done():
                    continue
                try:
                    data = future.result() or {}
                except Exception:
                    data = {}
                _merge_result_maps(external_merged, data)
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    return {
        "keyword": display_keyword,
        "hdhive_data": {
            "merged_by_type": hdhive_merged,
            "total": sum(len(v) for v in hdhive_merged.values()),
        },
        "external_data": {
            "merged_by_type": external_merged,
            "total": sum(len(v) for v in external_merged.values()),
        },
        "hdhive_resources": hdhive_resources,
    }


def _hdhive_wx_resource_name(keyword, title, remark):
    text = _compact_text(remark)
    if not text:
        return ""

    for prefix in (title, keyword):
        prefix = _compact_text(prefix)
        if prefix and text.startswith(prefix):
            text = text[len(prefix):].strip()

    text = text.replace("][", " / ")
    text = text.replace("【", "").replace("】", "")
    text = text.replace("[", "").replace("]", "")
    text = re.sub(r"\s*/\s*", " / ", text)
    text = text.strip(" -_/|")
    if not text:
        return ""
    if _normalize_tmdb_title(text) in {"", _normalize_tmdb_title(title), _normalize_tmdb_title(keyword)}:
        return ""
    return text[:36].rstrip()


def _wx_external_result_items_in_order(external_data):
    merged = ((external_data or {}).get("merged_by_type") or {})
    if not merged:
        return []

    ordered = []
    source_order = ["gying", "panso"]
    for cloud_type in WX_RESULT_CLOUD_TYPE_ORDER:
        items = list((merged or {}).get(cloud_type) or [])
        if not items:
            continue

        for source_key in source_order:
            for item in items:
                if _normalize_result_source_key(item.get("source")) != source_key:
                    continue
                ordered.append((cloud_type, item))

        for item in items:
            normalized_source = _normalize_result_source_key(item.get("source"))
            if normalized_source in source_order:
                continue
            ordered.append((cloud_type, item))
    return ordered


def _build_wx_selected_candidate_display_items(keyword, hdhive_resources, external_data):
    items = []

    hdhive_by_type = {}
    for resource in hdhive_resources or []:
        cloud_type = str(resource.get("pan_type") or "").strip().lower()
        if cloud_type not in {"115", "123"}:
            continue
        hdhive_by_type.setdefault(cloud_type, []).append(resource)

    for cloud_type in WX_RESULT_CLOUD_TYPE_ORDER:
        for resource in hdhive_by_type.get(cloud_type, []):
            source_label = _source_display_name("hdhive")
            pan_name = CLOUD_TYPE_NAMES.get(cloud_type, cloud_type)
            try:
                unlock_points = int(resource.get("unlock_points") or 0)
            except Exception:
                unlock_points = 0
            is_free = bool(resource.get("is_unlocked")) or unlock_points == 0
            title_parts = [f"[{source_label}]"]
            if resource.get("is_official"):
                title_parts.append("【官组】")
            if not is_free:
                title_parts.append(f"【💰{unlock_points}积分】")
            title_parts.append(pan_name)

            items.append({
                "kind": "hdhive",
                "source": "hdhive",
                "cloud_type": cloud_type,
                "title_text": " ".join(part for part in title_parts if part).strip(),
                "meta_text": _hdhive_item_meta_text(resource),
                "resolution": _compact_text(resource.get("resolution")),
                "size": _compact_text(resource.get("size")),
                "detail_text": _hdhive_wx_resource_name(
                    keyword,
                    _compact_text(resource.get("title")),
                    resource.get("remark", ""),
                ),
                "slug": str(resource.get("slug") or "").strip(),
                "display_title": _compact_text(resource.get("title")) or keyword or "未知资源",
                "direct_url": "",
                "is_paid": not is_free,
            })

    for cloud_type, item in _wx_external_result_items_in_order(external_data):
        source_key = _normalize_result_source_key(item.get("source"))
        source_label = _source_display_name(source_key) or "未知来源"
        pan_name = CLOUD_TYPE_NAMES.get(cloud_type, cloud_type)
        title_text = _compact_text(item.get("note") or item.get("title")) or "未知资源"
        title_text = re.sub(r"^(?:\[(?:plugin|tg|api|bot):[^\]]+\]\s*)+", "", title_text, flags=re.I)
        title_text = f"{title_text} {pan_name}".strip()
        items.append({
            "kind": "external",
            "source": source_key,
            "cloud_type": cloud_type,
            "title_text": f"[{source_label}] {title_text}",
            "meta_text": _hdhive_item_meta_text(item),
            "resolution": "",
            "size": "",
            "detail_text": "",
            "url": _sanitize_share_url(item.get("url", "")),
            "is_paid": False,
        })

    return items


def _split_selected_candidate_results_by_source(merged_by_type):
    grouped = {}
    for cloud_type, items in (merged_by_type or {}).items():
        normalized_cloud_type = str(cloud_type or "").strip().lower()
        if normalized_cloud_type not in ALLOWED_CLOUD_TYPES:
            continue
        for item in items or []:
            source_key = _normalize_result_source_key((item or {}).get("source"))
            if source_key not in {"hdhive", "gying", "panso"}:
                continue
            grouped.setdefault(source_key, {}).setdefault(normalized_cloud_type, []).append(dict(item or {}))
    return grouped


def _run_source_search_tasks(keyword, timeout=20):
    tasks = []
    if _has_hdhive():
        tasks.append(("影巢", lambda: {"merged_by_type": _search_hdhive_preview(keyword)}))
    if _has_gying():
        tasks.append(("观影", lambda: _search_gying(keyword)))
    if _has_panso():
        tasks.append(("盘搜", lambda: {"merged_by_type": (_search_panso(keyword) or {}).get("merged_by_type", {})}))

    if not tasks:
        return []

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks), thread_name_prefix="cms-src")
    futures = {}
    try:
        for source_name, func in tasks:
            futures[source_name] = executor.submit(func)

        concurrent.futures.wait(list(futures.values()), timeout=timeout)

        results = []
        for source_name, _ in tasks:
            future = futures[source_name]
            if not future.done():
                results.append((source_name, {"merged_by_type": {}}))
                continue
            try:
                data = future.result() or {}
            except Exception:
                data = {}
            if "merged_by_type" not in data:
                data = {"merged_by_type": {}}
            results.append((source_name, data))
        return results
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _subscription_sync_active():
    return bool(getattr(_sub_sync_local, "active", False))


def _subscription_keyword_variants(keyword):
    value = re.sub(r"\s+", " ", (keyword or "")).strip()
    if not value:
        return []

    variants = []
    seen_normalized = set()

    def add(candidate):
        candidate = re.sub(r"\s+", " ", (candidate or "")).strip(" -_|,")
        normalized = _normalize_tmdb_title(candidate)
        if candidate and normalized and normalized not in seen_normalized:
            seen_normalized.add(normalized)
            variants.append(candidate)

    add(value)
    cleaned = re.sub(r"\b(?:tmdb|imdb)[:#\s_-]*[a-z0-9]+\b", "", value, flags=re.I)
    add(cleaned)

    yearless = re.sub(
        r"(?:[\s._-]*[\(\[（【]?\s*(?:19|20)\d{2}\s*[\)\]）】]?\s*)$",
        "",
        cleaned,
        flags=re.I,
    )
    add(yearless)

    for base in [cleaned, yearless]:
        primary = re.split(r"\s*[|,]\s*", base, 1)[0]
        add(primary)
        for piece in re.split(r"\s*[/／]\s*", primary):
            add(piece)
    return variants


def _subscription_virtual_sub_sources(SubSource):
    configs = []
    if _has_hdhive():
        configs.append(("hdhive", "影巢"))
    if _has_gying():
        configs.append(("gying", "观影"))
    if _has_panso():
        configs.append(("panso", "盘搜"))

    items = []
    for idx, (source_key, source_name) in enumerate(configs, 1):
        items.append(SubSource(
            id=-(1000 + idx),
            type="tg_channel",
            sub_name=source_name,
            sub_url=f"https://t.me/{SUB_SYNC_CHANNELS[source_key]}",
            status=1,
            remark="virtual",
            level=0,
        ))
    return items


def _subscription_prepare_url(url, password=""):
    full_url = _sanitize_share_url(url or "")
    password = (password or "").strip()
    if not full_url:
        return ""
    if password and "password=" not in full_url:
        full_url = f"{full_url}?password={password}" if "?" not in full_url else f"{full_url}&password={password}"
    return full_url


def _subscription_link_type(url):
    return "share_115" if _detect_cloud_type("", url) == "115" else ""


def _subscription_result_text(item, keyword):
    text = (item.get("note") or item.get("title") or keyword or "").strip()
    resolution = (item.get("resolution") or "").strip()
    size = str(item.get("size") or "").strip()

    if resolution and resolution.lower() not in text.lower():
        text = f"{text} {resolution}".strip()
    if size and size.lower() not in text.lower():
        text = f"{text} 大小：{size}".strip()
    return text or (keyword or "未知资源")


def _subscription_empty_resource(ResourceInfo, source_name, title):
    info = ResourceInfo()
    info.name = (title or "未知资源").strip() or "未知资源"
    info.year = None
    info.resource_pix = None
    info.resource_type = None
    info.type = None
    info.link = None
    info.tmdb_id = None
    info.link_type = None
    info.source = source_name
    info.id = None
    info.time = None
    info.latest_episode = None
    info.season_num = None
    info.episode_num = None
    info.size = None
    return info


def _subscription_result_record(info, item, keyword):
    name = (getattr(info, "name", None) or item.get("title") or keyword or "未知资源").strip() or "未知资源"
    size = str(item.get("size") or getattr(info, "size", None) or "").strip() or None
    record = {
        "name": name,
        "title": name,
        "resource_name": name,
        "year": getattr(info, "year", None),
        "resource_pix": getattr(info, "resource_pix", None),
        "resource_type": getattr(info, "resource_type", None),
        "type": getattr(info, "type", None),
        "link": getattr(info, "link", None),
        "tmdb_id": getattr(info, "tmdb_id", None),
        "link_type": getattr(info, "link_type", None),
        "source": getattr(info, "source", None),
        "id": getattr(info, "id", None),
        "time": getattr(info, "time", None),
        "latest_episode": getattr(info, "latest_episode", None),
        "season_num": getattr(info, "season_num", None),
        "episode_num": getattr(info, "episode_num", None),
        "season": getattr(info, "season_num", None),
        "episode": getattr(info, "episode_num", None),
        "size": size,
    }
    return record


def _subscription_results_from_merged(keyword, source_name, merged_by_type, scraper, ResourceInfo):
    results = []
    seen = set()

    for items in (merged_by_type or {}).values():
        for item in items or []:
            url = _subscription_prepare_url(item.get("url", ""), item.get("password", ""))
            link_type = _subscription_link_type(url)
            if not url or not link_type or url in seen:
                continue
            seen.add(url)

            text = _subscription_result_text(item, keyword)
            try:
                info = scraper.parse_resource_info(text)
            except Exception:
                info = None

            if info is None:
                info = _subscription_empty_resource(ResourceInfo, source_name, item.get("title") or keyword)

            info.link = url
            info.link_type = link_type
            info.source = source_name

            size = str(item.get("size") or "").strip()
            if size and not getattr(info, "size", None):
                info.size = size

            if not getattr(info, "name", None):
                info.name = (item.get("title") or keyword or "未知资源").strip() or "未知资源"

            results.append(_subscription_result_record(info, item, keyword))
    return results


def _subscription_fetch_merged(source_key, keyword):
    if source_key == "hdhive":
        return _search_hdhive(keyword)
    if source_key == "gying":
        return (_search_gying(keyword) or {}).get("merged_by_type", {})
    if source_key == "panso":
        return (_search_panso(keyword, ["115"]) or {}).get("merged_by_type", {})
    return {}


def _subscription_search_virtual_source(source_key, source_name, keyword, scraper, ResourceInfo):
    for candidate in _subscription_keyword_variants(keyword):
        merged = _subscription_fetch_merged(source_key, candidate)
        results = _subscription_results_from_merged(candidate, source_name, merged, scraper, ResourceInfo)
        if results:
            if candidate != (keyword or "").strip():
                _log(f"订阅搜源回退命中 {source_name}: {keyword} -> {candidate} ({len(results)})")
            return results
    if keyword:
        _log(f"订阅搜源无结果 {source_name}: {keyword}")
    return []


def _is_search_query(text):
    """判断搜索请求，返回 (keyword, source) 或 False
    后置？→ hdhive，前置？→ panso"""
    if not text:
        return False
    text = text.strip()
    if text.endswith("？") or text.endswith("?"):
        kw = text[:-1].strip()
        return (kw, "hdhive") if kw else False
    if text.startswith("？") or text.startswith("?"):
        kw = text[1:].strip()
        return (kw, "panso") if kw else False
    return False


def _is_wx_direct_search_text(text):
    value = _compact_text(text)
    if not value:
        return False
    if _is_search_query(value):
        return False
    return value if re.search(r"[\u3400-\u9fff]", value) else False


def _is_save_command(text):
    if not text:
        return False
    text = text.strip()
    return int(text) if text.isdigit() else False


def _is_subscribe_command(text):
    """判断企微订阅请求，返回 {"keyword": ..., "prefer_type": ...} 或 False"""
    if not text:
        return False

    text = re.sub(r"\s+", " ", text.strip())
    if _is_list_subscriptions_command(text):
        return False
    if not text.startswith("订阅"):
        return False

    keyword = text[2:].strip().strip("：:")
    if not keyword:
        return False

    keyword, prefer_type, target_year, prefer_animation = _parse_subscribe_keyword(keyword)
    if not keyword:
        return False

    return {
        "keyword": keyword,
        "prefer_type": prefer_type,
        "target_year": target_year,
        "prefer_animation": prefer_animation,
    }


def _is_unsubscribe_command(text):
    """判断企微退订请求，返回 {"keyword": ..., "prefer_type": ...} 或 False"""
    if not text:
        return False

    text = re.sub(r"\s+", " ", text.strip())
    prefix = None
    for item in ["退订", "取消订阅"]:
        if text.startswith(item):
            prefix = item
            break
    if not prefix:
        return False

    keyword = text[len(prefix):].strip().strip("：:")
    if not keyword:
        return False

    if keyword.isdigit():
        return False

    prefer_type = None
    prefix_hints = [
        ("电视剧", "tv"),
        ("剧集", "tv"),
        ("连续剧", "tv"),
        ("电影", "movie"),
        ("影片", "movie"),
        ("tv", "tv"),
        ("movie", "movie"),
    ]
    for hint, media_type in prefix_hints:
        if keyword.lower().startswith(hint.lower()):
            keyword = keyword[len(hint):].strip()
            prefer_type = media_type
            break

    keyword = keyword.strip("：: ")
    keyword = re.sub(r"^[的\s]+", "", keyword)
    if not keyword:
        return False

    return {"keyword": keyword, "prefer_type": prefer_type}


def _is_unsubscribe_index_command(text):
    """判断企微数字退订请求，返回序号或 False"""
    if not text:
        return False

    normalized = re.sub(r"\s+", "", text.strip())
    for prefix in ["退订", "取消订阅"]:
        if normalized.startswith(prefix):
            number = normalized[len(prefix):].strip("：:")
            parsed = _parse_subscription_indices(number)
            if parsed:
                return parsed
    return False


def _parse_subscription_indices(text):
    normalized = re.sub(r"\s+", "", str(text or "").strip())
    if not normalized:
        return False
    if not re.fullmatch(r"\d+(?:[，,]\d+)*", normalized):
        return False
    values = []
    seen = set()
    for part in re.split(r"[，,]", normalized):
        if not part.isdigit():
            return False
        number = int(part)
        if number not in seen:
            seen.add(number)
            values.append(number)
    if not values:
        return False
    return values[0] if len(values) == 1 else values


def _parse_subscribe_keyword(raw_keyword):
    value = _compact_text(raw_keyword).strip("：:")
    if not value:
        return "", None, "", False

    prefer_type = None
    prefer_animation = False
    type_hints = [
        ("电视剧", "tv"),
        ("剧集", "tv"),
        ("连续剧", "tv"),
        ("电影", "movie"),
        ("影片", "movie"),
        ("tv", "tv"),
        ("movie", "movie"),
    ]
    animation_hints = [
        "动漫版",
        "动画版",
        "番剧版",
        "动漫",
        "动画",
        "番剧",
        "国漫",
        "日漫",
        "美漫",
        "anime",
        "cartoon",
    ]

    changed = True
    while value and changed:
        changed = False
        normalized = value.lower()

        for hint, media_type in type_hints:
            if normalized.startswith(hint.lower()):
                value = value[len(hint):].strip()
                prefer_type = media_type
                changed = True
                break
        if changed:
            value = re.sub(r"^[的\s]+", "", value)
            continue

        for hint in animation_hints:
            if normalized.startswith(hint.lower()):
                value = value[len(hint):].strip()
                prefer_animation = True
                changed = True
                break
        if changed:
            value = re.sub(r"^[的\s]+", "", value)

    target_year = ""
    year_match = re.search(
        r"(?:[\s._-]*[\(\[（【]?\s*((?:19|20)\d{2})\s*[\)\]）】]?\s*)$",
        value,
        flags=re.I,
    )
    if year_match:
        stripped = _compact_text(value[:year_match.start()])
        if stripped:
            target_year = year_match.group(1)
            value = stripped

    value = value.strip("：: ")
    value = re.sub(r"^[的\s]+", "", value)
    value = re.sub(r"[的\s]+$", "", value)
    return value, prefer_type, target_year, prefer_animation


def _is_delete_completed_command(text):
    """判断企微删除已完成订阅请求"""
    if not text:
        return False

    text = re.sub(r"\s+", "", text.strip().lower())
    return text in {
        "删除已完成",
        "删除已完成订阅",
        "删除完成订阅",
        "清理已完成",
        "清理已完成订阅",
        "清理完成订阅",
    }


def _is_douban_hot_command(text):
    if not text:
        return ""

    normalized = _normalize_wx_text(text)
    if normalized in {
        _normalize_wx_text(WX_DOUBAN_HOT_MOVIE_MENU_TITLE),
        _normalize_wx_text(WX_DOUBAN_HOT_MOVIE_COMMAND),
        "豆瓣电影热门",
        "热门电影",
    }:
        return "movie"
    if normalized in {
        _normalize_wx_text(WX_DOUBAN_HOT_TV_MENU_TITLE),
        _normalize_wx_text(WX_DOUBAN_HOT_TV_COMMAND),
        "豆瓣热门剧集",
        "热门电视剧",
        "热门剧集",
    }:
        return "tv"
    return ""


def _is_update_file_command(text):
    if not text:
        return False

    normalized = re.sub(r"\s+", "", str(text or "").strip().lower())
    if normalized.startswith("/") and "@" in normalized:
        normalized = normalized.split("@", 1)[0]
    return normalized in {
        "文件更新",
        "更新文件",
        "检查更新",
        "检测更新",
        "插件更新",
        "更新插件",
        "/file_update",
        "file_update",
        "/check_update",
        "check_update",
    }


def _is_list_subscriptions_command(text):
    """判断企微订阅列表请求"""
    if not text:
        return False

    text = re.sub(r"\s+", "", text.strip().lower())
    if text.startswith("/") and "@" in text:
        command, mention = text.split("@", 1)
        if mention:
            text = command
    aliases = {
        WX_LIST_SUBSCRIPTIONS_MENU_TITLE.lower(),
        "订阅列表",
        "我的订阅",
        "所有订阅",
        "当前订阅",
        "全部订阅",
        "查看订阅",
        "查询订阅",
        "查询所有订阅",
        "查询当前订阅",
        "查询当前所有订阅",
        WX_LIST_SUBSCRIPTIONS_COMMAND.lower(),
        WX_LIST_SUBSCRIPTIONS_COMMAND.lstrip("/").lower(),
        LEGACY_LIST_SUBSCRIPTIONS_COMMAND.lower(),
        LEGACY_LIST_SUBSCRIPTIONS_COMMAND.lstrip("/").lower(),
    }
    return text in aliases


def _is_supported_text_command(text, include_internal_wx=False):
    if not text:
        return False
    if (
        _is_search_query(text)
        or _is_wx_direct_search_text(text)
        or _is_subscribe_command(text)
        or _is_unsubscribe_index_command(text)
        or _is_unsubscribe_command(text)
        or _is_delete_completed_command(text)
        or _is_douban_hot_command(text)
        or _is_update_file_command(text)
    ):
        return True
    if _is_list_subscriptions_command(text):
        return include_internal_wx or not _is_internal_wx_list_subscriptions_command(text)
    return False


def _reserve_command_dispatch(channel, target_id, text, ttl=3):
    normalized_text = re.sub(r"\s+", " ", str(text or "").strip()).lower()
    if not normalized_text:
        return False

    now = time.time()
    key = (str(channel or "").strip().lower(), str(target_id or "").strip(), normalized_text)
    with _recent_command_lock:
        stale_keys = [
            item_key
            for item_key, item_time in _recent_command_cache.items()
            if (now - item_time) > ttl
        ]
        for item_key in stale_keys:
            _recent_command_cache.pop(item_key, None)

        if key in _recent_command_cache:
            return False

        _recent_command_cache[key] = now
        return True


def _is_animation_keyword(text):
    normalized = _compact_text(text).lower()
    if not normalized:
        return False
    for token in ["动漫", "动画", "番剧", "国漫", "日漫", "美漫", "anime", "cartoon"]:
        if token in normalized:
            return True
    return False


def _candidate_is_animation(candidate):
    if not isinstance(candidate, dict):
        return False
    try:
        genre_ids = [int(item) for item in (candidate.get("genre_ids") or []) if str(item).strip()]
    except Exception:
        genre_ids = []
    return 16 in genre_ids


def _subscribe_candidate_type_label(candidate):
    media_type = str((candidate or {}).get("media_type") or "").strip().lower()
    if _candidate_is_animation(candidate):
        return "动漫" if media_type == "tv" else "动画电影"
    if media_type == "tv":
        return "电视剧"
    if media_type == "movie":
        return "电影"
    return media_type or "未知"


def _resolve_subscribe_candidates(keyword, prefer_type=None, target_year="", prefer_animation=False, page_size=50):
    return _build_ranked_tmdb_candidate_items(
        keyword,
        prefer_type=prefer_type,
        target_year=target_year,
        prefer_animation=prefer_animation,
        max_results=page_size,
        cache_scope="subscribe_candidates",
    )


def _choose_subscribe_candidate(keyword, prefer_type=None, target_year="", prefer_animation=False):
    ranked = _resolve_subscribe_candidates(
        keyword,
        prefer_type=prefer_type,
        target_year=target_year,
        prefer_animation=prefer_animation,
    )
    if not ranked:
        return None, []
    return ranked[0], ranked


def _format_subscribe_candidate_prompt(keyword, candidates):
    items = list(candidates or [])
    if not items:
        return f"❌ 未找到《{keyword}》对应的媒体信息"

    display_counts = {}
    rows = []
    for candidate in items:
        title = str(candidate.get("title") or keyword or "未知").strip() or "未知"
        media_label = _subscribe_candidate_type_label(candidate)
        year = str(candidate.get("year") or "").strip() or "未知"
        display_key = (title, media_label, year)
        display_counts[display_key] = display_counts.get(display_key, 0) + 1
        rows.append((title, media_label, year, str(candidate.get("tmdb_id") or "").strip(), display_key))

    lines = [f"📚 《{keyword}》找到 {len(items)} 个匹配，请回复序号确认订阅", ""]
    for idx, (title, media_label, year, tmdb_id, display_key) in enumerate(rows, start=1):
        line = f"{idx}. {title}｜{media_label}｜{year}"
        if display_counts.get(display_key, 0) > 1 and tmdb_id:
            line = f"{line}｜{tmdb_id}"
        lines.append(line)
    return "\n".join(lines)


def _build_wx_subscribe_candidate_items(keyword, candidates):
    normalized_items = []
    for candidate in list(candidates or []):
        if not isinstance(candidate, dict):
            continue
        item = dict(candidate or {})
        item["title"] = str(item.get("title") or keyword or "未知").strip() or "未知"
        item["original_title"] = str(item.get("original_title") or "").strip()
        item["year"] = str(item.get("year") or "").strip()
        item["media_label"] = (
            str(item.get("media_label") or _hdhive_candidate_media_label(item) or _subscribe_candidate_type_label(item)).strip()
            or "未知"
        )
        item["rating_text"] = str(item.get("rating_text") or _format_tmdb_rating(item.get("vote_average"))).strip() or "暂无评分"
        item["vote_average"] = item.get("vote_average") or 0
        item["poster_path"] = str(item.get("poster_path") or "").strip()
        item["backdrop_path"] = str(item.get("backdrop_path") or "").strip()
        item["poster_url"] = str(item.get("poster_url") or _build_tmdb_image_url(item.get("poster_path"), size="w500")).strip()
        item["backdrop_url"] = str(item.get("backdrop_url") or _build_tmdb_image_url(item.get("backdrop_path"), size="w780")).strip()
        item["image_url"] = str(item.get("image_url") or item["backdrop_url"] or item["poster_url"]).strip()
        item["tmdb_url"] = str(item.get("tmdb_url") or _build_tmdb_url(item.get("media_type"), item.get("tmdb_id"))).strip()
        normalized_items.append(item)
    return normalized_items


def _format_wx_subscribe_candidate_prompt(keyword, total, page, total_pages):
    prompt = f"📚 《{keyword}》找到 {total} 个匹配，请回复序号确认订阅"
    if total_pages > 1:
        prompt = f"{prompt}（第{page}/{total_pages}页 p: 上一页 n: 下一页）"
    return prompt


def _build_ranked_tmdb_candidate_items(keyword, prefer_type=None, target_year="", prefer_animation=False, max_results=50, cache_scope="tmdb_candidates"):
    keyword = _compact_text(keyword)
    if not keyword:
        return []

    target_year = _extract_subscribe_year(target_year)
    cache_key = _build_search_cache_key(
        cache_scope,
        keyword,
        prefer_type or "",
        target_year or "",
        "1" if prefer_animation else "0",
        max(1, int(max_results or 50)),
    )
    cached = _get_cached_search_result(cache_key)
    if cached is not None:
        return cached

    candidates = _search_tmdb_candidates(
        keyword,
        collect_all=True,
        page_size=max(10, int(max_results or 50)),
    )
    if not candidates:
        return _set_cached_search_result(cache_key, [], SEARCH_EMPTY_CACHE_TTL_SECONDS)

    normalized_keyword = _normalize_tmdb_title(keyword)

    def _candidate_match_level(candidate):
        titles = []
        for value in [
            candidate.get("title"),
            candidate.get("original_title"),
        ]:
            text = _compact_text(value)
            if text and text not in titles:
                titles.append(text)

        for title in titles:
            if _normalize_tmdb_title(title) == normalized_keyword:
                return 0

        direct_digit_pattern = re.compile(
            rf"^{re.escape(keyword)}(?:\d+|[一二三四五六七八九十]+|[：:：\-_\s].+)$",
            re.I,
        )
        for title in titles:
            if direct_digit_pattern.search(title):
                return 1
            if _is_precise_source_title_match(keyword, title):
                return 2

        for title in titles:
            if normalized_keyword and normalized_keyword in _normalize_tmdb_title(title):
                return 3

        return 9

    filtered = []
    for candidate in candidates:
        media_type = str(candidate.get("media_type") or "").strip().lower()
        if prefer_type in {"movie", "tv"} and media_type != prefer_type:
            continue
        if prefer_animation and not _candidate_is_animation(candidate):
            continue
        if target_year and str(candidate.get("year") or "").strip() != target_year:
            continue
        match_level = _candidate_match_level(candidate)
        if match_level >= 9:
            continue
        normalized = dict(candidate or {})
        normalized["_match_level"] = match_level
        filtered.append(normalized)

    if not filtered:
        return _set_cached_search_result(cache_key, [], SEARCH_EMPTY_CACHE_TTL_SECONDS)

    filtered.sort(
        key=lambda item: (
            0 if int(item.get("_match_level", 9)) == 0 else 1,
            int(item.get("_match_level", 9)),
            -(int(str(item.get("year") or "0").strip() or "0")),
            0 if str(item.get("media_type") or "").strip().lower() == "movie" else 1,
            str(item.get("title") or ""),
            int(str(item.get("tmdb_id") or "0").strip() or "0"),
        ),
    )

    normalized_items = []
    for candidate in filtered[:max(1, int(max_results or 20))]:
        detail = _fetch_tmdb_detail(candidate.get("tmdb_id"), candidate.get("media_type"))
        poster_url, backdrop_url = _resolve_tmdb_image_urls(
            detail,
            poster_path=candidate.get("poster_path"),
            backdrop_path=candidate.get("backdrop_path"),
        )
        rating_value = ""
        if isinstance(detail, dict):
            rating_value = detail.get("vote_average")
        if rating_value in {"", None}:
            rating_value = candidate.get("vote_average")

        detail_title = str((detail or {}).get("title") or "").strip()
        candidate_title = str(candidate.get("title") or "").strip()
        display_title = detail_title or candidate_title or keyword or "未知"
        if candidate_title and len(candidate_title) > len(display_title):
            display_title = candidate_title

        item = dict(candidate or {})
        item["title"] = display_title.strip() or "未知"
        item["original_title"] = str(candidate.get("original_title") or "").strip()
        item["year"] = str((detail or {}).get("year") or candidate.get("year") or "").strip()
        item["media_label"] = _hdhive_candidate_media_label(candidate) or _subscribe_candidate_type_label(candidate)
        item["rating_text"] = _format_tmdb_rating(rating_value)
        item["vote_average"] = rating_value or 0
        item["poster_path"] = str((detail or {}).get("poster_path") or candidate.get("poster_path") or "").strip()
        item["backdrop_path"] = str((detail or {}).get("backdrop_path") or candidate.get("backdrop_path") or "").strip()
        item["poster_url"] = poster_url
        item["backdrop_url"] = backdrop_url
        item["image_url"] = backdrop_url or poster_url
        item["tmdb_url"] = _build_tmdb_url(item.get("media_type"), item.get("tmdb_id"))
        normalized_items.append(item)

    ttl = TMDB_SEARCH_CACHE_TTL_SECONDS if normalized_items else SEARCH_EMPTY_CACHE_TTL_SECONDS
    return _set_cached_search_result(cache_key, normalized_items, ttl)


def _build_hdhive_search_candidate_items(keyword, max_results=50):
    parsed_keyword, prefer_type, target_year, prefer_animation = _parse_subscribe_keyword(keyword)
    if not parsed_keyword:
        return []
    return _build_ranked_tmdb_candidate_items(
        parsed_keyword,
        prefer_type=prefer_type,
        target_year=target_year,
        prefer_animation=prefer_animation,
        max_results=max_results,
        cache_scope="hdhive_search_candidates",
    )


def _format_hdhive_search_candidate_text(index, candidate):
    title = str((candidate or {}).get("title") or "未知").strip() or "未知"
    year = str((candidate or {}).get("year") or "").strip() or "未知"
    media_label = str((candidate or {}).get("media_label") or _subscribe_candidate_type_label(candidate)).strip() or "未知"
    rating_text = str((candidate or {}).get("rating_text") or _format_tmdb_rating((candidate or {}).get("vote_average"))).strip()
    original_title = str((candidate or {}).get("original_title") or "").strip()
    lines = [f"{index}. {title}", f"年份：{year}", f"类型：{media_label}", f"评分：{rating_text}"]
    if original_title and _normalize_tmdb_title(original_title) != _normalize_tmdb_title(title):
        lines.append(f"原名：{original_title}")
    return "\n".join(lines)


def _format_hdhive_search_candidate_row(index, candidate):
    title = str((candidate or {}).get("title") or "未知").strip() or "未知"
    year = str((candidate or {}).get("year") or "").strip() or "未知"
    media_label = str((candidate or {}).get("media_label") or _subscribe_candidate_type_label(candidate)).strip() or "未知"
    rating_text = str((candidate or {}).get("rating_text") or _format_tmdb_rating((candidate or {}).get("vote_average"))).strip()
    parts = [f"{index}. {title}", year, media_label, rating_text]
    return "｜".join([part for part in parts if part])


def _format_hdhive_search_candidate_prompt(keyword, candidates):
    count = len(list(candidates or []))
    if count <= 0:
        return f"❌ 未找到《{keyword}》对应的影视条目"
    return f"🎬 《{keyword}》找到 {count} 个影视候选，请回复序号继续搜索影巢"


_WX_IMAGE_FONT_CACHE = {}


def _wx_candidate_font(size, bold=False):
    cache_key = (int(size or 0), bool(bold))
    cached = _WX_IMAGE_FONT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        from PIL import ImageFont
    except Exception:
        return None

    font_paths = []
    if bold:
        font_paths.extend([
            "/System/Library/Fonts/PingFang.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc",
            "/usr/share/fonts/opentype/noto/NotoSerifCJK-Bold.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
            "/Library/Fonts/Arial Unicode.ttf",
        ])
    else:
        font_paths.extend([
            "/System/Library/Fonts/PingFang.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc",
            "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
            "/Library/Fonts/Arial Unicode.ttf",
        ])
    font_paths.extend([
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode MS.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ])

    for path in font_paths:
        if not os.path.exists(path):
            continue
        try:
            font = ImageFont.truetype(path, int(size or 14))
            _WX_IMAGE_FONT_CACHE[cache_key] = font
            return font
        except Exception:
            continue

    try:
        font = ImageFont.load_default()
    except Exception:
        font = None
    _WX_IMAGE_FONT_CACHE[cache_key] = font
    return font


def _wx_draw_wrap_lines(draw, text, font, max_width, max_lines=1):
    text = str(text or "").strip()
    if not text:
        return []
    if font is None:
        return [text[: max(1, int(max_width // 12) or 1)]]

    lines = []
    current = ""
    for ch in text:
        trial = current + ch
        try:
            bbox = draw.textbbox((0, 0), trial, font=font)
            width = bbox[2] - bbox[0]
        except Exception:
            width = len(trial) * 12
        if current and width > max_width:
            lines.append(current)
            current = ch
            if len(lines) >= max_lines:
                break
        else:
            current = trial

    if len(lines) < max_lines and current:
        lines.append(current)

    if len(lines) > max_lines:
        lines = lines[:max_lines]

    consumed = "".join(lines)
    if len(consumed) < len(text) and lines:
        tail = lines[-1].rstrip()
        ellipsis = "…"
        while tail:
            trial = tail + ellipsis
            try:
                bbox = draw.textbbox((0, 0), trial, font=font)
                width = bbox[2] - bbox[0]
            except Exception:
                width = len(trial) * 12
            if width <= max_width:
                lines[-1] = trial
                break
            tail = tail[:-1]
        else:
            lines[-1] = ellipsis
    return lines


def _wx_fetch_pil_image(url, timeout=12):
    image_url = str(url or "").strip()
    if not image_url:
        return None
    try:
        import requests
        from PIL import Image
        proxy = os.environ.get("HDHIVE_PROXY", "") or _get_cms_proxy()
        proxies = {"http": proxy, "https": proxy} if proxy else None
        resp = requests.get(image_url, timeout=timeout, proxies=proxies)
        resp.raise_for_status()
        return Image.open(io.BytesIO(resp.content)).convert("RGB")
    except Exception:
        return None


def _wx_fetch_pil_image_any(urls, timeout=4):
    for url in list(urls or []):
        image = _wx_fetch_pil_image(url, timeout=timeout)
        if image is not None:
            return image
    return None


def _wx_fit_cover_image(image, size, fill_color=(228, 231, 237)):
    try:
        from PIL import Image
    except Exception:
        return None

    width, height = size
    canvas = Image.new("RGB", (int(width), int(height)), fill_color)
    if image is None:
        return canvas

    src_w, src_h = image.size
    if src_w <= 0 or src_h <= 0:
        return canvas

    scale = max(width / float(src_w), height / float(src_h))
    resized = image.resize((max(1, int(src_w * scale)), max(1, int(src_h * scale))))
    left = max(0, (resized.size[0] - width) // 2)
    top = max(0, (resized.size[1] - height) // 2)
    canvas.paste(resized.crop((left, top, left + width, top + height)), (0, 0))
    return canvas


def _wx_fit_contain_image(image, size, fill_color=(255, 255, 255)):
    try:
        from PIL import Image
    except Exception:
        return None

    width, height = size
    canvas = Image.new("RGB", (int(width), int(height)), fill_color)
    if image is None:
        return canvas

    src_w, src_h = image.size
    if src_w <= 0 or src_h <= 0:
        return canvas

    scale = min(width / float(src_w), height / float(src_h))
    resized = image.resize((max(1, int(src_w * scale)), max(1, int(src_h * scale))))
    paste_x = max(0, (width - resized.size[0]) // 2)
    paste_y = max(0, (height - resized.size[1]) // 2)
    canvas.paste(resized, (paste_x, paste_y))
    return canvas


def _build_wx_hdhive_candidate_image(keyword, candidates):
    items = list(candidates or [])
    if not items:
        return ""

    signature = hashlib.md5(
        json.dumps(
            {
                "keyword": str(keyword or "").strip(),
                "items": [
                    {
                        "tmdb_id": item.get("tmdb_id"),
                        "type": item.get("media_type"),
                        "title": item.get("title"),
                        "year": item.get("year"),
                        "rating": item.get("rating_text"),
                        "poster": item.get("poster_url"),
                        "backdrop": item.get("backdrop_url"),
                    }
                    for item in items
                ],
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    image_path = f"/tmp/cms_wx_hdhive_candidates_{signature}.jpg"
    if os.path.exists(image_path) and (time.time() - os.path.getmtime(image_path)) < 1800:
        return image_path

    try:
        from PIL import Image, ImageDraw
    except Exception:
        return ""

    def _featured_urls(item):
        item = item or {}
        return [
            _build_tmdb_image_url(item.get("backdrop_path"), size="w780"),
            item.get("backdrop_url"),
            _build_tmdb_image_url(item.get("poster_path"), size="w500"),
            item.get("poster_url"),
        ]

    def _poster_urls(item):
        item = item or {}
        return [
            _build_tmdb_image_url(item.get("poster_path"), size="w500"),
            item.get("poster_url"),
            _build_tmdb_image_url(item.get("backdrop_path"), size="w500"),
            item.get("backdrop_url"),
        ]

    image_jobs = [("featured", _featured_urls(items[0]))]
    for idx, item in enumerate(items[1:], start=1):
        image_jobs.append((idx, _poster_urls(item)))

    fetched_images = {}
    max_workers = min(8, max(2, len(image_jobs)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="cms-wximg") as executor:
        future_map = {
            executor.submit(_wx_fetch_pil_image_any, urls, 4): key
            for key, urls in image_jobs
        }
        for future in concurrent.futures.as_completed(future_map):
            key = future_map[future]
            try:
                fetched_images[key] = future.result()
            except Exception:
                fetched_images[key] = None

    width = 860
    side = 46
    featured_height = 268
    row_height = 126
    gap = 16
    content_width = width - side * 2
    total_height = 28 + featured_height + gap + max(0, len(items) - 1) * row_height + 28
    canvas = Image.new("RGB", (width, total_height), (244, 245, 247))
    draw = ImageDraw.Draw(canvas)

    title_font = _wx_candidate_font(30, bold=True)
    meta_font = _wx_candidate_font(21, bold=False)
    small_font = _wx_candidate_font(19, bold=False)
    featured = dict(items[0] or {})
    featured_bg = fetched_images.get("featured")
    featured_img = _wx_fit_cover_image(featured_bg, (content_width, featured_height))
    canvas.paste(featured_img, (side, 28))
    overlay_height = 92
    overlay = Image.new("RGBA", (content_width, overlay_height), (0, 0, 0, 132))
    canvas.paste(overlay, (side, 28 + featured_height - overlay_height), overlay)

    feat_title = f"1. {str(featured.get('title') or '未知').strip()} ({str(featured.get('year') or '未知').strip()})"
    feat_meta_parts = [
        f"类型：{str(featured.get('media_label') or '未知').strip()}",
        f"评分：{str(featured.get('rating_text') or '暂无评分').strip()}",
    ]
    feat_title_lines = _wx_draw_wrap_lines(draw, feat_title, title_font, content_width - 28, max_lines=1)
    feat_meta_lines = _wx_draw_wrap_lines(draw, "，".join(feat_meta_parts), meta_font, content_width - 28, max_lines=1)
    title_y = 28 + featured_height - overlay_height + 16
    for line in feat_title_lines:
        draw.text((side + 14, title_y), line, fill=(255, 255, 255), font=title_font)
        title_y += 34
    for line in feat_meta_lines:
        draw.text((side + 14, title_y), line, fill=(238, 238, 238), font=meta_font)
        title_y += 26

    current_y = 28 + featured_height + gap
    for idx, item in enumerate(items[1:], start=2):
        bg_color = (255, 255, 255) if idx % 2 == 0 else (242, 242, 244)
        draw.rectangle((side, current_y, side + content_width, current_y + row_height), fill=bg_color)

        poster_box = (84, 100)
        poster = fetched_images.get(idx - 1)
        poster_img = _wx_fit_contain_image(poster, poster_box)
        poster_x = side + content_width - poster_box[0] - 18
        poster_y = current_y + (row_height - poster_box[1]) // 2
        canvas.paste(poster_img, (poster_x, poster_y))

        text_left = side + 18
        text_width = content_width - poster_box[0] - 52
        row_title = f"{idx}. {str(item.get('title') or '未知').strip()} ({str(item.get('year') or '未知').strip()})"
        row_meta = f"类型：{str(item.get('media_label') or '未知').strip()}，评分：{str(item.get('rating_text') or '暂无评分').strip()}"

        line_y = current_y + 20
        for line in _wx_draw_wrap_lines(draw, row_title, meta_font, text_width, max_lines=1):
            draw.text((text_left, line_y), line, fill=(24, 24, 24), font=meta_font)
            line_y += 28
        for line in _wx_draw_wrap_lines(draw, row_meta, small_font, text_width, max_lines=1):
            draw.text((text_left, line_y), line, fill=(60, 60, 60), font=small_font)
            line_y += 24

        current_y += row_height

    try:
        canvas.save(image_path, format="JPEG", quality=90, optimize=True)
        return image_path
    except Exception:
        return ""


def _douban_jsonp_to_dict(payload):
    text = str(payload or "").strip()
    if not text:
        return {}
    if text.startswith(";"):
        text = text[1:].strip()
    match = re.match(r"^[\w$]+\((.*)\)\s*;?\s*$", text, flags=re.S)
    if match:
        text = match.group(1)
    try:
        data = json.loads(text)
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _fetch_douban_jsonp(url, referer):
    import requests

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": str(referer or "").strip(),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Origin": "https://m.douban.com",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=DOUBAN_REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = _douban_jsonp_to_dict(resp.text)
        if data:
            return data
    except Exception:
        pass

    curl_cmd = [
        "curl",
        "-sS",
        "--max-time",
        str(int(DOUBAN_REQUEST_TIMEOUT_SECONDS or 20)),
        "-A",
        "Mozilla/5.0",
        "-H",
        f"Referer: {str(referer or '').strip()}",
        "-H",
        "Accept: application/json, text/plain, */*",
        "-H",
        "Accept-Language: zh-CN,zh;q=0.9",
        url,
    ]
    try:
        result = subprocess.run(
            curl_cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return {}
    if result.returncode != 0:
        return {}
    return _douban_jsonp_to_dict(result.stdout)


def _build_douban_subject_collection_url(collection_id, count, start=0, loc_id=DOUBAN_DEFAULT_LOC_ID):
    collection_id = str(collection_id or "").strip()
    loc_id = str(loc_id or DOUBAN_DEFAULT_LOC_ID).strip() or DOUBAN_DEFAULT_LOC_ID
    return (
        "https://m.douban.com/rexxar/api/v2/subject_collection/"
        f"{collection_id}/items?os=other&start={int(start or 0)}&count={int(count or 0)}"
        f"&loc_id={loc_id}&callback=jsonp1"
    )


def _fetch_douban_subject_collection_items(collection_id, referer, count, start=0, loc_id=DOUBAN_DEFAULT_LOC_ID):
    data = _fetch_douban_jsonp(
        _build_douban_subject_collection_url(collection_id, count=count, start=start, loc_id=loc_id),
        referer=referer,
    )
    return list(data.get("subject_collection_items") or [])


def _fetch_douban_movie_hot_collection_id(loc_id=DOUBAN_DEFAULT_LOC_ID):
    url = (
        "https://m.douban.com/rexxar/api/v2/movie/modules"
        f"?need_manual_chart_card=1&loc_id={str(loc_id or DOUBAN_DEFAULT_LOC_ID).strip() or DOUBAN_DEFAULT_LOC_ID}"
        "&callback=jsonp1"
    )
    data = _fetch_douban_jsonp(url, referer="https://m.douban.com/movie/")
    modules = list(data.get("modules") or [])
    for module in modules:
        if str(module.get("module_name") or "").strip() != "movie_subject_unions":
            continue
        for item in list(module.get("data") or []):
            collections = [str(value or "").strip() for value in (item.get("collections") or []) if str(value or "").strip()]
            if not collections:
                continue
            title = str(item.get("title") or "").strip()
            if "豆瓣热门" in title:
                return collections[0]
    return "movie_hot_gaia"


def _fetch_douban_hot_items(hot_type, count=DOUBAN_HOT_FETCH_COUNT):
    hot_type = str(hot_type or "").strip().lower()
    fetch_count = max(int(count or DOUBAN_HOT_FETCH_COUNT), DOUBAN_HOT_DISPLAY_COUNT)
    if hot_type == "movie":
        collection_id = _fetch_douban_movie_hot_collection_id()
        return _fetch_douban_subject_collection_items(
            collection_id,
            referer="https://m.douban.com/movie/",
            count=fetch_count,
        )
    if hot_type == "tv":
        return _fetch_douban_subject_collection_items(
            "tv_hot",
            referer="https://m.douban.com/tv/",
            count=fetch_count,
        )
    return []


def _resolve_douban_hot_candidate(item, hot_type):
    title = str((item or {}).get("title") or "").strip()
    if not title:
        return None

    year = str((item or {}).get("year") or "").strip()
    subtitle = str((item or {}).get("card_subtitle") or "").strip()
    prefer_type = "tv" if str(hot_type or "").strip().lower() == "tv" else "movie"
    prefer_animation = _is_animation_keyword(subtitle)
    candidate, _ = _choose_subscribe_candidate(
        title,
        prefer_type=prefer_type,
        target_year=year,
        prefer_animation=prefer_animation,
    )
    return candidate


def _format_douban_rating_text(item):
    rating = (item or {}).get("rating") or {}
    value = rating.get("value")
    try:
        value = float(value)
    except Exception:
        value = 0
    return f"{value:.1f}" if value > 0 else "暂无评分"


def _format_douban_hot_prompt(title, rows):
    rows = list(rows or [])
    if not rows:
        return f"❌ {title} 暂无可订阅结果"

    lines = [f"📚 {title}（前 {len(rows)} 条）", "回复序号确认订阅", ""]
    for idx, row in enumerate(rows, start=1):
        item = row.get("item") or {}
        display_title = str(item.get("title") or "未知").strip() or "未知"
        subtitle = str(item.get("card_subtitle") or "").strip()
        rating_text = _format_douban_rating_text(item)
        lines.append(f"{idx}. {display_title}")
        if subtitle:
            lines.append(subtitle)
        lines.append(f"豆瓣：{rating_text}")
        if idx != len(rows):
            lines.append("")
    return "\n".join(lines)


def _normalize_external_http_url(value, default_base=""):
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("//"):
        return "https:" + text
    if re.match(r"^https?://", text, re.I):
        return text
    base = str(default_base or "").strip().rstrip("/")
    if base and text.startswith("/"):
        return f"{base}{text}"
    return ""


def _build_douban_subject_url(item):
    item = item or {}
    subject_id = ""
    for key in ["id", "subject_id", "target_id"]:
        value = re.sub(r"\D+", "", str(item.get(key) or "").strip())
        if value:
            subject_id = value
            break

    if not subject_id:
        for key in ["uri", "url", "share_url", "sharing_url", "target_url"]:
            value = str(item.get(key) or "").strip()
            match = re.search(r"douban://douban\.com/(?:movie|tv)/(\d+)", value, re.I)
            if not match:
                match = re.search(r"/subject/(\d+)/?", value, re.I)
            if match:
                subject_id = match.group(1)
                break

    if not subject_id:
        return ""
    return f"https://movie.douban.com/subject/{subject_id}/"


def _extract_douban_hot_link_url(item):
    item = item or {}
    subject_url = _build_douban_subject_url(item)
    if subject_url:
        return subject_url
    for key in ["url", "uri", "share_url", "sharing_url", "target_url"]:
        url = _normalize_external_http_url(item.get(key), default_base="https://m.douban.com")
        if url:
            return url
    return ""


def _collect_douban_image_urls(value, results=None):
    if results is None:
        results = []
    if isinstance(value, dict):
        for key in ["large", "normal", "medium", "small", "url", "image", "pic"]:
            if key in value:
                _collect_douban_image_urls(value.get(key), results)
        for sub_value in value.values():
            if isinstance(sub_value, (dict, list, tuple)):
                _collect_douban_image_urls(sub_value, results)
        return results
    if isinstance(value, (list, tuple)):
        for item in value:
            _collect_douban_image_urls(item, results)
        return results
    url = _normalize_external_http_url(value)
    if url and url not in results:
        results.append(url)
    return results


def _extract_douban_hot_image_url(item):
    item = item or {}
    image_urls = []
    for key in ["pic", "cover_url", "cover", "poster", "image", "photos", "images", "cover_pic"]:
        _collect_douban_image_urls(item.get(key), image_urls)
    return image_urls[0] if image_urls else ""


def _build_douban_hot_display_candidates(rows):
    display_candidates = []
    for row in list(rows or []):
        candidate = row.get("candidate") or {}
        item = row.get("item") or {}
        if not isinstance(candidate, dict):
            continue

        display = dict(candidate)
        subscribe_title = str(candidate.get("title") or "").strip()
        subscribe_year = str(candidate.get("year") or "").strip()
        douban_title = str(item.get("title") or "").strip()
        douban_year = str(item.get("year") or "").strip()
        douban_url = _extract_douban_hot_link_url(item)
        douban_image = _extract_douban_hot_image_url(item)
        douban_rating = _format_douban_rating_text(item)

        if subscribe_title:
            display["_subscribe_title"] = subscribe_title
        if subscribe_year:
            display["_subscribe_year"] = subscribe_year
        if douban_title:
            display["title"] = douban_title
        if douban_year:
            display["year"] = douban_year
        if douban_rating:
            display["rating_text"] = douban_rating
        if douban_image:
            display["douban_image_url"] = douban_image
        if douban_url:
            display["tmdb_url"] = douban_url
            display["douban_url"] = douban_url

        display_candidates.append(display)
    return display_candidates


def _build_douban_hot_subscription_rows(hot_type, limit=DOUBAN_HOT_DISPLAY_COUNT, use_cache=True):
    hot_type = str(hot_type or "").strip().lower()
    limit = max(1, int(limit or DOUBAN_HOT_DISPLAY_COUNT))
    if use_cache:
        cached = _get_douban_hot_cached_rows(hot_type, limit)
        if cached is not None:
            return list(cached or [])

    hot_items = list(_fetch_douban_hot_items(hot_type, count=max(limit, DOUBAN_HOT_FETCH_COUNT)) or [])
    if not hot_items:
        return []

    rows = []
    max_workers = min(8, max(2, len(hot_items)))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(_resolve_douban_hot_candidate, item, hot_type): (index, item)
            for index, item in enumerate(hot_items)
        }
        for future in concurrent.futures.as_completed(future_map):
            index, item = future_map[future]
            try:
                candidate = future.result()
            except Exception:
                candidate = None
            if not candidate:
                continue
            rows.append((index, item, candidate))

    rows.sort(key=lambda entry: entry[0])
    normalized_rows = [{"item": item, "candidate": candidate} for _, item, candidate in rows[:limit]]
    if not normalized_rows:
        return []
    return _set_douban_hot_cached_rows(hot_type, limit, normalized_rows, DOUBAN_HOT_DAILY_CACHE_TTL_SECONDS)


def _build_subscribe_candidate_result(candidate, fallback_keyword=""):
    payload = _build_subscribe_payload(candidate, fallback_keyword=fallback_keyword)
    if not payload:
        return None

    ok, msg = _cms_add_submedia(payload)
    detail = _fetch_tmdb_detail(payload["tmdb_id"], payload["type"]) if ok else None
    poster_url, backdrop_url = _resolve_tmdb_image_urls(
        detail,
        poster_path=payload.get("poster_path"),
        backdrop_path=payload.get("backdrop_path"),
    )
    feedback = _format_subscribe_feedback(
        payload["title"],
        payload["type"],
        payload["year"],
        ok=ok,
        msg=msg,
        tmdb_id=payload["tmdb_id"],
        poster_path=payload.get("poster_path"),
        tmdb_detail=detail,
    )
    return {
        "ok": ok,
        "msg": msg,
        "payload": payload,
        "detail": detail,
        "poster_url": poster_url,
        "backdrop_url": backdrop_url,
        "feedback": feedback,
    }


def _build_subscribe_payload(candidate, fallback_keyword=""):
    if not isinstance(candidate, dict):
        return None

    payload = {
        "title": candidate.get("_subscribe_title") or candidate.get("title") or fallback_keyword,
        "year": candidate.get("_subscribe_year") or candidate.get("year") or "",
        "tmdb_id": str(candidate.get("tmdb_id") or "").strip(),
        "type": candidate.get("media_type") or "",
        "poster_path": candidate.get("poster_path") or "",
        "backdrop_path": candidate.get("backdrop_path") or "",
    }
    if not payload["tmdb_id"] or not payload["type"]:
        return None
    return payload


def _extract_subscribe_year(value):
    text = str(value or "").strip()
    if not text:
        return ""
    match = re.search(r"(19|20)\d{2}", text)
    return match.group(0) if match else ""


def _normalize_subscribe_payload(payload):
    if not isinstance(payload, dict):
        return payload, False

    title = str(payload.get("title") or "").strip()
    media_type = str(payload.get("type") or payload.get("media_type") or "").strip().lower()
    current_tmdb_id = str(payload.get("tmdb_id") or "").strip()
    target_year = _extract_subscribe_year(payload.get("year"))

    if not title or media_type not in {"movie", "tv"}:
        return payload, False

    candidates = _search_tmdb_candidates(title, strict=True)
    if not candidates:
        return payload, False

    exact_candidates = [
        item for item in candidates
        if str(item.get("media_type") or "").strip().lower() == media_type
        and _normalize_tmdb_title(item.get("title", "")) == _normalize_tmdb_title(title)
    ]
    if not exact_candidates:
        return payload, False

    selected = None
    if target_year:
        for candidate in exact_candidates:
            if str(candidate.get("year") or "").strip() == target_year:
                selected = candidate
                break

    if selected is None and current_tmdb_id:
        for candidate in exact_candidates:
            if str(candidate.get("tmdb_id") or "").strip() == current_tmdb_id:
                selected = candidate
                break

    if selected is None:
        return payload, False

    selected_tmdb_id = str(selected.get("tmdb_id") or "").strip()
    selected_year = str(selected.get("year") or "").strip()
    changed = False
    normalized = dict(payload)

    if selected_tmdb_id and selected_tmdb_id != current_tmdb_id:
        normalized["tmdb_id"] = selected_tmdb_id
        changed = True
    if selected_year and target_year and selected_year != str(payload.get("year") or "").strip():
        normalized["year"] = selected_year
        changed = True

    selected_poster = str(selected.get("poster_path") or "").strip()
    selected_backdrop = str(selected.get("backdrop_path") or "").strip()
    if selected_poster and selected_poster != str(payload.get("poster_path") or "").strip():
        normalized["poster_path"] = selected_poster
        changed = True
    if selected_backdrop and selected_backdrop != str(payload.get("backdrop_path") or "").strip():
        normalized["backdrop_path"] = selected_backdrop
        changed = True

    if changed:
        _log(
            f"订阅请求已校正: title={title}, type={media_type}, year={target_year or '-'}, "
            f"tmdb_id {current_tmdb_id or '-'} -> {selected_tmdb_id or '-'}"
        )

    return normalized, changed


def _enabled_subscribe_sources():
    names = []
    if _has_hdhive():
        names.append("影巢")
    if _has_gying():
        names.append("观影")
    if _has_panso():
        names.append("盘搜")
    return names


def _submedia_items_from_result(result):
    if result is None:
        return []
    if isinstance(result, (list, tuple, set)):
        return list(result)
    if isinstance(result, dict):
        for key in ["data", "rows", "items", "list"]:
            value = result.get(key)
            if isinstance(value, list):
                return value
        return []
    return []


def _submedia_value(item, key):
    if isinstance(item, dict):
        return item.get(key)
    return getattr(item, key, None)


def _cms_list_submedia(type_name=None, status=None, page_size=200):
    from app.db.sub_op import SubMediaOp

    op = SubMediaOp()
    items = []
    page = 1
    total = None

    while True:
        result = op.list_page(type=type_name, status=status, page=page, page_size=page_size)
        batch = _submedia_items_from_result(result)
        if not batch:
            break

        items.extend(batch)
        if isinstance(result, dict):
            total = result.get("total")
        if total is not None and len(items) >= int(total or 0):
            break
        if len(batch) < page_size:
            break
        page += 1

    return items


def _submedia_status_label(status):
    status_text = str(status if status is not None else "").strip()
    return {
        "0": "待处理",
        "1": "订阅中",
        "2": "已完成",
    }.get(status_text, f"状态{status_text}" if status_text else "未知")


def _cms_add_submedia_db(payload):
    from app.db.sub_op import SubMediaOp
    from app.db.models.submedia import SubMedia

    target_tmdb_id = str(payload.get("tmdb_id") or "").strip()
    target_type = str(payload.get("type") or "").strip()
    op = SubMediaOp()

    duplicate_groups = []
    for status in [0, 1]:
        try:
            duplicate_groups.append(_submedia_items_from_result(op.get_by_status(status)))
        except Exception:
            continue
    try:
        duplicate_groups.append(_submedia_items_from_result(op.list_page(type=target_type, page=1, page_size=500)))
    except Exception:
        pass

    for items in duplicate_groups:
        for item in items:
            existing_tmdb_id = str(_submedia_value(item, "tmdb_id") or "").strip()
            existing_type = str(_submedia_value(item, "type") or "").strip()
            if existing_tmdb_id == target_tmdb_id and existing_type == target_type:
                return True, "订阅已存在"

    record = {
        "title": str(payload.get("title") or "").strip(),
        "year": str(payload.get("year") or "").strip(),
        "tmdb_id": target_tmdb_id,
        "type": target_type,
        "poster_path": str(payload.get("poster_path") or "").strip(),
        "backdrop_path": str(payload.get("backdrop_path") or "").strip(),
        "sub_type": str(payload.get("sub_type") or "").strip(),
        "season_info": str(payload.get("season_info") or "").strip(),
        "max_season": int(payload.get("max_season") or 0),
        "max_season_episodes": int(payload.get("max_season_episodes") or 0),
        "status": int(payload.get("status") or 1),
    }

    op.save(SubMedia(**record))
    return True, "订阅成功"


def _choose_unsubscribe_item(keyword, prefer_type=None):
    normalized_keyword = _normalize_tmdb_title(keyword)
    items = _cms_list_submedia(type_name=prefer_type)

    title_matches = []
    for item in items:
        title = str(_submedia_value(item, "title") or "").strip()
        if _normalize_tmdb_title(title) == normalized_keyword:
            title_matches.append(item)

    if prefer_type == "movie":
        type_order = {"movie": 0, "tv": 1}
    else:
        type_order = {"tv": 0, "movie": 1}

    ranked_title_matches = sorted(
        title_matches,
        key=lambda item: (
            type_order.get(str(_submedia_value(item, "type") or ""), 9),
            -int(_submedia_value(item, "id") or 0),
        ),
    )

    # 不指定类型时，同名的电视剧/电影一起退订
    if ranked_title_matches and not prefer_type:
        return ranked_title_matches[0], ranked_title_matches

    candidate, _ = _choose_subscribe_candidate(keyword, prefer_type)
    if candidate:
        target_tmdb_id = str(candidate.get("tmdb_id") or "").strip()
        target_type = str(candidate.get("media_type") or "").strip()
        tmdb_matches = [
            item for item in items
            if str(_submedia_value(item, "tmdb_id") or "").strip() == target_tmdb_id
            and str(_submedia_value(item, "type") or "").strip() == target_type
        ]
        if tmdb_matches:
            ranked_tmdb_matches = sorted(
                tmdb_matches,
                key=lambda item: (
                    type_order.get(str(_submedia_value(item, "type") or ""), 9),
                    -int(_submedia_value(item, "id") or 0),
                ),
            )
            return ranked_tmdb_matches[0], ranked_tmdb_matches

    if not ranked_title_matches:
        return None, []

    return ranked_title_matches[0], ranked_title_matches


def _cms_delete_submedia_db(ids):
    from app.db.sub_op import SubMediaOp

    ids = [int(x) for x in (ids or []) if str(x).strip()]
    if not ids:
        return False, "未找到可删除的订阅"

    SubMediaOp().delete_by_ids(ids)
    return True, "退订成功"


def _cms_add_submedia(payload):
    import requests

    payload, _ = _normalize_subscribe_payload(payload)
    body = {
        "title": str(payload.get("title") or "").strip(),
        "year": str(payload.get("year") or "").strip(),
        "tmdb_id": str(payload.get("tmdb_id") or "").strip(),
        "type": str(payload.get("type") or "").strip(),
        "poster_path": str(payload.get("poster_path") or "").strip(),
        "backdrop_path": str(payload.get("backdrop_path") or "").strip(),
    }

    for port in [9527, 9528]:
        try:
            resp = requests.post(f"http://127.0.0.1:{port}/api/submedia/add", json=body, timeout=10)
            data = resp.json()
            msg = str(data.get("msg") or data.get("message") or "").strip()
            if data.get("code") == 200:
                return True, msg or "订阅成功"
            if msg and ("已存在" in msg or "重复" in msg):
                return True, msg
            if resp.status_code in [401, 403]:
                break
        except requests.exceptions.ConnectionError:
            continue
        except Exception:
            break

    return _cms_add_submedia_db(body)


def _format_subscribe_feedback(title, media_type, year="", ok=False, msg="", tmdb_id="", poster_path="", tmdb_detail=None):
    title = str(title or "未知").strip() or "未知"
    media_type = str(media_type or "").strip().lower()
    year = str(year or "").strip()
    msg = str(msg or "").strip()
    detail = tmdb_detail if isinstance(tmdb_detail, dict) else None
    if detail is None and tmdb_id and media_type in {"movie", "tv"}:
        detail = _fetch_tmdb_detail(tmdb_id, media_type)

    display_title = str((detail or {}).get("title") or title).strip() or title
    display_year = str((detail or {}).get("year") or year).strip()
    if display_year and display_year not in display_title:
        display_title = f"{display_title} ({display_year})"

    generic_success_tokens = {"订阅成功", "订阅已存在"}
    is_duplicate = "已存在" in msg
    is_added = "添加成功" in msg or msg in generic_success_tokens

    if ok and is_duplicate:
        lines = [f"✅ 《{display_title}》已在订阅列表中"]
    elif ok:
        lines = [f"✅ 已添加订阅：{display_title}"]
    else:
        lines = [f"❌ 订阅失败：{display_title}"]

    details = _format_subscribe_detail_lines(detail, media_type, fallback_year=year) if detail else []
    if not details:
        type_name = "电视剧" if media_type == "tv" else "电影"
        details = [f"类型：{type_name}"]
        if year:
            details.append(f"年份：{year}")

    if msg and not (is_duplicate or is_added):
        details.append(f"说明：{msg}")

    return "\n".join(lines + details)


def _resolve_tmdb_image_urls(tmdb_detail=None, poster_path="", backdrop_path=""):
    detail = tmdb_detail if isinstance(tmdb_detail, dict) else {}
    poster_url = str(detail.get("poster_url") or "").strip() or _build_tmdb_image_url(poster_path)
    backdrop_url = str(detail.get("backdrop_url") or "").strip() or _build_tmdb_image_url(backdrop_path, size="original")
    return poster_url, backdrop_url


def _build_unsubscribe_feedback_payload(item, target_items=None, title_fallback=""):
    target_items = list(target_items or [])
    if not target_items and item is not None:
        target_items = [item]

    title = str(_submedia_value(item, "title") or title_fallback or "未知").strip() or "未知"
    media_type = str(_submedia_value(item, "type") or "").strip().lower()
    year = str(_submedia_value(item, "year") or "").strip()
    tmdb_id = str(_submedia_value(item, "tmdb_id") or "").strip()
    poster_path = str(_submedia_value(item, "poster_path") or "").strip()
    backdrop_path = str(_submedia_value(item, "backdrop_path") or "").strip()

    if len(target_items) <= 1:
        detail = _fetch_tmdb_detail(tmdb_id, media_type) if tmdb_id and media_type in {"movie", "tv"} else None
        poster_url, backdrop_url = _resolve_tmdb_image_urls(
            detail,
            poster_path=poster_path,
            backdrop_path=backdrop_path,
        )
        display_title = str((detail or {}).get("title") or title).strip() or title
        display_year = str((detail or {}).get("year") or year).strip()
        if display_year and display_year not in display_title:
            display_title = f"{display_title} ({display_year})"

        details = _format_subscribe_detail_lines(detail, media_type, fallback_year=year) if detail else []
        if not details:
            type_name = "电视剧" if media_type == "tv" else "电影"
            details = [f"类型：{type_name}"]
            if year:
                details.append(f"年份：{year}")

        feedback = "\n".join([f"✅ 已删除订阅：{display_title}"] + details)
        return feedback, poster_url, backdrop_url

    type_names = []
    for target_item in target_items:
        target_type = str(_submedia_value(target_item, "type") or "").strip()
        type_name = "电视剧" if target_type == "tv" else "电影"
        if type_name not in type_names:
            type_names.append(type_name)

    lines = [f"✅ 已删除订阅：{title}", f"共删除：{len(target_items)} 条"]
    if type_names:
        lines.append(f"类型：{' / '.join(type_names)}")
    return "\n".join(lines), "", ""


def _handle_wx_subscribe(command, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_subscribe():
        _clear_wx_subscribe_candidates()
        keyword = command["keyword"]
        prefer_type = command.get("prefer_type")
        target_year = command.get("target_year")
        prefer_animation = bool(command.get("prefer_animation"))
        candidates = _resolve_subscribe_candidates(
            keyword,
            prefer_type=prefer_type,
            target_year=target_year,
            prefer_animation=prefer_animation,
        )
        if not candidates:
            _send_wx_message(f"❌ 未找到《{keyword}》对应的媒体信息")
            return

        if len(candidates) > 1:
            _cache_wx_subscribe_candidates(keyword, candidates)
            _reply_wx_subscribe_candidates(keyword, candidates)
            return

        _set_wx_action_context("")
        result = _build_subscribe_candidate_result(candidates[0], fallback_keyword=keyword)
        if not result:
            _send_wx_message(f"❌ 《{keyword}》缺少订阅所需的媒体信息")
            return

        _send_wx_rich_message(result["feedback"], image_url=result["backdrop_url"] or result["poster_url"])

    threading.Thread(target=do_subscribe, daemon=True).start()


def _handle_wx_unsubscribe(command, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_unsubscribe():
        keyword = command["keyword"]
        prefer_type = command.get("prefer_type")
        item, matched_items = _choose_unsubscribe_item(keyword, prefer_type)
        if not item or not matched_items:
            _send_wx_message(f"❌ 订阅列表里没有找到《{keyword}》")
            return

        target_items = matched_items if not prefer_type else [item]
        target_ids = [
            int(_submedia_value(target_item, "id"))
            for target_item in target_items
            if str(_submedia_value(target_item, "id") or "").strip()
        ]
        title = _submedia_value(item, "title") or keyword
        ok, msg = _cms_delete_submedia_db(target_ids)

        if ok:
            feedback, poster_url, backdrop_url = _build_unsubscribe_feedback_payload(
                item,
                target_items=target_items,
                title_fallback=keyword,
            )
            _send_wx_rich_message(feedback, image_url=backdrop_url or poster_url)
            return

        _send_wx_message(f"❌ 退订失败：{msg or title}")

    threading.Thread(target=do_unsubscribe, daemon=True).start()


def _handle_wx_delete_completed(wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_delete_completed():
        _reconcile_submedia_completion_wait(timeout=2.5, force=True, min_interval=5)
        all_items = _cms_list_submedia()
        completed_items = [
            item for item in all_items
            if str(_submedia_value(item, "status") or "").strip() == "2"
        ]
        if not completed_items:
            _send_wx_message("📚 当前没有已完成订阅")
            return

        target_ids = [
            int(_submedia_value(item, "id"))
            for item in completed_items
            if str(_submedia_value(item, "id") or "").strip()
        ]
        if not target_ids:
            _send_wx_message("❌ 未找到可删除的已完成订阅")
            return

        ok, msg = _cms_delete_submedia_db(target_ids)
        if not ok:
            _send_wx_message(f"❌ 删除失败：{msg or '已完成订阅'}")
            return

        titles = [
            str(_submedia_value(item, "title") or "").strip()
            for item in completed_items
            if str(_submedia_value(item, "title") or "").strip()
        ]
        preview = "、".join(titles[:5])
        lines = [f"✅ 已删除 {len(target_ids)} 条已完成订阅"]
        if preview:
            if len(titles) > 5:
                lines.append(f"示例：{preview} 等")
            else:
                lines.append(f"包含：{preview}")
        _send_wx_message("\n".join(lines))

    threading.Thread(target=do_delete_completed, daemon=True).start()


def _handle_wx_douban_hot(hot_type, wechat_instance=None):
    hot_type = str(hot_type or "").strip().lower()
    display_title = (
        WX_DOUBAN_HOT_MOVIE_MENU_TITLE
        if hot_type == "movie"
        else WX_DOUBAN_HOT_TV_MENU_TITLE
    )
    cached_rows = _get_douban_hot_cached_rows(hot_type, DOUBAN_HOT_DISPLAY_COUNT)
    started = _begin_wx_douban_hot_request(hot_type)
    if not started:
        if cached_rows is None and _should_notify_wx_douban_hot_pending(hot_type):
            _send_wx_message(f"⏳ 正在获取{display_title}，请稍候...")
        return

    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance
    elif not _state.get("wechat_instance"):
        recovered_wechat = _get_wx_instance()
        if recovered_wechat is not None:
            _state["wechat_instance"] = recovered_wechat

    if cached_rows is None and _should_notify_wx_douban_hot_pending(hot_type):
        _send_wx_message(f"⏳ 正在获取{display_title}，请稍候...")

    def do_hot():
        try:
            if cached_rows is not None:
                # Let the platform-level "开始执行..." ack land before cached data is sent.
                time.sleep(WX_COMMAND_ACK_GRACE_SECONDS)
            rows = _build_douban_hot_subscription_rows(hot_type)
            if not rows:
                _send_wx_message(f"❌ {display_title} 暂无可订阅结果")
                return

            candidates = _build_douban_hot_display_candidates(rows)
            if not candidates:
                _send_wx_message(f"❌ {display_title} 暂无可订阅结果")
                return

            _cache_wx_subscribe_candidates(display_title, candidates)
            _reply_wx_subscribe_candidates(display_title, candidates)
            _mark_wx_douban_hot_notice(hot_type)
        except Exception as e:
            _log(f"{display_title} 获取失败: {e}\n{traceback.format_exc()}")
            _send_wx_message(f"❌ {display_title} 获取失败：{e}")
        finally:
            _finish_wx_douban_hot_request(hot_type)

    threading.Thread(target=do_hot, daemon=True).start()


def _sort_submedia_items(items):
    return sorted(
        items or [],
        key=lambda item: (
            {"1": 0, "0": 1, "2": 2}.get(str(_submedia_value(item, "status") or ""), 9),
            0 if str(_submedia_value(item, "type") or "") == "tv" else 1,
            -int(_submedia_value(item, "id") or 0),
        ),
    )


def _cache_wx_subscriptions(items):
    _search_cache.pop("wx_subscribe_candidates", None)
    _search_cache.pop("wx_hdhive_candidates", None)
    _search_cache.pop("wx_hdhive", None)
    _search_cache["wx_subscriptions"] = {
        "items": list(items or []),
        "updated_at": time.time(),
    }
    _search_cache["wx_action_context"] = {
        "action": "subscriptions",
        "updated_at": time.time(),
    }


def _cache_wx_subscribe_candidates(keyword, candidates):
    _search_cache.pop("wx_hdhive_candidates", None)
    _search_cache.pop("wx_hdhive", None)
    normalized_items = _build_wx_subscribe_candidate_items(keyword, candidates)
    _search_cache["wx_subscribe_candidates"] = {
        "keyword": str(keyword or "").strip(),
        "items": normalized_items,
        "page": 1,
        "updated_at": time.time(),
    }
    _set_wx_action_context("subscribe_candidates")


def _cache_wx_hdhive_candidates(keyword, candidates):
    _search_cache.pop("wx_subscribe_candidates", None)
    _search_cache.pop("wx_hdhive", None)
    _search_cache["wx_hdhive_candidates"] = {
        "keyword": str(keyword or "").strip(),
        "items": list(candidates or []),
        "page": 1,
        "updated_at": time.time(),
    }
    _set_wx_action_context("hdhive_candidates")


def _get_wx_subscribe_candidates(max_age=WX_INTERACTIVE_CONTEXT_TTL_SECONDS):
    cache = _search_cache.get("wx_subscribe_candidates") or {}
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return "", []
    return str(cache.get("keyword") or "").strip(), list(cache.get("items") or [])


def _get_wx_subscribe_candidate_page(max_age=WX_INTERACTIVE_CONTEXT_TTL_SECONDS):
    cache = _search_cache.get("wx_subscribe_candidates") or {}
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return 1
    try:
        return max(1, int(cache.get("page") or 1))
    except Exception:
        return 1


def _set_wx_subscribe_candidate_page(page):
    cache = _search_cache.get("wx_subscribe_candidates") or {}
    if not cache:
        return
    try:
        page = max(1, int(page or 1))
    except Exception:
        page = 1
    cache["page"] = page
    cache["updated_at"] = time.time()
    _search_cache["wx_subscribe_candidates"] = cache


def _get_wx_hdhive_candidates(max_age=WX_INTERACTIVE_CONTEXT_TTL_SECONDS):
    cache = _search_cache.get("wx_hdhive_candidates") or {}
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return "", []
    return str(cache.get("keyword") or "").strip(), list(cache.get("items") or [])


def _get_wx_hdhive_candidate_page(max_age=WX_INTERACTIVE_CONTEXT_TTL_SECONDS):
    cache = _search_cache.get("wx_hdhive_candidates") or {}
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return 1
    try:
        return max(1, int(cache.get("page") or 1))
    except Exception:
        return 1


def _set_wx_hdhive_candidate_page(page):
    cache = _search_cache.get("wx_hdhive_candidates") or {}
    if not cache:
        return
    try:
        page = max(1, int(page or 1))
    except Exception:
        page = 1
    cache["page"] = page
    cache["updated_at"] = time.time()
    _search_cache["wx_hdhive_candidates"] = cache


def _cache_wx_hdhive_results(keyword, items):
    _search_cache.pop("wx_subscribe_candidates", None)
    _search_cache.pop("wx_hdhive_candidates", None)
    _search_cache["wx_hdhive"] = {
        "keyword": str(keyword or "").strip(),
        "items": list(items or []),
        "page": 1,
        "updated_at": time.time(),
    }
    _set_wx_action_context("hdhive")


def _get_wx_hdhive_results(max_age=WX_INTERACTIVE_CONTEXT_TTL_SECONDS):
    cache = _search_cache.get("wx_hdhive") or {}
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return "", []
    return str(cache.get("keyword") or "").strip(), list(cache.get("items") or [])


def _get_wx_hdhive_result_page(max_age=WX_INTERACTIVE_CONTEXT_TTL_SECONDS):
    cache = _search_cache.get("wx_hdhive") or {}
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return 1
    try:
        return max(1, int(cache.get("page") or 1))
    except Exception:
        return 1


def _set_wx_hdhive_result_page(page):
    cache = _search_cache.get("wx_hdhive") or {}
    if not cache:
        return
    try:
        page = max(1, int(page or 1))
    except Exception:
        page = 1
    cache["page"] = page
    cache["updated_at"] = time.time()
    _search_cache["wx_hdhive"] = cache


def _clear_wx_subscribe_candidates(reset_action=True):
    _search_cache.pop("wx_subscribe_candidates", None)
    if reset_action and _get_wx_action_context() == "subscribe_candidates":
        _set_wx_action_context("")


def _clear_wx_hdhive_candidates(reset_action=True):
    _search_cache.pop("wx_hdhive_candidates", None)
    if reset_action and _get_wx_action_context() == "hdhive_candidates":
        _set_wx_action_context("")


def _clear_wx_hdhive_results(reset_action=True):
    _search_cache.pop("wx_hdhive", None)
    if reset_action and _get_wx_action_context() == "hdhive":
        _set_wx_action_context("")


def _parse_wx_page_command(text):
    value = re.sub(r"\s+", "", _compact_text(text).lower())
    matched = re.fullmatch(r"([pny])(\d+)?", value)
    if not matched:
        return None
    action = matched.group(1)
    try:
        step = int(matched.group(2) or "1")
    except Exception:
        return None
    if step < 1:
        return None
    if action == "p":
        return {"mode": "relative", "value": -step}
    if action == "n":
        return {"mode": "relative", "value": step}
    if action == "y":
        return {"mode": "absolute", "value": step}
    return None


def _parse_wx_hdhive_candidate_page_command(text):
    return _parse_wx_page_command(text)


def _format_wx_invalid_page_command_message(current_page, total_pages, command=None):
    try:
        current_page = max(1, int(current_page or 1))
    except Exception:
        current_page = 1
    try:
        total_pages = max(1, int(total_pages or 1))
    except Exception:
        total_pages = 1

    if total_pages <= 1:
        return "❌ 翻页无效，当前只有 1 页"

    command = command or {}
    if str(command.get("mode") or "").strip() == "absolute":
        return f"❌ 跳页无效，请输入 y1-y{total_pages}"

    max_prev = max(0, current_page - 1)
    max_next = max(0, total_pages - current_page)
    parts = [f"❌ 翻页无效，当前第{current_page}/{total_pages}页"]
    if max_prev > 0:
        parts.append(f"向前最多 {'p' if max_prev == 1 else f'p{max_prev}'}")
    if max_next > 0:
        parts.append(f"向后最多 {'n' if max_next == 1 else f'n{max_next}'}")
    return "，".join(parts)


def _resolve_wx_target_page(current_page, total_pages, command):
    command = command or {}
    mode = str(command.get("mode") or "").strip()
    try:
        value = int(command.get("value") or 0)
    except Exception:
        value = 0

    if total_pages <= 0:
        return False, current_page, "❌ 翻页无效，当前没有可用页"
    if mode == "absolute":
        if 1 <= value <= total_pages:
            return True, value, ""
        return False, current_page, _format_wx_invalid_page_command_message(current_page, total_pages, command)
    if mode == "relative":
        target_page = current_page + value
        if value != 0 and 1 <= target_page <= total_pages:
            return True, target_page, ""
        return False, current_page, _format_wx_invalid_page_command_message(current_page, total_pages, command)
    return False, current_page, _format_wx_invalid_page_command_message(current_page, total_pages, command)


def _set_wx_action_context(action):
    _search_cache["wx_action_context"] = {
        "action": str(action or "").strip(),
        "updated_at": time.time(),
    }


def _get_wx_action_context(max_age=WX_INTERACTIVE_CONTEXT_TTL_SECONDS):
    context = _search_cache.get("wx_action_context") or {}
    action = str(context.get("action") or "").strip()
    updated_at = float(context.get("updated_at") or 0)
    if not action or not updated_at:
        return ""
    if (time.time() - updated_at) > max_age:
        return ""
    return action


def _cache_tg_subscriptions(chat_id, items):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return

    payload = {
        "items": list(items or []),
        "updated_at": time.time(),
    }
    action_payload = {
        "action": "subscriptions",
        "updated_at": time.time(),
    }
    with _search_cache_lock:
        tg_candidates = _search_cache.setdefault("tg_subscribe_candidates", {})
        tg_candidates.pop(chat_key, None)
        _search_cache.setdefault("tg_hdhive_candidates", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_hdhive_selected_results", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_selected_candidate_results", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_subscriptions", {})[chat_key] = payload
        _search_cache.setdefault("tg_action_context", {})[chat_key] = action_payload


def _cache_tg_subscribe_candidates(chat_id, keyword, candidates):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return

    payload = {
        "keyword": str(keyword or "").strip(),
        "items": list(candidates or []),
        "updated_at": time.time(),
    }
    action_payload = {
        "action": "subscribe_candidates",
        "updated_at": time.time(),
    }
    with _search_cache_lock:
        _search_cache.setdefault("tg_hdhive_candidates", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_selected_candidate_results", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_subscribe_candidates", {})[chat_key] = payload
        _search_cache.setdefault("tg_action_context", {})[chat_key] = action_payload


def _cache_tg_hdhive_candidates(chat_id, keyword, candidates):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return

    payload = {
        "keyword": str(keyword or "").strip(),
        "items": list(candidates or []),
        "updated_at": time.time(),
    }
    action_payload = {
        "action": "hdhive_candidates",
        "updated_at": time.time(),
    }
    with _search_cache_lock:
        _search_cache.setdefault("tg_subscribe_candidates", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_hdhive_selected_results", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_selected_candidate_results", {}).pop(chat_key, None)
        _search_cache.setdefault("tg_hdhive_candidates", {})[chat_key] = payload
        _search_cache.setdefault("tg_action_context", {})[chat_key] = action_payload


def _cache_tg_selected_hdhive_results(chat_id, keyword, grouped_items):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return

    payload = {
        "keyword": str(keyword or "").strip(),
        "items": dict(grouped_items or {}),
        "updated_at": time.time(),
    }
    with _search_cache_lock:
        _search_cache.setdefault("tg_hdhive_selected_results", {})[chat_key] = payload


def _cache_tg_selected_candidate_results(chat_id, keyword, grouped_items):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return

    payload = {
        "keyword": str(keyword or "").strip(),
        "items": dict(grouped_items or {}),
        "updated_at": time.time(),
    }
    with _search_cache_lock:
        _search_cache.setdefault("tg_selected_candidate_results", {})[chat_key] = payload


def _set_tg_action_context(chat_id, action):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return

    with _search_cache_lock:
        _search_cache.setdefault("tg_action_context", {})[chat_key] = {
            "action": str(action or "").strip(),
            "updated_at": time.time(),
        }


def _get_tg_action_context(chat_id, max_age=TG_INTERACTIVE_CONTEXT_TTL_SECONDS):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return ""

    with _search_cache_lock:
        context = ((_search_cache.get("tg_action_context") or {}).get(chat_key) or {}).copy()
    action = str(context.get("action") or "").strip()
    updated_at = float(context.get("updated_at") or 0)
    if not action or not updated_at:
        return ""
    if (time.time() - updated_at) > max_age:
        return ""
    return action


def _get_tg_subscription_items(chat_id, max_age=TG_INTERACTIVE_CONTEXT_TTL_SECONDS):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return []

    with _search_cache_lock:
        cache = ((_search_cache.get("tg_subscriptions") or {}).get(chat_key) or {}).copy()
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return []
    return list(cache.get("items") or [])


def _get_tg_subscribe_candidates(chat_id, max_age=TG_INTERACTIVE_CONTEXT_TTL_SECONDS):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return "", []

    with _search_cache_lock:
        cache = ((_search_cache.get("tg_subscribe_candidates") or {}).get(chat_key) or {}).copy()
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return "", []
    return str(cache.get("keyword") or "").strip(), list(cache.get("items") or [])


def _get_tg_hdhive_candidates(chat_id, max_age=TG_INTERACTIVE_CONTEXT_TTL_SECONDS):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return "", []

    with _search_cache_lock:
        cache = ((_search_cache.get("tg_hdhive_candidates") or {}).get(chat_key) or {}).copy()
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return "", []
    return str(cache.get("keyword") or "").strip(), list(cache.get("items") or [])


def _get_tg_selected_hdhive_results(chat_id, max_age=TG_INTERACTIVE_CONTEXT_TTL_SECONDS):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return "", {}

    with _search_cache_lock:
        cache = ((_search_cache.get("tg_hdhive_selected_results") or {}).get(chat_key) or {}).copy()
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return "", {}
    return str(cache.get("keyword") or "").strip(), dict(cache.get("items") or {})


def _get_tg_selected_candidate_results(chat_id, max_age=TG_INTERACTIVE_CONTEXT_TTL_SECONDS):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return "", {}

    with _search_cache_lock:
        cache = ((_search_cache.get("tg_selected_candidate_results") or {}).get(chat_key) or {}).copy()
    updated_at = float(cache.get("updated_at") or 0)
    if not updated_at or (time.time() - updated_at) > max_age:
        return "", {}
    return str(cache.get("keyword") or "").strip(), dict(cache.get("items") or {})


def _clear_tg_subscribe_candidates(chat_id, reset_action=True):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return
    with _search_cache_lock:
        tg_candidates = _search_cache.setdefault("tg_subscribe_candidates", {})
        tg_candidates.pop(chat_key, None)
    if reset_action and _get_tg_action_context(chat_id) == "subscribe_candidates":
        _set_tg_action_context(chat_id, "")


def _clear_tg_hdhive_candidates(chat_id, reset_action=True):
    chat_key = str(_normalize_event_chat_id(chat_id) or "").strip()
    if not chat_key:
        return
    with _search_cache_lock:
        _search_cache.setdefault("tg_hdhive_candidates", {}).pop(chat_key, None)
    if reset_action and _get_tg_action_context(chat_id) == "hdhive_candidates":
        _set_tg_action_context(chat_id, "")


def _hdhive_candidate_display_keyword(candidate, fallback_keyword=""):
    title = str((candidate or {}).get("title") or fallback_keyword or "未知").strip() or "未知"
    year = str((candidate or {}).get("year") or "").strip()
    if year and year not in title:
        return f"{title}（{year}）"
    return title


def _wx_hdhive_candidate_page_meta(candidates, page=1, page_size=WX_HDHIVE_CANDIDATE_PAGE_SIZE):
    items = list(candidates or [])
    total = len(items)
    total_pages = max(1, (total + max(1, int(page_size or 1)) - 1) // max(1, int(page_size or 1)))
    try:
        page = int(page or 1)
    except Exception:
        page = 1
    page = max(1, min(page, total_pages))
    start = (page - 1) * max(1, int(page_size or 1))
    end = start + max(1, int(page_size or 1))
    return items, total, total_pages, page, start, end


def _format_wx_hdhive_candidate_prompt(keyword, total, page, total_pages):
    prompt = f"【{keyword}】 共找到{total}条相关信息，请回复对应数字选择"
    if total_pages > 1:
        prompt = f"{prompt}（p: 上一页 n: 下一页）"
    return prompt


def _format_wx_hdhive_candidate_card(index, candidate):
    title = str((candidate or {}).get("title") or "未知").strip() or "未知"
    year = str((candidate or {}).get("year") or "").strip()
    media_label = str((candidate or {}).get("media_label") or _subscribe_candidate_type_label(candidate)).strip() or "未知"
    rating_text = str((candidate or {}).get("rating_text") or _format_tmdb_rating((candidate or {}).get("vote_average"))).strip()
    text_line = f"类型：{media_label}，评分：{rating_text}"
    title_line = f"{index}. {title}" + (f" ({year})" if year else "")
    title_line = f"{title_line}\n{text_line}"
    return title_line, text_line


def _pick_wx_subscribe_candidate_image(candidate, featured=False):
    item = candidate if isinstance(candidate, dict) else {}
    backdrop_url = str(item.get("backdrop_url") or "").strip()
    poster_url = str(item.get("poster_url") or "").strip()

    def _tmdb_resize(url, size):
        url = str(url or "").strip()
        size = str(size or "").strip()
        if not url or not size:
            return url
        match = re.match(r"^(https?://[^/]+/t/p/)([^/]+)(/[^?#]+.*)$", url, re.I)
        if not match:
            return url
        return f"{match.group(1)}{size}{match.group(3)}"

    if featured:
        return (
            _tmdb_resize(backdrop_url, "w780")
            or _tmdb_resize(poster_url, "w342")
            or backdrop_url
            or poster_url
        )

    return (
        _tmdb_resize(poster_url, "w342")
        or _tmdb_resize(backdrop_url, "w300")
        or poster_url
        or backdrop_url
    )


def _reply_wx_subscribe_candidates(keyword, candidates, page=1):
    items = list(candidates or [])
    if not items:
        _send_wx_message(f"❌ 未找到《{keyword}》对应的媒体信息")
        return

    items, total, total_pages, page, start, end = _wx_hdhive_candidate_page_meta(items, page=page)
    page_items = items[start:end]
    _set_wx_subscribe_candidate_page(page)
    _send_wx_message(_format_wx_subscribe_candidate_prompt(keyword, total, page, total_pages))

    news_articles = []
    for local_idx, candidate in enumerate(page_items):
        global_idx = start + local_idx + 1
        title_line, text_line = _format_wx_hdhive_candidate_card(global_idx, candidate)
        link_url = str((candidate or {}).get("tmdb_url") or "").strip()
        if not link_url:
            from urllib.parse import quote
            fallback_title = str((candidate or {}).get("title") or keyword or "").strip()
            if fallback_title:
                link_url = f"https://www.themoviedb.org/search?query={quote(fallback_title)}"
        image_url = _pick_wx_subscribe_candidate_image(candidate, featured=(local_idx == 0))
        news_articles.append({
            "title": title_line,
            "description": text_line,
            "picurl": image_url,
            "url": link_url,
        })

    if _send_wx_news_message(news_articles):
        return

    fallback_lines = []
    for idx, candidate in enumerate(page_items, start=start + 1):
        title_line, text_line = _format_wx_hdhive_candidate_card(idx, candidate)
        image_url = _pick_wx_subscribe_candidate_image(candidate, featured=True)
        link_url = str((candidate or {}).get("tmdb_url") or "").strip()
        if not link_url:
            from urllib.parse import quote
            fallback_title = str((candidate or {}).get("title") or keyword or "").strip()
            if fallback_title:
                link_url = f"https://www.themoviedb.org/search?query={quote(fallback_title)}"
        content = f"{title_line}\n{text_line}"
        if image_url and _send_wx_rich_message(content, image_url=image_url, link=link_url):
            continue
        fallback_line = _format_hdhive_search_candidate_row(idx, candidate)
        if link_url:
            fallback_line = f"{fallback_line}\nTMDB：{link_url}"
        fallback_lines.append(fallback_line)

    if fallback_lines:
        fallback_lines.extend(["", "💡 回复序号确认订阅"])
        _send_wx_message("\n".join(fallback_lines))


def _reply_wx_hdhive_search_candidates(keyword, candidates, page=1):
    items = list(candidates or [])
    if not items:
        _send_wx_message(f"❌ 未找到《{keyword}》对应的影视条目")
        return

    items, total, total_pages, page, start, end = _wx_hdhive_candidate_page_meta(items, page=page)
    page_items = items[start:end]
    _set_wx_hdhive_candidate_page(page)
    _send_wx_message(_format_wx_hdhive_candidate_prompt(keyword, total, page, total_pages))

    news_articles = []
    for local_idx, candidate in enumerate(page_items):
        global_idx = start + local_idx + 1
        title_line, text_line = _format_wx_hdhive_candidate_card(global_idx, candidate)
        backdrop_url = str((candidate or {}).get("backdrop_url") or "").strip()
        poster_url = str((candidate or {}).get("poster_url") or "").strip()
        link_url = str((candidate or {}).get("tmdb_url") or "").strip()
        if local_idx == 0:
            image_url = backdrop_url or poster_url
        else:
            image_url = poster_url or backdrop_url
        news_articles.append({
            "title": title_line,
            "description": text_line,
            "picurl": image_url,
            "url": link_url,
        })

    if _send_wx_news_message(news_articles):
        return

    fallback_lines = []
    for idx, candidate in enumerate(page_items, start=start + 1):
        title_line, text_line = _format_wx_hdhive_candidate_card(idx, candidate)
        backdrop_url = str((candidate or {}).get("backdrop_url") or "").strip()
        poster_url = str((candidate or {}).get("poster_url") or "").strip()
        image_url = backdrop_url or poster_url
        link_url = str((candidate or {}).get("tmdb_url") or "").strip()
        content = f"{title_line}\n{text_line}"
        if image_url and _send_wx_rich_message(content, image_url=image_url, link=link_url):
            continue
        fallback_line = _format_hdhive_search_candidate_row(idx, candidate)
        if link_url:
            fallback_line = f"{fallback_line}\nTMDB：{link_url}"
        fallback_lines.append(fallback_line)

    if fallback_lines:
        _send_wx_message("\n".join(fallback_lines))
        return


def _reply_tg_hdhive_search_candidates(chat_id, keyword, candidates, bot=None):
    tg_bot = bot or _get_tg_bot()
    target_chat_id = _normalize_event_chat_id(chat_id)
    if tg_bot is None or target_chat_id is None:
        return

    items = list(candidates or [])
    if not items:
        _send_tg_message(target_chat_id, f"❌ 未找到《{keyword}》对应的影视条目", bot=tg_bot)
        return

    page_size = max(1, int(TG_SUBSCRIBE_CARD_PAGE_SIZE or 8))
    total = len(items)
    for start in range(0, total, page_size):
        chunk = items[start:start + page_size]
        buttons = [
            {"text": str(index), "callback_data": f"tg_hd:{index}"}
            for index in range(start + 1, start + 1 + len(chunk))
        ]
        markup = _build_tg_inline_markup(buttons, row_width=4)
        image_url = _pick_wx_subscribe_candidate_image(chunk[0], featured=True)
        content = _format_tg_subscribe_candidate_card(
            keyword,
            chunk,
            start_index=start,
            total=total,
            continued=(start > 0),
        )
        _send_tg_rich_message(
            target_chat_id,
            content,
            image_url=image_url,
            bot=tg_bot,
            parse_mode="HTML",
            reply_markup=markup,
        )


def _format_tg_subscribe_candidate_card(keyword, candidates, start_index=0, total=None, continued=False):
    items = list(candidates or [])
    total = int(total or len(items) or 0)
    escaped_keyword = html.escape(str(keyword or "").strip() or "未知")
    header = f"【{escaped_keyword}】找到{total}条相关信息，请选择操作"
    if continued:
        header = f"【{escaped_keyword}】更多相关信息，请选择操作"

    lines = [header]
    for offset, candidate in enumerate(items, start=1):
        index = start_index + offset
        title = html.escape(str((candidate or {}).get("title") or "未知").strip() or "未知")
        year = str((candidate or {}).get("year") or "").strip()
        media_label = html.escape(
            str((candidate or {}).get("media_label") or _subscribe_candidate_type_label(candidate)).strip() or "未知"
        )
        rating_text = html.escape(
            str((candidate or {}).get("rating_text") or _format_tmdb_rating((candidate or {}).get("vote_average"))).strip()
            or "暂无评分"
        )
        link_url = str((candidate or {}).get("tmdb_url") or "").strip()
        title_text = f"{title} ({html.escape(year)})" if year else title
        if link_url:
            lines.append(f'{index}. <a href="{html.escape(link_url, quote=True)}">{title_text}</a>')
        else:
            lines.append(f"{index}. {title_text}")
        lines.append(f"类型：{media_label}，评分：{rating_text}")
    return "\n".join(lines)


def _reply_tg_subscribe_candidates(chat_id, keyword, candidates, bot=None):
    tg_bot = bot or _get_tg_bot()
    target_chat_id = _normalize_event_chat_id(chat_id)
    if tg_bot is None or target_chat_id is None:
        return

    items = list(candidates or [])
    if not items:
        _send_tg_message(target_chat_id, f"❌ 未找到《{keyword}》对应的媒体信息", bot=tg_bot)
        return

    page_size = max(1, int(TG_SUBSCRIBE_CARD_PAGE_SIZE or 8))
    total = len(items)
    for start in range(0, total, page_size):
        chunk = items[start:start + page_size]
        buttons = [
            {"text": str(index), "callback_data": f"tg_sub:{index}"}
            for index in range(start + 1, start + 1 + len(chunk))
        ]
        markup = _build_tg_inline_markup(buttons, row_width=4)
        image_url = _pick_wx_subscribe_candidate_image(chunk[0], featured=True)
        content = _format_tg_subscribe_candidate_card(
            keyword,
            chunk,
            start_index=start,
            total=total,
            continued=(start > 0),
        )
        _send_tg_rich_message(
            target_chat_id,
            content,
            image_url=image_url,
            bot=tg_bot,
            parse_mode="HTML",
            reply_markup=markup,
        )


def _reply_tg_hdhive_selected_results(candidate, chat_id=None, bot=None):
    tg_bot = bot or _get_tg_bot()
    target_chat_id = _normalize_event_chat_id(chat_id)
    if tg_bot is None or target_chat_id is None:
        return False
    return _reply_tg_selected_candidate_results(candidate, chat_id=target_chat_id, bot=tg_bot)


def _reply_tg_selected_candidate_results(candidate, fallback_keyword="", chat_id=None, bot=None, preserve_candidate_context=False):
    tg_bot = bot or _get_tg_bot()
    target_chat_id = _normalize_event_chat_id(chat_id)
    if tg_bot is None or target_chat_id is None:
        return False

    merged_result = _search_wx_selected_candidate_results(candidate, fallback_keyword=fallback_keyword)
    display_keyword = str(
        merged_result.get("keyword")
        or _hdhive_candidate_display_keyword(candidate, fallback_keyword=fallback_keyword)
    ).strip()
    hdhive_grouped = dict(((merged_result.get("hdhive_data") or {}).get("merged_by_type") or {}))
    external_grouped = _split_selected_candidate_results_by_source(
        ((merged_result.get("external_data") or {}).get("merged_by_type") or {})
    )
    selected_grouped = dict(external_grouped or {})
    if hdhive_grouped:
        selected_grouped["hdhive"] = hdhive_grouped

    _cache_tg_selected_hdhive_results(target_chat_id, display_keyword, hdhive_grouped)
    _cache_tg_selected_candidate_results(target_chat_id, display_keyword, selected_grouped)

    sent = False
    for source_key in ["hdhive", "gying", "panso"]:
        grouped_items = dict((selected_grouped or {}).get(source_key) or {})
        text, buttons = _format_results_tg(
            display_keyword,
            {"merged_by_type": grouped_items},
            _source_display_name(source_key),
            button_callback_builder=lambda cloud_type, items, source_key=source_key: {
                "text": f"📁 {CLOUD_TYPE_NAMES.get(cloud_type, cloud_type)} ({len(items)})",
                "data": f"tgsel:{source_key}:{cloud_type}",
            },
        )
        if not text:
            continue
        markup = _build_tg_inline_markup(buttons, row_width=1)
        _send_tg_message(
            target_chat_id,
            text,
            bot=tg_bot,
            parse_mode="HTML",
            reply_markup=markup,
        )
        sent = True

    if not sent:
        lines = [f"🔍 <b>{html.escape(display_keyword or '未知')}</b>（影巢/观影/盘搜）", "", "暂无相关资源"]
        if preserve_candidate_context:
            lines.extend(["", "💡 可继续回复其他序号选择候选条目"])
        _send_tg_message(target_chat_id, "\n".join(lines), bot=tg_bot, parse_mode="HTML")
        return False

    return True


def _reply_wx_selected_candidate_results(candidate, fallback_keyword="", preserve_candidate_context=False):
    merged_result = _search_wx_selected_candidate_results(candidate, fallback_keyword=fallback_keyword)
    display_keyword = str(
        merged_result.get("keyword")
        or _hdhive_candidate_display_keyword(candidate, fallback_keyword=fallback_keyword)
    ).strip()
    external_data = merged_result.get("external_data") or {"merged_by_type": {}, "total": 0}
    hdhive_resources = list(merged_result.get("hdhive_resources") or [])
    display_items = _build_wx_selected_candidate_display_items(
        display_keyword,
        hdhive_resources,
        external_data,
    )

    with _search_cache_lock:
        _clear_wx_hdhive_results(reset_action=False)

    if not display_items:
        if not preserve_candidate_context:
            _set_wx_action_context("")
        lines = [f"🔍 {display_keyword}（影巢/观影/盘搜）", "", "暂无相关资源"]
        if preserve_candidate_context:
            lines.append("")
            lines.append("💡 可继续回复其他序号选择候选条目")
        _send_wx_message("\n".join(lines))
        return False

    with _search_cache_lock:
        _cache_wx_hdhive_results(display_keyword, display_items)
    _reply_wx_hdhive_result_page(page=1)
    return True


def _reply_wx_hdhive_result_page(page=1):
    with _search_cache_lock:
        cache = _search_cache.get("wx_hdhive") or {}
        keyword = str(cache.get("keyword") or "").strip()
        items = list(cache.get("items") or [])
    if not items:
        return False

    text, _, _, current_page = _format_wx_hdhive_result_page(keyword, items, page=page)
    if not text:
        return False

    with _search_cache_lock:
        cache = _search_cache.get("wx_hdhive") or {}
        cache["items"] = items
        cache["updated_at"] = time.time()
        _search_cache["wx_hdhive"] = cache
        _set_wx_hdhive_result_page(current_page)
    _send_wx_message(text)
    return True


def _handle_wx_subscribe_candidate_index(index, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_subscribe_index():
        keyword, items = _get_wx_subscribe_candidates()
        if not items:
            _send_wx_message("❌ 候选已失效，请重新发送订阅命令")
            return

        indices = index if isinstance(index, (list, tuple, set)) else [index]
        ordered_indices = []
        seen_indices = set()
        for value in indices:
            try:
                number = int(value)
            except Exception:
                continue
            if number not in seen_indices:
                seen_indices.add(number)
                ordered_indices.append(number)

        if len(ordered_indices) != 1:
            _send_wx_message("❌ 请选择一个序号确认订阅")
            return

        number = ordered_indices[0]
        if number < 1 or number > len(items):
            _send_wx_message(f"❌ 序号无效，请输入 1-{len(items)}")
            return

        _clear_wx_subscribe_candidates()
        result = _build_subscribe_candidate_result(items[number - 1], fallback_keyword=keyword)
        if not result:
            _send_wx_message(f"❌ 《{keyword or '该资源'}》缺少订阅所需的媒体信息")
            return

        _send_wx_rich_message(result["feedback"], image_url=result["backdrop_url"] or result["poster_url"])

    threading.Thread(target=do_subscribe_index, daemon=True).start()


def _handle_wx_hdhive_candidate_index(index, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_hdhive_index():
        keyword, items = _get_wx_hdhive_candidates()
        if not items:
            _send_wx_message("❌ 影视候选已失效，请重新搜索")
            return

        indices = index if isinstance(index, (list, tuple, set)) else [index]
        ordered_indices = []
        seen_indices = set()
        for value in indices:
            try:
                number = int(value)
            except Exception:
                continue
            if number not in seen_indices:
                seen_indices.add(number)
                ordered_indices.append(number)

        if len(ordered_indices) != 1:
            _send_wx_message("❌ 请选择一个序号继续搜索影巢")
            return

        number = ordered_indices[0]
        if number < 1 or number > len(items):
            _send_wx_message(f"❌ 序号无效，请输入 1-{len(items)}")
            return

        selected = items[number - 1]
        found = _reply_wx_selected_candidate_results(
            selected,
            fallback_keyword=keyword,
            preserve_candidate_context=True,
        )
        if found:
            _clear_wx_hdhive_candidates()

    threading.Thread(target=do_hdhive_index, daemon=True).start()


def _handle_wx_subscribe_candidate_page_command(direction, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_page():
        keyword, items = _get_wx_subscribe_candidates()
        if not items:
            _send_wx_message("❌ 订阅候选已失效，请重新发送订阅命令")
            return

        items, _, total_pages, current_page, _, _ = _wx_hdhive_candidate_page_meta(
            items,
            page=_get_wx_subscribe_candidate_page(),
        )
        ok, new_page, message = _resolve_wx_target_page(current_page, total_pages, direction)
        if not ok:
            _send_wx_message(message)
            return
        _reply_wx_subscribe_candidates(keyword, items, page=new_page)

    threading.Thread(target=do_page, daemon=True).start()


def _handle_wx_hdhive_candidate_page_command(direction, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_page():
        keyword, items = _get_wx_hdhive_candidates()
        if not items:
            _send_wx_message("❌ 影视候选已失效，请重新搜索")
            return

        items, total, total_pages, current_page, _, _ = _wx_hdhive_candidate_page_meta(
            items,
            page=_get_wx_hdhive_candidate_page(),
        )
        ok, new_page, message = _resolve_wx_target_page(current_page, total_pages, direction)
        if not ok:
            _send_wx_message(message)
            return
        _reply_wx_hdhive_search_candidates(keyword, items, page=new_page)

    threading.Thread(target=do_page, daemon=True).start()


def _handle_wx_hdhive_page_command(direction, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_page():
        keyword, items = _get_wx_hdhive_results()
        if not items:
            _send_wx_message("❌ 资源列表已失效，请重新搜索")
            return

        _, _, total_pages, current_page, _, _ = _wx_hdhive_result_page_meta(
            items,
            page=_get_wx_hdhive_result_page(),
        )
        ok, new_page, message = _resolve_wx_target_page(current_page, total_pages, direction)
        if not ok:
            _send_wx_message(message)
            return
        _reply_wx_hdhive_result_page(page=new_page)

    threading.Thread(target=do_page, daemon=True).start()


def _handle_tg_subscribe_candidate_index(index, chat_id=None, bot=None):
    def do_subscribe_index():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        try:
            tg_bot.send_chat_action(target_chat_id, "typing")
        except Exception:
            pass

        keyword, items = _get_tg_subscribe_candidates(target_chat_id)
        if not items:
            _send_tg_message(target_chat_id, "❌ 候选已失效，请重新发送订阅命令", bot=tg_bot)
            return

        indices = index if isinstance(index, (list, tuple, set)) else [index]
        ordered_indices = []
        seen_indices = set()
        for value in indices:
            try:
                number = int(value)
            except Exception:
                continue
            if number not in seen_indices:
                seen_indices.add(number)
                ordered_indices.append(number)

        if len(ordered_indices) != 1:
            _send_tg_message(target_chat_id, "❌ 请选择一个序号确认订阅", bot=tg_bot)
            return

        number = ordered_indices[0]
        if number < 1 or number > len(items):
            _send_tg_message(target_chat_id, f"❌ 序号无效，请输入 1-{len(items)}", bot=tg_bot)
            return

        result = _build_subscribe_candidate_result(items[number - 1], fallback_keyword=keyword)
        if not result:
            _send_tg_message(target_chat_id, f"❌ 《{keyword or '该资源'}》缺少订阅所需的媒体信息", bot=tg_bot)
            return

        _send_tg_rich_message(
            target_chat_id,
            result["feedback"],
            image_url=result["poster_url"],
            bot=tg_bot,
        )

    threading.Thread(target=do_subscribe_index, daemon=True).start()


def _handle_tg_hdhive_candidate_index(index, chat_id=None, bot=None):
    def do_hdhive_index():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        try:
            tg_bot.send_chat_action(target_chat_id, "typing")
        except Exception:
            pass

        keyword, items = _get_tg_hdhive_candidates(target_chat_id)
        if not items:
            _send_tg_message(target_chat_id, "❌ 影视候选已失效，请重新搜索", bot=tg_bot)
            return

        indices = index if isinstance(index, (list, tuple, set)) else [index]
        ordered_indices = []
        seen_indices = set()
        for value in indices:
            try:
                number = int(value)
            except Exception:
                continue
            if number not in seen_indices:
                seen_indices.add(number)
                ordered_indices.append(number)

        if len(ordered_indices) != 1:
            _send_tg_message(target_chat_id, "❌ 请选择一个序号继续搜索影巢", bot=tg_bot)
            return

        number = ordered_indices[0]
        if number < 1 or number > len(items):
            _send_tg_message(target_chat_id, f"❌ 序号无效，请输入 1-{len(items)}", bot=tg_bot)
            return

        selected = items[number - 1]
        found = _reply_tg_selected_candidate_results(
            selected,
            fallback_keyword=keyword,
            chat_id=target_chat_id,
            bot=tg_bot,
            preserve_candidate_context=True,
        )

    threading.Thread(target=do_hdhive_index, daemon=True).start()


def _handle_tg_hdhive_candidate_callback(bot, call):
    data = getattr(call, "data", "") or ""
    _state["tg_bot"] = bot
    parts = data.split(":", 1)
    if len(parts) != 2:
        return False

    try:
        index = int(parts[1])
    except Exception:
        return False

    try:
        bot.answer_callback_query(call.id, "正在搜索资源...")
    except Exception:
        pass

    chat = getattr(getattr(call, "message", None), "chat", None)
    chat_id = getattr(chat, "id", None)
    if chat_id is None:
        return False

    _handle_tg_hdhive_candidate_index(index, chat_id=chat_id, bot=bot)
    return True


def _handle_wx_unsubscribe_index(index, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_unsubscribe_index():
        cache = _search_cache.get("wx_subscriptions") or {}
        items = list(cache.get("items") or [])
        if not items:
            _send_wx_message("❌ 没有可删除的订阅列表，请先发送“当前订阅”")
            return

        indices = index if isinstance(index, (list, tuple, set)) else [index]
        ordered_indices = []
        seen_indices = set()
        for value in indices:
            try:
                number = int(value)
            except Exception:
                continue
            if number not in seen_indices:
                seen_indices.add(number)
                ordered_indices.append(number)
        if not ordered_indices:
            _send_wx_message("❌ 序号格式无效")
            return
        invalid = [str(number) for number in ordered_indices if number < 1 or number > len(items)]
        if invalid:
            _send_wx_message(f"❌ 序号无效：{', '.join(invalid)}，请输入 1-{len(items)}")
            return

        target_items = [items[number - 1] for number in ordered_indices]
        target_ids = []
        for item in target_items:
            target_id = str(_submedia_value(item, "id") or "").strip()
            if target_id:
                target_ids.append(int(target_id))
        if not target_ids:
            _send_wx_message("❌ 选中的订阅缺少 ID，无法删除")
            return

        title = str(_submedia_value(target_items[0], "title") or "未知").strip() or "未知"
        ok, msg = _cms_delete_submedia_db(target_ids)
        if not ok:
            _send_wx_message(f"❌ 删除失败：{msg or title}")
            return

        target_index_set = set(ordered_indices)
        remaining_items = [entry for idx, entry in enumerate(items, start=1) if idx not in target_index_set]
        _cache_wx_subscriptions(remaining_items)

        if len(target_items) == 1:
            feedback, poster_url, backdrop_url = _build_unsubscribe_feedback_payload(
                target_items[0],
                target_items=target_items,
                title_fallback=title,
            )
            _send_wx_rich_message(feedback, image_url=backdrop_url or poster_url)
            return

        titles = [
            str(_submedia_value(item, "title") or "").strip()
            for item in target_items
            if str(_submedia_value(item, "title") or "").strip()
        ]
        preview = "、".join(titles[:5])
        lines = [f"✅ 已删除 {len(target_items)} 条订阅"]
        if preview:
            lines.append(f"包含：{preview} 等" if len(titles) > 5 else f"包含：{preview}")
        _send_wx_message("\n".join(lines))

    threading.Thread(target=do_unsubscribe_index, daemon=True).start()


def _handle_wx_list_subscriptions(wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_list():
        _reconcile_submedia_completion_async(min_interval=5)
        items = _cms_list_submedia()
        if not items:
            _send_wx_message("📚 当前没有订阅")
            return

        sorted_items = _sort_submedia_items(items)
        _cache_wx_subscriptions(sorted_items)

        chunk_size = 25
        total = len(sorted_items)
        for start in range(0, total, chunk_size):
            chunk = sorted_items[start:start + chunk_size]
            if start == 0:
                lines = [f"📚 当前订阅共 {total} 条", ""]
            else:
                lines = [f"📚 当前订阅续页 {start + 1}-{start + len(chunk)}", ""]

            for idx, item in enumerate(chunk, start=start + 1):
                title = str(_submedia_value(item, "title") or "未知").strip()
                media_type = "电视剧" if str(_submedia_value(item, "type") or "") == "tv" else "电影"
                year = str(_submedia_value(item, "year") or "").strip()
                status = _submedia_status_label(_submedia_value(item, "status"))
                meta_parts = [media_type]
                if year:
                    meta_parts.append(year)
                meta_parts.append(status)
                lines.append(f"{idx}. {title}｜{'｜'.join(meta_parts)}")

            if start + len(chunk) >= total:
                lines.extend([
                    "",
                    "💡 回复序号删除订阅，如 2 或 1,2,3",
                    "💡 发送 删除已完成 可删除已完成订阅",
                ])
            _send_wx_message("\n".join(lines))

    threading.Thread(target=do_list, daemon=True).start()


# ============================================================
# 格式化
# ============================================================
def _format_results_tg(keyword, data, source="盘搜", prefix_override=None, button_callback_builder=None):
    merged = data.get("merged_by_type", {})
    filtered = {k: v for k, v in merged.items() if k in ALLOWED_CLOUD_TYPES and v}
    total = sum(len(v) for v in filtered.values())

    if not filtered or total == 0:
        return None, None

    lines = [f"🔍 <b>{keyword}</b>（{source}）共 {total} 个结果\n"]
    buttons = []
    # 按钮前缀区分来源
    prefix = prefix_override or {"盘搜": "ps", "影巢": "hd", "观影": "gy"}.get(source, "ps")
    for cloud_type, items in filtered.items():
        type_name = CLOUD_TYPE_NAMES.get(cloud_type, cloud_type)
        lines.append(f"  📁 {type_name}: {len(items)} 个资源")
        button = None
        if callable(button_callback_builder):
            try:
                button = button_callback_builder(cloud_type, items)
            except Exception:
                button = None
        else:
            btn_data = f"{prefix}_type:{cloud_type}:{keyword}"
            if len(btn_data) <= 64:
                button = {"text": f"📁 {type_name} ({len(items)})", "data": btn_data}
        if isinstance(button, dict):
            button.setdefault("text", f"📁 {type_name} ({len(items)})")
            buttons.append(button)

    text = "\n".join(lines) + "\n\n👇 点击查看详细链接"
    return text, buttons


def _format_type_detail_tg(keyword, cloud_type, items):
    type_name = CLOUD_TYPE_NAMES.get(cloud_type, cloud_type)
    lines = [f"📁 <b>{type_name}</b> — {keyword}"]
    buttons = []

    for i, item in enumerate(items[:15]):
        note = _tg_resource_note_text(item)
        meta_text = _tg_resource_meta_text(item)
        if meta_text:
            note = f"{meta_text}｜{note}"
        url = _subscription_prepare_url(item.get("url", ""), item.get("password", ""))
        slug = item.get("slug", "")
        source_key = _normalize_result_source_key(item.get("source"))
        is_free_hdhive = source_key == "hdhive" and _is_free_hdhive_item(item)

        if is_free_hdhive and not url:
            direct_url = _sanitize_share_url(item.get("direct_url", ""))
            if direct_url:
                url = _subscription_prepare_url(direct_url, item.get("password", ""))
            elif slug:
                try:
                    unlock_data = hdhive_client.unlock(slug)
                except Exception:
                    unlock_data = {}

                if unlock_data.get("success"):
                    link_data = unlock_data.get("data", {}) or {}
                    full_url = _sanitize_share_url(link_data.get("full_url", "") or link_data.get("url", ""))
                    access_code = str(link_data.get("access_code", "") or item.get("password", "")).strip()
                    url = _subscription_prepare_url(full_url, access_code)
                    if full_url:
                        item["direct_url"] = full_url
                        item["url"] = full_url
                    if access_code:
                        item["password"] = access_code

        if slug and not is_free_hdhive:
            btn_data = f"hd_u:{slug}"
            if len(btn_data) <= 64:
                buttons.append({
                    "text": f"{i+1}. {note}",
                    "callback_data": btn_data,
                })
        elif url:
            buttons.append({
                "text": f"{i+1}. {note}",
                "input_url": url,
            })
        else:
            lines.append(f"{i+1}. {note}")

    if len(items) > 15:
        lines.append(f"... 还有 {len(items) - 15} 个结果未显示")
    return "\n".join(lines), buttons


def _wx_hdhive_result_page_meta(items, page=1, page_size=WX_HDHIVE_RESULT_PAGE_SIZE):
    all_items = list(items or [])
    total = len(all_items)
    if total <= 0:
        return [], 0, 0, 1, 0, 0

    try:
        page = int(page or 1)
    except Exception:
        page = 1
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = min(total, start + page_size)
    return all_items[start:end], total, total_pages, page, start, end


def _ensure_wx_hdhive_result_item_url(item):
    item = item or {}
    kind = str(item.get("kind") or "").strip()
    if kind == "external":
        return _sanitize_share_url(item.get("url", ""))

    if kind != "hdhive" or item.get("is_paid"):
        return str(item.get("direct_url") or "").strip()

    direct_url = _sanitize_share_url(item.get("direct_url", ""))
    if direct_url:
        item["direct_url"] = direct_url
        return direct_url

    slug = str(item.get("slug") or "").strip()
    if not slug:
        return ""

    try:
        unlock_data = hdhive_client.unlock(slug)
    except Exception:
        return ""

    if not unlock_data.get("success"):
        return ""

    link_data = unlock_data.get("data", {}) or {}
    direct_url = _sanitize_share_url(link_data.get("full_url", "") or link_data.get("url", ""))
    if direct_url:
        item["direct_url"] = direct_url
    return direct_url


def _wx_hdhive_result_stats(items):
    stats = {
        "hdhive_free": 0,
        "hdhive_paid": 0,
        "gying": 0,
        "panso": 0,
    }
    for item in items or []:
        source_key = _normalize_result_source_key(item.get("source"))
        if source_key == "hdhive":
            if item.get("is_paid"):
                stats["hdhive_paid"] += 1
            else:
                stats["hdhive_free"] += 1
        elif source_key == "gying":
            stats["gying"] += 1
        elif source_key == "panso":
            stats["panso"] += 1
    return stats


def _wx_hdhive_result_type_page_hints(items, page_size=WX_HDHIVE_RESULT_PAGE_SIZE):
    hints = []
    for cloud_type in ["123", "magnet"]:
        first_index = None
        for idx, item in enumerate(items or [], start=1):
            if str(item.get("cloud_type") or "").strip().lower() == cloud_type:
                first_index = idx
                break
        if not first_index:
            continue
        page_no = ((first_index - 1) // max(1, int(page_size or 1))) + 1
        label = "123网盘" if cloud_type == "123" else "磁链"
        hints.append(f"{label}第{page_no}页起")
    return hints


def _format_wx_hdhive_result_page(keyword, items, page=1):
    page_items, total, total_pages, page, start, _ = _wx_hdhive_result_page_meta(items, page=page)
    if total <= 0:
        return None, 0, 0, 1

    has_paid = any(item.get("kind") == "hdhive" and item.get("is_paid") for item in (items or []))
    stats = _wx_hdhive_result_stats(items)
    type_page_hints = _wx_hdhive_result_type_page_hints(items)
    header = f"【{keyword}】共找到{total}条相关资源"
    stats_line = (
        f"影巢 免费{stats['hdhive_free']}条 / 收费{stats['hdhive_paid']}条"
        f"｜观影 {stats['gying']}条"
        f"｜盘搜 {stats['panso']}条"
    )

    lines = [header, stats_line, ""]
    if total_pages > 1 and type_page_hints:
        lines = [header, stats_line, "｜".join(type_page_hints), ""]
    for offset, item in enumerate(page_items, start=1):
        global_index = start + offset
        indent = " " * (len(str(global_index)) + 2)
        title_text = str(item.get("title_text") or "未知资源").strip() or "未知资源"
        meta_text = str(item.get("meta_text") or "").strip()
        resolution = str(item.get("resolution") or "").strip()
        size = str(item.get("size") or "").strip()
        detail_text = str(item.get("detail_text") or "").strip()
        direct_url = _ensure_wx_hdhive_result_item_url(item)

        lines.append(f"{global_index}. {title_text}")
        if meta_text:
            lines.append(f"{indent}{meta_text}")

        size_parts = []
        if resolution:
            size_parts.append(resolution)
        if size:
            size_parts.append(size)
        if size_parts:
            lines.append(f"{indent}{'｜'.join(size_parts)}")

        if detail_text:
            lines.append(f"{indent}{detail_text}")

        if direct_url:
            lines.append(f"{indent}{direct_url}")

        lines.append("")

    if total_pages > 1:
        hint_parts = []
        if has_paid:
            hint_parts.append("影巢付费资源回复对应数字解锁")
        hint_parts.append(f"第{page}/{total_pages}页 p: 上一页 n: 下一页")
        lines.append(f"💡 {'；'.join(hint_parts)}")

    return "\n".join(lines), total, total_pages, page


def _format_results_wx(keyword, data, source="盘搜", show_item_source=False):
    merged = data.get("merged_by_type", {})
    items_115 = merged.get("115", [])
    if not items_115:
        return None

    lines = [f"🔍 {keyword}（{source}）\n", f"📁 115网盘 ({len(items_115)}个)\n"]
    for i, item in enumerate(items_115[:10]):
        note = item.get("note", "") or item.get("title", "") or "未知"
        meta_text = _hdhive_item_meta_text(item)
        url = item.get("url", "")
        pwd = item.get("password", "")
        if show_item_source:
            source_name = _source_display_name(item.get("source"))
            if source_name:
                note = f"[{source_name}] {note}"
        lines.append(f"{i+1}. {note}")
        if meta_text:
            lines.append(f"   {meta_text}")
        if url:
            lines.append(f"   {url}")
        if pwd:
            lines.append(f"   密码: {pwd}")
        lines.append("")
    if len(items_115) > 10:
        lines.append(f"... 还有 {len(items_115) - 10} 条未显示")
    lines.append("\n💡 复制链接发送即可转存")
    return "\n".join(lines)


# ============================================================
# CMS 转存
# ============================================================
def _cms_add_share_down(url):
    import requests
    cms_token = os.environ.get("CMS_API_TOKEN", "cloud_media_sync")
    try:
        resp = requests.post(
            "http://127.0.0.1:9527/api/cloud/add_share_down_by_token",
            json={"url": url, "token": cms_token},
            timeout=10,
        )
        data = resp.json()
        return data.get("code") == 200, data.get("msg", "未知")
    except Exception as e:
        return False, str(e)


# ============================================================
# 企微发送
# ============================================================
def _get_wx_instance():
    wechat_inst = _state.get("wechat_instance")
    if not wechat_inst:
        try:
            import gc
            WechatModule = _state.get("WechatModule")
            if WechatModule:
                for obj in gc.get_objects():
                    try:
                        if isinstance(obj, WechatModule):
                            wechat_inst = obj
                            _state["wechat_instance"] = obj
                            break
                    except Exception:
                        pass
        except Exception:
            pass
    return wechat_inst


def _send_wx_message(content):
    wechat_inst = _get_wx_instance()

    if wechat_inst and hasattr(wechat_inst, "send_msg"):
        try:
            wechat_inst.send_msg(content)
            return True
        except Exception:
            pass
    return False


def _send_wx_image_message(image_source):
    image_source = str(image_source or "").strip()
    if not image_source:
        return False

    wechat_inst = _get_wx_instance()
    if not wechat_inst or not hasattr(wechat_inst, "send_msg"):
        return False

    image_payloads = [image_source]
    if os.path.isfile(image_source):
        try:
            with open(image_source, "rb") as f:
                raw_bytes = f.read()
            if raw_bytes:
                image_payloads.append(raw_bytes)
                image_payloads.append("data:image/jpeg;base64," + base64.b64encode(raw_bytes).decode("ascii"))
        except Exception:
            pass

    send_attempts = []
    for payload in image_payloads:
        send_attempts.extend([
            lambda payload=payload: wechat_inst.send_msg("", image=payload),
            lambda payload=payload: wechat_inst.send_msg(" ", image=payload),
            lambda payload=payload: wechat_inst.send_msg(image=payload),
        ])

    for method_name in ["send_image", "send_img", "send_pic", "send_picture", "send_file"]:
        method = getattr(wechat_inst, method_name, None)
        if callable(method):
            for payload in image_payloads:
                send_attempts.append(lambda method=method, payload=payload: method(payload))

    for attempt in send_attempts:
        try:
            attempt()
            return True
        except TypeError:
            continue
        except Exception:
            continue
    return False


def _send_wx_news_message(articles, userid=None):
    wechat_inst = _get_wx_instance()
    if wechat_inst is None:
        return False

    send_url = getattr(wechat_inst, "_send_msg_url", "")
    get_access_token = getattr(wechat_inst, "_WeChat__get_access_token", None)
    post_request = getattr(wechat_inst, "_WeChat__post_request", None)
    config_env = getattr(wechat_inst, "_configEnv", None)
    agent_id = getattr(config_env, "wechat_app_id", None)

    if not send_url or not callable(get_access_token) or not callable(post_request) or not agent_id:
        return False

    token = str(get_access_token() or "").strip()
    if not token:
        return False

    normalized_articles = []
    for article in list(articles or []):
        if not isinstance(article, dict):
            continue
        title = str(article.get("title") or "").strip()
        description = str(article.get("description") or "").replace("\n\n", "\n").strip()
        picurl = str(article.get("picurl") or "").strip()
        url = str(article.get("url") or "").strip()
        if not title:
            continue
        normalized_articles.append({
            "title": title,
            "description": description,
            "picurl": picurl,
            "url": url,
        })

    if not normalized_articles:
        return False

    payload = {
        "touser": str(userid or "@all").strip() or "@all",
        "msgtype": "news",
        "agentid": agent_id,
        "news": {
            "articles": normalized_articles,
        },
    }
    try:
        return bool(post_request(send_url % token, payload))
    except Exception:
        return False


def _split_message_title_and_text(content, default_title="CMS"):
    lines = [str(line or "").rstrip() for line in str(content or "").splitlines()]
    lines = [line for line in lines if line.strip()]
    if not lines:
        return default_title, ""
    return lines[0], "\n".join(lines[1:])


def _send_wx_rich_message(content, image_url="", link=""):
    image_url = str(image_url or "").strip()
    link = str(link or "").strip()
    wechat_inst = _state.get("wechat_instance")
    if wechat_inst and hasattr(wechat_inst, "send_msg"):
        title, text = _split_message_title_and_text(content)
        try:
            wechat_inst.send_msg(title, text=text, image=image_url, link=(link or None))
            return True
        except Exception:
            pass
    if image_url or link:
        fallback_lines = [str(content or "").strip()]
        if image_url:
            fallback_lines.append(f"海报：{image_url}")
        if link:
            fallback_lines.append(f"链接：{link}")
        return _send_wx_message("\n".join([line for line in fallback_lines if line]))
    return _send_wx_message(content)


def _normalize_event_source(value):
    text = str(value or "").strip().lower()
    if text in {"tg", "telegram"}:
        return "tg"
    if text in {"wx", "wechat", "qywx", "wecom"}:
        return "wx"
    return None


def _normalize_event_chat_id(value):
    text = str(value or "").strip()
    if text and re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except Exception:
            return None
    return None


def _get_tg_bot():
    bot = _state.get("tg_bot")
    if bot is not None:
        return bot

    module = sys.modules.get("app.modules.tg.tg_bot")
    if module is not None:
        for attr_name in ["bot", "_bot", "tg_bot"]:
            bot = getattr(module, attr_name, None)
            if bot is not None:
                _state["tg_bot"] = bot
                return bot

    try:
        from app.core import obj_factory

        for attr_name in ["bot", "tg_bot"]:
            bot = getattr(obj_factory, attr_name, None)
            if bot is not None:
                _state["tg_bot"] = bot
                return bot
    except Exception:
        pass

    try:
        import gc
        import telebot

        for obj in gc.get_objects():
            try:
                if isinstance(obj, telebot.TeleBot):
                    _state["tg_bot"] = obj
                    return obj
            except Exception:
                pass
    except Exception:
        pass
    return None


def _send_tg_message(chat_id, content, bot=None, parse_mode=None, reply_markup=None):
    bot = bot or _get_tg_bot()
    chat_id = _normalize_event_chat_id(chat_id)
    if bot is None or chat_id is None or not content:
        return False

    kwargs = {
        "disable_web_page_preview": True,
    }
    if parse_mode:
        kwargs["parse_mode"] = parse_mode
    if reply_markup is not None:
        kwargs["reply_markup"] = reply_markup

    try:
        bot.send_message(chat_id, content, **kwargs)
        return True
    except Exception:
        return False


def _send_tg_rich_message(chat_id, content, image_url="", bot=None, parse_mode=None, reply_markup=None):
    bot = bot or _get_tg_bot()
    chat_id = _normalize_event_chat_id(chat_id)
    image_url = str(image_url or "").strip()
    if bot is None or chat_id is None or not content:
        return False

    if image_url and hasattr(bot, "send_photo"):
        kwargs = {}
        if parse_mode:
            kwargs["parse_mode"] = parse_mode
        if reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        caption = str(content or "").strip()
        if len(caption) > 1000:
            caption = caption[:999].rstrip() + "…"
        try:
            bot.send_photo(chat_id, image_url, caption=caption, **kwargs)
            return True
        except Exception:
            pass

    return _send_tg_message(chat_id, content, bot=bot, parse_mode=parse_mode, reply_markup=reply_markup)


def _handle_wx_update_file(wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    def do_update():
        if not _begin_auto_update_check():
            _send_wx_message("⏳ 当前已有更新任务在执行，请稍后再试")
            return
        try:
            _send_wx_message("⏳ 正在检查更新，请稍候...")
            result = _check_auto_update_once()
            _send_wx_message(str((result or {}).get("message") or "更新检查完成"))
        except Exception as exc:
            _save_auto_update_state(
                last_result="error",
                last_checked_at=int(time.time()),
                last_error=str(exc),
                target_path=_get_auto_update_target_path(),
                target_name=_get_auto_update_target_name(),
            )
            _send_wx_message(f"❌ 更新检查失败：{exc}")
        finally:
            _finish_auto_update_check()

    threading.Thread(target=do_update, daemon=True).start()


def _handle_tg_update_file(chat_id=None, bot=None):
    def do_update():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        if not _begin_auto_update_check():
            _send_tg_message(target_chat_id, "⏳ 当前已有更新任务在执行，请稍后再试", bot=tg_bot)
            return

        try:
            try:
                tg_bot.send_chat_action(target_chat_id, "typing")
            except Exception:
                pass
            _send_tg_message(target_chat_id, "⏳ 正在检查更新，请稍候...", bot=tg_bot)
            result = _check_auto_update_once()
            _send_tg_message(target_chat_id, str((result or {}).get("message") or "更新检查完成"), bot=tg_bot)
        except Exception as exc:
            _save_auto_update_state(
                last_result="error",
                last_checked_at=int(time.time()),
                last_error=str(exc),
                target_path=_get_auto_update_target_path(),
                target_name=_get_auto_update_target_name(),
            )
            _send_tg_message(target_chat_id, f"❌ 更新检查失败：{exc}", bot=tg_bot)
        finally:
            _finish_auto_update_check()

    threading.Thread(target=do_update, daemon=True).start()


def _handle_tg_subscribe(command, chat_id=None, bot=None):
    def do_subscribe():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        try:
            tg_bot.send_chat_action(target_chat_id, "typing")
        except Exception:
            pass

        _clear_tg_subscribe_candidates(target_chat_id)
        keyword = command["keyword"]
        prefer_type = command.get("prefer_type")
        target_year = command.get("target_year")
        prefer_animation = bool(command.get("prefer_animation"))
        candidates = _resolve_subscribe_candidates(
            keyword,
            prefer_type=prefer_type,
            target_year=target_year,
            prefer_animation=prefer_animation,
        )
        if not candidates:
            _send_tg_message(target_chat_id, f"❌ 未找到《{keyword}》对应的媒体信息", bot=tg_bot)
            return

        if len(candidates) > 1:
            _cache_tg_subscribe_candidates(target_chat_id, keyword, candidates)
            _reply_tg_subscribe_candidates(target_chat_id, keyword, candidates, bot=tg_bot)
            return

        _set_tg_action_context(target_chat_id, "")
        result = _build_subscribe_candidate_result(candidates[0], fallback_keyword=keyword)
        if not result:
            _send_tg_message(target_chat_id, f"❌ 《{keyword}》缺少订阅所需的媒体信息", bot=tg_bot)
            return

        _send_tg_rich_message(
            target_chat_id,
            result["feedback"],
            image_url=result["poster_url"],
            bot=tg_bot,
        )

    threading.Thread(target=do_subscribe, daemon=True).start()


def _handle_tg_unsubscribe(command, chat_id=None, bot=None):
    def do_unsubscribe():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        try:
            tg_bot.send_chat_action(target_chat_id, "typing")
        except Exception:
            pass

        keyword = command["keyword"]
        prefer_type = command.get("prefer_type")
        item, matched_items = _choose_unsubscribe_item(keyword, prefer_type)
        if not item or not matched_items:
            _send_tg_message(target_chat_id, f"❌ 订阅列表里没有找到《{keyword}》", bot=tg_bot)
            return

        target_items = matched_items if not prefer_type else [item]
        target_ids = [
            int(_submedia_value(target_item, "id"))
            for target_item in target_items
            if str(_submedia_value(target_item, "id") or "").strip()
        ]
        title = _submedia_value(item, "title") or keyword
        ok, msg = _cms_delete_submedia_db(target_ids)
        if not ok:
            _send_tg_message(target_chat_id, f"❌ 退订失败：{msg or title}", bot=tg_bot)
            return

        _set_tg_action_context(target_chat_id, "")
        feedback, poster_url, _ = _build_unsubscribe_feedback_payload(
            item,
            target_items=target_items,
            title_fallback=keyword,
        )
        _send_tg_rich_message(target_chat_id, feedback, image_url=poster_url, bot=tg_bot)

    threading.Thread(target=do_unsubscribe, daemon=True).start()


def _handle_tg_delete_completed(chat_id=None, bot=None):
    def do_delete_completed():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        try:
            tg_bot.send_chat_action(target_chat_id, "typing")
        except Exception:
            pass

        _reconcile_submedia_completion_wait(timeout=2.5, force=True, min_interval=5)
        all_items = _cms_list_submedia()
        completed_items = [
            item for item in all_items
            if str(_submedia_value(item, "status") or "").strip() == "2"
        ]
        if not completed_items:
            _send_tg_message(target_chat_id, "📚 当前没有已完成订阅", bot=tg_bot)
            return

        target_ids = [
            int(_submedia_value(item, "id"))
            for item in completed_items
            if str(_submedia_value(item, "id") or "").strip()
        ]
        if not target_ids:
            _send_tg_message(target_chat_id, "❌ 未找到可删除的已完成订阅", bot=tg_bot)
            return

        ok, msg = _cms_delete_submedia_db(target_ids)
        if not ok:
            _send_tg_message(target_chat_id, f"❌ 删除失败：{msg or '已完成订阅'}", bot=tg_bot)
            return

        titles = [
            str(_submedia_value(item, "title") or "").strip()
            for item in completed_items
            if str(_submedia_value(item, "title") or "").strip()
        ]
        preview = "、".join(titles[:5])
        lines = [f"✅ 已删除 {len(target_ids)} 条已完成订阅"]
        if preview:
            lines.append(f"包含：{preview} 等" if len(titles) > 5 else f"包含：{preview}")

        _set_tg_action_context(target_chat_id, "")
        _send_tg_message(target_chat_id, "\n".join(lines), bot=tg_bot)

    threading.Thread(target=do_delete_completed, daemon=True).start()


def _handle_tg_unsubscribe_index(index, chat_id=None, bot=None):
    def do_unsubscribe_index():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        try:
            tg_bot.send_chat_action(target_chat_id, "typing")
        except Exception:
            pass

        items = _get_tg_subscription_items(target_chat_id)
        if not items:
            _send_tg_message(target_chat_id, "❌ 没有可删除的订阅列表，请先发送“当前订阅”", bot=tg_bot)
            return

        indices = index if isinstance(index, (list, tuple, set)) else [index]
        ordered_indices = []
        seen_indices = set()
        for value in indices:
            try:
                number = int(value)
            except Exception:
                continue
            if number not in seen_indices:
                seen_indices.add(number)
                ordered_indices.append(number)
        if not ordered_indices:
            _send_tg_message(target_chat_id, "❌ 序号格式无效", bot=tg_bot)
            return

        invalid = [str(number) for number in ordered_indices if number < 1 or number > len(items)]
        if invalid:
            _send_tg_message(target_chat_id, f"❌ 序号无效：{', '.join(invalid)}，请输入 1-{len(items)}", bot=tg_bot)
            return

        target_items = [items[number - 1] for number in ordered_indices]
        target_ids = []
        for item in target_items:
            target_id = str(_submedia_value(item, "id") or "").strip()
            if target_id:
                target_ids.append(int(target_id))
        if not target_ids:
            _send_tg_message(target_chat_id, "❌ 选中的订阅缺少 ID，无法删除", bot=tg_bot)
            return

        title = str(_submedia_value(target_items[0], "title") or "未知").strip() or "未知"
        ok, msg = _cms_delete_submedia_db(target_ids)
        if not ok:
            _send_tg_message(target_chat_id, f"❌ 删除失败：{msg or title}", bot=tg_bot)
            return

        target_index_set = set(ordered_indices)
        remaining_items = [entry for idx, entry in enumerate(items, start=1) if idx not in target_index_set]
        _cache_tg_subscriptions(target_chat_id, remaining_items)

        if len(target_items) == 1:
            feedback, poster_url, _ = _build_unsubscribe_feedback_payload(
                target_items[0],
                target_items=target_items,
                title_fallback=title,
            )
            _send_tg_rich_message(target_chat_id, feedback, image_url=poster_url, bot=tg_bot)
            return

        titles = [
            str(_submedia_value(item, "title") or "").strip()
            for item in target_items
            if str(_submedia_value(item, "title") or "").strip()
        ]
        preview = "、".join(titles[:5])
        lines = [f"✅ 已删除 {len(target_items)} 条订阅"]
        if preview:
            lines.append(f"包含：{preview} 等" if len(titles) > 5 else f"包含：{preview}")
        _send_tg_message(target_chat_id, "\n".join(lines), bot=tg_bot)

    threading.Thread(target=do_unsubscribe_index, daemon=True).start()


def _handle_tg_list_subscriptions(chat_id=None, bot=None):
    def do_list():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            return

        try:
            try:
                tg_bot.send_chat_action(target_chat_id, "typing")
            except Exception:
                pass

            _reconcile_submedia_completion_async(min_interval=5)
            items = _cms_list_submedia()
            if not items:
                _send_tg_message(target_chat_id, "📚 当前没有订阅", bot=tg_bot)
                return

            sorted_items = _sort_submedia_items(items)
            _cache_tg_subscriptions(target_chat_id, sorted_items)

            chunk_size = 25
            total = len(sorted_items)
            for start in range(0, total, chunk_size):
                chunk = sorted_items[start:start + chunk_size]
                if start == 0:
                    lines = [f"📚 当前订阅共 {total} 条", ""]
                else:
                    lines = [f"📚 当前订阅续页 {start + 1}-{start + len(chunk)}", ""]

                for idx, item in enumerate(chunk, start=start + 1):
                    title = str(_submedia_value(item, "title") or "未知").strip()
                    media_type = "电视剧" if str(_submedia_value(item, "type") or "") == "tv" else "电影"
                    year = str(_submedia_value(item, "year") or "").strip()
                    status = _submedia_status_label(_submedia_value(item, "status"))
                    meta_parts = [media_type]
                    if year:
                        meta_parts.append(year)
                    meta_parts.append(status)
                    lines.append(f"{idx}. {title}｜{'｜'.join(meta_parts)}")

                if start + len(chunk) >= total:
                    lines.extend([
                        "",
                        "💡 回复序号删除订阅，如 2 或 1,2,3",
                        "💡 发送 删除已完成 可删除已完成订阅",
                    ])
                _send_tg_message(target_chat_id, "\n".join(lines), bot=tg_bot)
        except Exception as e:
            _log(f"TG当前订阅查询失败: {e}\n{traceback.format_exc()}")
            _send_tg_message(target_chat_id, f"❌ 当前订阅查询失败: {e}", bot=tg_bot)

    threading.Thread(target=do_list, daemon=True).start()


def _handle_tg_text_command(content, chat_id=None, bot=None):
    text = str(content or "").strip()
    target_chat_id = _normalize_event_chat_id(chat_id)
    if not text or target_chat_id is None:
        return False

    if _get_tg_action_context(target_chat_id) == "subscribe_candidates":
        plain_indices = _parse_subscription_indices(text)
        if plain_indices:
            _handle_tg_subscribe_candidate_index(plain_indices, chat_id=target_chat_id, bot=bot)
            return True

    if _get_tg_action_context(target_chat_id) == "hdhive_candidates":
        plain_indices = _parse_subscription_indices(text)
        if plain_indices:
            _handle_tg_hdhive_candidate_index(plain_indices, chat_id=target_chat_id, bot=bot)
            return True

    if _get_tg_action_context(target_chat_id) == "subscriptions":
        plain_indices = _parse_subscription_indices(text)
        if plain_indices:
            _handle_tg_unsubscribe_index(plain_indices, chat_id=target_chat_id, bot=bot)
            return True

    unsubscribe_index = _is_unsubscribe_index_command(text)
    if unsubscribe_index:
        _handle_tg_unsubscribe_index(unsubscribe_index, chat_id=target_chat_id, bot=bot)
        return True

    if _is_list_subscriptions_command(text):
        _handle_tg_list_subscriptions(chat_id=target_chat_id, bot=bot)
        return True

    if _is_delete_completed_command(text):
        _handle_tg_delete_completed(chat_id=target_chat_id, bot=bot)
        return True

    if _is_update_file_command(text):
        _handle_tg_update_file(chat_id=target_chat_id, bot=bot)
        return True

    subscribe_command = _is_subscribe_command(text)
    if subscribe_command:
        _handle_tg_subscribe(subscribe_command, chat_id=target_chat_id, bot=bot)
        return True

    unsubscribe_command = _is_unsubscribe_command(text)
    if unsubscribe_command:
        _handle_tg_unsubscribe(unsubscribe_command, chat_id=target_chat_id, bot=bot)
        return True

    if _is_search_query(text):
        _handle_tg_event_search(text, chat_id=target_chat_id, bot=bot)
        return True

    if _is_wx_direct_search_text(text):
        _handle_tg_event_search(text, chat_id=target_chat_id, bot=bot)
        return True

    return False


def _extract_event_message_context(*values):
    context = {
        "text": None,
        "source": None,
        "chat_id": None,
        "wechat_instance": None,
    }
    visited = set()
    internal_list_command_candidates = []
    numeric_text_candidates = []
    source_keys = {"source", "msg_source", "platform", "channel", "from_type"}
    chat_keys = {
        "chat_id",
        "chatid",
        "from_chat_id",
        "from_user_id",
        "fromuserid",
        "user",
        "user_id",
        "userid",
        "uid",
        "sender_id",
        "to_user_id",
        "tg_chat_id",
    }
    message_text_keys = {"text", "content", "message", "msg", "body", "query", "keyword"}
    inspect_attrs = [
        "text",
        "content",
        "message",
        "msg",
        "body",
        "query",
        "keyword",
        "source",
        "platform",
        "channel",
        "from_type",
        "chat_id",
        "chatId",
        "from_chat_id",
        "from_user_id",
        "user_id",
        "sender_id",
        "wechat",
        "chat",
        "from_user",
        "data",
        "params",
    ]

    def remember_wechat_instance(value):
        if context["wechat_instance"] is not None or value is None:
            return
        try:
            if hasattr(value, "send_msg"):
                context["wechat_instance"] = value
                return
        except Exception:
            pass
        try:
            nested = getattr(value, "wechat", None)
            if nested is not None and hasattr(nested, "send_msg"):
                context["wechat_instance"] = nested
        except Exception:
            pass

    def walk(value, depth=0, key_hint=""):
        if value is None or depth > 4:
            return

        key_name = str(key_hint or "").strip().lower()
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return
            if context["source"] is None and key_name in source_keys:
                context["source"] = _normalize_event_source(text)
            if context["chat_id"] is None and key_name in chat_keys:
                context["chat_id"] = _normalize_event_chat_id(text)
            if context["text"] is None and _is_supported_text_command(text):
                context["text"] = text
            elif key_name in message_text_keys and _is_internal_wx_list_subscriptions_command(text):
                internal_list_command_candidates.append(text)
            elif key_name in message_text_keys and _parse_subscription_indices(text):
                numeric_text_candidates.append(text)
            return

        if isinstance(value, (int, float)):
            if context["chat_id"] is None and key_name in chat_keys:
                context["chat_id"] = _normalize_event_chat_id(value)
            return

        obj_id = id(value)
        if obj_id in visited:
            return
        visited.add(obj_id)
        remember_wechat_instance(value)

        if isinstance(value, dict):
            for item_key, item_value in list(value.items())[:80]:
                walk(item_value, depth + 1, str(item_key))
            return

        if isinstance(value, (list, tuple, set)):
            for item in list(value)[:50]:
                walk(item, depth + 1, key_hint)
            return

        for attr_name in inspect_attrs:
            try:
                attr_value = getattr(value, attr_name, None)
            except Exception:
                continue
            if attr_value is not None:
                walk(attr_value, depth + 1, attr_name)

        try:
            data = getattr(value, "__dict__", None)
        except Exception:
            data = None
        if isinstance(data, dict):
            for item_key, item_value in list(data.items())[:50]:
                walk(item_value, depth + 1, str(item_key))

    for value in values:
        walk(value)

    if context["text"] is None and numeric_text_candidates:
        is_tg_context = context["source"] == "tg" or (
            context["chat_id"] is not None and context["source"] != "wx"
        )
        if is_tg_context and context["chat_id"] is not None:
            tg_action = _get_tg_action_context(context["chat_id"])
            if tg_action in {"subscribe_candidates", "hdhive_candidates", "subscriptions"}:
                context["text"] = numeric_text_candidates[0]
        else:
            wx_action = _get_wx_action_context()
            if wx_action in {"subscribe_candidates", "hdhive_candidates", "subscriptions"}:
                context["text"] = numeric_text_candidates[0]

    if context["text"] is None and internal_list_command_candidates:
        is_tg_context = context["source"] == "tg" or (
            context["chat_id"] is not None and context["source"] != "wx"
        )
        if is_tg_context:
            context["text"] = internal_list_command_candidates[0]
    return context


def _reply_tg_search_results(keyword, chat_id, bot=None, source_filter=None):
    bot = bot or _get_tg_bot()
    chat_id = _normalize_event_chat_id(chat_id)
    if bot is None or chat_id is None:
        return False

    try:
        bot.send_chat_action(chat_id, "typing")
    except Exception:
        pass

    sent = False
    if source_filter == "panso":
        source_results = [("盘搜", {"merged_by_type": (_search_panso(keyword) or {}).get("merged_by_type", {})})]
    elif source_filter == "gying":
        source_results = [("观影", _search_gying(keyword))]
    elif source_filter == "hdhive":
        source_results = [("影巢", {"merged_by_type": _search_hdhive_preview(keyword)})]
    else:
        source_results = _run_source_search_tasks(keyword, timeout=20)
    for source_name, source_data in source_results:
        text, buttons = _format_results_tg(keyword, source_data, source_name)
        if not text:
            continue

        markup = None
        if buttons:
            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

            markup = InlineKeyboardMarkup(row_width=2)
            for btn in buttons:
                markup.add(InlineKeyboardButton(text=btn["text"], callback_data=btn["data"]))

        bot.send_message(
            chat_id,
            text,
            parse_mode="HTML",
            reply_markup=markup,
            disable_web_page_preview=True,
        )
        sent = True

    if not sent:
        bot.send_message(chat_id, f"🔍 <b>{keyword}</b>\n\n暂无搜索结果", parse_mode="HTML")
    return True


def _handle_tg_event_search(content, chat_id=None, bot=None):
    result = _is_search_query(content)
    if result:
        keyword, source = result
    else:
        keyword = _is_wx_direct_search_text(content)
        if not keyword:
            return
        source = "hdhive"

    def do_search():
        tg_bot = bot or _get_tg_bot()
        target_chat_id = _normalize_event_chat_id(chat_id)
        if tg_bot is None or target_chat_id is None:
            _log(f"TG消息增强命中但未拿到回包上下文：{content}")
            return

        _set_tg_action_context(target_chat_id, "")
        _clear_tg_hdhive_candidates(target_chat_id, reset_action=False)
        with _search_cache_lock:
            _search_cache.setdefault("tg_hdhive_selected_results", {}).pop(str(target_chat_id), None)
            _search_cache.setdefault("tg_selected_candidate_results", {}).pop(str(target_chat_id), None)

        try:
            if source == "hdhive":
                candidates = _build_hdhive_search_candidate_items(keyword)
                if not candidates:
                    _send_tg_message(target_chat_id, f"❌ 未找到《{keyword}》对应的影视条目", bot=tg_bot)
                    return
                if len(candidates) > 1:
                    _cache_tg_hdhive_candidates(target_chat_id, keyword, candidates)
                    _reply_tg_hdhive_search_candidates(target_chat_id, keyword, candidates, bot=tg_bot)
                    return
                _reply_tg_selected_candidate_results(
                    candidates[0],
                    fallback_keyword=keyword,
                    chat_id=target_chat_id,
                    bot=tg_bot,
                )
                return

            _reply_tg_search_results(keyword, target_chat_id, bot=tg_bot, source_filter=source)
        except Exception as e:
            try:
                tg_bot.send_message(target_chat_id, f"❌ 搜索异常: {e}")
            except Exception:
                pass

    threading.Thread(target=do_search, daemon=True).start()


def _build_tg_inline_markup(buttons, row_width=1):
    if not buttons:
        return None
    try:
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    except Exception:
        return None

    markup = InlineKeyboardMarkup(row_width=row_width)
    tg_buttons = []
    for btn in buttons:
        if "callback_data" in btn:
            tg_buttons.append(InlineKeyboardButton(
                text=btn["text"],
                callback_data=btn["callback_data"],
            ))
        elif "data" in btn:
            tg_buttons.append(InlineKeyboardButton(
                text=btn["text"],
                callback_data=btn["data"],
            ))
        elif "input_url" in btn:
            tg_buttons.append(InlineKeyboardButton(
                text=btn["text"],
                switch_inline_query_current_chat=btn["input_url"],
            ))
    row_size = max(1, int(row_width or 1))
    for start in range(0, len(tg_buttons), row_size):
        markup.row(*tg_buttons[start:start + row_size])
    return markup


def _handle_tg_subscribe_callback(bot, call):
    data = getattr(call, "data", "") or ""
    _state["tg_bot"] = bot
    parts = data.split(":", 1)
    if len(parts) != 2:
        return False

    try:
        index = int(parts[1])
    except Exception:
        return False

    try:
        bot.answer_callback_query(call.id, "正在订阅...")
    except Exception:
        pass

    chat = getattr(getattr(call, "message", None), "chat", None)
    chat_id = getattr(chat, "id", None)
    if chat_id is None:
        return False

    _handle_tg_subscribe_candidate_index(index, chat_id=chat_id, bot=bot)
    return True


def _handle_tg_detail_callback(bot, call):
    data = getattr(call, "data", "") or ""
    _state["tg_bot"] = bot
    _log(f"TG详情回调: {data}")

    parts = data.split(":", 2)
    source_prefix = parts[0]
    if source_prefix == "tgsel":
        if len(parts) < 3:
            return False
        selected_source_key, cloud_type = parts[1], parts[2]
        keyword = ""
    else:
        if len(parts) < 3:
            return False
        cloud_type, keyword = parts[1], parts[2]

    try:
        bot.answer_callback_query(call.id, "正在加载...")
    except Exception:
        pass

    if source_prefix == "hd_type":
        items = _search_hdhive(keyword).get(cloud_type, []) if _has_hdhive() else []
    elif source_prefix == "hds_type":
        chat_id = getattr(getattr(call, "message", None), "chat", None)
        chat_id = getattr(chat_id, "id", None)
        _, cached_grouped = _get_tg_selected_hdhive_results(chat_id)
        items = (cached_grouped or {}).get(cloud_type, [])
    elif source_prefix == "tgsel":
        chat_id = getattr(getattr(call, "message", None), "chat", None)
        chat_id = getattr(chat_id, "id", None)
        keyword, cached_grouped = _get_tg_selected_candidate_results(chat_id)
        items = ((cached_grouped or {}).get(selected_source_key) or {}).get(cloud_type, [])
    elif source_prefix == "gy_type":
        items = _search_gying(keyword).get("merged_by_type", {}).get(cloud_type, []) if _has_gying() else []
    else:
        items = _search_panso(keyword, [cloud_type]).get("merged_by_type", {}).get(cloud_type, []) if _has_panso() else []

    chat_id = getattr(getattr(call, "message", None), "chat", None)
    chat_id = getattr(chat_id, "id", None)
    if chat_id is None:
        return True

    display_keyword = keyword or "当前搜索"
    if not items:
        bot.send_message(chat_id, f"📁 {CLOUD_TYPE_NAMES.get(cloud_type, cloud_type)} — {display_keyword}\n\n暂无结果")
        return True

    text, buttons = _format_type_detail_tg(display_keyword, cloud_type, items)
    markup = _build_tg_inline_markup(buttons, row_width=1)
    bot.send_message(
        chat_id,
        text,
        parse_mode="HTML",
        reply_markup=markup,
        disable_web_page_preview=True,
    )
    return True


def _handle_tg_hdhive_unlock_callback(bot, call):
    data = getattr(call, "data", "") or ""
    _state["tg_bot"] = bot
    slug = data[5:]
    try:
        bot.answer_callback_query(call.id)
    except Exception:
        pass

    try:
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton(text="✅ 确认解锁", callback_data=f"hd_ok:{slug}"),
            InlineKeyboardButton(text="❌ 取消", callback_data="hd_cancel"),
        )
        bot.send_message(
            call.message.chat.id,
            "⚠️ 该资源需要消耗积分解锁，是否继续？",
            reply_markup=markup,
        )
    except Exception as e:
        _log(f"TG解锁确认回调异常: {e}")
    return True


def _handle_tg_hdhive_confirm_callback(bot, call):
    data = getattr(call, "data", "") or ""
    _state["tg_bot"] = bot
    slug = data[6:]

    try:
        bot.answer_callback_query(call.id, "正在解锁...")
    except Exception:
        pass

    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass

    unlock_data = hdhive_client.unlock(slug)
    if not unlock_data.get("success"):
        bot.send_message(call.message.chat.id, f"❌ 解锁失败: {unlock_data.get('message', '解锁失败')}")
        return True

    link_data = unlock_data.get("data", {})
    full_url = _sanitize_share_url(link_data.get("full_url", "") or link_data.get("url", ""))
    access_code = link_data.get("access_code", "")

    if not full_url:
        bot.send_message(call.message.chat.id, "❌ 解锁成功但未获取到链接")
        return True

    link_text = full_url
    if access_code and "password=" not in full_url:
        link_text = f"{full_url}?password={access_code}" if "?" not in full_url else f"{full_url}&password={access_code}"

    markup = _build_tg_inline_markup([{
        "text": "📥 点击发送链接",
        "input_url": link_text,
    }], row_width=1)
    bot.send_message(
        call.message.chat.id,
        "✅ 解锁成功",
        reply_markup=markup,
        disable_web_page_preview=True,
    )
    return True


def _handle_tg_hdhive_cancel_callback(bot, call):
    _state["tg_bot"] = bot
    try:
        bot.answer_callback_query(call.id, "已取消")
    except Exception:
        pass
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    return True


def _dispatch_tg_callback(bot, call):
    data = getattr(call, "data", "") or ""
    if not data:
        return False
    if data.startswith(("ps_type:", "hd_type:", "hds_type:", "gy_type:", "tgsel:")):
        return _handle_tg_detail_callback(bot, call)
    if data.startswith("hd_u:"):
        return _handle_tg_hdhive_unlock_callback(bot, call)
    if data.startswith("hd_ok:"):
        return _handle_tg_hdhive_confirm_callback(bot, call)
    if data == "hd_cancel":
        return _handle_tg_hdhive_cancel_callback(bot, call)
    return False


_telebot_runtime_patched = False


def _patch_telebot_runtime():
    global _telebot_runtime_patched
    if _telebot_runtime_patched:
        return

    try:
        import telebot
        import telebot.apihelper as telebot_apihelper
        import telebot.util as telebot_util
        import telebot.types as telebot_types
    except Exception:
        return

    _telebot_runtime_patched = True

    default_updates = list(getattr(telebot_util, "update_types", []) or [])
    if "callback_query" not in default_updates:
        default_updates.append("callback_query")

    def _merge_allowed_updates(value):
        if value is None:
            return list(default_updates)
        if isinstance(value, str):
            merged = [value]
        else:
            try:
                merged = list(value)
            except Exception:
                merged = [str(value)]
        if "callback_query" not in merged:
            merged.append("callback_query")
        return merged

    original_get_updates = getattr(telebot_apihelper, "get_updates", None)
    if callable(original_get_updates) and not getattr(original_get_updates, "_cms_enhanced", False):
        def patched_get_updates(*args, **kwargs):
            args = list(args)
            if len(args) >= 5:
                args[4] = _merge_allowed_updates(args[4])
            else:
                kwargs["allowed_updates"] = _merge_allowed_updates(kwargs.get("allowed_updates"))
            return original_get_updates(*args, **kwargs)

        patched_get_updates._cms_enhanced = True
        telebot_apihelper.get_updates = patched_get_updates

    original_process_new_callback_query = getattr(telebot.TeleBot, "process_new_callback_query", None)
    if callable(original_process_new_callback_query) and not getattr(original_process_new_callback_query, "_cms_enhanced", False):
        def patched_process_new_callback_query(self, new_callback_queries):
            remaining = []
            for call in list(new_callback_queries or []):
                try:
                    handled = _dispatch_tg_callback(self, call)
                except Exception as e:
                    _log(f"TG回调分发异常: {e}")
                    handled = False
                if not handled:
                    remaining.append(call)

            if remaining:
                return original_process_new_callback_query(self, remaining)
            return None

        patched_process_new_callback_query._cms_enhanced = True
        telebot.TeleBot.process_new_callback_query = patched_process_new_callback_query

    def _normalize_tg_menu_command(value):
        text = str(value or "").strip().lstrip("/").lower()
        text = re.sub(r"[^a-z0-9_]", "", text)
        if not re.fullmatch(r"[a-z0-9_]{1,32}", text):
            return ""
        return text

    def _sanitize_tg_bot_commands(items):
        sanitized = []
        seen = set()
        for item in list(items or []):
            command = ""
            description = ""
            if isinstance(item, dict):
                command = item.get("command") or item.get("cmd") or ""
                description = item.get("description") or item.get("desc") or ""
            else:
                command = getattr(item, "command", "") or ""
                description = getattr(item, "description", "") or ""

            normalized = _normalize_tg_menu_command(command)
            if not normalized or normalized in seen:
                continue
            if normalized.startswith("wx_"):
                continue
            seen.add(normalized)
            description = str(description or normalized).strip() or normalized
            sanitized.append(telebot_types.BotCommand(normalized, description[:256]))
        return sanitized

    original_set_my_commands = getattr(telebot.TeleBot, "set_my_commands", None)
    if callable(original_set_my_commands) and not getattr(original_set_my_commands, "_cms_enhanced", False):
        def patched_set_my_commands(self, commands, *args, **kwargs):
            sanitized_commands = _sanitize_tg_bot_commands(commands)
            try:
                _write_json_debug(
                    "/tmp/cms_tg_menu_commands.json",
                    {
                        "raw": [
                            {
                                "command": getattr(item, "command", None) if not isinstance(item, dict) else item.get("command") or item.get("cmd"),
                                "description": getattr(item, "description", None) if not isinstance(item, dict) else item.get("description") or item.get("desc"),
                            }
                            for item in list(commands or [])
                        ],
                        "sanitized": [
                            {"command": item.command, "description": item.description}
                            for item in sanitized_commands
                        ],
                    },
                )
            except Exception:
                pass
            return original_set_my_commands(self, sanitized_commands, *args, **kwargs)

        patched_set_my_commands._cms_enhanced = True
        telebot.TeleBot.set_my_commands = patched_set_my_commands

    original_api_set_my_commands = getattr(telebot_apihelper, "set_my_commands", None)
    if callable(original_api_set_my_commands) and not getattr(original_api_set_my_commands, "_cms_enhanced", False):
        def patched_api_set_my_commands(token, commands, *args, **kwargs):
            sanitized_commands = _sanitize_tg_bot_commands(commands)
            try:
                _write_json_debug(
                    "/tmp/cms_tg_menu_commands.json",
                    {
                        "raw": [
                            {
                                "command": getattr(item, "command", None) if not isinstance(item, dict) else item.get("command") or item.get("cmd"),
                                "description": getattr(item, "description", None) if not isinstance(item, dict) else item.get("description") or item.get("desc"),
                            }
                            for item in list(commands or [])
                        ],
                        "sanitized": [
                            {"command": item.command, "description": item.description}
                            for item in sanitized_commands
                        ],
                    },
                )
            except Exception:
                pass
            return original_api_set_my_commands(token, sanitized_commands, *args, **kwargs)

        patched_api_set_my_commands._cms_enhanced = True
        telebot_apihelper.set_my_commands = patched_api_set_my_commands


def _write_json_debug(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        pass


def _normalize_wx_text(text):
    return re.sub(r"\s+", "", str(text or "").strip()).lower()


def _is_plugin_menu_label(text):
    return _normalize_wx_text(text) in {"插件", "插件菜单", "plugin", "plugins"}


def _is_subscription_menu_label(text):
    normalized = _normalize_wx_text(text)
    return normalized in {
        _normalize_wx_text(WX_LIST_SUBSCRIPTIONS_MENU_TITLE),
        "当前订阅",
        "查看订阅",
        "订阅列表",
        "我的订阅",
    }


def _extract_wechat_instance_from_values(values):
    for value in values:
        try:
            if hasattr(value, "send_msg"):
                return value
            nested = getattr(value, "wechat", None)
            if nested is not None and hasattr(nested, "send_msg"):
                return nested
        except Exception:
            continue
    return None


def _is_internal_wx_list_subscriptions_command(text):
    normalized = _normalize_wx_text(text)
    return normalized in {
        _normalize_wx_text(WX_LIST_SUBSCRIPTIONS_COMMAND),
        _normalize_wx_text(WX_LIST_SUBSCRIPTIONS_COMMAND.lstrip("/")),
        _normalize_wx_text(LEGACY_LIST_SUBSCRIPTIONS_COMMAND),
        _normalize_wx_text(LEGACY_LIST_SUBSCRIPTIONS_COMMAND.lstrip("/")),
    }


def _build_wx_click_menu_item(title, key=None, template=None):
    item = {
        "type": "click",
        "name": str(title or "").strip(),
        "key": str(key or title or "").strip(),
    }
    if isinstance(template, dict):
        # 只沿用常见字段，不复制原有 key/url，避免菜单点击走偏。
        for field in ["type", "name", "key", "url", "appid", "pagepath"]:
            if field in template and field not in item:
                item[field] = template.get(field)
        item["type"] = "click"
        item["name"] = str(title or "").strip()
        item["key"] = str(key or title or "").strip()
        item.pop("url", None)
        item.pop("appid", None)
        item.pop("pagepath", None)
    return item


def _build_wx_subscription_menu_item(template=None):
    return _build_wx_click_menu_item(
        WX_LIST_SUBSCRIPTIONS_MENU_TITLE,
        key=WX_LIST_SUBSCRIPTIONS_COMMAND,
        template=template,
    )


def _build_wx_douban_hot_movie_menu_item(template=None):
    return _build_wx_click_menu_item(
        WX_DOUBAN_HOT_MOVIE_MENU_TITLE,
        key=WX_DOUBAN_HOT_MOVIE_COMMAND,
        template=template,
    )


def _build_wx_douban_hot_tv_menu_item(template=None):
    return _build_wx_click_menu_item(
        WX_DOUBAN_HOT_TV_MENU_TITLE,
        key=WX_DOUBAN_HOT_TV_COMMAND,
        template=template,
    )


def _inject_wx_subscription_menu(commands):
    injected = False
    sample_entry = None
    preferred_children = None
    fallback_children = None
    top_level_buttons = None

    def _menu_item_meta(item):
        if not isinstance(item, dict):
            return "", ""
        label = (
            item.get("name")
            or item.get("text")
            or item.get("title")
            or item.get("label")
        )
        key_value = item.get("key") or item.get("command") or item.get("cmd")
        return _normalize_wx_text(label), _normalize_wx_text(key_value)

    def _desired_menu_items(template=None):
        return [
            _build_wx_subscription_menu_item(template),
            _build_wx_douban_hot_movie_menu_item(template),
            _build_wx_douban_hot_tv_menu_item(template),
        ]

    def _inject_into_children(children, template=None, limit=5):
        existing_labels = set()
        existing_keys = set()
        for child in children:
            item_label, item_key = _menu_item_meta(child)
            if item_label:
                existing_labels.add(item_label)
            if item_key:
                existing_keys.add(item_key)

        changed = False
        for item in _desired_menu_items(template):
            item_label, item_key = _menu_item_meta(item)
            if item_label in existing_labels or item_key in existing_keys:
                continue
            if limit and len(children) >= limit:
                break
            children.append(item)
            changed = True
            if item_label:
                existing_labels.add(item_label)
            if item_key:
                existing_keys.add(item_key)
        return changed

    def _children_contain_target_items(children):
        desired_labels = {
            _normalize_wx_text(WX_LIST_SUBSCRIPTIONS_MENU_TITLE),
            _normalize_wx_text(WX_DOUBAN_HOT_MOVIE_MENU_TITLE),
            _normalize_wx_text(WX_DOUBAN_HOT_TV_MENU_TITLE),
        }
        desired_keys = {
            _normalize_wx_text(WX_LIST_SUBSCRIPTIONS_COMMAND),
            _normalize_wx_text(WX_DOUBAN_HOT_MOVIE_COMMAND),
            _normalize_wx_text(WX_DOUBAN_HOT_TV_COMMAND),
        }
        for child in children:
            item_label, item_key = _menu_item_meta(child)
            if item_label in desired_labels or item_key in desired_keys:
                return True
        return False

    def _build_top_level_menu(template=None):
        submenu_key = "sub_button"
        entry = {"name": "插件"}
        if isinstance(template, dict):
            for candidate_key in ["sub_button", "subButton", "children", "items", "buttons", "submenu"]:
                if isinstance(template.get(candidate_key), list):
                    submenu_key = candidate_key
                    break
            if "name" not in entry and any(key in template for key in ["text", "title", "label"]):
                entry["name"] = "插件"
        entry[submenu_key] = _desired_menu_items(None)
        return entry

    def walk(node):
        nonlocal injected, sample_entry, preferred_children, fallback_children, top_level_buttons
        if injected:
            return
        if isinstance(node, dict):
            dict_values = [value for value in node.values() if isinstance(value, dict)]
            if sample_entry is None and dict_values:
                candidate = dict_values[0]
                if any(key in candidate for key in ["group", "menu", "category", "command", "cmd", "key"]):
                    sample_entry = candidate

            label = (
                node.get("name")
                or node.get("text")
                or node.get("title")
                or node.get("label")
            )
            for child_key in ["button", "buttons", "sub_button", "subButton", "children", "items", "submenu"]:
                children = node.get(child_key)
                if not isinstance(children, list):
                    continue
                if child_key in {"button", "buttons"} and top_level_buttons is None:
                    top_level_buttons = children
                template = next((child for child in children if isinstance(child, dict)), None)
                if child_key not in {"button", "buttons"} and _children_contain_target_items(children) and preferred_children is None:
                    preferred_children = (children, template)
                elif child_key not in {"button", "buttons"} and fallback_children is None and len(children) < 5:
                    fallback_children = (children, template)
                if _is_plugin_menu_label(label):
                    _inject_into_children(children, template=template, limit=5)
                    injected = True
                    return
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            if top_level_buttons is None and all(isinstance(item, dict) for item in node):
                top_level_buttons = node
            for item in node:
                walk(item)

    walk(commands)

    if not injected and preferred_children:
        children, template = preferred_children
        if _inject_into_children(children, template=template, limit=5):
            injected = True

    if not injected and fallback_children:
        children, template = fallback_children
        if _inject_into_children(children, template=template, limit=5):
            injected = True

    if not injected and isinstance(top_level_buttons, list) and len(top_level_buttons) < 3:
        template = next((item for item in top_level_buttons if isinstance(item, dict)), None)
        top_level_buttons.append(_build_top_level_menu(template))
        injected = True

    if not injected and isinstance(commands, dict):
        sample = sample_entry or next((value for value in commands.values() if isinstance(value, dict)), None)
        if isinstance(sample, dict) and any(key in sample for key in ["group", "menu", "category", "command", "cmd", "key"]):
            entry = {}
            if "group" in sample:
                entry["group"] = "插件"
            if "menu" in sample:
                entry["menu"] = "插件"
            if "category" in sample:
                entry["category"] = "插件"
            def build_entry(title, command, func):
                entry = {}
                if "group" in sample:
                    entry["group"] = "插件"
                if "menu" in sample:
                    entry["menu"] = "插件"
                if "category" in sample:
                    entry["category"] = "插件"
                if "description" in sample:
                    entry["description"] = title
                if "name" in sample:
                    entry["name"] = title
                elif "title" in sample:
                    entry["title"] = title
                elif "text" in sample:
                    entry["text"] = title
                elif "description" not in entry:
                    entry["name"] = title
                if "desc" in sample:
                    entry["desc"] = title
                if "func" in sample:
                    entry["func"] = func
                if "data" in sample:
                    entry["data"] = {}
                if "command" in sample:
                    entry["command"] = command
                elif "cmd" in sample:
                    entry["cmd"] = command
                elif "key" in sample:
                    entry["key"] = command
                else:
                    entry["command"] = command
                return entry

            commands[WX_LIST_SUBSCRIPTIONS_COMMAND] = build_entry(
                WX_LIST_SUBSCRIPTIONS_MENU_TITLE,
                WX_LIST_SUBSCRIPTIONS_COMMAND,
                _wx_list_subscriptions_command,
            )
            commands[WX_DOUBAN_HOT_MOVIE_COMMAND] = build_entry(
                WX_DOUBAN_HOT_MOVIE_MENU_TITLE,
                WX_DOUBAN_HOT_MOVIE_COMMAND,
                _wx_douban_hot_movie_command,
            )
            commands[WX_DOUBAN_HOT_TV_COMMAND] = build_entry(
                WX_DOUBAN_HOT_TV_MENU_TITLE,
                WX_DOUBAN_HOT_TV_COMMAND,
                _wx_douban_hot_tv_command,
            )
            injected = True

    return commands, injected


def _wx_list_subscriptions_command(*args, **kwargs):
    wechat_instance = _extract_wechat_instance_from_values(list(args) + list(kwargs.values()))
    _handle_wx_list_subscriptions(wechat_instance=wechat_instance)
    return True


def _wx_douban_hot_movie_command(*args, **kwargs):
    wechat_instance = _extract_wechat_instance_from_values(list(args) + list(kwargs.values()))
    _handle_wx_douban_hot("movie", wechat_instance=wechat_instance)
    return True


def _wx_douban_hot_tv_command(*args, **kwargs):
    wechat_instance = _extract_wechat_instance_from_values(list(args) + list(kwargs.values()))
    _handle_wx_douban_hot("tv", wechat_instance=wechat_instance)
    return True


def _patch_wechat_runtime(WechatModule):
    try:
        original_register_commands = getattr(WechatModule, "register_commands", None)
        if callable(original_register_commands) and not getattr(original_register_commands, "_cms_enhanced", False):
            def wrapped_register_commands(self, commands):
                _state["wechat_instance"] = self
                try:
                    _write_json_debug("/tmp/cms_wechat_register_commands_before.json", commands)
                except Exception:
                    pass

                patched_commands = commands
                injected = False
                try:
                    patched_commands, injected = _inject_wx_subscription_menu(commands)
                except Exception:
                    pass

                try:
                    _write_json_debug("/tmp/cms_wechat_register_commands_after.json", patched_commands)
                except Exception:
                    pass
                return original_register_commands(self, patched_commands)

            wrapped_register_commands._cms_enhanced = True
            WechatModule.register_commands = wrapped_register_commands
    except Exception:
        pass


# ============================================================
# 企微消息处理
# ============================================================
def _handle_wx_search(content, wechat_instance=None):
    if wechat_instance and not _state.get("wechat_instance"):
        _state["wechat_instance"] = wechat_instance

    wx_action = _get_wx_action_context()
    if wx_action == "subscribe_candidates":
        page_direction = _parse_wx_page_command(content)
        if page_direction:
            _handle_wx_subscribe_candidate_page_command(page_direction, wechat_instance=wechat_instance)
            return
        index = _parse_subscription_indices(content)
        if index:
            _handle_wx_subscribe_candidate_index(index, wechat_instance=wechat_instance)
            return
    if wx_action == "hdhive_candidates":
        page_direction = _parse_wx_hdhive_candidate_page_command(content)
        if page_direction:
            _handle_wx_hdhive_candidate_page_command(page_direction, wechat_instance=wechat_instance)
            return
        index = _parse_subscription_indices(content)
        if index:
            _handle_wx_hdhive_candidate_index(index, wechat_instance=wechat_instance)
            return
    if wx_action == "hdhive":
        page_direction = _parse_wx_page_command(content)
        if page_direction:
            _handle_wx_hdhive_page_command(page_direction, wechat_instance=wechat_instance)
            return
    if wx_action == "subscriptions":
        index = _parse_subscription_indices(content)
        if index:
            _handle_wx_unsubscribe_index(index, wechat_instance=wechat_instance)
            return

    # 纯数字：根据最近一次企微上下文决定是删除订阅还是解锁付费资源
    if content.strip().isdigit():
        index = int(content.strip())
        if wx_action == "subscribe_candidates":
            _handle_wx_subscribe_candidate_index(index, wechat_instance=wechat_instance)
            return
        if wx_action == "hdhive_candidates":
            _handle_wx_hdhive_candidate_index(index, wechat_instance=wechat_instance)
            return
        if wx_action == "subscriptions":
            _handle_wx_unsubscribe_index(index, wechat_instance=wechat_instance)
            return
        if wx_action == "hdhive":
            if not _has_hdhive():
                _send_wx_message("❌ 当前未配置影巢，无法解锁资源")
                return

            def do_unlock():
                result = _unlock_hdhive_resource(index)
                _send_wx_message(result)

            threading.Thread(target=do_unlock, daemon=True).start()
            return
        return

    unsubscribe_index = _is_unsubscribe_index_command(content)
    if unsubscribe_index:
        _handle_wx_unsubscribe_index(unsubscribe_index, wechat_instance=wechat_instance)
        return

    # 订阅列表
    if _is_list_subscriptions_command(content):
        _handle_wx_list_subscriptions(wechat_instance=wechat_instance)
        return

    # 删除已完成
    if _is_delete_completed_command(content):
        _handle_wx_delete_completed(wechat_instance=wechat_instance)
        return

    if _is_update_file_command(content):
        _handle_wx_update_file(wechat_instance=wechat_instance)
        return

    douban_hot_type = _is_douban_hot_command(content)
    if douban_hot_type:
        _handle_wx_douban_hot(douban_hot_type, wechat_instance=wechat_instance)
        return

    # 订阅
    subscribe_command = _is_subscribe_command(content)
    if subscribe_command:
        _handle_wx_subscribe(subscribe_command, wechat_instance=wechat_instance)
        return

    # 退订
    unsubscribe_command = _is_unsubscribe_command(content)
    if unsubscribe_command:
        _handle_wx_unsubscribe(unsubscribe_command, wechat_instance=wechat_instance)
        return

    # 搜索
    result = _is_search_query(content)
    if not result:
        keyword = _is_wx_direct_search_text(content)
        if not keyword:
            return
        source = "hdhive"
    else:
        keyword, source = result

    def do_search():
        _set_wx_action_context("")
        with _search_cache_lock:
            _clear_wx_hdhive_results(reset_action=False)
            _search_cache.pop("wx_hdhive_candidates", None)
        if source == "hdhive":
            if not _has_hdhive():
                _send_wx_message("❌ 当前未配置影巢搜索")
                return
            candidates = _build_hdhive_search_candidate_items(keyword)
            if not candidates:
                _send_wx_message(f"❌ 未找到《{keyword}》对应的影视条目")
                return
            if len(candidates) > 1:
                _cache_wx_hdhive_candidates(keyword, candidates)
                _reply_wx_hdhive_search_candidates(keyword, candidates)
                return

            _reply_wx_selected_candidate_results(candidates[0], fallback_keyword=keyword)
        else:
            if not _has_panso():
                _send_wx_message("❌ 当前未配置盘搜搜索")
                return
            panso_data = _search_panso(keyword)
            wx_text = _format_results_wx(keyword, {"merged_by_type": panso_data.get("merged_by_type", {})}, "盘搜")
            _send_wx_message(wx_text or f"🔍 {keyword}（盘搜）\n\n暂无搜索结果")
    threading.Thread(target=do_search, daemon=True).start()


# ============================================================
# 企微日志监听（监控 CMS 日志捕获消息内容）
# ============================================================
class _WxLogHandler(logging.Handler):
    _pattern = re.compile(r"收到消息-wx-用户：.*?，内容：(.+)")

    def emit(self, record):
        try:
            msg = record.getMessage()
            m = self._pattern.search(msg)
            if m:
                content = m.group(1).strip()
                if (
                    _is_search_query(content)
                    or _is_wx_direct_search_text(content)
                    or _is_save_command(content)
                    or _is_douban_hot_command(content)
                    or _is_update_file_command(content)
                    or _is_subscribe_command(content)
                    or _is_unsubscribe_index_command(content)
                    or (_get_wx_action_context() == "subscribe_candidates" and _parse_wx_page_command(content))
                    or (_get_wx_action_context() == "subscribe_candidates" and _parse_subscription_indices(content))
                    or (_get_wx_action_context() == "hdhive_candidates" and _parse_wx_hdhive_candidate_page_command(content))
                    or (_get_wx_action_context() == "hdhive_candidates" and _parse_subscription_indices(content))
                    or (_get_wx_action_context() == "hdhive" and _parse_wx_page_command(content))
                    or (_get_wx_action_context() == "hdhive" and content.strip().isdigit())
                    or (_get_wx_action_context() == "subscriptions" and _parse_subscription_indices(content))
                    or _is_unsubscribe_command(content)
                    or _is_delete_completed_command(content)
                    or (
                        _is_list_subscriptions_command(content)
                        and not _is_internal_wx_list_subscriptions_command(content)
                    )
                ):
                    def delayed_dispatch():
                        time.sleep(0.6)
                        if not _reserve_command_dispatch("wx", None, content):
                            return
                        _handle_wx_search(content)

                    threading.Thread(target=delayed_dispatch, daemon=True).start()
        except Exception:
            pass


# ============================================================
# TG Bot Hook
# ============================================================
def _patch_tg_bot(tg_bot_module):
    global _tg_bot_patch_started
    if not _has_any_search_source():
        return
    with _inject_patch_lock:
        if _tg_bot_patch_started:
            return
        _tg_bot_patch_started = True

    def _do_patch():
        _patch_telebot_runtime()
        time.sleep(5)
        bot = None

        def _register_callback_handler(handler_func, predicate):
            if not hasattr(bot, "callback_query_handlers"):
                return False
            try:
                bot.callback_query_handlers.insert(0, {
                    "function": handler_func,
                    "filters": {"func": predicate},
                })
                return True
            except Exception:
                return False

        for attr_name in ["bot", "_bot", "tg_bot"]:
            bot = getattr(tg_bot_module, attr_name, None)
            if bot is not None:
                break

        if bot is None:
            try:
                from app.core import obj_factory
                for attr_name in ["bot", "tg_bot"]:
                    bot = getattr(obj_factory, attr_name, None)
                    if bot is not None:
                        break
            except Exception:
                pass

        if bot is None:
            try:
                import gc, telebot
                for obj in gc.get_objects():
                    try:
                        if isinstance(obj, telebot.TeleBot):
                            bot = obj
                            break
                    except Exception:
                        pass
            except Exception:
                pass

        if bot is None:
            time.sleep(10)
            try:
                import gc, telebot
                for obj in gc.get_objects():
                    try:
                        if isinstance(obj, telebot.TeleBot):
                            bot = obj
                            break
                    except Exception:
                        pass
            except Exception:
                pass

        if bot is None:
            return

        _state["tg_bot"] = bot

        def tg_text_command_handler(message):
            text = str(getattr(message, "text", "") or "").strip()
            chat_id = getattr(getattr(message, "chat", None), "id", None)
            if not text:
                return
            if not _reserve_command_dispatch("tg", chat_id, text):
                return
            try:
                _handle_tg_text_command(text, chat_id=chat_id, bot=bot)
            except Exception as e:
                try:
                    bot.send_message(chat_id, f"❌ 命令处理异常: {e}")
                except Exception:
                    pass

        handler_dict = {
            "function": tg_text_command_handler,
            "filters": {
                "content_types": ["text"],
                "func": lambda msg: bool(
                    getattr(msg, "text", None)
                    and (
                        _is_search_query(msg.text)
                        or _is_wx_direct_search_text(msg.text)
                        or _is_list_subscriptions_command(msg.text)
                        or _is_delete_completed_command(msg.text)
                        or _is_update_file_command(msg.text)
                        or _is_subscribe_command(msg.text)
                        or _is_unsubscribe_command(msg.text)
                        or _is_unsubscribe_index_command(msg.text)
                        or (
                            _parse_subscription_indices(msg.text)
                            and _get_tg_action_context(getattr(getattr(msg, "chat", None), "id", None)) == "subscribe_candidates"
                        )
                        or (
                            _parse_subscription_indices(msg.text)
                            and _get_tg_action_context(getattr(getattr(msg, "chat", None), "id", None)) == "hdhive_candidates"
                        )
                        or (
                            _parse_subscription_indices(msg.text)
                            and _get_tg_action_context(getattr(getattr(msg, "chat", None), "id", None)) == "subscriptions"
                        )
                    )
                ),
            },
        }
        if hasattr(bot, "message_handlers"):
            bot.message_handlers.insert(0, {
                "function": tg_text_command_handler,
                "filters": {
                    "content_types": ["text"],
                    "commands": [
                        WX_LIST_SUBSCRIPTIONS_COMMAND.lstrip("/"),
                        LEGACY_LIST_SUBSCRIPTIONS_COMMAND.lstrip("/"),
                    ],
                },
            })
            bot.message_handlers.insert(0, handler_dict)

        # 详情查看回调 (ps_type / hd_type / gy_type)
        def tg_subscribe_candidate_handler(call):
            return _handle_tg_subscribe_callback(bot, call)

        _register_callback_handler(
            tg_subscribe_candidate_handler,
            lambda call: call.data and call.data.startswith("tg_sub:"),
        )

        def tg_hdhive_candidate_handler(call):
            return _handle_tg_hdhive_candidate_callback(bot, call)

        _register_callback_handler(
            tg_hdhive_candidate_handler,
            lambda call: call.data and call.data.startswith("tg_hd:"),
        )

        # 详情查看回调 (ps_type / hd_type / gy_type)
        def panso_type_detail_handler(call):
            return _handle_tg_detail_callback(bot, call)

        _register_callback_handler(
            panso_type_detail_handler,
            lambda call: call.data and (
                call.data.startswith("ps_type:")
                or call.data.startswith("hd_type:")
                or call.data.startswith("hds_type:")
                or call.data.startswith("gy_type:")
                or call.data.startswith("tgsel:")
            ),
        )

        # HDHive 解锁回调
        # hd_u:slug → 弹出确认消息
        # hd_ok:slug → 确认解锁
        def hdhive_unlock_handler(call):
            return _handle_tg_hdhive_unlock_callback(bot, call)

        def hdhive_confirm_handler(call):
            return _handle_tg_hdhive_confirm_callback(bot, call)

        def hdhive_cancel_handler(call):
            return _handle_tg_hdhive_cancel_callback(bot, call)

        if _has_hdhive():
            for handler_info in [
                (hdhive_unlock_handler, lambda call: call.data and call.data.startswith("hd_u:")),
                (hdhive_confirm_handler, lambda call: call.data and call.data.startswith("hd_ok:")),
                (hdhive_cancel_handler, lambda call: call.data == "hd_cancel"),
            ]:
                _register_callback_handler(handler_info[0], handler_info[1])

        _log("TG机器人增强已注入")

    threading.Thread(target=_do_patch, daemon=True).start()


# ============================================================
# event_service Hook（保存 WechatModule 引用）
# ============================================================
def _patch_event_service(event_module):
    global _event_service_patch_started
    if not _has_any_search_source():
        return
    with _inject_patch_lock:
        if _event_service_patch_started:
            return
        _event_service_patch_started = True

    try:
        WechatModule = getattr(event_module, "WechatModule", None)
        if WechatModule:
            _state["WechatModule"] = WechatModule
            _patch_wechat_runtime(WechatModule)
    except Exception:
        pass

    def _do_patch():
        time.sleep(3)

        # Hook EventService 的所有方法（之前验证过能触发企微搜索）
        EventService = getattr(event_module, "EventService", None)
        if not EventService:
            return

        for method_name in dir(EventService):
            if method_name.startswith("__"):
                continue
            try:
                original_method = getattr(EventService, method_name)
                if not callable(original_method) or isinstance(original_method, type):
                    continue

                def make_wrapper(orig, mname):
                    def wrapper(self_or_cls, *args, **kwargs):
                        result = orig(self_or_cls, *args, **kwargs)
                        try:
                            context = _extract_event_message_context(args, kwargs)
                            text = context.get("text")
                            if not text:
                                return result

                            source = context.get("source")
                            chat_id = context.get("chat_id")
                            is_tg = source == "tg" or (chat_id is not None and source != "wx")
                            dispatch_channel = "tg" if is_tg else "wx"
                            dispatch_target = chat_id if is_tg else None

                            if not _reserve_command_dispatch(dispatch_channel, dispatch_target, text):
                                return result

                            if is_tg:
                                _handle_tg_text_command(text, chat_id=chat_id)
                            else:
                                _handle_wx_search(text, wechat_instance=context.get("wechat_instance"))
                        except Exception:
                            pass
                        return result
                    wrapper.__name__ = mname
                    return wrapper

                setattr(EventService, method_name, make_wrapper(original_method, method_name))
            except Exception:
                pass

        _log("企微消息增强已注入")
    threading.Thread(target=_do_patch, daemon=True).start()


def _patch_share_down_service(share_down_module):
    global _sub_sync_patched
    if not _has_any_search_source() or _sub_sync_patched:
        return

    try:
        ShareDownService = getattr(share_down_module, "ShareDownService", None)
        if not ShareDownService or not hasattr(ShareDownService, "sync_sub"):
            return

        from app.db.sub_op import SubSourceOp
        from app.db.models.subsource import SubSource
        from app.modules.tg.tg_scraper import TelegramScraper, ResourceInfo

        original_sync_sub = getattr(ShareDownService, "sync_sub", None)
        original_list = getattr(SubSourceOp, "list", None)
        original_search = getattr(TelegramScraper, "search", None)
        if not callable(original_sync_sub) or not callable(original_list) or not callable(original_search):
            return

        def patched_sync_sub(self, *args, **kwargs):
            _sub_sync_local.active = True
            try:
                return original_sync_sub(self, *args, **kwargs)
            finally:
                _sub_sync_local.active = False
                _reconcile_submedia_completion_later(delay=20)

        def patched_list(self, *args, **kwargs):
            sources = original_list(self, *args, **kwargs)
            if not _subscription_sync_active():
                return sources
            try:
                items = list(sources or [])
                existing_urls = {getattr(item, "sub_url", "") for item in items}
                for virtual in _subscription_virtual_sub_sources(SubSource):
                    if virtual.sub_url not in existing_urls:
                        items.append(virtual)
                return items
            except Exception:
                return sources

        def patched_search(self, q=None, after=None):
            source_key = SUB_SYNC_CHANNEL_TO_SOURCE.get(getattr(self, "channel", ""))
            if not source_key:
                return original_search(self, q, after)
            source_name = getattr(self, "name", None) or SUB_SYNC_SOURCE_NAMES.get(source_key, source_key)
            return _subscription_search_virtual_source(source_key, source_name, q or "", self, ResourceInfo)

        ShareDownService.sync_sub = patched_sync_sub
        SubSourceOp.list = patched_list
        TelegramScraper.search = patched_search
        _sub_sync_patched = True
        _log("订阅搜源增强已注入")
    except Exception:
        pass


def _patch_wechat_module(wechat_module):
    try:
        WechatModule = getattr(wechat_module, "WechatModule", None)
        if WechatModule is not None:
            original_register_commands = getattr(WechatModule, "register_commands", None)
            if callable(original_register_commands) and not getattr(original_register_commands, "_cms_enhanced", False):
                def wrapped_register_commands(self, commands):
                    _state["wechat_instance"] = self
                    try:
                        _write_json_debug("/tmp/cms_wechat_register_commands_before.json", commands)
                    except Exception:
                        pass
                    patched_commands = commands
                    try:
                        patched_commands, _ = _inject_wx_subscription_menu(commands)
                    except Exception:
                        patched_commands = commands
                    try:
                        _write_json_debug("/tmp/cms_wechat_register_commands_after.json", patched_commands)
                    except Exception:
                        pass
                    return original_register_commands(self, patched_commands)

                wrapped_register_commands._cms_enhanced = True
                WechatModule.register_commands = wrapped_register_commands

        WeChat = getattr(wechat_module, "WeChat", None)
        if WeChat and hasattr(WeChat, "create_menus"):
            original_create_menus = getattr(WeChat, "create_menus", None)
            if callable(original_create_menus) and not getattr(original_create_menus, "_cms_enhanced", False):
                def wrapped_create_menus(self, commands):
                    _state["wechat_instance"] = self
                    try:
                        serialized = json.dumps(commands, ensure_ascii=False)
                        try:
                            with open("/tmp/cms_wechat_menu_commands.json", "w", encoding="utf-8") as f:
                                f.write(serialized)
                        except Exception:
                            pass
                    except Exception:
                        pass

                    patched_commands = commands
                    try:
                        patched_commands, _ = _inject_wx_subscription_menu(commands)
                    except Exception:
                        patched_commands = commands

                    try:
                        patched_serialized = json.dumps(patched_commands, ensure_ascii=False)
                        try:
                            with open("/tmp/cms_wechat_menu_commands_patched.json", "w", encoding="utf-8") as f:
                                f.write(patched_serialized)
                        except Exception:
                            pass
                    except Exception:
                        pass
                    return original_create_menus(self, patched_commands)

                wrapped_create_menus._cms_enhanced = True
                WeChat.create_menus = wrapped_create_menus
    except Exception:
        pass


def _late_patch_wechat_module():
    if not _has_any_search_source():
        return

    def _do_patch():
        for _ in range(80):
            modules = []
            try:
                for name, mod in list(sys.modules.items()):
                    if name == "app.modules.wechat" or name == "app.modules.wechat.wechat" or name.endswith(".wechat"):
                        modules.append(mod)
            except Exception:
                modules = []

            patched = False
            for module in modules:
                if module is None:
                    continue
                _patch_wechat_module(module)
                patched = True
            if patched:
                return
            time.sleep(0.25)
    threading.Thread(target=_do_patch, daemon=True).start()


def _late_patch_event_service():
    if not _has_any_search_source():
        return

    def _do_patch():
        for _ in range(80):
            module = None
            try:
                module = sys.modules.get("app.services.event_service")
            except Exception:
                module = None
            if module is not None:
                _patch_event_service(module)
                return
            time.sleep(0.25)
    threading.Thread(target=_do_patch, daemon=True).start()


def _late_patch_tg_bot_module():
    if not _has_any_search_source():
        return

    def _do_patch():
        _patch_telebot_runtime()
        for _ in range(80):
            modules = []
            try:
                for name, mod in list(sys.modules.items()):
                    if name == "app.modules.tg.tg_bot" or name.endswith(".tg_bot"):
                        modules.append(mod)
            except Exception:
                modules = []
            if modules:
                for module in modules:
                    if module is not None:
                        _patch_tg_bot(module)
                return
            time.sleep(0.25)
    threading.Thread(target=_do_patch, daemon=True).start()


def _late_patch_share_down_service():
    if not _has_any_search_source():
        return

    def _do_patch():
        time.sleep(5)
        try:
            module = sys.modules.get("app.services.share_down")
            if module is None:
                module = _original_import("app.services.share_down", fromlist=["ShareDownService"])
            _patch_share_down_service(module)
        except Exception:
            pass

    threading.Thread(target=_do_patch, daemon=True).start()


# ============================================================
# ASGI 中间件（Panso API 代理）
# ============================================================
def _patch_flask():
    if not _has_any_search_source():
        return

    def _do_patch():
        try:
            import uvicorn
            _original_run = uvicorn.run

            def patched_uvicorn_run(app, *args, **kwargs):
                wrapped = _make_panso_middleware(app)
                return _original_run(wrapped, *args, **kwargs)

            uvicorn.run = patched_uvicorn_run
        except Exception:
            pass

    threading.Thread(target=_do_patch, daemon=True).start()


def _make_panso_middleware(original_app):
    async def _send_json(send, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        await send({"type": "http.response.start", "status": status,
                     "headers": [[b"content-type", b"application/json; charset=utf-8"],
                                  [b"access-control-allow-origin", b"*"]]})
        await send({"type": "http.response.body", "body": body})

    async def _read_body(receive):
        parts = []
        while True:
            msg = await receive()
            parts.append(msg.get("body", b""))
            if not msg.get("more_body", False):
                break
        return b"".join(parts)

    def _build_body_receive(body_bytes):
        sent = False

        async def _receive():
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        return _receive

    async def panso_middleware(scope, receive, send):
        if scope["type"] != "http":
            await original_app(scope, receive, send)
            return

        path = scope["path"]

        # 盘搜搜索代理
        if path == "/api/panso/search":
            if not _has_panso():
                await _send_json(send, {"code": 200, "msg": "盘搜未配置", "data": {"merged_by_type": {}, "total": 0}})
                return
            raw_body = await _read_body(receive)
            try:
                params = json.loads(raw_body) if raw_body else {}
            except Exception:
                params = {}
            keyword = _compact_text(params.get("kw") or params.get("keyword") or "")
            cloud_types = params.get("cloud_types")
            if isinstance(cloud_types, str):
                cloud_types = [cloud_types]
            if not keyword:
                await _send_json(send, {"success": True, "data": {"merged_by_type": {}, "total": 0}})
                return
            try:
                import asyncio

                loop = asyncio.get_event_loop()
                panso_data = await loop.run_in_executor(None, _search_panso, keyword, cloud_types)
                await _send_json(send, {
                    "success": True,
                    "data": {
                        "merged_by_type": panso_data.get("merged_by_type", {}),
                        "total": panso_data.get("total", 0),
                    },
                })
            except Exception as e:
                await _send_json(send, {"code": 500, "msg": str(e)}, 500)
            return

        # 影巢搜索
        if path == "/api/hdhive/search":
            raw_body = await _read_body(receive)
            try:
                params = json.loads(raw_body) if raw_body else {}
            except Exception:
                params = {}
            keyword = params.get("keyword", "")
            if not keyword or not hdhive_client.available:
                await _send_json(send, {"success": True, "data": []})
                return
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                resources = await loop.run_in_executor(None, _search_hdhive_raw, keyword)
                decorated_resources = [
                    _decorate_hdhive_item_for_web(item)
                    for item in (resources or [])
                ]
                await _send_json(send, {"success": True, "data": decorated_resources})
            except Exception as e:
                await _send_json(send, {"success": False, "message": str(e)}, 500)
            return

        # 观影搜索
        if path == "/api/gying/search":
            raw_body = await _read_body(receive)
            try:
                params = json.loads(raw_body) if raw_body else {}
            except Exception:
                params = {}
            keyword = params.get("keyword", "")
            max_results = params.get("max_results", 20)
            if not keyword or not _has_gying():
                await _send_json(send, {"success": True, "data": {"merged_by_type": {}, "total": 0}})
                return
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                gying_data = await loop.run_in_executor(None, _search_gying, keyword, max_results)
                await _send_json(send, {
                    "success": gying_data.get("success", True),
                    "message": gying_data.get("message", ""),
                    "data": {
                        "merged_by_type": gying_data.get("merged_by_type", {}),
                        "total": gying_data.get("total", 0),
                    },
                })
            except Exception as e:
                await _send_json(send, {"success": False, "message": str(e)}, 500)
            return

        # 影巢解锁
        if path == "/api/hdhive/unlock":
            if not _has_hdhive():
                await _send_json(send, {"success": False, "message": "影巢未配置"}, 400)
                return
            raw_body = await _read_body(receive)
            try:
                params = json.loads(raw_body) if raw_body else {}
            except Exception:
                params = {}
            slug = params.get("slug", "")
            if not slug:
                await _send_json(send, {"success": False, "message": "缺少slug"}, 400)
                return
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, hdhive_client.unlock, slug)
                await _send_json(send, result)
            except Exception as e:
                await _send_json(send, {"success": False, "message": str(e)}, 500)
            return

        # 转存
        if path == "/api/save":
            raw_body = await _read_body(receive)
            try:
                params = json.loads(raw_body) if raw_body else {}
            except Exception:
                params = {}
            url = params.get("url", "")
            if not url:
                await _send_json(send, {"success": False, "message": "缺少url"}, 400)
                return
            try:
                import asyncio
                loop = asyncio.get_event_loop()
                ok, msg = await loop.run_in_executor(None, _cms_add_share_down, url)
                await _send_json(send, {"success": ok, "message": msg})
            except Exception as e:
                await _send_json(send, {"success": False, "message": str(e)}, 500)
            return

        if path == "/api/submedia/add":
            raw_body = await _read_body(receive)
            try:
                params = json.loads(raw_body) if raw_body else {}
            except Exception:
                params = None

            if isinstance(params, dict):
                normalized_params, changed = _normalize_subscribe_payload(params)
                if changed:
                    raw_body = json.dumps(normalized_params, ensure_ascii=False).encode("utf-8")

            await original_app(scope, _build_body_receive(raw_body), send)
            return

        await original_app(scope, receive, send)
    return panso_middleware


# ============================================================
# Web UI 注入
# ============================================================
def _inject_web_script():
    def _do_inject():
        time.sleep(15)
        for d in ["/cms/web", "/cms/cms-api/web"]:
            index = os.path.join(d, "index.html")
            if os.path.exists(index):
                try:
                    js_file = os.path.join(d, "panso-inject.js")
                    js_content = _get_inject_js()
                    js_version = hashlib.md5(js_content.encode("utf-8")).hexdigest()[:12]
                    script_tag = f'<script src="/panso-inject.js?v={js_version}"></script>'
                    with open(js_file, "w", encoding="utf-8") as f:
                        f.write(js_content)
                    with open(index, "r", encoding="utf-8") as f:
                        html = f.read()
                    html = re.sub(
                        r'<script[^>]+src="/panso-inject\.js(?:\?[^"]*)?"[^>]*></script>\s*',
                        "",
                        html,
                        flags=re.I,
                    )
                    html = html.replace("</body>", f"{script_tag}\n</body>")
                    with open(index, "w", encoding="utf-8") as f:
                        f.write(html)
                except Exception:
                    pass
                break
    threading.Thread(target=_do_inject, daemon=True).start()


def _get_inject_js():
    js = """
(function() {
  'use strict';
  const style = document.createElement('style');
  style.textContent = `
    .ps-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,.6);z-index:9999;display:flex;justify-content:center;align-items:center}
    .ps-modal{background:#fff;border-radius:12px;width:92%;max-width:780px;height:75vh;display:flex;flex-direction:column;overflow:hidden}
    .ps-header{padding:12px 16px;border-bottom:1px solid #eee;display:flex;justify-content:space-between;align-items:center}
    .ps-header h2{margin:0;font-size:16px}
    .ps-close{font-size:20px;cursor:pointer;color:#999}
    .ps-close:hover{color:#333}
    .ps-source-tabs{display:flex;border-bottom:1px solid #eee;flex-shrink:0}
    .ps-source-tab{flex:1;padding:10px;text-align:center;cursor:pointer;font-size:14px;color:#666;border-bottom:2px solid transparent;transition:all .2s}
    .ps-source-tab:hover{background:#f5f5f5}
    .ps-source-tab.active{color:#1890ff;border-bottom-color:#1890ff;font-weight:bold}
    .ps-main{display:flex;flex:1;overflow:hidden}
    .ps-sidebar{width:90px;border-right:1px solid #eee;overflow-y:auto;flex-shrink:0}
    .ps-side-item{padding:12px 8px;text-align:center;cursor:pointer;font-size:13px;color:#666;border-left:3px solid transparent;transition:all .2s}
    .ps-side-item:hover{background:#f5f5f5}
    .ps-side-item.active{background:#e6f7ff;border-left-color:#1890ff;color:#1890ff;font-weight:bold}
    .ps-content{flex:1;overflow-y:auto;padding:12px}
    .ps-loading{text-align:center;padding:40px;color:#666}
    .ps-item{border:1px solid #eee;border-radius:8px;padding:8px 10px;margin-bottom:4px;transition:background .2s}
    .ps-item:hover{background:#f9f9f9}
    .ps-item-title{font-weight:bold;font-size:13px;margin-bottom:3px}
    .ps-item-info{font-size:12px;color:#888;margin-top:2px}
    .ps-item-url{font-size:12px;color:#1890ff;word-break:break-all;margin-top:3px}
    .ps-empty{text-align:center;padding:40px;color:#999}
    .ps-toast{position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);color:#fff;padding:10px 20px;border-radius:8px;z-index:10000}
    .ps-action-bar{display:flex;gap:6px;align-items:center;flex-shrink:0;margin-left:10px}
    .ps-open-btn,.ps-save-btn{color:#fff;border:none;padding:3px 10px;border-radius:4px;cursor:pointer;white-space:nowrap;font-size:12px}
    .ps-open-btn{background:#13c2c2}
    .ps-save-btn{background:#1890ff}
  `;
  document.head.appendChild(style);

  const allTypeNames = {'115':'115网盘','123':'123网盘','magnet':'磁链','quark':'夸克','baidu':'百度','aliyun':'阿里云','uc':'UC','xunlei':'迅雷','tianyi':'天翼'};
  const sourceAvailable = { hdhive: __HDHIVE_ENABLED__, gying: __GYING_ENABLED__, panso: __PANSO_ENABLED__ };
  const sourceLabels = { hdhive: '影巢', gying: '观影', panso: '盘搜' };
  const sourceLoading = { hdhive: '🎬 影巢搜索中...', gying: '🎞️ 观影搜索中...', panso: '📁 盘搜搜索中...' };
  const hdhiveUnlockCache = {};
  const hdhiveUnlockPending = {};

  function showToast(msg, color) {
    const el = document.createElement('div');
    el.className = 'ps-toast'; el.style.background = color || '#333'; el.textContent = msg;
    document.body.appendChild(el); setTimeout(() => el.remove(), 2000);
  }

  function fetchJsonWithTimeout(url, payload, timeoutMs) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs || 20000);
    return fetch(url, {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload || {}),
      signal: controller.signal
    }).then(r => r.json()).finally(() => clearTimeout(timer));
  }

  function saveUrl(url, btn) {
    btn.disabled = true; btn.textContent = '转存中...';
    fetch('/api/save', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({url})})
    .then(r=>r.json()).then(d=>{
      if(d.success){btn.textContent='✅ 已转存';btn.style.background='#52c41a';showToast('转存成功','#52c41a')}
      else{btn.textContent='❌ 失败';btn.style.background='#ff4d4f';showToast('转存失败: '+(d.message||''),'#ff4d4f');setTimeout(()=>{btn.textContent='转存';btn.style.background='#1890ff';btn.disabled=false},2000)}
    }).catch(()=>{btn.textContent='转存';btn.style.background='#1890ff';btn.disabled=false});
  }

  function openShareUrl(url) {
    if (!url) return;
    window.open(url, '_blank', 'noopener,noreferrer');
  }

  function buildActionButtons(fullUrl, allowSave, hideOpen) {
    if (!fullUrl) return '';
    let html = '<div class="ps-action-bar">';
    if (!hideOpen) html += '<button class="ps-open-btn" data-url="'+fullUrl.replace(/"/g,'&quot;')+'">打开链接</button>';
    if (allowSave) html += '<button class="ps-save-btn" data-url="'+fullUrl.replace(/"/g,'&quot;')+'">转存</button>';
    html += '</div>';
    return html;
  }

  function bindActionButtons(root) {
    const openBtn = root.querySelector('.ps-open-btn');
    if (openBtn) openBtn.onclick = (ev) => { ev.stopPropagation(); openShareUrl(openBtn.dataset.url); };
    const saveBtn = root.querySelector('.ps-save-btn');
    if (saveBtn) saveBtn.onclick = (ev) => { ev.stopPropagation(); saveUrl(saveBtn.dataset.url, saveBtn); };
  }

  function appendHdhiveUnlockedRow(el, url, code, fullUrl, allowSave) {
    if (!url || el.querySelector('.ps-item-save-row')) return;
    const row = document.createElement('div');
    row.className = 'ps-item-save-row';
    row.style.cssText = 'display:flex;justify-content:space-between;align-items:center;margin-top:6px;';
    let h = '<div><div class="ps-item-url">'+url+'</div>';
    if (code) h += '<div class="ps-item-info">密码: '+code+'</div>';
    h += '</div>';
    row.innerHTML = h + buildActionButtons(fullUrl || url, allowSave);
    el.appendChild(row);
    bindActionButtons(row);
  }

  function appendHdhiveUnlockError(el, message) {
    if (el.querySelector('.ps-item-unlock-error')) return;
    const err = document.createElement('div');
    err.className = 'ps-item-info ps-item-unlock-error';
    err.style.color = 'red';
    err.textContent = '❌ ' + (message || '解锁失败');
    el.appendChild(err);
  }

  function unlockAndShowSave(slug, el, allowSave) {
    if (!slug) return;

    const cached = hdhiveUnlockCache[slug];
    if (cached && cached.url) {
      el.onclick = null;
      el.style.cursor = 'default';
      appendHdhiveUnlockedRow(el, cached.url, cached.code, cached.fullUrl, allowSave);
      return;
    }
    if (cached && cached.error) {
      appendHdhiveUnlockError(el, cached.error);
      return;
    }

    el.style.opacity = '0.5';

    const pending = hdhiveUnlockPending[slug] || fetch('/api/hdhive/unlock', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({slug})
    }).then(r=>r.json()).then(d => {
      if (!d.success) {
        return { success:false, error:d.message || '解锁失败' };
      }
      const ld = d.data || {};
      const url = ld.full_url || ld.url || '';
      const code = ld.access_code || '';
      let fullUrl = url;
      if (code && url && !url.includes('password=')) fullUrl = url.includes('?') ? url+'&password='+code : url+'?password='+code;
      if (!url) return { success:false, error:'未获取到链接' };
      return { success:true, url, code, fullUrl };
    }).catch(() => {
      return { success:false, error:'解锁失败' };
    }).finally(() => {
      delete hdhiveUnlockPending[slug];
    });

    hdhiveUnlockPending[slug] = pending;
    pending.then(result => {
      el.style.opacity = '1';
      el.onclick = null;
      el.style.cursor = 'default';
      if (!result || !result.success) {
        hdhiveUnlockCache[slug] = { error:(result && result.error) || '解锁失败' };
        appendHdhiveUnlockError(el, (result && result.error) || '解锁失败');
        return;
      }
      hdhiveUnlockCache[slug] = result;
      appendHdhiveUnlockedRow(el, result.url, result.code, result.fullUrl, allowSave);
    });
  }

  function renderItems(items, container, isHdhive, currentType) {
    container.innerHTML = '';
    if (!items || !items.length) { container.innerHTML = '<div class="ps-empty">暂无资源</div>'; return; }
    items.forEach((item, i) => {
      const el = document.createElement('div');
      el.className = 'ps-item';

      if (isHdhive) {
        const isFree = item.is_unlocked || item.unlock_points === 0;
        const tag = isFree ? '' : '【'+item.unlock_points+'💰】';
        const desc = item.remark ? item.title+' | '+item.remark : item.title;
        const metaText = item.title_has_meta ? '' : [item.media_label || '', item.year || ''].filter(Boolean).join('｜');
        const info = [item.resolution, item.size].filter(Boolean).join(' | ');
        const official = item.is_official ? '【官组】' : '';
        const uploader = item.uploader ? '👤 '+item.uploader : '';
        const titlePrefix = [tag, official].filter(Boolean).join('');
        let h = '<div class="ps-item-title">'+(i+1)+'. '+(titlePrefix ? titlePrefix+' ' : '')+desc+'</div>';
        if (metaText) h += '<div class="ps-item-info">'+metaText+'</div>';
        if (info) h += '<div class="ps-item-info">'+info+'</div>';
        if (uploader) h += '<div class="ps-item-info">'+uploader+'</div>';
        el.innerHTML = h;
        container.appendChild(el);
        if (isFree) { unlockAndShowSave(item.slug, el, currentType === '115'); }
        else {
          el.style.cursor = 'pointer';
          el.onclick = () => {
            if (el.querySelector('.ps-confirm-bar')) return;
            const bar = document.createElement('div');
            bar.className = 'ps-confirm-bar';
            bar.style.cssText = 'margin-top:8px;display:flex;gap:8px;';
            bar.innerHTML = '<button style="background:#1890ff;color:#fff;border:none;padding:3px 10px;border-radius:4px;cursor:pointer">✅ 确认解锁</button><button style="background:#ccc;color:#333;border:none;padding:3px 10px;border-radius:4px;cursor:pointer">❌ 取消</button>';
            bar.children[0].onclick = (ev) => {ev.stopPropagation();bar.remove();unlockAndShowSave(item.slug, el, currentType === '115')};
            bar.children[1].onclick = (ev) => {ev.stopPropagation();bar.remove()};
            el.appendChild(bar);
          };
        }
      } else {
        const note = item.note || item.title || '未知';
        const metaText = [item.media_label || '', item.year || ''].filter(Boolean).join('｜');
        const url = item.url || '', pwd = item.password || '';
        let fullUrl = url;
        if (pwd && url && !url.includes('password=')) fullUrl = url.includes('?') ? url+'&password='+pwd : url+'?password='+pwd;
        let h = '<div style="display:flex;justify-content:space-between;align-items:flex-start"><div style="flex:1">';
        h += '<div class="ps-item-title">'+(i+1)+'. '+note+'</div>';
        if (metaText) h += '<div class="ps-item-info">'+metaText+'</div>';
        if (url) h += '<div class="ps-item-url">'+url+'</div>';
        if (pwd) h += '<div class="ps-item-info">密码: '+pwd+'</div>';
        h += '</div>';
        if (fullUrl) h += buildActionButtons(fullUrl, currentType === '115' || currentType === 'magnet', currentType === 'magnet');
        h += '</div>';
        el.innerHTML = h;
        bindActionButtons(el);
        container.appendChild(el);
      }
    });
  }

  function normalizeSearchTitle(primary, fallback) {
    const cleaned = [];

    function add(value) {
      value = String(value || '').replace(/\\s+/g, ' ').trim();
      if (!value) return;
      value = value.replace(/\\s*[（(]\\s*(?:19|20)\\d{2}\\s*[）)]\\s*$/, '');
      value = value.replace(/\\s*[（(]\\s*$/, '');
      value = value.replace(/\\s*[-|｜:：]\\s*(?:19|20)\\d{2}\\s*$/, '');
      value = value.trim();
      if (value && !cleaned.includes(value)) cleaned.push(value);
    }

    add(primary);
    add(fallback);
    return cleaned.sort((a, b) => b.length - a.length)[0] || '';
  }

  function showSearchModal(title) {
    title = normalizeSearchTitle(title, '');
    if (!sourceAvailable.hdhive && !sourceAvailable.gying && !sourceAvailable.panso) {
      showToast('未配置任何搜索源', '#ff4d4f');
      return;
    }

    const sourceData = { hdhive: null, gying: null, panso: null };
    const sourceStatus = {
      hdhive: sourceAvailable.hdhive ? 'idle' : 'disabled',
      gying: sourceAvailable.gying ? 'idle' : 'disabled',
      panso: sourceAvailable.panso ? 'idle' : 'disabled'
    };
    const sourceErrors = { hdhive: '', gying: '', panso: '' };
    const sourcePromises = { hdhive: null, gying: null, panso: null };
    let currentSource = '__DEFAULT_SOURCE__';
    let currentType = null;

    const overlay = document.createElement('div');
    overlay.className = 'ps-overlay';
    overlay.innerHTML = `
      <div class="ps-modal">
        <div class="ps-header"><h2>🔍 ${title}</h2><span class="ps-close">&times;</span></div>
        <div class="ps-source-tabs">
          <div class="ps-source-tab active" data-source="hdhive">🎬 影巢</div>
          <div class="ps-source-tab" data-source="panso">📁 盘搜</div>
          <div class="ps-source-tab" data-source="gying">🎞️ 观影</div>
        </div>
        <div class="ps-main">
          <div class="ps-sidebar"></div>
          <div class="ps-content"><div class="ps-loading">__DEFAULT_LOADING__</div></div>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
    overlay.querySelector('.ps-close').onclick = () => overlay.remove();
    overlay.onclick = (e) => { if (e.target === overlay) overlay.remove() };

    const sidebar = overlay.querySelector('.ps-sidebar');
    const content = overlay.querySelector('.ps-content');
    const sourceTabs = overlay.querySelectorAll('.ps-source-tab');
    sourceTabs.forEach(t => t.classList.toggle('active', t.dataset.source === currentSource));

    function renderSourceState(source) {
      const status = sourceStatus[source];
      if (status === 'disabled') {
        sidebar.innerHTML = '';
        content.innerHTML = '<div class="ps-empty">' + (sourceLabels[source] || '当前来源') + '未配置</div>';
        return;
      }
      if (status === 'loading' || status === 'idle') {
        sidebar.innerHTML = '';
        content.innerHTML = '<div class="ps-loading">' + (sourceLoading[source] || '搜索中...') + '</div>';
        return;
      }
      if (status === 'error') {
        sidebar.innerHTML = '';
        content.innerHTML = '<div class="ps-empty">' + (sourceErrors[source] || ((sourceLabels[source] || '当前来源') + '搜索失败')) + '</div>';
        return;
      }
      buildSidebar(sourceData[source] || {}, source === 'hdhive');
    }

    function showSource(source) {
      currentSource = source;
      sourceTabs.forEach(t => t.classList.toggle('active', t.dataset.source === source));
      renderSourceState(source);
      if (sourceStatus[source] === 'idle') {
        if (source === 'hdhive') loadHdhive();
        else if (source === 'gying') loadGying();
        else loadPanso();
      }
    }

    function buildSidebar(grouped, isHdhive) {
      sidebar.innerHTML = '';
      const types = Object.keys(grouped);
      if (!types.length) { sidebar.innerHTML = ''; content.innerHTML = '<div class="ps-empty">暂无资源</div>'; return; }
      let first = true;
      types.forEach(type => {
        const items = grouped[type];
        const el = document.createElement('div');
        el.className = 'ps-side-item' + (first ? ' active' : '');
        el.dataset.type = type;
        el.textContent = (allTypeNames[type] || type) + ' (' + items.length + ')';
        el.onclick = () => {
          sidebar.querySelectorAll('.ps-side-item').forEach(s => s.classList.remove('active'));
          el.classList.add('active');
          currentType = type;
          renderItems(items, content, isHdhive, type);
        };
        sidebar.appendChild(el);
        if (first) { currentType = type; renderItems(items, content, isHdhive, type); first = false; }
      });
    }

    function loadHdhive() {
      if (!sourceAvailable.hdhive) {
        sourceStatus.hdhive = 'disabled';
        if (currentSource === 'hdhive') renderSourceState('hdhive');
        return Promise.resolve({});
      }
      if (sourcePromises.hdhive) return sourcePromises.hdhive;
      sourceStatus.hdhive = 'loading';
      sourceErrors.hdhive = '';
      if (currentSource === 'hdhive') renderSourceState('hdhive');
      sourcePromises.hdhive = fetchJsonWithTimeout('/api/hdhive/search', {keyword:title}, 20000).then(data => {
        const resources = data.data || [];
        // 按 pan_type 分组
        const grouped = {};
        resources.forEach(res => {
          const t = res.pan_type || 'other';
          if (!grouped[t]) grouped[t] = [];
          grouped[t].push(res);
        });
        sourceData.hdhive = grouped;
        sourceStatus.hdhive = 'done';
        if (currentSource === 'hdhive') renderSourceState('hdhive');
        return grouped;
      }).catch(() => {
        sourceStatus.hdhive = 'error';
        sourceErrors.hdhive = '影巢搜索失败';
        if (currentSource === 'hdhive') renderSourceState('hdhive');
        return {};
      });
      return sourcePromises.hdhive;
    }

    function loadGying() {
      if (!sourceAvailable.gying) {
        sourceStatus.gying = 'disabled';
        if (currentSource === 'gying') renderSourceState('gying');
        return Promise.resolve({});
      }
      if (sourcePromises.gying) return sourcePromises.gying;
      sourceStatus.gying = 'loading';
      sourceErrors.gying = '';
      if (currentSource === 'gying') renderSourceState('gying');
      sourcePromises.gying = fetchJsonWithTimeout('/api/gying/search', {keyword:title}, 20000).then(data => {
        if (data.success === false) {
          sourceStatus.gying = 'error';
          sourceErrors.gying = data.message || '观影搜索失败';
          if (currentSource === 'gying') renderSourceState('gying');
          return {};
        }
        let inner = data;
        if (data.data && typeof data.data === 'object') inner = data.data;
        const merged = inner.merged_by_type || {};
        sourceData.gying = merged;
        sourceStatus.gying = 'done';
        if (currentSource === 'gying') renderSourceState('gying');
        return merged;
      }).catch(() => {
        sourceStatus.gying = 'error';
        sourceErrors.gying = '观影搜索失败';
        if (currentSource === 'gying') renderSourceState('gying');
        return {};
      });
      return sourcePromises.gying;
    }

    function loadPanso() {
      if (!sourceAvailable.panso) {
        sourceStatus.panso = 'disabled';
        if (currentSource === 'panso') renderSourceState('panso');
        return Promise.resolve({});
      }
      if (sourcePromises.panso) return sourcePromises.panso;
      sourceStatus.panso = 'loading';
      sourceErrors.panso = '';
      if (currentSource === 'panso') renderSourceState('panso');
      sourcePromises.panso = fetchJsonWithTimeout('/api/panso/search', {kw:title}, 20000).then(data => {
        let inner = data;
        if (data.data && typeof data.data === 'object') inner = data.data;
        const merged = inner.merged_by_type || {};
        sourceData.panso = merged;
        sourceStatus.panso = 'done';
        if (currentSource === 'panso') renderSourceState('panso');
        return merged;
      }).catch(() => {
        sourceStatus.panso = 'error';
        sourceErrors.panso = '盘搜搜索失败';
        if (currentSource === 'panso') renderSourceState('panso');
        return {};
      });
      return sourcePromises.panso;
    }

    sourceTabs.forEach(t => t.onclick = () => showSource(t.dataset.source));
    renderSourceState(currentSource);

    loadHdhive();
    loadPanso();
    loadGying();
  }

  // 单击搜索，双击订阅
  let _clickTimer = null;
  let _psPass = false;
  document.addEventListener('click', function(e) {
    if (_psPass) { _psPass = false; return; }

    const path = window.location.hash || window.location.pathname;
    if (!path.includes('recommend')) return;
    const btn = e.target.closest('.subscribe-btn');
    if (!btn) return;
    const card = btn.closest('.movie-card');
    if (!card) return;

    e.preventDefault();
    e.stopPropagation();
    e.stopImmediatePropagation();

    if (_clickTimer) {
      // 500ms内第二次点击：执行原始订阅
      clearTimeout(_clickTimer);
      _clickTimer = null;
      // 用 pointerdown+pointerup 模拟完整点击，绕过我们的拦截
      _psPass = true;
      const rect = btn.getBoundingClientRect();
      const x = rect.left + rect.width/2, y = rect.top + rect.height/2;
      btn.dispatchEvent(new PointerEvent('pointerdown', {bubbles:true,clientX:x,clientY:y}));
      btn.dispatchEvent(new PointerEvent('pointerup', {bubbles:true,clientX:x,clientY:y}));
      btn.dispatchEvent(new MouseEvent('click', {bubbles:true,cancelable:true,clientX:x,clientY:y}));
    } else {
      // 第一次点击：等500ms，没有第二次就搜索
      const savedCard = card;
      _clickTimer = setTimeout(() => {
        _clickTimer = null;
        const titleEl = savedCard.querySelector('.title');
        const img = savedCard.querySelector('.poster img');
        const rawTitle = titleEl ? titleEl.textContent.trim() : '';
        const altTitle = img ? img.alt.trim() : '';
        let title = normalizeSearchTitle(rawTitle, altTitle);
        if (title) showSearchModal(title);
      }, 500);
    }
  }, true);
})();
"""
    default_source = "hdhive" if _has_hdhive() else ("panso" if _has_panso() else "gying")
    default_loading = {
        "hdhive": "🎬 影巢搜索中...",
        "gying": "🎞️ 观影搜索中...",
        "panso": "📁 盘搜搜索中...",
    }.get(default_source, "搜索中...")
    return (js.replace("__HDHIVE_ENABLED__", "true" if _has_hdhive() else "false")
             .replace("__GYING_ENABLED__", "true" if _has_gying() else "false")
             .replace("__PANSO_ENABLED__", "true" if _has_panso() else "false")
             .replace("__DEFAULT_SOURCE__", default_source)
             .replace("__DEFAULT_LOADING__", default_loading))


# ============================================================
# Import Hook
# ============================================================
_original_import = builtins.__import__
_hooked = False
_tg_patched = False


def _patched_import(name, *args, **kwargs):
    global _tg_patched
    module = _original_import(name, *args, **kwargs)
    try:
        mod_name = getattr(module, "__name__", "")
        if not _tg_patched and (
            name == "app.modules.tg.tg_bot"
            or mod_name == "app.modules.tg.tg_bot"
            or str(name).endswith(".tg_bot")
            or str(mod_name).endswith(".tg_bot")
        ):
            _tg_patched = True
            _patch_tg_bot(module)
        elif (
            name == "app.modules.wechat"
            or mod_name == "app.modules.wechat"
            or name == "app.modules.wechat.wechat"
            or mod_name == "app.modules.wechat.wechat"
            or str(name).endswith(".wechat")
            or str(mod_name).endswith(".wechat")
        ):
            _patch_wechat_module(module)
        elif name == "app.services.event_service" or mod_name == "app.services.event_service":
            _patch_event_service(module)
        elif name == "app.services.share_down" or mod_name == "app.services.share_down":
            _patch_share_down_service(module)
    except Exception:
        pass
    return module


def enhance():
    global _hooked
    if _hooked:
        return
    _hooked = True

    try:
        with open("/tmp/cms_usercustomize_enhance.txt", "w", encoding="utf-8") as _f:
            _f.write(f"enhance {time.time()}\n")
    except Exception:
        pass

    if not _has_any_search_source():
        return

    _patch_telebot_runtime()
    builtins.__import__ = _patched_import
    _patch_flask()
    _inject_web_script()
    _late_patch_wechat_module()
    _late_patch_event_service()
    _late_patch_tg_bot_module()
    _late_patch_share_down_service()
    _start_douban_hot_refresh_scheduler()
    _reconcile_submedia_completion_later(delay=20)

    # 企微日志监听 - 添加到所有可能的 logger
    handler = _WxLogHandler()
    handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(handler)
    for name in ["event_serv", "event_service", "app.services.event_service"]:
        try:
            logging.getLogger(name).addHandler(handler)
        except Exception:
            pass


# ============================================================
# 入口
# ============================================================
script_name = ""
if sys.argv and sys.argv[0]:
    script_name = os.path.basename(sys.argv[0])

if script_name == "main.py":
    try:
        enhance()
    except Exception:
        pass

__test__ = {}
