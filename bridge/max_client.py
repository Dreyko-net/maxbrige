"""
Обёртка над pymax.Client для одного пользователя.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Callable, Optional

from pymax import Client, Message, ExtraConfig
from pymax.api.session.enums import DeviceType

from bridge.queue import BridgeEvent, max_to_tg_queue
from config import SESSIONS_DIR

log = logging.getLogger(__name__)


class MaxUserClient:
    def __init__(
        self,
        tg_user_id:        int,
        max_phone:         str,
        session_path:      str,
        on_ready:          Optional[Callable] = None,
        sms_code_provider = None,
        password_provider = None,
    ):
        self.tg_user_id          = tg_user_id
        self.max_phone           = max_phone
        self.session_path        = session_path
        self.on_ready            = on_ready
        self._sms_provider       = sms_code_provider
        self._password_provider       = password_provider
        self._client: Optional[Client] = None
        self._task:   Optional[asyncio.Task] = None
        self.me                  = None
        self._ready              = asyncio.Event()
        self._on_session_revoked = None

    def _build_client(self) -> Client:
        Path(self.session_path).mkdir(parents=True, exist_ok=True)
        return Client(
            phone             = self.max_phone,
            work_dir          = self.session_path,
            session_name      = "session.db",
            sms_code_provider = self._sms_provider,
            password_provider = self._password_provider,
            extra_config=ExtraConfig(device_type=DeviceType.ANDROID)
        )

    async def start(self) -> None:
        """
        Запускает клиент.

        Стратегия: запускаем _app.start() напрямую (авторизация + логин),
        ждём его завершения — это и есть сигнал готовности.
        После этого запускаем бесконечный reconnect-цикл как Task.
        """
        self._client = self._build_client()
        self._register_handlers()

        # Шаг 1: авторизация + первый логин.
        # _app.start() завершается после успешного логина.
        # Таймаут большой (5 мин) — пользователь может долго вводить SMS.
        log.info("[user=%s] running _app.start() (auth + login)", self.tg_user_id)
        # Без внешнего таймаута — таймаут уже есть в SMS-провайдере (300 сек).
        # wait_for нельзя использовать: он отменяет корутину пока SMS-провайдер ждёт ввода.
        await self._client._app.start()

        # Авторизация успешна
        self.me = getattr(self._client, "me", None)
        log.info("[user=%s] MAX auth done, me=%s", self.tg_user_id,
                 getattr(self.me, "id", "?") if self.me else "?")
        self._ready.set()

        if self.on_ready:
            await self.on_ready(self)

        # Шаг 2: запускаем reconnect-цикл в фоне
        # (он будет переподключаться при обрывах)
        self._task = asyncio.create_task(
            self._reconnect_loop(),
            name=f"max_client_{self.tg_user_id}",
        )

    async def _reconnect_loop(self):
        """
        Поддерживает соединение живым после первого логина.
        Повторяет цикл: wait_closed → reconnect при обрыве.
        """
        while True:
            try:
                await self._client._connection.wait_closed()
            except asyncio.CancelledError:
                return
            except Exception:
                pass

            # Соединение оборвалось — переподключаемся
            log.info("[user=%s] connection lost, reconnecting…", self.tg_user_id)
            try:
                self._client._reset_runtime()
                await self._client._app.start()
                log.info("[user=%s] reconnected", self.tg_user_id)
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                return
            except Exception as e:
                err = str(e).lower()
                if "fail_logout_all" in err or "login.token" in err:
                    log.error("[user=%s] session revoked on reconnect", self.tg_user_id)
                    if self._on_session_revoked:
                        asyncio.create_task(self._on_session_revoked(self.tg_user_id))
                    return
                log.error("[user=%s] reconnect error: %s", self.tg_user_id, e)
                await asyncio.sleep(5)

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        try:
            await self._client.close()
        except Exception:
            pass

    def _register_handlers(self):
        client = self._client

        @client.on_message()
        async def handle_message(msg: Message, _client: Client) -> None:
            try:
                text      = getattr(msg, "text",      "") or ""
                msg_id    = str(getattr(msg, "id",    "") or "")
                chat_id   = str(getattr(msg, "chat_id", "") or "")
                max_sender_id   = str(getattr(msg, "sender", "") or "")
                timestamp = getattr(msg, "timestamp", None) or int(time.time() * 1000)
                has_media, media_type = _detect_media(msg)

                # ── Пересланные сообщения ──
                fwd_attaches_list = None
                fwd_source = None
                fwd_msg = None
                fwd_link = None
                if hasattr(msg, "model_extra"):
                    link = msg.model_extra.get("link")
                    if isinstance(link, dict) and link.get("type") == "FORWARD":
                        fwd_link = link
                        fwd_msg = link.get("message", {})
                        fwd_text = fwd_msg.get("text", "") or ""
                        fwd_attaches = fwd_attaches_list = fwd_msg.get("attaches", [])
                        fwd_source = link.get("chatName") or fwd_msg.get("chatName", "")
                        if fwd_text or fwd_attaches_list:
                            text = fwd_text

                # ── Формируем пометку о пересылке ──
                fwd_prefix = ""
                if fwd_source:
                    fwd_prefix = f"↩️ Переслано из «{fwd_source}»\n"

                # ── Обычное сообщение (не пересылка или пересылка без вложений) ──
                if not fwd_attaches_list:
                    media_bytes = None
                    media_name  = None
                    if has_media:
                        media_bytes, media_name = await self._download_live_media(msg, chat_id, msg_id)

                    event = BridgeEvent(
                        direction   = "max_to_tg",
                        tg_user_id  = self.tg_user_id,
                        max_chat_id = chat_id,
                        max_sender_id = max_sender_id,
                        text        = fwd_prefix + text if (fwd_prefix and text) else (fwd_prefix or text),
                        timestamp   = timestamp,
                        max_msg_id  = msg_id,
                        has_media   = has_media,
                        media_type  = media_type,
                        media_bytes = media_bytes,
                        media_name  = media_name,
                    )
                    await max_to_tg_queue.put(event)
                else:
                    # ── Пересылка с вложениями: каждое вложение — отдельный event ──
                    first_text = fwd_prefix + text if (fwd_prefix and text) else (fwd_prefix or text)

                    media_sent = False
                    for i, attach in enumerate(fwd_attaches_list):
                        _type = (attach.get("_type") or "").upper()
                        if _type == "PHOTO":
                            m_type = "photo"
                        elif _type == "VIDEO":
                            m_type = "video"
                        else:
                            continue

                        media_bytes, media_name = await self._download_fwd_media(attach, fwd_msg, fwd_link, chat_id, msg_id)

                        event = BridgeEvent(
                            direction   = "max_to_tg",
                            tg_user_id  = self.tg_user_id,
                            max_chat_id = chat_id,
                            max_sender_id = max_sender_id,
                            text        = first_text if i == 0 else "",
                            timestamp   = timestamp,
                            max_msg_id  = msg_id,
                            has_media   = bool(media_bytes),
                            media_type  = m_type,
                            media_bytes = media_bytes,
                            media_name  = media_name,
                        )
                        await max_to_tg_queue.put(event)
                        media_sent = True

                    # Если вложения были, но ни одно не скачалось — хотя бы текст
                    if not media_sent and first_text:
                        event = BridgeEvent(
                            direction   = "max_to_tg",
                            tg_user_id  = self.tg_user_id,
                            max_chat_id = chat_id,
                            max_sender_id = max_sender_id,
                            text        = first_text,
                            timestamp   = timestamp,
                            max_msg_id  = msg_id,
                        )
                        await max_to_tg_queue.put(event)
            except Exception as e:
                log.error("[user=%s] handle_message error: %s", self.tg_user_id, e)
    
    async def _download_live_media(self, msg, chat_id: str, msg_id: str) -> tuple[bytes | None, str | None]:
        """Скачивает медиа из входящего сообщения MAX для живой пересылки в TG.

        Возвращает (media_bytes, media_name) или (None, None) при ошибке.
        """
        import aiohttp
        from telegram.sender import extract_attachment

        attach_info = extract_attachment(msg)
        if not attach_info:
            return None, None

        atype = attach_info["type"]
        url   = attach_info.get("url")
        filename = attach_info.get("filename", "file")
        int_chat_id = int(chat_id) if chat_id else 0
        int_msg_id  = int(msg_id) if msg_id else 0

        # Прямой URL (photo, audio, sticker)
        if url:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if data:
                                return data, filename
                log.warning("[user=%s] download live media (url) failed: status=%s", self.tg_user_id, resp.status)
            except Exception as e:
                log.error("[user=%s] download live media (url) error: %s", self.tg_user_id, e)
            return None, None

        # Видео — нужен VideoRequest
        if atype == "video":
            video_id = attach_info.get("video_id", 0)
            if not video_id or not int_chat_id:
                return None, None
            try:
                req = await self._client.get_video_by_id(
                    chat_id=int_chat_id, message_id=int_msg_id, video_id=int(video_id))
                if req and getattr(req, "url", None):
                    async with aiohttp.ClientSession() as session:
                        async with session.get(req.url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                if data:
                                    return data, filename
            except Exception as e:
                log.error("[user=%s] download live video error: %s", self.tg_user_id, e)
            return None, None

        # Файл — нужен FileRequest
        if atype == "document":
            file_id = attach_info.get("file_id", 0)
            if not file_id or not int_chat_id:
                return None, None
            try:
                req = await self._client.get_file_by_id(
                    chat_id=int_chat_id, message_id=int_msg_id, file_id=int(file_id))
                if req and getattr(req, "url", None):
                    async with aiohttp.ClientSession() as session:
                        async with session.get(req.url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                if data:
                                    return data, filename
            except Exception as e:
                log.error("[user=%s] download live file error: %s", self.tg_user_id, e)
            return None, None

        log.warning("[user=%s] no download method for live media type=%s", self.tg_user_id, atype)
        return None, None
    
    async def _download_fwd_media(self, attach: dict, fwd_msg: dict, fwd_link: dict, current_chat_id: str, current_msg_id: str) -> tuple[bytes | None, str | None]:
        """Скачивает медиа из пересланного сообщения (сырой dict из model_extra).

        Для видео: chatId берём из fwd_link (link), а не из fwd_msg (link.message),
        т.к. chatId находится на уровне link, а не внутри link.message.
        Также пробуем текущий chat_id как fallback.

        Возвращает (media_bytes, media_name) или (None, None) при ошибке.
        """
        import aiohttp

        _type = (attach.get("_type") or "").upper()

        # ── Фото: прямой URL (baseUrl) ──
        if _type == "PHOTO":
            url = attach.get("baseUrl")
            photo_id = attach.get("photoId", 0)
            filename = f"photo_{photo_id}.jpg"
            if not url:
                return None, None
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                        if resp.status == 200:
                            data = await resp.read()
                            if data:
                                return data, filename
                log.warning("[user=%s] download fwd photo failed: status=%s", self.tg_user_id, resp.status)
            except Exception as e:
                log.error("[user=%s] download fwd photo error: %s", self.tg_user_id, e)
            return None, None

        # ── Видео: нужен VideoRequest через API ──
        if _type == "VIDEO":
            video_id = attach.get("videoId", 0)
            source_msg_id = fwd_msg.get("id")
            filename = f"video_{video_id}.mp4"
            
            # chatId находится на уровне link, а не link.message
            source_chat_id = None
            if fwd_link:
                source_chat_id = fwd_link.get("chatId")

            log.info("[user=%s] fwd video: videoId=%s linkChatId=%s currentChatId=%s srcMsg=%s",
                     self.tg_user_id, video_id, source_chat_id, current_chat_id, source_msg_id)

            # Пробуем скачать видео — сначала с chatId из link, потом с текущим chat_id
            for try_chat_id, label in [(source_chat_id, "link"), (current_chat_id, "current")]:
                if not try_chat_id or not source_msg_id or not video_id:
                    continue
                try:
                    video_url = await self._get_video_url_raw(
                        int(try_chat_id), int(source_msg_id), int(video_id))
                    log.info("[user=%s] fwd video (%s) url=%s",
                             self.tg_user_id, label, video_url)
                    if video_url:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(video_url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                                if resp.status == 200:
                                    data = await resp.read()
                                    if data:
                                        return data, filename
                except Exception as e:
                    log.warning("[user=%s] fwd video (%s) error: %s", self.tg_user_id, label, e)

            log.warning("[user=%s] fwd video: all attempts failed", self.tg_user_id)
            return None, None

        log.warning("[user=%s] no download method for fwd media _type=%s", self.tg_user_id, _type)
        return None, None

    async def _get_video_url_raw(self, chat_id: int, message_id: int, video_id: int) -> str | None:
        """Получает URL видео через сырой invoke, обходя валидацию VideoRequest.

        Для пересланных видео MAX API возвращает url как список доменов,
        а VideoRequest.url: str → ValidationError. Поэтому используем
        client._app.invoke(Opcode.VIDEO_PLAY) напрямую — тот же механизм
        что и get_video_by_id, но без parse_payload_model.
        """
        from pymax.protocol.enums import Opcode

        # Сначала стандартный путь (для обычных видео url — строка)
        try:
            req = await self._client.get_video_by_id(
                chat_id=chat_id, message_id=message_id, video_id=video_id)
            if req and getattr(req, "url", None):
                return req.url
        except Exception:
            pass  # Ожидаемо для пересланных видео

        # Сырой запрос — получаем payload dict без валидации Pydantic
        response = await self._client._app.invoke(
            opcode=Opcode.VIDEO_PLAY,
            payload={
                "chatId": chat_id,
                "messageId": str(message_id),
                "videoId": video_id,
            },
        )
        raw = response.payload if hasattr(response, "payload") else None
        log.info("[user=%s] raw VIDEO_PLAY payload: %s", self.tg_user_id, raw)
        return self._extract_video_url(raw, video_id)

    def _extract_video_url(self, raw: dict | None, video_id: int) -> str | None:
        """Извлекает URL видео из сырого payload VIDEO_PLAY.

        Форматы ответа:
        1) url: "https://..." — готовый URL
        2) url: ["domain"] + другой ключ с токеном — склеиваем
        3) Произвольный ключ (не EXTERNAL/cache/url) — как unwrap_dynamic_url
        """
        if not raw or not isinstance(raw, dict):
            return None

        # 1) url — готовая строка
        url_val = raw.get("url")
        if isinstance(url_val, str) and url_val:
            return url_val

        # 2) url — список доменов, ищем токен в других ключах
        if isinstance(url_val, list) and url_val:
            domain = url_val[0]
            token = ""
            for key, val in raw.items():
                if key.lower() in ("external", "cache", "url"):
                    continue
                if isinstance(val, str) and val:
                    token = val
                    break
            result = f"https://{domain}/{video_id}/{token}" if token else f"https://{domain}/{video_id}"
            log.info("[user=%s] constructed url: %s", self.tg_user_id, result)
            return result

        # 3) Произвольный ключ с URL (как unwrap_dynamic_url в VideoRequest)
        for key, val in raw.items():
            if key.lower() in ("external", "cache", "url"):
                continue
            if isinstance(val, str) and val:
                return val

        return None

    # ── API методы ────────────────────────────────────────────────────────────

    async def get_chats(self) -> list:
        all_chats = []
        marker = None
        try:
            while True:
                page = await self._client.fetch_chats(marker) if marker else \
                       await self._client.fetch_chats()
                if not page:
                    break
                all_chats.extend(page)
                if len(page) < 20:
                    break
                last   = page[-1]
                marker = getattr(last, "id", None) or getattr(last, "chat_id", None)
                if not marker:
                    break
            log.info("[user=%s] get_chats: found %d", self.tg_user_id, len(all_chats))
            return all_chats
        except Exception as e:
            log.error("[user=%s] get_chats error: %s", self.tg_user_id, e)
            return []
        
    async def get_client(self, marker: int = None):
        try:
            if marker:
                user = await self._client.get_user(marker)
                #else await self._client.fetch_users([marker])
                if not user:
                    log.warning("[user=%s] get_user not found user id: %d", self.tg_user_id, marker)
                    return None
                # log.info("[user=%s] get_user: found user %s", self.tg_user_id, user)
                names = getattr(user, "names", None)
                if names:
                    try:
                        name = f"{getattr(names[0], 'first_name', '')} {getattr(names[0], 'last_name', '')}"
                        if name:
                            return " ".join(str(name).split())
                    except (IndexError, StopIteration, KeyError):
                        pass
            log.error("[user=%s] get_user error: %s user_id = %d", self.tg_user_id, marker)
        except Exception as e:
            log.error("[user=%s] get_user error: %s", self.tg_user_id, e)
            return None

    async def get_history(self, max_chat_id: str, from_ts: int, to_ts: int, limit: int = 100) -> list:
        """
        Пагинированно загружает всю историю от from_ts до to_ts.
        Возвращает сообщения отсортированные от старых к новым.
        """
        all_messages = []
        anchor = self._client.me.contact.registration_time # Запрашиваем все сообщения с момента регистрации #to_ts

        while True:
            try:
                batch = await self._client.fetch_history(
                    chat_id   = int(max_chat_id),
                    from_time = anchor,
                    forward   = limit,
                )
            except Exception as e:
                log.error("[user=%s] get_history fetch error: %s",
                          self.tg_user_id, e)
                break

            
            # Добавляем все
            for m in batch:
                all_messages.append(m)
            if len(batch) < limit:
                break

            # # Фильтруем по диапазону и добавляем
            # for m in batch:
            #     msg_time = getattr(m, "time", 0) or 0
            #     if from_ts <= msg_time <= to_ts:
            #         all_messages.append(m)

            # # Смещаем якорь на время самого старого сообщения в батче
            # times = [getattr(m, "time", 0) or 0 for m in batch]
            # oldest = max(times)

            # if oldest <= from_ts:
            #     break  # дошли до начала нужного диапазона

            anchor = batch[limit-1].time + 1
            await asyncio.sleep(0.3)  # пауза между запросами

        # Сортируем: старые → новые
        all_messages.sort(key=lambda m: getattr(m, "time", 0) or 0)

        log.info("[user=%s] get_history chat=%s: total %d messages",
                 self.tg_user_id, max_chat_id, len(all_messages))
        return all_messages

    async def send_message(self, max_chat_id: str, text: str, sender_name: Optional[str] = None) -> Optional[str]:
        try:
            if sender_name:
                text = f"{sender_name}: {text}"
            result = await self._client.send_message(
                chat_id=int(max_chat_id), text=text)
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_message error: %s", self.tg_user_id, e)
            return None


    async def send_file(self, max_chat_id, data, filename, caption="", sender_name: Optional[str] = None):
        try:
            from pymax.files.file import File
            result = await self._client.send_message(
                chat_id=int(max_chat_id), text=caption,
                attachments=[File(raw=data, name=filename or "file")])
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_file error: %s", self.tg_user_id, e)
            return None

    async def send_photo(self, max_chat_id, data, caption="", sender_name: Optional[str] = None):
        try:
            from pymax.files.photo import Photo
            result = await self._client.send_message(
                chat_id=int(max_chat_id), text=caption,
                attachments=[Photo(raw=data, name="photo.jpg")])
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_photo error: %s", self.tg_user_id, e)
            return None

    async def send_video(self, max_chat_id, data, filename="video.mp4", caption="", sender_name: Optional[str] = None):
        try:
            from pymax.files.video import Video
            result = await self._client.send_message(
                chat_id=int(max_chat_id), text=caption,
                attachments=[Video(raw=data, name=filename or "video.mp4")])
            return str(getattr(result, "id", "") or "")
        except Exception as e:
            log.error("[user=%s] send_video error: %s", self.tg_user_id, e)
            return None


    async def download_file(self, chat_id, message_id, file_id):
        try:
            req = await self._client.get_file_by_id(
                chat_id=int(chat_id), message_id=message_id, file_id=int(file_id))
            if not req or not getattr(req, "url", None):
                return None
            import aiohttp
            async with aiohttp.ClientSession() as s:
                async with s.get(req.url) as resp:
                    resp.raise_for_status()
                    return await resp.read()
        except Exception as e:
            log.error("[user=%s] download_file error: %s", self.tg_user_id, e)
            return None

# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_media(msg) -> tuple[bool, Optional[str]]:
    attaches = getattr(msg, "attaches", None) or getattr(msg, "attachments", None) or []
    if attaches:
        t = type(attaches[0]).__name__.lower()
        if "photo"  in t: return True, "photo"
        if "video"  in t: return True, "video"
        if "voice"  in t: return True, "voice"
        if "audio"  in t: return True, "audio"
        if "file"   in t: return True, "document"
        return True, "document"
    for attr, kind in [("photo","photo"),("video","video"),("document","document"),
                       ("voice","voice"),("audio","audio"),("sticker","sticker")]:
        if getattr(msg, attr, None):
            return True, kind
    return False, None


def session_path_for(tg_user_id: int) -> str:
    return str(SESSIONS_DIR / f"user_{tg_user_id}")