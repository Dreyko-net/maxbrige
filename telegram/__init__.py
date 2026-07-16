import logging
import socket
from aiogram import Bot, Dispatcher, BaseMiddleware
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import TelegramObject, BotCommand

from config import TG_PROXY, DEBUG
from telegram.handlers import auth, messages, callbacks, commands

log = logging.getLogger(__name__)


class LogAllUpdatesMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data: dict):
        log.info("[MIDDLEWARE] update type=%s data_keys=%s",
                 type(event).__name__, list(data.keys()))
        result = await handler(event, data)
        return result


async def _set_bot_commands(bot: Bot):
    """Устанавливает меню команд бота."""
    commands = [
        BotCommand(command="start", description="Подключить MAX"),
        BotCommand(command="status", description="Статус подключения"),
        BotCommand(command="sync_chats", description="Синхронизировать чаты"),
        BotCommand(command="sync", description="Полная синхронизация"),
        BotCommand(command="history", description="Скачать историю (в топике)"),
    ]
    try:
        await bot.set_my_commands(commands)
        log.info("Bot commands menu set")
    except Exception as e:
        log.error("Failed to set bot commands: %s", e)


def create_bot(token: str) -> tuple[Bot, Dispatcher]:
    proxy = TG_PROXY.rstrip("/") if TG_PROXY != '' else ""
    print('Прокси установлен:' + str(proxy))
    if proxy != '':
        server = TelegramAPIServer.from_base(proxy)
        if DEBUG:
            log.info("Using Telegram API proxy: %s", proxy)
            log.info("API URL template: %s", server.base)
            test_url = server.base.format(token=token, method="getMe")
            log.info("Test getMe URL: %s", test_url)
        session = AiohttpSession(api=server)
        bot = Bot(token=token, session=session)
    else:
        log.info("Using direct Telegram connection")
        _diag_direct_connection()
        bot = Bot(token=token)

    dp = Dispatcher(storage=MemoryStorage())
    dp.update.middleware(LogAllUpdatesMiddleware())

    dp.include_router(auth.router)
    dp.include_router(commands.router)
    dp.include_router(callbacks.router)
    dp.include_router(messages.router)

    # Устанавливаем меню после включения роутеров
    dp.startup.register(_set_bot_commands)

    return bot, dp


def _diag_direct_connection():
    """Диагностика прямого подключения к Telegram (DNS + TCP + TLS).

    Помогает отличить проблемы DNS, TCP-блокировки и DPI/TLS-блокировки.
    """
    host = "api.telegram.org"
    port = 443

    # 1. DNS-резолвинг
    try:
        ips = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        resolved = list(set(addr[4][0] for addr in ips))
        log.info("[DIAG] DNS resolved %s → %s", host, resolved)
    except Exception as e:
        log.error("[DIAG] DNS failed for %s: %s", host, e)
        return

    # 2. TCP-подключение
    for ip in resolved:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                result = s.connect_ex((ip, port))
                if result == 0:
                    log.info("[DIAG] TCP %s:%d — OK", ip, port)
                else:
                    log.warning("[DIAG] TCP %s:%d — FAILED (errno=%d)", ip, port, result)
        except Exception as e:
            log.warning("[DIAG] TCP %s:%d — ERROR: %s", ip, port, e)

    # 3. TLS-проверка (имитируем реальное HTTPS-подключение)
    import ssl
    ctx = ssl.create_default_context()
    try:
        with socket.create_connection((host, port), timeout=10) as s:
            with ctx.wrap_socket(s, server_hostname=host) as tls:
                log.info("[DIAG] TLS to %s — OK (cipher: %s, version: %s)",
                         host, tls.cipher(), tls.version())
    except ssl.SSLError as e:
        log.error("[DIAG] TLS to %s — SSL ERROR (likely DPI/SNI block): %s", host, e)
    except ConnectionResetError:
        log.error("[DIAG] TLS to %s — CONNECTION RESET (likely DPI/SNI block)", host)
    except OSError as e:
        log.error("[DIAG] TLS to %s — OS ERROR: %s", host, e)
    except Exception as e:
        log.error("[DIAG] TLS to %s — FAILED: %s", host, e)