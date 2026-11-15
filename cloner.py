import io
import os
import re
import math
import time
import uuid
import socket
import random
import asyncio
from datetime import datetime, timezone

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

from pymongo import AsyncMongoClient
from telethon import TelegramClient, helpers
from telethon.sessions import StringSession
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.help import GetConfigRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import GetFileRequest, SaveFilePartRequest, SaveBigFilePartRequest
from telethon.tl.functions.messages import SetTypingRequest
from telethon.tl.types import (
    PeerChannel,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputMediaUploadedDocument,
    DocumentAttributeVideo,
    DocumentAttributeFilename,
    SendMessageUploadVideoAction,
    SendMessageUploadDocumentAction
)

# ==========================================
# ENVIRONMENT VARIABLES
# ==========================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
DUMP_CHANNEL_ID = int(os.environ.get("DUMP_CHANNEL_ID", "0"))

WORKER_REPO_NAME = os.environ.get("WORKER_REPO_NAME", "standby-worker")
WORKER_ID = f"{WORKER_REPO_NAME.split('/')[-1]}-{os.environ.get('GITHUB_RUN_ID', uuid.uuid4().hex[:6])}"

MAX_DAEMON_LIFETIME = 19000
PART_SIZE = 512 * 1024  # 512 KB

# ==========================================
# UI HELPERS & SOCKET TUNING
# ==========================================
def make_block_bar(pct: float, length: int = 10) -> str:
    filled = min(length, max(0, int(round((pct / 100) * length))))
    bar = "█" * filled + "░" * (length - filled)
    return f"[{bar}] {int(pct)}%"

def format_eta(seconds: float) -> str:
    sec = max(1, int(seconds))
    if sec < 60:
        return f"{sec}s"
    mins, rem_s = divmod(sec, 60)
    return f"{mins}m {rem_s}s"

def extract_wait_seconds(err: Exception) -> float:
    if hasattr(err, "seconds") and getattr(err, "seconds"):
        return float(err.seconds)
    msg = str(err)
    match = re.search(r"wait of (\d+) seconds", msg, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return 0.0

def tune_sender_socket(sender: MTProtoSender):
    try:
        conn = getattr(sender, "_connection", None)
        sock = getattr(conn, "socket", None)
        if sock is not None:
            raw_sock = getattr(sock, "_sock", sock)
            raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
            raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 * 1024 * 1024)
    except Exception:
        pass

async def safe_download_media(client: TelegramClient, msg, **kwargs):
    for attempt in range(8):
        try:
            return await client.download_media(msg, **kwargs)
        except Exception as e:
            wait_sec = extract_wait_seconds(e)
            if wait_sec > 0:
                await asyncio.sleep(wait_sec + 1.0)
            else:
                if attempt == 7:
                    raise
                await asyncio.sleep(1.2)

async def resolve_dump_channel(client: TelegramClient):
    if not DUMP_CHANNEL_ID:
        return None
    try:
        return await client.get_input_entity(DUMP_CHANNEL_ID)
    except Exception:
        raw_id = int(str(abs(DUMP_CHANNEL_ID))[3:]) if str(abs(DUMP_CHANNEL_ID)).startswith("100") else abs(DUMP_CHANNEL_ID)
        return await client.get_input_entity(PeerChannel(raw_id))

async def send_chat_upload_action(bot_client: TelegramClient, chat_id: int, is_video: bool, pct: int = 50):
    try:
        action = SendMessageUploadVideoAction(progress=pct) if is_video else SendMessageUploadDocumentAction(progress=pct)
        await bot_client(SetTypingRequest(peer=chat_id, action=action))
    except Exception:
        pass

# ==========================================
# PRE-WARMED SENDER POOL & SESSION CACHE
# ==========================================
class PersistentSenderPool:
    def __init__(self):
        self.dl_pools = {}
        self.ul_pools = {}

    async def get_dl_senders(self, user_client: TelegramClient, dc_id: int, count: int):
        if dc_id in self.dl_pools and len(self.dl_pools[dc_id]) >= count:
            return self.dl_pools[dc_id][:count]
        senders = await self._create_pool(user_client, dc_id, count, is_upload=False)
        self.dl_pools[dc_id] = senders
        return senders

    async def get_ul_senders(self, upload_client: TelegramClient, bot_key: int, count: int):
        if bot_key in self.ul_pools and len(self.ul_pools[bot_key]) >= count:
            return self.ul_pools[bot_key][:count]
        dc_id = upload_client.session.dc_id
        senders = await self._create_pool(upload_client, dc_id, count, is_upload=True)
        self.ul_pools[bot_key] = senders
        return senders

    async def _create_pool(self, client: TelegramClient, dc_id: int, count: int, is_upload: bool):
        dc = await client._get_dc(dc_id)
        senders = []
        auth_keys = []

        if dc_id == client.session.dc_id:
            auth_keys.append(client.session.auth_key)
        else:
            first_sender = MTProtoSender(None, loggers=client._log)
            await first_sender.connect(
                client._connection(
                    dc.ip_address, dc.port, dc.id,
                    loggers=client._log, proxy=client._proxy
                )
            )
            tune_sender_socket(first_sender)
            auth = await client(ExportAuthorizationRequest(dc_id))
            client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            await first_sender.send(InvokeWithLayerRequest(LAYER, client._init_request))
            auth_keys.append(first_sender.auth_key)
            senders.append(first_sender)

        if not is_upload:
            for _ in range(3):
                if len(senders) >= count:
                    break
                try:
                    ind_s = MTProtoSender(None, loggers=client._log)
                    await ind_s.connect(
                        client._connection(
                            dc.ip_address, dc.port, dc.id,
                            loggers=client._log, proxy=client._proxy
                        )
                    )
                    tune_sender_socket(ind_s)
                    auth = await client(ExportAuthorizationRequest(dc_id))
                    client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
                    await ind_s.send(InvokeWithLayerRequest(LAYER, client._init_request))
                    auth_keys.append(ind_s.auth_key)
                    senders.append(ind_s)
                except Exception:
                    break

        remaining = count - len(senders)

        async def connect_single(idx: int):
            s = MTProtoSender(auth_keys[idx % len(auth_keys)], loggers=client._log)
            await s.connect(
                client._connection(
                    dc.ip_address, dc.port, dc.id,
                    loggers=client._log, proxy=client._proxy
                )
            )
            tune_sender_socket(s)
            client._init_request.query = GetConfigRequest()
            await s.send(InvokeWithLayerRequest(LAYER, client._init_request))
            return s

        if remaining > 0:
            extra_senders = await asyncio.gather(*(connect_single(i) for i in range(remaining)))
            senders.extend(extra_senders)

        return senders

    async def close_dl_pools(self):
        for pool in self.dl_pools.values():
            for s in pool:
                try:
                    await s.disconnect()
                except Exception:
                    pass
        self.dl_pools.clear()

    async def close_all(self):
        await self.close_dl_pools()
        for pool in self.ul_pools.values():
            for s in pool:
                try:
                    await s.disconnect()
                except Exception:
                    pass
        self.ul_pools.clear()

# ==========================================
# SWEET-SPOT PACED + BANDWIDTH SHAPED ENGINE
# ==========================================
async def download_part_once(dl_sender: MTProtoSender, location, part_idx: int, file_size: int) -> bytes:
    offset = part_idx * PART_SIZE
    expected_len = min(PART_SIZE, file_size - offset)
    buf = bytearray()
    curr_off = offset

    while len(buf) < expected_len:
        res = await dl_sender.send(
            GetFileRequest(location, offset=curr_off, limit=PART_SIZE)
        )
        if not res.bytes:
            break
        buf.extend(res.bytes)
        curr_off += len(res.bytes)

    return bytes(buf[:expected_len])

async def upload_part_with_retry(
    ul_sender: MTProtoSender,
    is_big: bool,
    file_id: int,
    part_idx: int,
    total_parts: int,
    chunk_data: bytes
):
    for attempt in range(14):
        try:
            if is_big:
                await ul_sender.send(
                    SaveBigFilePartRequest(file_id, part_idx, total_parts, chunk_data)
                )
            else:
                await ul_sender.send(
                    SaveFilePartRequest(file_id, part_idx, chunk_data)
                )
            return len(chunk_data)
        except Exception as e:
            wait_sec = extract_wait_seconds(e)
            if wait_sec > 0:
                await asyncio.sleep(wait_sec + 0.5)
            else:
                if attempt == 13:
                    raise
                await asyncio.sleep(0.5)

async def stream_document_with_shaper(
    pool: PersistentSenderPool,
    user_client: TelegramClient,
    upload_client: TelegramClient,
    upload_bot_key: int,
    notify_bot: TelegramClient,
    chat_id: int,
    status_msg_id: int,
    document,
    file_name: str,
    step_counter: str,
    worker_count: int,
    max_speed_mb: int,
    is_vip_mode: bool,
    mode_badge: str,
    is_video: bool
):
    file_size = document.size
    total_parts = math.ceil(file_size / PART_SIZE)
    file_id = helpers.generate_random_long()
    is_big = file_size > 10 * 1024 * 1024

    location = InputDocumentFileLocation(
        id=document.id,
        access_hash=document.access_hash,
        file_reference=document.file_reference,
        thumb_size=""
    )

    if is_vip_mode:
        dl_worker_count = min(10, worker_count, total_parts)
        ul_worker_count = min(worker_count, total_parts)
        ul_pipeline = 4
    else:
        dl_worker_count = min(worker_count, 4, total_parts)
        ul_worker_count = min(worker_count, 6, total_parts)
        ul_pipeline = 2

    dl_senders, ul_senders = await asyncio.gather(
        pool.get_dl_senders(user_client, document.dc_id, dl_worker_count),
        pool.get_ul_senders(upload_client, upload_bot_key, ul_worker_count)
    )

    part_queue = asyncio.Queue()
    for part_idx in range(total_parts):
        part_queue.put_nowait((part_idx, 0))

    chunk_queue = asyncio.Queue(maxsize=ul_worker_count * ul_pipeline * 3)

    downloaded_bytes = 0
    uploaded_bytes = 0
    start_time = time.time()
    last_edit_time = [0.0]

    max_speed_bps = (max_speed_mb * 1024 * 1024) if (max_speed_mb and max_speed_mb > 0 and not is_vip_mode) else 0

    last_req_ts = [0.0]
    pacer_lock = asyncio.Lock()

    async def dl_producer(idx: int, dl_sender: MTProtoSender):
        nonlocal downloaded_bytes
        await asyncio.sleep(idx * 0.05)
        while True:
            try:
                p_idx, retries = part_queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            async with pacer_lock:
                now_p = time.time()
                gap = 0.032 if is_vip_mode else 0.08
                if now_p - last_req_ts[0] < gap:
                    await asyncio.sleep(gap - (now_p - last_req_ts[0]))
                last_req_ts[0] = time.time()

            if max_speed_bps > 0:
                elapsed_dl = max(time.time() - start_time, 0.05)
                expected_time = downloaded_bytes / max_speed_bps
                if expected_time > elapsed_dl:
                    await asyncio.sleep(min(1.5, expected_time - elapsed_dl))

            try:
                c_data = await download_part_once(dl_sender, location, p_idx, file_size)
                downloaded_bytes += len(c_data)
                await chunk_queue.put((p_idx, c_data))
                part_queue.task_done()
            except Exception as e:
                wait_sec = extract_wait_seconds(e)
                if retries >= 18:
                    part_queue.task_done()
                    raise
                part_queue.put_nowait((p_idx, retries + 1))
                part_queue.task_done()
                if wait_sec > 0:
                    await asyncio.sleep(wait_sec + random.uniform(0.2, 0.5))
                else:
                    await asyncio.sleep(0.5)

    async def ul_consumer(ul_sender: MTProtoSender):
        nonlocal uploaded_bytes
        while True:
            first_item = await chunk_queue.get()
            if first_item is None:
                chunk_queue.task_done()
                break

            batch_items = [first_item]
            for _ in range(ul_pipeline - 1):
                try:
                    nxt = chunk_queue.get_nowait()
                    if nxt is None:
                        chunk_queue.put_nowait(None)
                        break
                    batch_items.append(nxt)
                except asyncio.QueueEmpty:
                    break

            sizes = await asyncio.gather(*(
                upload_part_with_retry(ul_sender, is_big, file_id, p_idx, total_parts, c_data)
                for p_idx, c_data in batch_items
            ))

            uploaded_bytes += sum(sizes)
            for _ in batch_items:
                chunk_queue.task_done()

            now = time.time()
            if now - last_edit_time[0] >= 2.5:
                last_edit_time[0] = now
                elapsed = max(now - start_time, 0.1)
                speed_bps = uploaded_bytes / elapsed
                speed_mb = speed_bps / (1024 * 1024)
                pct = min(99.0, (uploaded_bytes * 100) / file_size)
                curr_mb = uploaded_bytes / (1024 * 1024)
                tot_mb = file_size / (1024 * 1024)
                rem_sec = (file_size - uploaded_bytes) / max(speed_bps, 1)

                bar_line = make_block_bar(pct)
                asyncio.create_task(send_chat_upload_action(notify_bot, chat_id, is_video, int(pct)))
                try:
                    await notify_bot.edit_message(
                        chat_id,
                        status_msg_id,
                        f"⚙️ **Processing... ({step_counter})**\n"
                        f"`{bar_line}`\n\n"
                        f"🚀 **Mode:** `{mode_badge}`\n"
                        f"📦 **Size:** `{curr_mb:.1f} MB` / `{tot_mb:.1f} MB`\n"
                        f"⚡ **Speed:** `{speed_mb:.1f} MB/s` • ⏳ `{format_eta(rem_sec)}`"
                    )
                except Exception:
                    pass

    producers = [asyncio.create_task(dl_producer(i, s)) for i, s in enumerate(dl_senders)]
    consumers = [asyncio.create_task(ul_consumer(s)) for s in ul_senders]

    await part_queue.join()
    for p in producers:
        p.cancel()

    for _ in range(ul_worker_count):
        await chunk_queue.put(None)
    await asyncio.gather(*consumers)

    if is_big:
        return InputFileBig(id=file_id, parts=total_parts, name=file_name)
    return InputFile(id=file_id, parts=total_parts, name=file_name, md5_checksum="")

# ==========================================
# DB CHANNEL CACHE SAVER
# ==========================================
async def save_cache_record(
    bot_client: TelegramClient,
    main_dump_entity,
    cache_col,
    dump_msg_id: int,
    user_id: int,
    source_link: str,
    peer_key: str,
    msg_id: int,
    file_name: str,
    file_size: int,
    mime_type: str
):
    if not main_dump_entity or not dump_msg_id:
        return
    try:
        real_dump_id = dump_msg_id.id if hasattr(dump_msg_id, 'id') else int(dump_msg_id)
        await bot_client.send_message(
            main_dump_entity,
            f"🗂️ **Backup Audit Log**\n"
            f"👤 **User ID:** `{user_id}`\n"
            f"🔗 **Source:** `{source_link}` (Post `{msg_id}`)\n"
            f"📦 **Size:** `{file_size / (1024 * 1024):.2f} MB`",
            reply_to=real_dump_id
        )
        cache_key = f"{peer_key}:{msg_id}"
        await cache_col.update_one(
            {"cache_key": cache_key},
            {"$set": {
                "cache_key": cache_key,
                "peer_id": peer_key,
                "msg_id": msg_id,
                "dump_msg_id": real_dump_id,
                "file_name": file_name,
                "file_size": file_size,
                "mime_type": mime_type,
                "cached_by": user_id,
                "cached_at": time.time()
            }},
            upsert=True
        )
    except Exception as e:
        print(f"DB Cache Save Error: {e}")

# ==========================================
# SINGLE TASK EXECUTOR (WITH CHECKPOINTING)
# ==========================================
async def execute_task(
    task: dict,
    bot_client: TelegramClient,
    main_dump_entity,
    helper_pool: dict,
    sender_pool: PersistentSenderPool,
    users_col,
    cache_col,
    queue_col
):
    task_id = task["_id"]
    user_id = int(task["user_id"])
    chat_id = int(task["chat_id"])
    status_msg_id = int(task["status_msg_id"])
    peer_str = str(task["peer_id"])
    msg_ids = [int(x) for x in task["msg_ids"]]
    total_posts = int(task.get("total_count") or len(msg_ids))
    already_done_offset = max(0, total_posts - len(msg_ids))

    max_mb = int(task.get("max_mb", 500))
    workers = int(task.get("workers", 16))
    max_speed_mb = int(task.get("max_speed_mb", 0))
    is_vip_mode = bool(task.get("is_vip", False))
    source_link = str(task.get("source_link", ""))
    helper_bot_id = int(task.get("helper_bot_id", 0) or 0)

    mode_badge = (
        f"💎 VIP Turbo ({workers}x Workers)"
        if is_vip_mode else
        f"⚡ Standard ({workers}x • Max {max_speed_mb} MB/s)"
    )

    user_doc = await users_col.find_one({"user_id": user_id})
    if not user_doc or not user_doc.get("session_string"):
        await bot_client.edit_message(chat_id, status_msg_id, "❌ **Session မရှိတော့ပါ။** `/start` နှိပ်ပြီး ပြန်လည် Login ဝင်ပါ။")
        return

    upload_client = bot_client
    upload_bot_key = 0
    use_helper_bridge = False
    helper_dump_entity = None

    if helper_bot_id and helper_bot_id in helper_pool and main_dump_entity:
        h_info = helper_pool[helper_bot_id]
        upload_client = h_info["client"]
        helper_dump_entity = h_info["dump_entity"]
        upload_bot_key = helper_bot_id
        use_helper_bridge = True

    peer = int(peer_str) if peer_str.lstrip("-").isdigit() else peer_str

    user_client = TelegramClient(StringSession(user_doc["session_string"]), API_ID, API_HASH)
    await user_client.connect()

    if not await user_client.is_user_authorized():
        await bot_client.edit_message(chat_id, status_msg_id, "❌ **သင်၏ Session သက်တမ်းကုန်သွားပါပြီ။** `/start` နှိပ်ပြီး ပြန်ဝင်ပါ။")
        await user_client.disconnect()
        return

    max_allowed_bytes = max_mb * 1024 * 1024
    completed_files = 0
    completed_bytes = 0

    try:
        for idx, msg_id in enumerate(msg_ids, start=already_done_offset + 1):
            step_counter = f"{idx}/{total_posts}"
            msg = await user_client.get_messages(peer, ids=msg_id)

            if not msg:
                await bot_client.send_message(chat_id, f"⚠️ `({step_counter})` ပို့စ်နံပါတ် `{msg_id}` ကို ရှာမတွေ့ပါ။")
                await queue_col.update_one({"_id": task_id}, {"$pull": {"msg_ids": msg_id}, "$set": {"updated_at": time.time()}})
                continue

            # ၁။ စာသားသက်သက်
            if not msg.media:
                if msg.text:
                    sent_msg = await bot_client.send_message(chat_id, msg.text, formatting_entities=msg.entities)
                    if main_dump_entity:
                        d_msg = await bot_client.send_message(main_dump_entity, sent_msg)
                        await save_cache_record(
                            bot_client, main_dump_entity, cache_col, d_msg.id,
                            user_id, source_link, peer_str, msg_id, "text", 0, "text/plain"
                        )
                    completed_files += 1
                await queue_col.update_one({"_id": task_id}, {"$pull": {"msg_ids": msg_id}, "$set": {"updated_at": time.time()}})
                continue

            f_size = getattr(msg.file, "size", 0) or 0
            if f_size > max_allowed_bytes:
                await bot_client.send_message(
                    chat_id,
                    f"⚠️ `({step_counter})` ပို့စ် `{msg_id}` ၏ ဖိုင်ဆိုဒ် (`{f_size / (1024 * 1024):.1f} MB`) သည် သင်၏ Limit (`{max_mb} MB`) ထက် ကျော်လွန်နေပါသည်။"
                )
                await queue_col.update_one({"_id": task_id}, {"$pull": {"msg_ids": msg_id}, "$set": {"updated_at": time.time()}})
                continue

            # ၂။ ပုံအသေး သို့မဟုတ် 2MB အောက် ဖိုင်အသေးများ
            if not msg.document or f_size < 2 * 1024 * 1024:
                await bot_client.edit_message(
                    chat_id,
                    status_msg_id,
                    f"⚙️ **Processing... ({step_counter})**\n"
                    f"`{make_block_bar(85)}`\n\n"
                    f"🚀 **Mode:** `{mode_badge}`"
                )
                ram_file = io.BytesIO()
                await safe_download_media(user_client, msg, file=ram_file)
                ram_file.seek(0)
                f_name = getattr(msg.file, "name", None) or f"file_{msg_id}" + (getattr(msg.file, "ext", "") or ".jpg")
                ram_file.name = f_name

                sent_msg = await bot_client.send_file(
                    chat_id,
                    file=ram_file,
                    caption=msg.text or "",
                    formatting_entities=msg.entities
                )
                if main_dump_entity:
                    d_msg = await bot_client.send_message(main_dump_entity, sent_msg)
                    await save_cache_record(
                        bot_client, main_dump_entity, cache_col, d_msg.id,
                        user_id, source_link, peer_str, msg_id, f_name, f_size, getattr(msg.file, "mime_type", "image/jpeg")
                    )
                completed_files += 1
                completed_bytes += f_size
                await queue_col.update_one({"_id": task_id}, {"$pull": {"msg_ids": msg_id}, "$set": {"updated_at": time.time()}})
                if total_posts > 1:
                    await asyncio.sleep(1.0)
                continue

            # ၃။ 2MB အထက် ဖိုင်ကြီးများ
            doc = msg.document
            file_name = getattr(msg.file, "name", None) or f"video_{msg_id}{getattr(msg.file, 'ext', '') or '.mp4'}"
            size_mb = doc.size / (1024 * 1024)

            attributes = []
            has_filename = False
            is_video = False
            for attr in doc.attributes:
                if isinstance(attr, DocumentAttributeVideo):
                    is_video = True
                    attributes.append(DocumentAttributeVideo(
                        duration=attr.duration,
                        w=attr.w,
                        h=attr.h,
                        supports_streaming=True
                    ))
                elif isinstance(attr, DocumentAttributeFilename):
                    has_filename = True
                    attributes.append(attr)
                else:
                    attributes.append(attr)

            if not has_filename:
                attributes.append(DocumentAttributeFilename(file_name=file_name))

            asyncio.create_task(send_chat_upload_action(bot_client, chat_id, is_video, 5))
            await bot_client.edit_message(
                chat_id,
                status_msg_id,
                f"⚙️ **Processing... ({step_counter})**\n"
                f"`{make_block_bar(0)}`\n\n"
                f"🚀 **Mode:** `{mode_badge}`\n"
                f"📦 **Size:** `0.0 MB` / `{size_mb:.1f} MB`"
            )

            uploaded_thumb = None
            if doc.thumbs:
                try:
                    thumb_buf = io.BytesIO()
                    await safe_download_media(user_client, msg, thumb=-1, file=thumb_buf)
                    thumb_buf.seek(0)
                    thumb_buf.name = "thumb.jpg"
                    uploaded_thumb = await upload_client.upload_file(thumb_buf)
                except Exception:
                    uploaded_thumb = None

            input_file = await stream_document_with_shaper(
                pool=sender_pool,
                user_client=user_client,
                upload_client=upload_client,
                upload_bot_key=upload_bot_key,
                notify_bot=bot_client,
                chat_id=chat_id,
                status_msg_id=status_msg_id,
                document=doc,
                file_name=file_name,
                step_counter=step_counter,
                worker_count=workers,
                max_speed_mb=max_speed_mb,
                is_vip_mode=is_vip_mode,
                mode_badge=mode_badge,
                is_video=is_video
            )

            await bot_client.edit_message(
                chat_id,
                status_msg_id,
                f"⚙️ **Processing... ({step_counter})**\n"
                f"`{make_block_bar(100)}`\n\n"
                f"🚀 **Mode:** `{mode_badge}`\n"
                f"📦 **Size:** `{size_mb:.1f} MB` / `{size_mb:.1f} MB`\n"
                f"✨ **ပို့စ်ကို လက်ခံရရှိရန် အသင့်ဖြစ်ပါပြီ...**"
            )

            media = InputMediaUploadedDocument(
                file=input_file,
                mime_type=doc.mime_type or "application/octet-stream",
                attributes=attributes,
                thumb=uploaded_thumb,
                force_file=False
            )

            if use_helper_bridge and helper_dump_entity and main_dump_entity:
                helper_dump_msg = await upload_client.send_file(
                    helper_dump_entity,
                    file=media,
                    caption=msg.text or "",
                    formatting_entities=msg.entities
                )
                main_copy = await bot_client.get_messages(main_dump_entity, ids=helper_dump_msg.id)
                await bot_client.send_message(chat_id, main_copy)
                await save_cache_record(
                    bot_client, main_dump_entity, cache_col, helper_dump_msg.id,
                    user_id, source_link, peer_str, msg_id, file_name, doc.size, doc.mime_type or "video/mp4"
                )
            else:
                sent_msg = await bot_client.send_file(
                    chat_id,
                    file=media,
                    caption=msg.text or "",
                    formatting_entities=msg.entities
                )
                if main_dump_entity:
                    d_msg = await bot_client.send_message(main_dump_entity, sent_msg)
                    await save_cache_record(
                        bot_client, main_dump_entity, cache_col, d_msg.id,
                        user_id, source_link, peer_str, msg_id, file_name, doc.size, doc.mime_type or "video/mp4"
                    )

            completed_files += 1
            completed_bytes += doc.size

            # ပြီးသွားသော msg_id ကို Queue ထဲမှ ချက်ချင်း ဖယ်ရှားပေးခြင်း (Checkpoint)
            await queue_col.update_one(
                {"_id": task_id},
                {"$pull": {"msg_ids": msg_id}, "$set": {"updated_at": time.time()}}
            )

            if total_posts > 1:
                await asyncio.sleep(1.0)

        if completed_files > 0:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            await users_col.update_one(
                {"user_id": user_id},
                {
                    "$inc": {
                        "daily_count": completed_files,
                        "daily_bytes": completed_bytes,
                        "total_files": completed_files,
                        "total_bytes": completed_bytes
                    },
                    "$set": {"daily_date": today}
                }
            )

        await bot_client.delete_messages(chat_id, status_msg_id)

    except Exception as e:
        try:
            await bot_client.edit_message(
                chat_id,
                status_msg_id,
                f"❌ **အမှားဖြစ်ပေါ်နေပါသည်:** `{str(e)}`"
            )
        except Exception:
            pass
    finally:
        await sender_pool.close_dl_pools()
        await user_client.disconnect()

# ==========================================
# ALWAYS-READY STANDBY DAEMON MAIN LOOP
# ==========================================
async def run_standby_daemon():
    boot_ts = time.time()
    print(f"🔥 Starting Always-Ready Standby Worker Daemon: {WORKER_ID}")

    bot_client = TelegramClient(StringSession(), API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)
    main_dump_entity = await resolve_dump_channel(bot_client)

    mongo = AsyncMongoClient(MONGO_URI)
    db = mongo["restricted_saver_db"]
    users_col = db["users"]
    settings_col = db["settings"]
    cache_col = db["file_cache"]
    queue_col = db["task_queue"]
    nodes_col = db["worker_nodes"]

    helper_pool = {}

    async def sync_helper_bots():
        cfg_doc = await settings_col.find_one({"_id": "global_config"})
        if not cfg_doc:
            return
        for h in cfg_doc.get("helper_bots", []):
            hid = int(h["id"])
            if hid not in helper_pool:
                try:
                    h_client = TelegramClient(StringSession(), API_ID, API_HASH)
                    await h_client.start(bot_token=h["token"])
                    h_dump = await resolve_dump_channel(h_client)
                    if h_dump:
                        helper_pool[hid] = {
                            "client": h_client,
                            "dump_entity": h_dump,
                            "username": h["username"]
                        }
                        print(f"✅ Pre-warmed Helper Bot: @{h['username']}")
                except Exception as e:
                    print(f"Helper bot pre-warm warning: {e}")

    await sync_helper_bots()

    sender_pool = PersistentSenderPool()
    current_status = ["idle"]
    active_task_id = [None]
    tasks_done = [0]
    stop_signal = [False]

    async def heartbeat_reporter():
        while not stop_signal[0]:
            now_hb = time.time()
            try:
                await nodes_col.update_one(
                    {"worker_id": WORKER_ID},
                    {"$set": {
                        "worker_id": WORKER_ID,
                        "repo": WORKER_REPO_NAME,
                        "status": current_status[0],
                        "tasks_completed": tasks_done[0],
                        "started_at": boot_ts,
                        "last_heartbeat": now_hb
                    }},
                    upsert=True
                )
                # လက်ရှိ ဆွဲနေသော Task ကိုလည်း အသက်ရှင်နေကြောင်း 20s တစ်ကြိမ် Update လုပ်ပေးမည်
                if active_task_id[0] is not None:
                    await queue_col.update_one(
                        {"_id": active_task_id[0]},
                        {"$set": {"updated_at": now_hb}}
                    )
            except Exception:
                pass
            await asyncio.sleep(20)

    hb_task = asyncio.create_task(heartbeat_reporter())
    last_helper_sync = time.time()

    try:
        while time.time() - boot_ts < MAX_DAEMON_LIFETIME:
            if time.time() - last_helper_sync > 120:
                await sync_helper_bots()
                last_helper_sync = time.time()

            # Per-User Session Lock: အခြား Worker တစ်ခုက လက်ရှိဆွဲပေးနေသော User ၏ အကောင့်ကို
            # ပြိုင်တူ ဝင်မဆွဲမိစေရန် busy_users စစ်ဆေးခြင်း
            busy_users = []
            async for b_doc in queue_col.find({"status": "processing"}, {"user_id": 1}):
                busy_users.append(b_doc["user_id"])

            now_claim = time.time()
            task = await queue_col.find_one_and_update(
                {
                    "status": "pending",
                    "user_id": {"$nin": busy_users}
                },
                {"$set": {
                    "status": "processing",
                    "worker_id": WORKER_ID,
                    "started_at": now_claim,
                    "updated_at": now_claim
                }},
                sort=[("priority", 1), ("created_at", 1)]
            )

            if not task:
                await asyncio.sleep(0.35)
                continue

            current_status[0] = "busy"
            active_task_id[0] = task["_id"]
            await nodes_col.update_one(
                {"worker_id": WORKER_ID},
                {"$set": {"status": "busy", "last_heartbeat": time.time()}}
            )

            try:
                await execute_task(
                    task=task,
                    bot_client=bot_client,
                    main_dump_entity=main_dump_entity,
                    helper_pool=helper_pool,
                    sender_pool=sender_pool,
                    users_col=users_col,
                    cache_col=cache_col,
                    queue_col=queue_col
                )
                await queue_col.delete_one({"_id": task["_id"]})
                tasks_done[0] += 1
            except Exception as e:
                print(f"Task execution error: {e}")
                await queue_col.update_one(
                    {"_id": task["_id"]},
                    {"$set": {"status": "failed", "error": str(e)}}
                )
            finally:
                active_task_id[0] = None
                current_status[0] = "idle"
                await nodes_col.update_one(
                    {"worker_id": WORKER_ID},
                    {"$set": {"status": "idle", "last_heartbeat": time.time()}}
                )

    finally:
        stop_signal[0] = True
        hb_task.cancel()
        try:
            await nodes_col.delete_one({"worker_id": WORKER_ID})
        except Exception:
            pass
        await sender_pool.close_all()
        for h_info in helper_pool.values():
            try:
                await h_info["client"].disconnect()
            except Exception:
                pass
        await bot_client.disconnect()
        await mongo.close()
        print(f"👋 Standby Worker {WORKER_ID} gracefully shut down after completing {tasks_done[0]} tasks.")

if __name__ == "__main__":
    asyncio.run(run_standby_daemon())
