import io
import os
import re
import math
import time
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
# ENVIRONMENT & PAYLOAD VARIABLES
# ==========================================
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
MONGO_URI = os.environ.get("MONGO_URI", "")
DUMP_CHANNEL_ID = int(os.environ.get("DUMP_CHANNEL_ID", "0"))

USER_ID = int(os.environ.get("USER_ID", "0"))
CHAT_ID = int(os.environ.get("CHAT_ID", "0"))
STATUS_MSG_ID = int(os.environ.get("STATUS_MSG_ID", "0"))
PEER_ID = os.environ.get("PEER_ID", "")
MSG_IDS_RAW = os.environ.get("MSG_IDS", "")
MAX_MB = int(os.environ.get("MAX_MB", "500"))
WORKERS = int(os.environ.get("WORKERS", "16"))
SOURCE_LINK = os.environ.get("SOURCE_LINK", "")
HELPER_BOT_ID = int(os.environ.get("HELPER_BOT_ID", "0") or "0")

PART_SIZE = 512 * 1024  # 512 KB

# ==========================================
# BLOCK PROGRESS BAR [███░░░░░░░] 30% & HELPERS
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
                await asyncio.sleep(1.5)

async def resolve_dump_channel(client: TelegramClient):
    if not DUMP_CHANNEL_ID:
        return None
    try:
        return await client.get_input_entity(DUMP_CHANNEL_ID)
    except Exception:
        raw_id = int(str(abs(DUMP_CHANNEL_ID))[3:]) if str(abs(DUMP_CHANNEL_ID)).startswith("100") else abs(DUMP_CHANNEL_ID)
        return await client.get_input_entity(PeerChannel(raw_id))

async def send_chat_upload_action(bot_client: TelegramClient, is_video: bool, pct: int = 50):
    try:
        action = SendMessageUploadVideoAction(progress=pct) if is_video else SendMessageUploadDocumentAction(progress=pct)
        await bot_client(SetTypingRequest(peer=CHAT_ID, action=action))
    except Exception:
        pass

# ==========================================
# FAST PARALLEL MTPROTO SENDER POOL
# ==========================================
class PersistentSenderPool:
    def __init__(self):
        self.dl_pools = {}
        self.ul_pool = []

    async def get_dl_senders(self, user_client: TelegramClient, dc_id: int, count: int):
        if dc_id in self.dl_pools and len(self.dl_pools[dc_id]) >= count:
            return self.dl_pools[dc_id][:count]
        senders = await self._create_pool(user_client, dc_id, count)
        self.dl_pools[dc_id] = senders
        return senders

    async def get_ul_senders(self, upload_client: TelegramClient, count: int):
        dc_id = upload_client.session.dc_id
        if len(self.ul_pool) >= count:
            return self.ul_pool[:count]
        self.ul_pool = await self._create_pool(upload_client, dc_id, count)
        return self.ul_pool

    async def _create_pool(self, client: TelegramClient, dc_id: int, count: int):
        dc = await client._get_dc(dc_id)
        senders = []
        auth_key = None

        if dc_id == client.session.dc_id:
            auth_key = client.session.auth_key
        else:
            first_sender = MTProtoSender(None, loggers=client._log)
            await first_sender.connect(
                client._connection(
                    dc.ip_address, dc.port, dc.id,
                    loggers=client._log, proxy=client._proxy
                )
            )
            auth = await client(ExportAuthorizationRequest(dc_id))
            client._init_request.query = ImportAuthorizationRequest(id=auth.id, bytes=auth.bytes)
            await first_sender.send(InvokeWithLayerRequest(LAYER, client._init_request))
            auth_key = first_sender.auth_key
            senders.append(first_sender)

        remaining = count - len(senders)

        async def connect_single():
            s = MTProtoSender(auth_key, loggers=client._log)
            await s.connect(
                client._connection(
                    dc.ip_address, dc.port, dc.id,
                    loggers=client._log, proxy=client._proxy
                )
            )
            client._init_request.query = GetConfigRequest()
            await s.send(InvokeWithLayerRequest(LAYER, client._init_request))
            return s

        extra_senders = await asyncio.gather(*(connect_single() for _ in range(remaining)))
        senders.extend(extra_senders)
        return senders

    async def close_all(self):
        all_senders = list(self.ul_pool)
        for pool in self.dl_pools.values():
            all_senders.extend(pool)
        for s in all_senders:
            try:
                await s.disconnect()
            except Exception:
                pass

# ==========================================
# ADAPTIVE PIPELINED TURBO STREAMING ENGINE
# ==========================================
async def fetch_single_part(dl_sender: MTProtoSender, location, part_idx: int, file_size: int, throttled_flag: list):
    offset = part_idx * PART_SIZE
    expected_len = min(PART_SIZE, file_size - offset)

    for attempt in range(16):
        try:
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
            return part_idx, bytes(buf[:expected_len])
        except Exception as e:
            wait_sec = extract_wait_seconds(e)
            if wait_sec > 0:
                throttled_flag[0] = True
                jitter = random.uniform(0.3, 1.0)
                await asyncio.sleep(wait_sec + jitter)
            else:
                if attempt == 15:
                    raise
                await asyncio.sleep(0.8 + (attempt * 0.4))

async def upload_single_part(
    ul_sender: MTProtoSender,
    is_big: bool,
    file_id: int,
    part_idx: int,
    total_parts: int,
    chunk_data: bytes
):
    for attempt in range(12):
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
                await asyncio.sleep(wait_sec + 1.0)
            else:
                if attempt == 11:
                    raise
                await asyncio.sleep(0.8)

async def adaptive_pipelined_stream(
    pool: PersistentSenderPool,
    user_client: TelegramClient,
    upload_client: TelegramClient,
    notify_bot: TelegramClient,
    document,
    file_name: str,
    step_counter: str,
    worker_count: int,
    is_vip_mode: bool,
    mode_badge: str,
    is_video: bool
):
    """
    VIP Mode တွင် 16x-24x Workers အပြည့်နှင့် Adaptive Pipelining (Batch 2x DL / 4x UL) သုံးပြီး
    Standard Mode တွင် သတ်မှတ်ထားသော Free Workers (ဥပမာ 4x) ဖြင့် လုပ်ဆောင်ပေးသည့် အင်ဂျင်
    """
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

    # VIP ဖြစ်ပါက 16x/24x Workers အပြည့်ဖွင့်ပေးပြီး Free ဖြစ်ပါက Standard Workers သာ သုံးမည်
    if is_vip_mode:
        dl_worker_count = min(worker_count, total_parts)
        ul_worker_count = min(worker_count, total_parts)
        dl_pipeline = 2
        ul_pipeline = 4
    else:
        dl_worker_count = min(worker_count, 6, total_parts)
        ul_worker_count = min(worker_count, 6, total_parts)
        dl_pipeline = 1
        ul_pipeline = 2

    dl_senders, ul_senders = await asyncio.gather(
        pool.get_dl_senders(user_client, document.dc_id, dl_worker_count),
        pool.get_ul_senders(upload_client, ul_worker_count)
    )

    part_queue = asyncio.Queue()
    for part_idx in range(total_parts):
        part_queue.put_nowait(part_idx)

    chunk_queue = asyncio.Queue(maxsize=ul_worker_count * ul_pipeline * 2)

    uploaded_bytes = 0
    start_time = time.time()
    last_edit_time = [0.0]
    throttled_flag = [False]  # Non-Premium Wait ထိပါက Pipeline အလိုအလျောက် ချိန်ညှိရန်

    async def dl_producer(idx: int, dl_sender: MTProtoSender):
        await asyncio.sleep(idx * 0.06)
        while not part_queue.empty():
            # Non-Premium Wait မထိသေးသရွေ့ VIP တွင် Pipelining (2x) ဖြင့် အမြန်ဆုံး ဆွဲမည်
            curr_batch_size = 1 if throttled_flag[0] else dl_pipeline
            batch_indices = []
            for _ in range(curr_batch_size):
                if part_queue.empty():
                    break
                try:
                    batch_indices.append(part_queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if not batch_indices:
                break

            results = await asyncio.gather(*(
                fetch_single_part(dl_sender, location, p_idx, file_size, throttled_flag)
                for p_idx in batch_indices
            ))

            for p_idx, c_data in results:
                await chunk_queue.put((p_idx, c_data))
                part_queue.task_done()

            # Free User ဖြစ်ပါက Standard Speed ဖြစ်စေရန် အနည်းငယ် Pacing ထိန်းမည်
            if not is_vip_mode:
                await asyncio.sleep(0.08)

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
                upload_single_part(ul_sender, is_big, file_id, p_idx, total_parts, c_data)
                for p_idx, c_data in batch_items
            ))

            uploaded_bytes += sum(sizes)
            for _ in batch_items:
                chunk_queue.task_done()

            now = time.time()
            if now - last_edit_time[0] >= 2.6:
                last_edit_time[0] = now
                elapsed = max(now - start_time, 0.1)
                speed_bps = uploaded_bytes / elapsed
                speed_mb = speed_bps / (1024 * 1024)
                pct = min(99.0, (uploaded_bytes * 100) / file_size)
                curr_mb = uploaded_bytes / (1024 * 1024)
                tot_mb = file_size / (1024 * 1024)
                rem_sec = (file_size - uploaded_bytes) / max(speed_bps, 1)

                bar_line = make_block_bar(pct)
                asyncio.create_task(send_chat_upload_action(notify_bot, is_video, int(pct)))
                try:
                    await notify_bot.edit_message(
                        CHAT_ID,
                        STATUS_MSG_ID,
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

    await asyncio.gather(*producers)
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
    peer_key: str,
    msg_id: int,
    file_name: str,
    file_size: int,
    mime_type: str
):
    if not main_dump_entity or not dump_msg_id:
        return
    try:
        await bot_client.send_message(
            main_dump_entity,
            f"🗂️ **Backup Audit Log**\n"
            f"👤 **User ID:** `{USER_ID}`\n"
            f"🔗 **Source:** `{SOURCE_LINK}` (Post `{msg_id}`)\n"
            f"📦 **Size:** `{file_size / (1024 * 1024):.2f} MB`",
            reply_to=dump_msg_id.id if hasattr(dump_msg_id, 'id') else dump_msg_id
        )
        cache_key = f"{peer_key}:{msg_id}"
        await cache_col.update_one(
            {"cache_key": cache_key},
            {"$set": {
                "cache_key": cache_key,
                "peer_id": peer_key,
                "msg_id": msg_id,
                "dump_msg_id": dump_msg_id,
                "file_name": file_name,
                "file_size": file_size,
                "mime_type": mime_type,
                "cached_by": USER_ID,
                "cached_at": time.time()
            }},
            upsert=True
        )
    except Exception as e:
        print(f"DB Cache Save Error: {e}")

# ==========================================
# MAIN CLONING WORKFLOW
# ==========================================
async def process_cloning():
    bot_client = TelegramClient(StringSession(), API_ID, API_HASH)
    await bot_client.start(bot_token=BOT_TOKEN)

    mongo = AsyncMongoClient(MONGO_URI)
    db = mongo["restricted_saver_db"]
    users_col = db["users"]
    settings_col = db["settings"]
    cache_col = db["file_cache"]

    user_doc = await users_col.find_one({"user_id": USER_ID})
    if not user_doc or not user_doc.get("session_string"):
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **Session မရှိတော့ပါ။** `/start` နှိပ်ပြီး ပြန်လည် Login ဝင်ပါ။")
        await bot_client.disconnect()
        await mongo.close()
        return

    is_vip_mode = bool(user_doc.get("is_vip")) or (WORKERS >= 10)
    mode_badge = f"💎 VIP Turbo ({WORKERS}x Workers)" if is_vip_mode else f"⚡ Standard ({WORKERS}x Workers)"

    upload_client = bot_client
    use_helper_bridge = False
    helper_dump_entity = None

    main_dump_entity = await resolve_dump_channel(bot_client)

    if HELPER_BOT_ID and DUMP_CHANNEL_ID and main_dump_entity:
        cfg_doc = await settings_col.find_one({"_id": "global_config"})
        if cfg_doc:
            for h in cfg_doc.get("helper_bots", []):
                if h["id"] == HELPER_BOT_ID:
                    try:
                        h_client = TelegramClient(StringSession(), API_ID, API_HASH)
                        await h_client.start(bot_token=h["token"])
                        helper_dump_entity = await resolve_dump_channel(h_client)
                        if helper_dump_entity:
                            upload_client = h_client
                            use_helper_bridge = True
                    except Exception as e:
                        print(f"Helper bot init fallback to main bot: {e}")
                    break

    peer = int(PEER_ID) if PEER_ID.lstrip("-").isdigit() else PEER_ID
    msg_ids = [int(x) for x in MSG_IDS_RAW.split(",") if x.strip().isdigit()]

    user_client = TelegramClient(StringSession(user_doc["session_string"]), API_ID, API_HASH)
    await user_client.connect()

    if not await user_client.is_user_authorized():
        await bot_client.edit_message(CHAT_ID, STATUS_MSG_ID, "❌ **သင်၏ Session သက်တမ်းကုန်သွားပါပြီ။** `/start` နှိပ်ပြီး ပြန်ဝင်ပါ။")
        await user_client.disconnect()
        if use_helper_bridge:
            await upload_client.disconnect()
        await bot_client.disconnect()
        await mongo.close()
        return

    max_allowed_bytes = MAX_MB * 1024 * 1024
    completed_files = 0
    completed_bytes = 0
    sender_pool = PersistentSenderPool()

    try:
        total_posts = len(msg_ids)
        for idx, msg_id in enumerate(msg_ids, start=1):
            step_counter = f"{idx}/{total_posts}"
            msg = await user_client.get_messages(peer, ids=msg_id)

            if not msg:
                await bot_client.send_message(CHAT_ID, f"⚠️ `({step_counter})` ပို့စ်နံပါတ် `{msg_id}` ကို ရှာမတွေ့ပါ။")
                continue

            # ၁။ စာသားသက်သက်
            if not msg.media:
                if msg.text:
                    sent_msg = await bot_client.send_message(CHAT_ID, msg.text, formatting_entities=msg.entities)
                    if main_dump_entity:
                        d_msg = await bot_client.send_message(main_dump_entity, sent_msg)
                        await save_cache_record(bot_client, main_dump_entity, cache_col, d_msg.id, PEER_ID, msg_id, "text", 0, "text/plain")
                    completed_files += 1
                continue

            f_size = getattr(msg.file, "size", 0) or 0
            if f_size > max_allowed_bytes:
                await bot_client.send_message(
                    CHAT_ID,
                    f"⚠️ `({step_counter})` ပို့စ် `{msg_id}` ၏ ဖိုင်ဆိုဒ် (`{f_size / (1024 * 1024):.1f} MB`) သည် သင်၏ Limit (`{MAX_MB} MB`) ထက် ကျော်လွန်နေပါသည်။"
                )
                continue

            # ၂။ ပုံအသေး သို့မဟုတ် 2MB အောက် ဖိုင်အသေးများ
            if not msg.document or f_size < 2 * 1024 * 1024:
                await bot_client.edit_message(
                    CHAT_ID,
                    STATUS_MSG_ID,
                    f"⚙️ **Processing... ({step_counter})**\n"
                    f"`{make_block_bar(80)}`\n\n"
                    f"🚀 **Mode:** `{mode_badge}`"
                )
                ram_file = io.BytesIO()
                await safe_download_media(user_client, msg, file=ram_file)
                ram_file.seek(0)
                f_name = getattr(msg.file, "name", None) or f"file_{msg_id}" + (getattr(msg.file, "ext", "") or ".jpg")
                ram_file.name = f_name

                sent_msg = await bot_client.send_file(
                    CHAT_ID,
                    file=ram_file,
                    caption=msg.text or "",
                    formatting_entities=msg.entities
                )
                if main_dump_entity:
                    d_msg = await bot_client.send_message(main_dump_entity, sent_msg)
                    await save_cache_record(
                        bot_client, main_dump_entity, cache_col, d_msg.id,
                        PEER_ID, msg_id, f_name, f_size, getattr(msg.file, "mime_type", "image/jpeg")
                    )
                completed_files += 1
                completed_bytes += f_size
                if total_posts > 1:
                    await asyncio.sleep(1.2)
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

            asyncio.create_task(send_chat_upload_action(bot_client, is_video, 5))
            await bot_client.edit_message(
                CHAT_ID,
                STATUS_MSG_ID,
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

            input_file = await adaptive_pipelined_stream(
                pool=sender_pool,
                user_client=user_client,
                upload_client=upload_client,
                notify_bot=bot_client,
                document=doc,
                file_name=file_name,
                step_counter=step_counter,
                worker_count=WORKERS,
                is_vip_mode=is_vip_mode,
                mode_badge=mode_badge,
                is_video=is_video
            )

            await bot_client.edit_message(
                CHAT_ID,
                STATUS_MSG_ID,
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
                await bot_client.send_message(CHAT_ID, main_copy)
                await save_cache_record(
                    bot_client, main_dump_entity, cache_col, helper_dump_msg.id,
                    PEER_ID, msg_id, file_name, doc.size, doc.mime_type or "video/mp4"
                )
            else:
                sent_msg = await bot_client.send_file(
                    CHAT_ID,
                    file=media,
                    caption=msg.text or "",
                    formatting_entities=msg.entities
                )
                if main_dump_entity:
                    d_msg = await bot_client.send_message(main_dump_entity, sent_msg)
                    await save_cache_record(
                        bot_client, main_dump_entity, cache_col, d_msg.id,
                        PEER_ID, msg_id, file_name, doc.size, doc.mime_type or "video/mp4"
                    )

            completed_files += 1
            completed_bytes += doc.size

            if total_posts > 1:
                await asyncio.sleep(1.5)

        if completed_files > 0:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            await users_col.update_one(
                {"user_id": USER_ID},
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

        await bot_client.delete_messages(CHAT_ID, STATUS_MSG_ID)

    except Exception as e:
        await bot_client.edit_message(
            CHAT_ID,
            STATUS_MSG_ID,
            f"❌ **အမှားဖြစ်ပေါ်နေပါသည်:** `{str(e)}`"
        )
    finally:
        await sender_pool.close_all()
        await user_client.disconnect()
        if use_helper_bridge:
            await upload_client.disconnect()
        await bot_client.disconnect()
        await mongo.close()

if __name__ == "__main__":
    asyncio.run(process_cloning())
