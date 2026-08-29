# 群活跃度统计插件：记录群聊消息，提供发言排行榜、个人活跃档案与活跃时段分析。
import datetime
import os
import time
from typing import Optional

import aiosqlite

from astrbot.api import logger, AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register

MEDALS = ["🥇", "🥈", "🥉"]

RANGE_WORDS = {
    "今天": ("today", "今天"),
    "今日": ("today", "今天"),
    "today": ("today", "今天"),
    "昨天": ("yesterday", "昨天"),
    "昨日": ("yesterday", "昨天"),
    "yesterday": ("yesterday", "昨天"),
    "本周": ("week", "本周"),
    "这周": ("week", "本周"),
    "week": ("week", "本周"),
    "上周": ("lastweek", "上周"),
    "lastweek": ("lastweek", "上周"),
    "本月": ("month", "本月"),
    "这个月": ("month", "本月"),
    "month": ("month", "本月"),
    "总计": ("total", "历史总计"),
    "总": ("total", "历史总计"),
    "全部": ("total", "历史总计"),
    "total": ("total", "历史总计"),
    "all": ("total", "历史总计"),
}


def resolve_range(word: Optional[str]):
    """把范围词解析成 (key, label, start_date, end_date)；total 时 start/end 为 None。"""
    w = (word or "今天").strip()
    info = RANGE_WORDS.get(w) or RANGE_WORDS.get(w.lower())
    if not info:
        return None
    key, label = info
    today = datetime.date.today()
    if key == "today":
        return key, label, today, today
    if key == "yesterday":
        y = today - datetime.timedelta(days=1)
        return key, label, y, y
    if key == "week":
        monday = today - datetime.timedelta(days=today.weekday())
        return key, label, monday, today
    if key == "lastweek":
        monday = today - datetime.timedelta(days=today.weekday())
        return key, label, monday - datetime.timedelta(days=7), monday - datetime.timedelta(days=1)
    if key == "month":
        return key, label, today.replace(day=1), today
    return key, label, None, None


def display_name(uid: str, name: str) -> str:
    name = (name or "").strip()
    if not name:
        name = f"用户{uid[-4:] if len(uid) >= 4 else uid}"
    if len(name) > 12:
        name = name[:12] + "…"
    return name


class ActivityDB:
    """基于 aiosqlite 的活跃度数据存储。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def init(self):
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS total_stat (
                group_id    TEXT NOT NULL,
                user_id     TEXT NOT NULL,
                count       INTEGER NOT NULL DEFAULT 0,
                last_active REAL NOT NULL DEFAULT 0,
                PRIMARY KEY (group_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS daily_stat (
                date     TEXT NOT NULL,
                group_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                count    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (date, group_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS hour_stat (
                group_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                hour     INTEGER NOT NULL,
                count    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (group_id, user_id, hour)
            );
            CREATE TABLE IF NOT EXISTS user_name (
                group_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                name     TEXT NOT NULL,
                PRIMARY KEY (group_id, user_id)
            );
            CREATE TABLE IF NOT EXISTS group_conf (
                group_id TEXT PRIMARY KEY,
                enabled  INTEGER NOT NULL DEFAULT 1
            );
            CREATE INDEX IF NOT EXISTS idx_daily_group_date ON daily_stat(group_id, date);
            """
        )
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def record(self, group_id: str, user_id: str, name: str):
        now = datetime.datetime.now()
        today = now.strftime("%Y-%m-%d")
        cur = await self._db.cursor()
        try:
            await cur.execute(
                """INSERT INTO total_stat (group_id, user_id, count, last_active)
                   VALUES (?, ?, 1, ?)
                   ON CONFLICT(group_id, user_id)
                   DO UPDATE SET count = count + 1, last_active = excluded.last_active""",
                (group_id, user_id, time.time()),
            )
            await cur.execute(
                """INSERT INTO daily_stat (date, group_id, user_id, count)
                   VALUES (?, ?, ?, 1)
                   ON CONFLICT(date, group_id, user_id)
                   DO UPDATE SET count = count + 1""",
                (today, group_id, user_id),
            )
            await cur.execute(
                """INSERT INTO hour_stat (group_id, user_id, hour, count)
                   VALUES (?, ?, ?, 1)
                   ON CONFLICT(group_id, user_id, hour)
                   DO UPDATE SET count = count + 1""",
                (group_id, user_id, now.hour),
            )
            if name:
                await cur.execute(
                    """INSERT INTO user_name (group_id, user_id, name)
                       VALUES (?, ?, ?)
                       ON CONFLICT(group_id, user_id)
                       DO UPDATE SET name = excluded.name
                       WHERE user_name.name != excluded.name""",
                    (group_id, user_id, name),
                )
            await self._db.commit()
        finally:
            await cur.close()

    async def rank(self, group_id: str, start, end, limit: int):
        """返回 [(user_id, name, count), ...]，按消息数降序。"""
        if start is None:
            sql = """SELECT s.user_id, IFNULL(n.name, '') AS name, s.count
                     FROM total_stat s
                     LEFT JOIN user_name n
                            ON n.group_id = s.group_id AND n.user_id = s.user_id
                     WHERE s.group_id = ?
                     ORDER BY s.count DESC
                     LIMIT ?"""
            args = (group_id, limit)
        else:
            sql = """SELECT s.user_id, IFNULL(n.name, '') AS name, SUM(s.count) AS c
                     FROM daily_stat s
                     LEFT JOIN user_name n
                            ON n.group_id = s.group_id AND n.user_id = s.user_id
                     WHERE s.group_id = ? AND s.date >= ? AND s.date <= ?
                     GROUP BY s.user_id
                     ORDER BY c DESC
                     LIMIT ?"""
            args = (group_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), limit)
        async with self._db.execute(sql, args) as cur:
            return await cur.fetchall()

    async def total_messages(self, group_id: str, start, end) -> int:
        if start is None:
            sql = "SELECT SUM(count) FROM total_stat WHERE group_id = ?"
            args = (group_id,)
        else:
            sql = "SELECT SUM(count) FROM daily_stat WHERE group_id = ? AND date >= ? AND date <= ?"
            args = (group_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        async with self._db.execute(sql, args) as cur:
            row = await cur.fetchone()
        return row[0] or 0

    async def user_summary(self, group_id: str, user_id: str, start, end):
        """返回 (count, rank)；没有记录时返回 (0, 0)。"""
        date_strs = (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")) if start else ()
        if start is None:
            own_sql = "SELECT SUM(count) FROM total_stat WHERE group_id = ? AND user_id = ?"
            rank_sql = "SELECT COUNT(*) + 1 FROM total_stat WHERE group_id = ? AND count > ?"
        else:
            own_sql = (
                "SELECT SUM(count) FROM daily_stat "
                "WHERE group_id = ? AND user_id = ? AND date >= ? AND date <= ?"
            )
            rank_sql = (
                "SELECT COUNT(*) + 1 FROM ("
                "  SELECT user_id FROM daily_stat"
                "  WHERE group_id = ? AND date >= ? AND date <= ?"
                "  GROUP BY user_id"
                "  HAVING SUM(count) > ?"
                ")"
            )
        async with self._db.execute(own_sql, (group_id, user_id, *date_strs)) as cur:
            row = await cur.fetchone()
        own = row[0] or 0
        if own <= 0:
            return 0, 0
        async with self._db.execute(rank_sql, (group_id, *date_strs, own)) as cur:
            rrow = await cur.fetchone()
        return own, rrow[0] or 1

    async def top_hours(self, group_id: str, user_id: str, limit: int = 3):
        sql = """SELECT hour, count FROM hour_stat
                 WHERE group_id = ? AND user_id = ?
                 ORDER BY count DESC
                 LIMIT ?"""
        async with self._db.execute(sql, (group_id, user_id, limit)) as cur:
            return await cur.fetchall()

    async def get_group_enabled(self, group_id: str) -> Optional[bool]:
        async with self._db.execute(
            "SELECT enabled FROM group_conf WHERE group_id = ?", (group_id,)
        ) as cur:
            row = await cur.fetchone()
        return None if row is None else bool(row[0])

    async def set_group_enabled(self, group_id: str, enabled: bool):
        await self._db.execute(
            """INSERT INTO group_conf (group_id, enabled) VALUES (?, ?)
               ON CONFLICT(group_id) DO UPDATE SET enabled = excluded.enabled""",
            (group_id, 1 if enabled else 0),
        )
        await self._db.commit()

    async def reset_group(self, group_id: str):
        for table in ("total_stat", "daily_stat", "hour_stat", "user_name"):
            await self._db.execute(f"DELETE FROM {table} WHERE group_id = ?", (group_id,))
        await self._db.commit()


@register(
    "astrbot_plugin_group_activity",
    "xiaowan138",
    "群活跃度统计：发言排行榜、个人活跃档案、活跃时段分析",
    "v1.0.0",
    "https://github.com/xiaowan138/astrbot_plugin_group_activity",
)
class GroupActivityPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config
        self.db: Optional[ActivityDB] = None
        self._enabled_cache: dict = {}

    async def initialize(self):
        data_dir = None
        getter = getattr(self.context, "get_data_dir", None)
        if getter:
            try:
                data_dir = getter()
            except Exception:
                data_dir = None
        if not data_dir:
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)
        self.db = ActivityDB(os.path.join(str(data_dir), "group_activity.db"))
        await self.db.init()
        logger.info(f"[群活跃统计] 已加载，数据文件：{os.path.join(str(data_dir), 'group_activity.db')}")

    async def terminate(self):
        if self.db:
            await self.db.close()
        logger.info("[群活跃统计] 已卸载")

    # ---------- 配置与状态 ----------

    def _cfg_int(self, key: str, default: int) -> int:
        try:
            v = self.config.get(key, default) if self.config else default
            return max(1, int(v))
        except Exception:
            return default

    def _cfg_bool(self, key: str, default: bool) -> bool:
        try:
            return bool(self.config.get(key, default)) if self.config else default
        except Exception:
            return default

    async def _group_enabled(self, group_id: str) -> bool:
        if group_id in self._enabled_cache:
            return self._enabled_cache[group_id]
        stored = await self.db.get_group_enabled(group_id)
        enabled = self._cfg_bool("default_enabled", True) if stored is None else stored
        self._enabled_cache[group_id] = enabled
        return enabled

    def _help_text(self) -> str:
        return "\n".join(
            [
                "📖 群活跃度统计插件",
                "",
                "/活跃 [今天|昨天|本周|上周|本月|总计] — 群发言排行榜",
                "/我的活跃 [同上] — 个人活跃档案（排名、活跃时段）",
                "/活跃设置 开启|关闭|状态|清零 — 管理员控制本群统计",
            ]
        )

    def _args_of(self, event: AstrMessageEvent, cmd_words: set) -> list:
        tokens = [t for t in (event.message_str or "").strip().split() if t]
        if tokens and (tokens[0] in cmd_words or tokens[0].lstrip("/") in cmd_words):
            tokens = tokens[1:]
        return tokens

    # ---------- 消息监听 ----------

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def record_group_message(self, event: AstrMessageEvent):
        try:
            group_id = event.get_group_id()
            user_id = event.get_sender_id()
            if not group_id or not user_id:
                return
            gid, uid = str(group_id), str(user_id)
            try:
                self_id = event.get_self_id()
                if self_id and str(self_id) == uid:
                    return
            except Exception:
                pass
            if not self._cfg_bool("count_command", True):
                text = (event.message_str or "").strip()
                if text.startswith(("/", "！")):
                    return
            if not await self._group_enabled(gid):
                return
            name = (event.get_sender_name() or "").strip()
            await self.db.record(gid, uid, name)
        except Exception as e:
            logger.error(f"[群活跃统计] 记录消息失败：{e}")

    # ---------- 指令 ----------

    @filter.command("活跃")
    async def activity_rank(self, event: AstrMessageEvent):
        """群发言排行榜：/活跃 [今天|昨天|本周|上周|本月|总计]"""
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("请在群聊中使用 /活跃 查看排行榜~")
            return
        tokens = self._args_of(event, {"活跃"})
        rng = resolve_range(tokens[0] if tokens else None)
        if rng is None:
            yield event.plain_result(self._help_text())
            return
        _, label, start, end = rng
        top_n = self._cfg_int("top_n", 10)
        rows = await self.db.rank(group_id, start, end, top_n)
        if not rows:
            yield event.plain_result(f"📊 {label}还没有人发言，快来说句话吧~")
            return
        total = await self.db.total_messages(group_id, start, end)
        lines = [f"🏆 群活跃排行榜 · {label}", ""]
        for i, (uid, name, cnt) in enumerate(rows):
            icon = MEDALS[i] if i < 3 else f"{i + 1}."
            lines.append(f"{icon} {display_name(str(uid), str(name))}：{cnt} 条")
        lines.append("")
        lines.append(f"本期全群消息总数：{total} 条")
        yield event.plain_result("\n".join(lines))

    @filter.command("我的活跃")
    async def my_activity(self, event: AstrMessageEvent):
        """个人活跃档案：/我的活跃 [今天|昨天|本周|上周|本月|总计]"""
        group_id = str(event.get_group_id() or "")
        user_id = str(event.get_sender_id() or "")
        if not group_id or not user_id:
            yield event.plain_result("请在群聊中使用 /我的活跃~")
            return
        tokens = self._args_of(event, {"我的活跃"})
        rng = resolve_range(tokens[0] if tokens else None)
        if rng is None:
            yield event.plain_result(self._help_text())
            return
        _, label, start, end = rng
        own, rank = await self.db.user_summary(group_id, user_id, start, end)
        if own <= 0:
            yield event.plain_result(f"📊 {label}你还没有发言记录，说句话试试吧~")
            return
        today = datetime.date.today()
        monday = today - datetime.timedelta(days=today.weekday())
        week_own, _ = await self.db.user_summary(group_id, user_id, monday, today)
        total_own, _ = await self.db.user_summary(group_id, user_id, None, None)
        hours = await self.db.top_hours(group_id, user_id, 3)
        hour_txt = "、".join(f"{h}点({c}条)" for h, c in hours) if hours else "暂无"
        lines = [
            f"📈 你的活跃档案 · {label}",
            "",
            f"本期发言：{own} 条（全群第 {rank} 名）",
            f"本周：{week_own} 条　历史总计：{total_own} 条",
            f"🕐 常活跃时段：{hour_txt}",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("活跃设置")
    async def activity_admin(self, event: AstrMessageEvent):
        """管理本群统计：/活跃设置 开启|关闭|状态|清零"""
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("请在群聊中使用 /活跃设置~")
            return
        tokens = self._args_of(event, {"活跃设置"})
        action = tokens[0] if tokens else "状态"
        if action in ("开启", "开", "on"):
            await self.db.set_group_enabled(group_id, True)
            self._enabled_cache[group_id] = True
            yield event.plain_result("✅ 已开启本群活跃统计")
        elif action in ("关闭", "关", "off"):
            await self.db.set_group_enabled(group_id, False)
            self._enabled_cache[group_id] = False
            yield event.plain_result("⛔ 已关闭本群活跃统计（历史数据保留）")
        elif action in ("清零", "重置", "reset"):
            if len(tokens) >= 2 and tokens[1] in ("确认", "yes", "confirm"):
                await self.db.reset_group(group_id)
                self._enabled_cache.pop(group_id, None)
                yield event.plain_result("🧹 已清空本群全部活跃数据")
            else:
                yield event.plain_result(
                    "⚠️ 将清空本群全部活跃数据且不可恢复，如确认请发送：/活跃设置 清零 确认"
                )
        else:
            enabled = await self._group_enabled(group_id)
            state = "开启" if enabled else "关闭"
            yield event.plain_result(
                f"本群活跃统计当前状态：{state}\n用法：/活跃设置 开启|关闭|状态|清零"
            )
