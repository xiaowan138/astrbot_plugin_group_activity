# 群活跃度统计插件：记录群聊消息，提供发言排行榜、个人活跃档案、潜水榜、进步榜与趋势分析。
import csv
import datetime
import os
import re
import shutil
from typing import Optional

import aiosqlite

import astrbot.api.message_components as Comp
from astrbot.api import logger, AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register

try:
    from astrbot.api.star import StarTools
except ImportError:  # 兼容旧版 AstrBot
    StarTools = None

PLUGIN_NAME = "astrbot_plugin_group_activity"
MEDALS = ["🥇", "🥈", "🥉"]

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")
_SPACE_RE = re.compile(r"\s+")
_FILENAME_RE = re.compile(r'[\\/:*?"<>|\s]+')

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
    "上月": ("lastmonth", "上月"),
    "上个月": ("lastmonth", "上月"),
    "lastmonth": ("lastmonth", "上月"),
    "近7天": ("last7", "近7天"),
    "最近7天": ("last7", "近7天"),
    "近一周": ("last7", "近7天"),
    "7天": ("last7", "近7天"),
    "last7": ("last7", "近7天"),
    "近30天": ("last30", "近30天"),
    "最近30天": ("last30", "近30天"),
    "近一月": ("last30", "近30天"),
    "30天": ("last30", "近30天"),
    "last30": ("last30", "近30天"),
    "总计": ("total", "历史总计"),
    "总": ("total", "历史总计"),
    "全部": ("total", "历史总计"),
    "total": ("total", "历史总计"),
    "all": ("total", "历史总计"),
}


def clean_name(name: str) -> str:
    """清理昵称里的换行和控制字符，避免破坏排行榜排版。"""
    return _CONTROL_RE.sub(" ", name or "").strip()


def display_name(uid: str, name: str) -> str:
    name = _SPACE_RE.sub(" ", clean_name(name))
    if not name:
        name = f"用户{uid[-4:] if len(uid) >= 4 else uid}"
    if len(name) > 12:
        name = name[:12] + "…"
    return name


def _parse_custom_range(word: str, today: datetime.date):
    """解析 2026-11-01~2026-11-13 这类自定义范围。"""
    for sep in ("~", "～", "至", "到"):
        if sep in word:
            left, right = word.split(sep, 1)
            try:
                start = datetime.date.fromisoformat(left.strip())
                end = datetime.date.fromisoformat(right.strip())
            except ValueError:
                return None
            if start > end:
                start, end = end, start
            return "custom", f"{start.isoformat()} ~ {end.isoformat()}", start, end
    return None


def resolve_range(word: Optional[str], today: Optional[datetime.date] = None):
    """把范围词解析成 (key, label, start_date, end_date)；total 时 start/end 为 None。"""
    today = today or datetime.date.today()
    w = (word or "今天").strip()
    info = RANGE_WORDS.get(w) or RANGE_WORDS.get(w.lower())
    if not info:
        return _parse_custom_range(w, today)

    key, label = info
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
    if key == "lastmonth":
        first = today.replace(day=1)
        last = first - datetime.timedelta(days=1)
        return key, label, last.replace(day=1), last
    if key == "last7":
        return key, label, today - datetime.timedelta(days=6), today
    if key == "last30":
        return key, label, today - datetime.timedelta(days=29), today
    return key, label, None, None


class ActivityDB:
    """基于 aiosqlite 的活跃度数据存储。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def init(self):
        self._db = await aiosqlite.connect(self.db_path)
        try:
            for pragma in (
                "PRAGMA journal_mode=WAL",
                "PRAGMA synchronous=NORMAL",
                "PRAGMA busy_timeout=5000",
            ):
                async with self._db.execute(pragma) as cur:
                    await cur.fetchone()
        except Exception as e:
            logger.warning(f"[群活跃统计] 启用 SQLite WAL 失败：{e}")
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
            CREATE TABLE IF NOT EXISTS daily_hour_stat (
                date     TEXT NOT NULL,
                group_id TEXT NOT NULL,
                user_id  TEXT NOT NULL,
                hour     INTEGER NOT NULL,
                count    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (date, group_id, user_id, hour)
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
            CREATE INDEX IF NOT EXISTS idx_daily_hour_group_date ON daily_hour_stat(group_id, date);
            """
        )
        await self._db.commit()

    async def close(self):
        if self._db:
            await self._db.close()
            self._db = None

    async def record(
        self,
        group_id: str,
        user_id: str,
        name: str,
        now: Optional[datetime.datetime] = None,
    ):
        now = now or datetime.datetime.now()
        today = now.strftime("%Y-%m-%d")
        name = clean_name(name)
        cur = await self._db.cursor()
        try:
            await cur.execute(
                """INSERT INTO total_stat (group_id, user_id, count, last_active)
                   VALUES (?, ?, 1, ?)
                   ON CONFLICT(group_id, user_id)
                   DO UPDATE SET count = count + 1, last_active = excluded.last_active""",
                (group_id, user_id, now.timestamp()),
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
            await cur.execute(
                """INSERT INTO daily_hour_stat (date, group_id, user_id, hour, count)
                   VALUES (?, ?, ?, ?, 1)
                   ON CONFLICT(date, group_id, user_id, hour)
                   DO UPDATE SET count = count + 1""",
                (today, group_id, user_id, now.hour),
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
        except Exception:
            await self._db.rollback()
            raise
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

    async def top_hours(
        self,
        group_id: str,
        user_id: str,
        limit: int = 3,
        start: Optional[datetime.date] = None,
        end: Optional[datetime.date] = None,
    ):
        """返回 [(hour, count), ...]；传日期范围时只统计该范围。"""
        if start is None:
            sql = """SELECT hour, SUM(count) AS c FROM hour_stat
                     WHERE group_id = ? AND user_id = ?
                     GROUP BY hour
                     ORDER BY c DESC
                     LIMIT ?"""
            args = (group_id, user_id, limit)
        else:
            sql = """SELECT hour, SUM(count) AS c FROM daily_hour_stat
                     WHERE group_id = ? AND user_id = ? AND date >= ? AND date <= ?
                     GROUP BY hour
                     ORDER BY c DESC
                     LIMIT ?"""
            args = (
                group_id,
                user_id,
                start.strftime("%Y-%m-%d"),
                end.strftime("%Y-%m-%d"),
                limit,
            )
        async with self._db.execute(sql, args) as cur:
            return await cur.fetchall()

    async def streak(self, group_id: str, user_id: str, today: datetime.date) -> int:
        """连续活跃天数：从今天（或昨天）往前数连续有发言的天数。"""
        sql = """SELECT date FROM daily_stat
                 WHERE group_id = ? AND user_id = ? AND count > 0
                 ORDER BY date DESC"""
        async with self._db.execute(sql, (group_id, user_id)) as cur:
            rows = await cur.fetchall()
        active = {str(row[0]) for row in rows}
        cursor = today
        if cursor.isoformat() not in active:
            cursor -= datetime.timedelta(days=1)
        days = 0
        while cursor.isoformat() in active:
            days += 1
            cursor -= datetime.timedelta(days=1)
        return days

    async def divers(self, group_id: str, limit: int, before_ts: float):
        """返回最后发言时间早于 before_ts 的成员。"""
        sql = """SELECT s.user_id, IFNULL(n.name, '') AS name, s.last_active
                 FROM total_stat s
                 LEFT JOIN user_name n
                        ON n.group_id = s.group_id AND n.user_id = s.user_id
                 WHERE s.group_id = ? AND s.last_active > 0 AND s.last_active < ?
                 ORDER BY s.last_active ASC
                 LIMIT ?"""
        async with self._db.execute(sql, (group_id, before_ts, limit)) as cur:
            return await cur.fetchall()

    async def daily_totals(self, group_id: str, start: datetime.date, end: datetime.date):
        """返回 [(date_str, count), ...]，按日期升序。"""
        sql = """SELECT date, SUM(count) FROM daily_stat
                 WHERE group_id = ? AND date >= ? AND date <= ?
                 GROUP BY date ORDER BY date"""
        args = (group_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        async with self._db.execute(sql, args) as cur:
            return await cur.fetchall()

    async def growth(
        self,
        group_id: str,
        this_start: datetime.date,
        this_end: datetime.date,
        last_start: datetime.date,
        last_end: datetime.date,
        limit: int,
    ):
        """返回本周相对上周有正增长的 [(user_id, name, this_cnt, last_cnt, delta), ...]。"""
        this_counts = await self._counts_between(group_id, this_start, this_end)
        last_counts = await self._counts_between(group_id, last_start, last_end)
        user_ids = set(this_counts) | set(last_counts)
        if not user_ids:
            return []
        names = await self._names(group_id)
        rows = []
        for uid in user_ids:
            this_cnt = this_counts.get(uid, 0)
            last_cnt = last_counts.get(uid, 0)
            delta = this_cnt - last_cnt
            if delta <= 0:
                continue
            rows.append((uid, names.get(uid, ""), this_cnt, last_cnt, delta))
        rows.sort(key=lambda row: (-row[4], -row[2], row[0]))
        return rows[:limit]

    async def _counts_between(self, group_id: str, start: datetime.date, end: datetime.date):
        sql = """SELECT user_id, SUM(count) FROM daily_stat
                 WHERE group_id = ? AND date >= ? AND date <= ?
                 GROUP BY user_id"""
        args = (group_id, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        async with self._db.execute(sql, args) as cur:
            rows = await cur.fetchall()
        return {str(row[0]): int(row[1] or 0) for row in rows}

    async def _names(self, group_id: str):
        sql = "SELECT user_id, name FROM user_name WHERE group_id = ?"
        async with self._db.execute(sql, (group_id,)) as cur:
            rows = await cur.fetchall()
        return {str(row[0]): str(row[1] or "") for row in rows}

    async def overview(self, group_id: str):
        """返回 (统计起始日期, 最近记录日期, 参与人数, 历史消息数)。"""
        async with self._db.execute(
            "SELECT MIN(date), MAX(date) FROM daily_stat WHERE group_id = ?",
            (group_id,),
        ) as cur:
            row = await cur.fetchone()
        start, end = (row[0], row[1]) if row else (None, None)
        async with self._db.execute(
            "SELECT COUNT(*), IFNULL(SUM(count), 0) FROM total_stat WHERE group_id = ?",
            (group_id,),
        ) as cur:
            row = await cur.fetchone()
        members, total = (row[0], row[1]) if row else (0, 0)
        return start, end, int(members or 0), int(total or 0)

    async def get_group_enabled(self, group_id: str) -> Optional[bool]:
        async with self._db.execute(
            "SELECT enabled FROM group_conf WHERE group_id = ?", (group_id,)
        ) as cur:
            row = await cur.fetchone()
        return None if row is None else bool(row[0])

    async def set_group_enabled(self, group_id: str, enabled: bool):
        try:
            await self._db.execute(
                """INSERT INTO group_conf (group_id, enabled) VALUES (?, ?)
                   ON CONFLICT(group_id) DO UPDATE SET enabled = excluded.enabled""",
                (group_id, 1 if enabled else 0),
            )
            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise

    async def reset_group(self, group_id: str):
        try:
            for table in (
                "total_stat",
                "daily_stat",
                "hour_stat",
                "daily_hour_stat",
                "user_name",
            ):
                await self._db.execute(f"DELETE FROM {table} WHERE group_id = ?", (group_id,))
            await self._db.commit()
        except Exception:
            await self._db.rollback()
            raise


@register(
    PLUGIN_NAME,
    "xiaowan138",
    "群活跃度统计：发言排行榜、个人档案、潜水榜、进步榜、趋势与导出",
    "v1.1.0",
    "https://github.com/xiaowan138/astrbot_plugin_group_activity",
)
class GroupActivityPlugin(Star):
    """群活跃度统计插件。

    指令：
    /活跃 [范围]        群发言排行榜
    /我的活跃 [范围]    个人活跃档案
    /潜水榜 [天数]      超过 N 天未发言的成员
    /进步榜             本周相比上周的进步榜
    /活跃趋势 [天数]    最近 N 天消息趋势
    /活跃导出 [范围]    导出 CSV（管理员）
    /活跃设置 ...       管理员控制本群统计
    """

    def __init__(self, context: Context, config: AstrBotConfig = None):
        super().__init__(context)
        self.config = config
        self.db: Optional[ActivityDB] = None
        self.data_dir: Optional[str] = None
        self._enabled_cache: dict = {}

    async def initialize(self):
        data_dir = None
        if StarTools is not None:
            try:
                data_dir = str(StarTools.get_data_dir(PLUGIN_NAME))
            except Exception as e:
                logger.warning(f"[群活跃统计] 获取插件数据目录失败，回退到插件目录：{e}")
        if not data_dir:
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
        os.makedirs(data_dir, exist_ok=True)

        db_path = os.path.join(data_dir, "group_activity.db")
        old_db = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "data", "group_activity.db"
        )
        if (
            os.path.abspath(old_db) != os.path.abspath(db_path)
            and os.path.exists(old_db)
            and not os.path.exists(db_path)
        ):
            try:
                for suffix in ("", "-wal", "-shm"):
                    src = old_db + suffix
                    if os.path.exists(src):
                        shutil.copy2(src, db_path + suffix)
                logger.info(f"[群活跃统计] 已迁移旧数据库到 {db_path}")
            except Exception as e:
                logger.error(f"[群活跃统计] 迁移旧数据库失败：{e}")

        self.data_dir = data_dir
        self.db = ActivityDB(db_path)
        await self.db.init()
        logger.info(f"[群活跃统计] 已加载，数据文件：{db_path}")

    async def terminate(self):
        if self.db:
            await self.db.close()
            self.db = None
        logger.info("[群活跃统计] 已卸载")

    # ---------- 配置与状态 ----------

    def _cfg_int(self, key: str, default: int, minimum: int = 1, maximum: Optional[int] = None) -> int:
        try:
            value = self.config.get(key, default) if self.config else default
            value = int(value)
        except Exception:
            value = default
        value = max(minimum, value)
        if maximum is not None:
            value = min(maximum, value)
        return value

    def _cfg_bool(self, key: str, default: bool) -> bool:
        try:
            return bool(self.config.get(key, default)) if self.config else default
        except Exception:
            return default

    def _now(self) -> datetime.datetime:
        """返回插件使用的当前时间；timezone_offset=0 时跟随服务器本地时区。"""
        offset = self._cfg_int("timezone_offset", 0, -12, 14)
        if offset == 0:
            return datetime.datetime.now()
        tz = datetime.timezone(datetime.timedelta(hours=offset))
        return datetime.datetime.now(tz)

    def _today(self) -> datetime.date:
        return self._now().date()

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
                "/活跃 [范围] — 群发言排行榜",
                "/我的活跃 [范围] — 个人活跃档案（排名、连续活跃、活跃时段）",
                "/潜水榜 [天数] — 超过 N 天未发言的成员",
                "/进步榜 — 本周相比上周的进步榜",
                "/活跃趋势 [天数] — 最近 N 天消息趋势",
                "/活跃导出 [范围] — 导出本群 CSV（管理员）",
                "/活跃设置 开启|关闭|状态|清零 — 管理员控制本群统计",
                "",
                "范围：今天、昨天、本周、上周、本月、上月、近7天、近30天、总计，",
                "或自定义 2026-11-01~2026-11-13",
            ]
        )

    def _args_of(self, event: AstrMessageEvent, cmd_words: set) -> list:
        tokens = [t for t in (event.message_str or "").strip().split() if t]
        if tokens:
            head = tokens[0].lstrip("/!！")
            if head in cmd_words:
                tokens = tokens[1:]
        return tokens

    # ---------- 消息监听 ----------

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def record_group_message(self, event: AstrMessageEvent):
        try:
            if not self.db:
                return
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
            if not self._cfg_bool("count_command", False) and getattr(
                event, "is_at_or_wake_command", False
            ):
                return
            if not await self._group_enabled(gid):
                return
            name = clean_name(event.get_sender_name() or "")
            await self.db.record(gid, uid, name, now=self._now())
        except Exception as e:
            logger.error(f"[群活跃统计] 记录消息失败：{e}")

    # ---------- 指令 ----------

    @filter.command("活跃")
    async def activity_rank(self, event: AstrMessageEvent):
        """群发言排行榜：/活跃 [今天|昨天|本周|上周|本月|上月|近7天|近30天|总计|自定义]"""
        if not self.db:
            yield event.plain_result("统计数据库尚未初始化，请稍后再试~")
            return
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("请在群聊中使用 /活跃 查看排行榜~")
            return
        tokens = self._args_of(event, {"活跃"})
        rng = resolve_range(tokens[0] if tokens else None, self._today())
        if rng is None:
            yield event.plain_result("范围不支持~ 发送 /活跃 查看支持的写法。")
            return
        _, label, start, end = rng
        top_n = self._cfg_int("top_n", 10, 1, 50)
        total = await self.db.total_messages(group_id, start, end)
        rows = await self.db.rank(group_id, start, end, top_n)
        if not rows:
            yield event.plain_result(f"📊 {label}还没有人发言，快来说句话吧~")
            return
        lines = [f"🏆 群活跃排行榜 · {label}", ""]
        for i, (uid, name, cnt) in enumerate(rows):
            icon = MEDALS[i] if i < 3 else f"{i + 1}."
            pct = f"（{cnt * 100 / total:.1f}%）" if total else ""
            lines.append(f"{icon} {display_name(str(uid), str(name))}：{cnt} 条{pct}")
        lines.append("")
        lines.append(f"全群消息总数：{total} 条")
        lines.append("发送 /我的活跃 查看个人档案")
        yield event.plain_result("\n".join(lines))

    @filter.command("我的活跃")
    async def my_activity(self, event: AstrMessageEvent):
        """个人活跃档案：/我的活跃 [范围]"""
        if not self.db:
            yield event.plain_result("统计数据库尚未初始化，请稍后再试~")
            return
        group_id = str(event.get_group_id() or "")
        user_id = str(event.get_sender_id() or "")
        if not group_id or not user_id:
            yield event.plain_result("请在群聊中使用 /我的活跃~")
            return
        tokens = self._args_of(event, {"我的活跃"})
        rng = resolve_range(tokens[0] if tokens else None, self._today())
        if rng is None:
            yield event.plain_result("范围不支持~ 发送 /活跃 查看支持的写法。")
            return
        _, label, start, end = rng
        today = self._today()
        monday = today - datetime.timedelta(days=today.weekday())
        week_own, _ = await self.db.user_summary(group_id, user_id, monday, today)
        total_own, _ = await self.db.user_summary(group_id, user_id, None, None)
        streak = await self.db.streak(group_id, user_id, today)
        own, rank = await self.db.user_summary(group_id, user_id, start, end)
        if own <= 0:
            if total_own > 0:
                yield event.plain_result(
                    f"📊 {label}没有发言记录（历史总计 {total_own} 条，连续活跃 {streak} 天）~"
                )
            else:
                yield event.plain_result(f"📊 {label}你还没有发言记录，说句话试试吧~")
            return
        hours = await self.db.top_hours(group_id, user_id, 3, start, end)
        hour_txt = "、".join(f"{h}点({c}条)" for h, c in hours) if hours else "暂无"
        period = "历史" if start is None else "本期"
        lines = [
            f"📈 你的活跃档案 · {label}",
            "",
            f"{period}发言：{own} 条（全群第 {rank} 名）",
            f"本周：{week_own} 条　历史总计：{total_own} 条",
            f"连续活跃：{streak} 天",
            f"🕐 {period}常活跃时段：{hour_txt}",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.command("潜水榜")
    async def dive_rank(self, event: AstrMessageEvent):
        """潜水榜：/潜水榜 [天数]，默认取配置 dive_days。"""
        if not self.db:
            yield event.plain_result("统计数据库尚未初始化，请稍后再试~")
            return
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("请在群聊中使用 /潜水榜~")
            return
        tokens = self._args_of(event, {"潜水榜"})
        days = self._cfg_int("dive_days", 7, 1, 365)
        if tokens:
            try:
                days = max(1, min(365, int(tokens[0])))
            except ValueError:
                yield event.plain_result("用法：/潜水榜 [天数]，例如 /潜水榜 7")
                return
        top_n = self._cfg_int("top_n", 10, 1, 50)
        now = self._now()
        now_ts = now.timestamp()
        rows = await self.db.divers(group_id, top_n, now_ts - days * 86400)
        if not rows:
            yield event.plain_result(f"🤿 近 {days} 天没有发现潜水成员~")
            return
        lines = [f"🤿 潜水榜 · 超过 {days} 天未发言", ""]
        for i, (uid, name, last) in enumerate(rows):
            icon = MEDALS[i] if i < 3 else f"{i + 1}."
            idle = max(0, int((now_ts - float(last)) // 86400))
            lines.append(f"{icon} {display_name(str(uid), str(name))}：{idle} 天")
        lines.append("")
        lines.append("只统计有发言记录的成员")
        yield event.plain_result("\n".join(lines))

    @filter.command("进步榜")
    async def growth_rank(self, event: AstrMessageEvent):
        """进步榜：本周相比上周发言增量。"""
        if not self.db:
            yield event.plain_result("统计数据库尚未初始化，请稍后再试~")
            return
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("请在群聊中使用 /进步榜~")
            return
        today = self._today()
        monday = today - datetime.timedelta(days=today.weekday())
        last_end = monday - datetime.timedelta(days=1)
        last_start = last_end - datetime.timedelta(days=6)
        top_n = self._cfg_int("top_n", 10, 1, 50)
        rows = await self.db.growth(group_id, monday, today, last_start, last_end, top_n)
        if not rows:
            yield event.plain_result("📈 本周还没有人超过上周，继续加油~")
            return
        lines = ["📈 本周进步榜（对比上周）", ""]
        for i, (uid, name, this_cnt, last_cnt, delta) in enumerate(rows):
            icon = MEDALS[i] if i < 3 else f"{i + 1}."
            tag = "🌟新星 " if last_cnt == 0 else ""
            lines.append(
                f"{icon} {tag}{display_name(str(uid), str(name))}："
                f"+{delta} 条（本周 {this_cnt} / 上周 {last_cnt}）"
            )
        lines.append("")
        lines.append("统计周期：本周一 ~ 今天")
        yield event.plain_result("\n".join(lines))

    @filter.command("活跃趋势")
    async def activity_trend(self, event: AstrMessageEvent):
        """每日消息趋势：/活跃趋势 [天数]，最多 30 天。"""
        if not self.db:
            yield event.plain_result("统计数据库尚未初始化，请稍后再试~")
            return
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("请在群聊中使用 /活跃趋势~")
            return
        tokens = self._args_of(event, {"活跃趋势"})
        days = 7
        if tokens:
            try:
                days = max(1, min(30, int(tokens[0])))
            except ValueError:
                yield event.plain_result("用法：/活跃趋势 [天数]，天数范围 1~30")
                return
        today = self._today()
        start = today - datetime.timedelta(days=days - 1)
        rows = await self.db.daily_totals(group_id, start, today)
        if not rows:
            yield event.plain_result(f"📊 最近 {days} 天还没有发言记录~")
            return
        counts = {str(date): int(cnt or 0) for date, cnt in rows}
        max_count = max(counts.values())
        total = sum(counts.values())
        lines = [f"📊 最近 {days} 天消息趋势", ""]
        for i in range(days):
            day = start + datetime.timedelta(days=i)
            count = counts.get(day.isoformat(), 0)
            bar = "█" * max(1, round(count / max_count * 12)) if count else "·"
            lines.append(f"{day.strftime('%m-%d')} {bar} {count}")
        lines.append("")
        lines.append(f"合计：{total} 条，日均 {total / days:.1f} 条")
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("活跃导出")
    async def export_activity(self, event: AstrMessageEvent):
        """导出本群统计数据为 CSV（管理员）。"""
        if not self.db or not self.data_dir:
            yield event.plain_result("统计数据库尚未初始化，请稍后再试~")
            return
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("请在群聊中使用 /活跃导出~")
            return
        tokens = self._args_of(event, {"活跃导出"})
        rng = resolve_range(tokens[0] if tokens else "总计", self._today())
        if rng is None:
            yield event.plain_result("范围不支持~ 发送 /活跃 查看支持的写法。")
            return
        _, label, start, end = rng
        rows = await self.db.rank(group_id, start, end, -1)
        if not rows:
            yield event.plain_result(f"📊 {label}没有可导出的数据~")
            return
        total = await self.db.total_messages(group_id, start, end)
        export_dir = os.path.join(self.data_dir, "exports")
        os.makedirs(export_dir, exist_ok=True)
        safe_label = _FILENAME_RE.sub("_", label).strip("_") or "统计"
        safe_gid = _FILENAME_RE.sub("_", group_id).strip("_") or "group"
        stamp = self._now().strftime("%Y%m%d_%H%M%S")
        filename = f"群活跃_{safe_gid}_{safe_label}_{stamp}.csv"
        path = os.path.join(export_dir, filename)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["排名", "用户ID", "昵称", "消息数", "占比"])
            for i, (uid, name, cnt) in enumerate(rows):
                pct = f"{cnt * 100 / total:.2f}%" if total else "0%"
                writer.writerow([i + 1, uid, clean_name(str(name)), cnt, pct])
        yield event.plain_result(f"📄 已导出 {len(rows)} 条记录，正在发送文件~")
        yield event.chain_result([Comp.File(name=filename, file=path)])

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("活跃设置")
    async def activity_admin(self, event: AstrMessageEvent):
        """管理本群统计：/活跃设置 开启|关闭|状态|清零"""
        if not self.db:
            yield event.plain_result("统计数据库尚未初始化，请稍后再试~")
            return
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
            start, end, members, total = await self.db.overview(group_id)
            lines = [
                f"本群活跃统计当前状态：{state}",
                f"统计起始：{start or '暂无数据'}",
                f"最近记录：{end or '暂无'}",
                f"参与人数：{members} 人",
                f"历史消息：{total} 条",
                "",
                "用法：/活跃设置 开启|关闭|状态|清零",
            ]
            yield event.plain_result("\n".join(lines))
