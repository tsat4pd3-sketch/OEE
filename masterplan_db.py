"""
masterplan_db.py
================
MySQL database setup + CRUD for Masterplan dashboard.

Tables:
  projects   — top-level projects
  phases     — phases within each project
  tasks      — tasks within each phase
  status_log — audit trail of status changes

Requirements:
  pip install mysql-connector-python

Usage:
  python masterplan_db.py                    # create tables + seed default data
  python masterplan_db.py --reset            # drop + recreate + seed
  python masterplan_db.py --export           # export current DB to JSON (masterplan_export.json)
  python masterplan_db.py --summary          # print KPI summary
"""

import argparse
import json
import sys
from datetime import date, datetime

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    print("ERROR: mysql-connector-python not installed.")
    print("  pip install mysql-connector-python")
    sys.exit(1)

# ─────────────────────────────────────────
# CONFIG — แก้ให้ตรงกับ MySQL server ของคุณ
# ─────────────────────────────────────────
DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "user":     "root",
    "password": "your_password",   # ← แก้ตรงนี้
    "database": "project_masterplan",
    "charset":  "utf8mb4",
}

# สัปดาห์ใน sprint นี้  (ปรับได้ตามจริง)
WEEK_LABELS = [
    {"week_no": 1, "label": "W17", "start_date": "2025-04-21"},
    {"week_no": 2, "label": "W18", "start_date": "2025-04-28"},
    {"week_no": 3, "label": "W19", "start_date": "2025-05-05"},
    {"week_no": 4, "label": "W20", "start_date": "2025-05-12"},
    {"week_no": 5, "label": "W21", "start_date": "2025-05-19"},
    {"week_no": 6, "label": "W22", "start_date": "2025-05-26"},
]

# ข้อมูล default จาก masterplan .html
DEFAULT_DATA = {
    "projects": [
        {
            "id": "p1", "name": "Sensor Jig Loading Bending", "sub": "Line 297/306",
            "phases": [
                {
                    "id": "ph1", "name": "ทดสอบ & ยืนยัน",
                    "tasks": [
                        {"id": "t1",  "name": "ทดสอบการใช้งาน Sensor",          "status": "done",    "owner": "—",                    "next_action": "✓ สำเร็จ",          "note": "ทดสอบเรียบร้อย",  "start": 1, "dur": 1, "mile": 0},
                    ]
                },
                {
                    "id": "ph2", "name": "จัดซื้อ & ติดตั้งจริง",
                    "tasks": [
                        {"id": "t2",  "name": "สั่งซื้ออุปกรณ์",                "status": "pending", "owner": "—",                    "next_action": "→ ออก PO",           "note": "รอ PO อนุมัติ",    "start": 2, "dur": 1, "mile": 0},
                        {"id": "t3",  "name": "ติดตั้งจริงหน้างาน",             "status": "pending", "owner": "—",                    "next_action": "→ นัดช่าง",         "note": "หลัง PO ได้รับของ","start": 3, "dur": 1, "mile": 3},
                    ]
                },
            ]
        },
        {
            "id": "p2", "name": "Camera & Monitor LWR306", "sub": "E-day / Final Line",
            "phases": [
                {
                    "id": "ph3", "name": "Camera Check Hole (E-day)",
                    "tasks": [
                        {"id": "t4",  "name": "ส่งรายละเอียด + แนวทางให้คุณโฮม", "status": "done",   "owner": "คุณโฮม",               "next_action": "✓ ส่งแล้ว",         "note": "",                 "start": 1, "dur": 1, "mile": 0},
                    ]
                },
                {
                    "id": "ph4", "name": "Monitor Board On Final Line",
                    "tasks": [
                        {"id": "t5",  "name": "จัดซื้ออุปกรณ์",                "status": "done",    "owner": "คุณโน้ต",              "next_action": "✓ ครบแล้ว",         "note": "",                 "start": 1, "dur": 1, "mile": 0},
                        {"id": "t6",  "name": "เตรียมฐานสำหรับติดตั้ง",        "status": "inprog",  "owner": "ทีมช่าง",              "next_action": "→ เตรียมฐาน",       "note": "รออุปกรณ์มาส่ง",  "start": 2, "dur": 2, "mile": 0},
                        {"id": "t7",  "name": "ติดตั้ง Monitor",               "status": "pending", "owner": "คุณโน้ต",              "next_action": "→ นัดติดตั้ง",      "note": "",                 "start": 4, "dur": 1, "mile": 4},
                    ]
                },
            ]
        },
        {
            "id": "p3", "name": "Safety Door", "sub": "Laser 297 + ห้องเก็บของช่าง",
            "phases": [
                {
                    "id": "ph5", "name": "Safety Door Laser 297",
                    "tasks": [
                        {"id": "t8",  "name": "รวบรวมอุปกรณ์",                 "status": "done",    "owner": "—",                    "next_action": "✓ อยู่ที่โต๊ะ",     "note": "",                 "start": 1, "dur": 1, "mile": 0},
                        {"id": "t9",  "name": "ส่งต่อ + ติดตั้ง (คุณโฮม)",    "status": "pending", "owner": "คุณโฮม",               "next_action": "→ นัดช่าง",         "note": "",                 "start": 2, "dur": 2, "mile": 3},
                    ]
                },
                {
                    "id": "ph6", "name": "Safety Door ห้องเก็บของช่าง",
                    "tasks": [
                        {"id": "t10", "name": "ติดตั้งโครงสร้าง Door",         "status": "done",    "owner": "คุณโฮม",               "next_action": "✓ แล้ว",            "note": "",                 "start": 1, "dur": 1, "mile": 0},
                        {"id": "t11", "name": "เดินระบบไฟเข้าตู้ PLC",         "status": "inprog",  "owner": "คุณโฮม",               "next_action": "→ เดินสาย",         "note": "",                 "start": 2, "dur": 2, "mile": 3},
                    ]
                },
            ]
        },
        {
            "id": "p4", "name": "Jig Rivet 297", "sub": "",
            "phases": [
                {
                    "id": "ph7", "name": "ประสานงาน & ติดตั้ง",
                    "tasks": [
                        {"id": "t12", "name": "ประสานงานพี่เด่น",              "status": "done",    "owner": "พี่เด่น",              "next_action": "✓ แล้ว",            "note": "",                 "start": 1, "dur": 1, "mile": 0},
                        {"id": "t13", "name": "เข้าติดตั้ง (W18)",             "status": "inprog",  "owner": "พี่เด่น",              "next_action": "→ เข้าสัปดาห์หน้า", "note": "",                 "start": 2, "dur": 1, "mile": 2},
                    ]
                },
            ]
        },
        {
            "id": "p5", "name": "Support Part Auto", "sub": "",
            "phases": [
                {
                    "id": "ph8", "name": "ออกแบบ & ผลิต",
                    "tasks": [
                        {"id": "t14", "name": "รอรับแบบจาก K&K",               "status": "blocked", "owner": "พี่เด่น",              "next_action": "→ ติดตาม K&K",      "note": "Block หลัก",       "start": 1, "dur": 2, "mile": 0},
                        {"id": "t15", "name": "คอนเฟิร์มแบบรอบสุดท้าย",        "status": "pending", "owner": "พี่เด่น",              "next_action": "→ คอนเฟิร์ม",       "note": "",                 "start": 3, "dur": 1, "mile": 0},
                        {"id": "t16", "name": "ผลิตชิ้นงาน",                   "status": "pending", "owner": "พี่เด่น",              "next_action": "→ ผลิต",            "note": "",                 "start": 4, "dur": 2, "mile": 5},
                    ]
                },
            ]
        },
        {
            "id": "p6", "name": "Digital Display Pressure Switch", "sub": "Robot ทุกตัว",
            "phases": [
                {
                    "id": "ph9", "name": "จัดซื้อ & ประสานงาน",
                    "tasks": [
                        {"id": "t17", "name": "จัดซื้ออุปกรณ์",                "status": "done",    "owner": "—",                    "next_action": "✓ เก็บใต้โต๊ะ",     "note": "",                 "start": 1, "dur": 1, "mile": 0},
                        {"id": "t18", "name": "พี่เด่นส่ง Email แจ้งทีม",      "status": "pending", "owner": "พี่เด่น",              "next_action": "→ ส่ง Email",        "note": "",                 "start": 2, "dur": 1, "mile": 0},
                    ]
                },
                {
                    "id": "ph10", "name": "ติดตั้ง",
                    "tasks": [
                        {"id": "t19", "name": "ติดตั้ง Pressure Switch ทุก Robot", "status": "pending", "owner": "คุณเบียร์ / คุณโฮม", "next_action": "→ ติดตั้ง",        "note": "",                 "start": 3, "dur": 3, "mile": 5},
                    ]
                },
            ]
        },
    ]
}


# ═══════════════════════════════════════════
# CONNECTION
# ═══════════════════════════════════════════

def get_connection(with_db=True):
    cfg = dict(DB_CONFIG)
    if not with_db:
        cfg.pop("database", None)
    return mysql.connector.connect(**cfg)


# ═══════════════════════════════════════════
# SCHEMA
# ═══════════════════════════════════════════

DDL_CREATE_DB = "CREATE DATABASE IF NOT EXISTS `project_masterplan` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

DDL_TABLES = """
-- สัปดาห์ใน sprint
CREATE TABLE IF NOT EXISTS weeks (
    week_no     TINYINT     NOT NULL,
    label       VARCHAR(10) NOT NULL,
    start_date  DATE        NOT NULL,
    PRIMARY KEY (week_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Projects
CREATE TABLE IF NOT EXISTS projects (
    id          VARCHAR(20)   NOT NULL,
    name        VARCHAR(200)  NOT NULL,
    sub         VARCHAR(200)  DEFAULT '',
    sort_order  SMALLINT      DEFAULT 0,
    created_at  DATETIME      DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Phases (belong to a project)
CREATE TABLE IF NOT EXISTS phases (
    id          VARCHAR(20)   NOT NULL,
    project_id  VARCHAR(20)   NOT NULL,
    name        VARCHAR(200)  NOT NULL,
    sort_order  SMALLINT      DEFAULT 0,
    created_at  DATETIME      DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Tasks (belong to a phase)
CREATE TABLE IF NOT EXISTS tasks (
    id              VARCHAR(20)   NOT NULL,
    phase_id        VARCHAR(20)   NOT NULL,
    name            VARCHAR(300)  NOT NULL,
    status          ENUM('done','inprog','pending','blocked') NOT NULL DEFAULT 'pending',
    owner           VARCHAR(100)  DEFAULT '',
    next_action     VARCHAR(300)  DEFAULT '',
    note            TEXT          DEFAULT '',
    start_week      TINYINT       NOT NULL DEFAULT 1  COMMENT 'สัปดาห์ที่เริ่ม (1-based)',
    duration_weeks  TINYINT       NOT NULL DEFAULT 1  COMMENT 'ระยะเวลา (สัปดาห์)',
    milestone_week  TINYINT       NOT NULL DEFAULT 0  COMMENT '0 = ไม่มี milestone',
    sort_order      SMALLINT      DEFAULT 0,
    created_at      DATETIME      DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME      DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    FOREIGN KEY (phase_id) REFERENCES phases(id) ON DELETE CASCADE,
    INDEX idx_status  (status),
    INDEX idx_owner   (owner(50))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Status change history
CREATE TABLE IF NOT EXISTS status_log (
    log_id      INT           NOT NULL AUTO_INCREMENT,
    task_id     VARCHAR(20)   NOT NULL,
    old_status  ENUM('done','inprog','pending','blocked','') DEFAULT '',
    new_status  ENUM('done','inprog','pending','blocked')    NOT NULL,
    changed_by  VARCHAR(100)  DEFAULT 'system',
    note        TEXT          DEFAULT '',
    changed_at  DATETIME      DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (log_id),
    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
    INDEX idx_task    (task_id),
    INDEX idx_changed (changed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

DDL_DROP = """
DROP TABLE IF EXISTS status_log;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS phases;
DROP TABLE IF EXISTS projects;
DROP TABLE IF EXISTS weeks;
"""


# ═══════════════════════════════════════════
# SEED DATA
# ═══════════════════════════════════════════

def seed(conn):
    cur = conn.cursor()

    # weeks
    cur.executemany(
        "INSERT IGNORE INTO weeks (week_no, label, start_date) VALUES (%s, %s, %s)",
        [(w["week_no"], w["label"], w["start_date"]) for w in WEEK_LABELS]
    )

    for pi, proj in enumerate(DEFAULT_DATA["projects"]):
        cur.execute(
            "INSERT IGNORE INTO projects (id, name, sub, sort_order) VALUES (%s, %s, %s, %s)",
            (proj["id"], proj["name"], proj.get("sub", ""), pi)
        )
        for phi, phase in enumerate(proj["phases"]):
            cur.execute(
                "INSERT IGNORE INTO phases (id, project_id, name, sort_order) VALUES (%s, %s, %s, %s)",
                (phase["id"], proj["id"], phase["name"], phi)
            )
            for ti, t in enumerate(phase["tasks"]):
                cur.execute(
                    """INSERT IGNORE INTO tasks
                       (id, phase_id, name, status, owner, next_action, note,
                        start_week, duration_weeks, milestone_week, sort_order)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (t["id"], phase["id"], t["name"], t["status"],
                     t.get("owner",""), t.get("next_action",""), t.get("note",""),
                     t["start"], t["dur"], t["mile"], ti)
                )

    conn.commit()
    cur.close()
    print("  ✓ Seeded default data")


# ═══════════════════════════════════════════
# CRUD HELPERS  (สามารถ import ไปใช้ใน project อื่นได้)
# ═══════════════════════════════════════════

def get_all_projects(conn):
    """คืน list of dicts  project → phases → tasks (nested)"""
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM projects ORDER BY sort_order")
    projects = cur.fetchall()

    for proj in projects:
        cur.execute("SELECT * FROM phases WHERE project_id=%s ORDER BY sort_order", (proj["id"],))
        proj["phases"] = cur.fetchall()
        for phase in proj["phases"]:
            cur.execute("SELECT * FROM tasks WHERE phase_id=%s ORDER BY sort_order", (phase["id"],))
            phase["tasks"] = cur.fetchall()

    cur.close()
    return projects


def get_kpi_summary(conn):
    """คืน dict KPI: total_tasks, done, inprog, pending, blocked, pct_complete"""
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
            COUNT(*)                                          AS total_tasks,
            SUM(status='done')                               AS done,
            SUM(status='inprog')                             AS inprog,
            SUM(status='pending')                            AS pending,
            SUM(status='blocked')                            AS blocked,
            ROUND(SUM(status='done') / COUNT(*) * 100, 1)   AS pct_complete
        FROM tasks
    """)
    row = cur.fetchone()
    cur.close()
    return row


def update_task_status(conn, task_id, new_status, changed_by="system", note=""):
    """อัปเดต status ของ task และบันทึกลง status_log"""
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT status FROM tasks WHERE id=%s", (task_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        raise ValueError(f"Task '{task_id}' not found")

    old_status = row["status"]
    cur.execute("UPDATE tasks SET status=%s WHERE id=%s", (new_status, task_id))
    cur.execute(
        "INSERT INTO status_log (task_id, old_status, new_status, changed_by, note) VALUES (%s,%s,%s,%s,%s)",
        (task_id, old_status, new_status, changed_by, note)
    )
    conn.commit()
    cur.close()
    return {"task_id": task_id, "old": old_status, "new": new_status}


def add_task(conn, phase_id, name, status="pending", owner="", next_action="",
             note="", start_week=1, duration_weeks=1, milestone_week=0):
    """เพิ่ม task ใหม่ — คืน id ที่สร้าง"""
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT MAX(sort_order) AS mx FROM tasks WHERE phase_id=%s", (phase_id,))
    mx = cur.fetchone()["mx"] or 0

    # auto-generate id
    cur.execute("SELECT COUNT(*) AS cnt FROM tasks")
    cnt = cur.fetchone()["cnt"] + 1
    new_id = f"t{cnt}"

    cur.execute(
        """INSERT INTO tasks
           (id, phase_id, name, status, owner, next_action, note,
            start_week, duration_weeks, milestone_week, sort_order)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (new_id, phase_id, name, status, owner, next_action, note,
         start_week, duration_weeks, milestone_week, mx + 1)
    )
    conn.commit()
    cur.close()
    return new_id


def get_status_history(conn, task_id):
    """ดู log การเปลี่ยน status ของ task"""
    cur = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT * FROM status_log WHERE task_id=%s ORDER BY changed_at DESC",
        (task_id,)
    )
    rows = cur.fetchall()
    cur.close()
    return rows


def get_blocked_tasks(conn):
    """ดู tasks ที่ blocked ทั้งหมด พร้อม project/phase context"""
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT t.id, t.name, t.owner, t.next_action, t.note,
               ph.name AS phase_name,
               pr.name AS project_name
        FROM tasks t
        JOIN phases ph ON t.phase_id = ph.id
        JOIN projects pr ON ph.project_id = pr.id
        WHERE t.status = 'blocked'
        ORDER BY pr.sort_order, ph.sort_order, t.sort_order
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


def get_milestone_tasks(conn):
    """ดู tasks ที่มี milestone (milestone_week > 0)"""
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT t.id, t.name, t.status, t.milestone_week,
               w.label AS week_label, w.start_date,
               ph.name AS phase_name, pr.name AS project_name
        FROM tasks t
        JOIN phases ph ON t.phase_id = ph.id
        JOIN projects pr ON ph.project_id = pr.id
        LEFT JOIN weeks w ON t.milestone_week = w.week_no
        WHERE t.milestone_week > 0
        ORDER BY t.milestone_week, pr.sort_order
    """)
    rows = cur.fetchall()
    cur.close()
    return rows


# ═══════════════════════════════════════════
# EXPORT TO JSON
# ═══════════════════════════════════════════

def export_json(conn, path="masterplan_export.json"):
    def serial(obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return str(obj)

    data = {
        "exported_at": datetime.now().isoformat(),
        "kpi": get_kpi_summary(conn),
        "projects": get_all_projects(conn),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=serial)
    print(f"  ✓ Exported to {path}")


# ═══════════════════════════════════════════
# PRINT SUMMARY
# ═══════════════════════════════════════════

def print_summary(conn):
    kpi = get_kpi_summary(conn)
    print("\n╔══════════════════════════════╗")
    print("║   MASTERPLAN  KPI SUMMARY   ║")
    print("╠══════════════════════════════╣")
    print(f"║  Total Tasks  : {kpi['total_tasks']:>4}          ║")
    print(f"║  ✅ Done      : {kpi['done']:>4}          ║")
    print(f"║  🔄 In-Prog   : {kpi['inprog']:>4}          ║")
    print(f"║  ⏳ Pending   : {kpi['pending']:>4}          ║")
    print(f"║  🔴 Blocked   : {kpi['blocked']:>4}          ║")
    print(f"║  % Complete   : {kpi['pct_complete']:>5}%         ║")
    print("╚══════════════════════════════╝")

    blocked = get_blocked_tasks(conn)
    if blocked:
        print("\n⚠️  BLOCKED TASKS:")
        for t in blocked:
            print(f"  [{t['project_name']}] {t['name']}")
            print(f"      → {t['next_action']}  ({t['owner']})")
            if t["note"]:
                print(f"      📝 {t['note']}")

    milestones = get_milestone_tasks(conn)
    if milestones:
        print("\n🏁 MILESTONES:")
        for m in milestones:
            status_icon = {"done":"✅","inprog":"🔄","pending":"⏳","blocked":"🔴"}.get(m["status"],"❓")
            print(f"  {status_icon} {m['week_label']} ({m['start_date']}) — {m['name']}")
            print(f"      [{m['project_name']}] / {m['phase_name']}")
    print()


# ═══════════════════════════════════════════
# SETUP FLOW
# ═══════════════════════════════════════════

def setup(reset=False):
    print("🔧 Connecting to MySQL...")
    try:
        # สร้าง database ก่อน (ไม่ระบุ db)
        conn0 = get_connection(with_db=False)
        cur0 = conn0.cursor()
        cur0.execute(DDL_CREATE_DB)
        cur0.close()
        conn0.close()
        print(f"  ✓ Database '{DB_CONFIG['database']}' ready")
    except Error as e:
        print(f"  ✗ Cannot connect: {e}")
        sys.exit(1)

    conn = get_connection()
    cur = conn.cursor()

    if reset:
        print("  ⚠️  Dropping existing tables...")
        for stmt in DDL_DROP.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()
        print("  ✓ Tables dropped")

    print("  Creating tables...")
    for stmt in DDL_TABLES.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    conn.commit()
    cur.close()
    print("  ✓ Tables created")

    print("  Seeding default data...")
    seed(conn)

    print_summary(conn)
    conn.close()


# ═══════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Masterplan MySQL DB setup")
    parser.add_argument("--reset",   action="store_true", help="Drop + recreate all tables")
    parser.add_argument("--export",  action="store_true", help="Export DB to JSON file")
    parser.add_argument("--summary", action="store_true", help="Print KPI summary")
    args = parser.parse_args()

    if args.export or args.summary:
        conn = get_connection()
        if args.summary:
            print_summary(conn)
        if args.export:
            export_json(conn)
        conn.close()
    else:
        setup(reset=args.reset)
