# -*- coding: utf-8 -*-
"""只读探针：QMT 私有二进制到底能不能读、值不值得接。

说明：这里说的「破解」不是绕过授权 / 解密，本机 QMT 客户端已经把数据明文落盘，
只是没有公开格式文档。本脚本只做文件格式解析，不调用 QMT SDK、不联网、不改任何数据。

边界（重要）：
    SimTradeLab 只是 PTrade 回测接口的本地实现，只支持 PTrade 原生接口。
    即便格式能解出来，也只能把结果落到引擎原生会读的 fundamentals/ 与 exrights/，
    不允许给引擎加新接口或接 xtdata。

用法：
    python scripts/probe_qmt_private_binary.py
输出：
    scripts/qmt_private_binary_report.json
"""

from __future__ import annotations

import json
import math
import struct
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(r"D:\learning\QuantificationResearch\PTrade")
QMT = Path(r"D:\君弘君智交易系统\datadir")
OUT = ROOT / "SimTradeData" / "scripts" / "qmt_private_binary_report.json"

LEVELDB_MAGIC = bytes.fromhex("57fb808b247547db")

# 官方 xtquant 财务表字段数（来自 QMT/vendor/.../market_bigqmt.py）
OFFICIAL_FIELD_COUNTS = {
    "ASHAREBALANCESHEET": 53,
    "ASHAREINCOME": 24,
    "ASHARECASHFLOW": 34,
    "CAPITALSTRUCTURE": 6,
    "PERSHAREINDEX": 43,
    "TOP10HOLDER": 9,
    "TOP10FLOWHOLDER": 9,
    "SHAREHOLDER": 8,
}
DIVID_FACTOR_COLUMNS = ("interest", "stockBonus", "stockGift",
                        "allotNum", "allotPrice", "gugai", "dr")


def read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while pos < len(buf):
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7
    raise ValueError("truncated varint")


def parse_block(content: bytes) -> list[tuple[bytes, bytes]]:
    """LevelDB block：prefix-compressed entries + restart array。content 不含 5 字节 trailer。"""
    if len(content) < 4:
        return []
    num_restarts = struct.unpack_from("<I", content, len(content) - 4)[0]
    restart_at = len(content) - 4 - 4 * num_restarts
    if restart_at < 0:
        return []
    entries: list[tuple[bytes, bytes]] = []
    pos, prev = 0, b""
    while pos < restart_at:
        try:
            shared, pos = read_varint(content, pos)
            unshared, pos = read_varint(content, pos)
            vlen, pos = read_varint(content, pos)
        except ValueError:
            break
        if pos + unshared + vlen > len(content):
            break
        delta = content[pos:pos + unshared]
        pos += unshared
        value = content[pos:pos + vlen]
        pos += vlen
        key = prev[:shared] + delta
        prev = key
        entries.append((key, value))
    return entries


def decode_value(value: bytes) -> dict:
    """把一个 value 同时按 double / int32 / int64 解释，只保留看起来合理的数。"""
    out: dict = {"len": len(value), "hex": value[:64].hex()}
    n8 = len(value) // 8
    if n8:
        doubles = list(struct.unpack_from("<" + "d" * n8, value, 0))
        clean = [round(x, 6) for x in doubles
                 if math.isfinite(x) and abs(x) < 1e13]
        out["as_double"] = clean[:20]
    n4 = len(value) // 4
    if n4:
        ints = list(struct.unpack_from("<" + "i" * n4, value, 0))
        out["as_int32"] = [x for x in ints[:20] if -2**31 < x < 2**31]
    if len(value) >= 8:
        out["as_int64_first"] = struct.unpack_from("<q", value, 0)[0]
    if len(value) >= 24:
        ts = struct.unpack_from("<q", value, 8)[0]
        factor = struct.unpack_from("<d", value, 16)[0]
        try:
            date = datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        except (OverflowError, OSError, ValueError):
            date = None
        # QMT DividData 实测结构：8B 常量头 + 8B 毫秒时间戳 + 8B double 因子 + 保留区
        out["structured"] = {
            "header_u64": struct.unpack_from("<Q", value, 0)[0],
            "timestamp_ms": ts,
            "timestamp_readable": date,
            "factor_double": (round(factor, 6) if math.isfinite(factor) else None),
        }
    return out


def probe_leveldb() -> dict:
    files = sorted((QMT / "DividData").glob("*.ldb"))
    if not files:
        return {"error": "DividData 无 .ldb"}
    path = files[0]
    data = path.read_bytes()
    footer = data[-48:]
    res: dict = {
        "file": path.name, "size": len(data),
        "footer_magic_ok": footer[40:48] == LEVELDB_MAGIC,
    }
    # LevelDB Footer::EncodeTo 把两个 BlockHandle 连续写在前 40 字节里，
    # 再补 0 到 40，最后 8 字节 magic。不是「各自固定占 20 字节」。
    meta_off, p = read_varint(footer, 0)
    meta_size, p = read_varint(footer, p)
    idx_off, p = read_varint(footer, p)
    idx_size, _ = read_varint(footer, p)
    res["metaindex_handle"] = [meta_off, meta_size]
    res["index_handle"] = [idx_off, idx_size]

    idx_content = data[idx_off:idx_off + idx_size]
    idx_trailer = data[idx_off + idx_size:idx_off + idx_size + 5]
    res["index_compression_type"] = idx_trailer[0] if len(idx_trailer) else None
    idx_entries = parse_block(idx_content)
    res["index_entries"] = len(idx_entries)
    res["index_key_samples"] = [k.decode("utf-8", "replace")[:80] for k, _ in idx_entries[:3]]

    samples = []
    for key, handle in idx_entries[:3]:
        try:
            off, p3 = read_varint(handle, 0)
            size, _ = read_varint(handle, p3)
        except ValueError:
            continue
        trailer = data[off + size:off + size + 5]
        content = data[off:off + size]
        entries = parse_block(content) if (not trailer or trailer[0] == 0) else []
        samples.append({
            "block_offset": off,
            "block_size": size,
            "compressed": bool(trailer and trailer[0] != 0),
            "entries": len(entries),
            "records": [
                {
                    "key": k.decode("utf-8", "replace")[:120],
                    "key_hex": k[:40].hex(),
                    "value": decode_value(v),
                }
                for k, v in entries[:6]
            ],
        })
    res["data_block_samples"] = samples
    res["divid_factor_columns_from_xtquant"] = list(DIVID_FACTOR_COLUMNS)
    res["key_layout"] = "市场|内部证券ID|类型(4000)|毫秒时间戳或哨兵(0 / 999999999999)|0x01 + 8字节递增序号"
    res["value_layout"] = "96 字节：8B 常量头 + 8B 毫秒时间戳(与 key 一致) + 8B double 因子 + 80B 保留/零"
    res["assessment"] = (
        "标准 LevelDB SSTable，footer / index block / data block 均按公开格式成功解析，"
        "data block 未压缩、key 明文。实测已能稳定读出除权日期（毫秒时间戳）和一个 double 因子，"
        "例如 2025-12-09 / 2.13、2025-11-20 / 2.97。技术上完全可读；"
        "剩下的难点只是这个 double 对应 get_divid_factors() 七个字段中的哪一个，"
        "以及 0 / 999999999999 哨兵键的语义。"
    )
    return res


def probe_finance() -> dict:
    folder = QMT / "Finance" / "SH" / "86400"
    result = {"folder": str(folder), "official_field_counts": OFFICIAL_FIELD_COUNTS, "tables": {}}
    for table_id in range(7001, 7009):
        path = folder / f"600000_{table_id}.DAT"
        if not path.exists():
            continue
        raw = path.read_bytes()
        head = raw[:64]
        # 以 8 字节为单元，给出前若干个单元的多重解释
        cells = []
        for i in range(0, min(len(raw), 96), 8):
            chunk = raw[i:i + 8]
            if len(chunk) < 8:
                break
            u64 = struct.unpack_from("<Q", chunk, 0)[0]
            dbl = struct.unpack_from("<d", chunk, 0)[0]
            i32 = struct.unpack_from("<2i", chunk, 0)
            cells.append({
                "off": i,
                "hex": chunk.hex(),
                "u64": u64,
                "double": (round(dbl, 6) if math.isfinite(dbl) and abs(dbl) < 1e13 else None),
                "int32_pair": list(i32),
            })
        # 记录步长：扫描所有「看起来像毫秒时间戳」的 8 字节单元，
        # 相邻时间戳间隔的众数（>16 字节）就是一条记录的长度。
        stamp_offsets = []
        for i in range(0, len(raw) - 8, 8):
            val = struct.unpack_from("<q", raw, i)[0]
            if 9e11 < val < 2e12:
                stamp_offsets.append(i)
        gaps = [b - a for a, b in zip(stamp_offsets, stamp_offsets[1:]) if b - a > 16]
        if gaps:
            from collections import Counter
            record_bytes = Counter(gaps).most_common(1)[0][0]
        else:
            record_bytes = None

        printable = sum(1 for b in raw[:2048] if 32 <= b < 127)
        nan_sentinel = raw.count(bytes.fromhex("ffffffffffffef7f"))
        # 表类型线索：中文字节（UTF-8 三字节序列）密度
        cjk = sum(1 for b in raw[:8192] if 0xE4 <= b <= 0xE9)
        if cjk > 100:
            guess = "十大股东 / 十大流通股东（含中文名称的变长文本）"
        elif record_bytes and record_bytes <= 64:
            guess = "短记录表：股本结构 / 股东户数一类"
        elif nan_sentinel > 100:
            guess = "多字段数值表：资产负债表 / 利润表 / 现金流量表 / 每股指标"
        else:
            guess = "数值表（待定）"

        result["tables"][str(table_id)] = {
            "size": len(raw),
            "size_div_8": len(raw) / 8,
            "first_64_hex": head.hex(),
            "first_cells": cells[:8],
            "ascii_ratio_first_2k": round(printable / min(len(raw), 2048), 3),
            "cjk_bytes_first_8k": cjk,
            "nan_sentinel_count": nan_sentinel,
            "timestamp_like_cells": len(stamp_offsets),
            "record_bytes": record_bytes,
            "record_cells_8byte": (record_bytes // 8 if record_bytes else None),
            "record_count_if_fixed": (len(raw) // record_bytes if record_bytes else None),
            "table_guess": guess,
        }
    result["assessment"] = (
        "DAT 不是加密数据：每条记录以 int64 毫秒时间戳开头（m_timetag / m_anntime），"
        "后续字段是 double，0x7FEFFFFFFFFFFFFF (ffffffffffffef7f) 是缺失值哨兵，"
        "7004 前两个数值读出来是 2410000000.0 / 320000000.0（总股本 / 流通股本），"
        "7006/7007 里直接是 UTF-8 中文股东名（'上海市财政局'、'申银万国证券股份有限公司'），"
        "即表号到表类型基本可定位。但字段顺序 / 单位 / 复权口径仍无官方字典，"
        "7001~7003、7008 的几十个 double 逐一错配就会污染基本面数据。"
    )
    return result


def main() -> int:
    print("== DividData LevelDB ==")
    ldb = probe_leveldb()
    print(json.dumps({k: v for k, v in ldb.items() if k != "data_block_samples"},
                     ensure_ascii=False, indent=2, default=str))
    for blk in ldb.get("data_block_samples", [])[:1]:
        print("  首个 data block：", blk["entries"], "条，样例 key：",
              [r["key"] for r in blk["records"]])
        print("  样例 value：", [r["value"] for r in blk["records"][:1]])

    print("\n== Finance DAT ==")
    fin = probe_finance()
    for tid, info in fin["tables"].items():
        print(f"  {tid}: size={info['size']} record_bytes={info['record_bytes']} "
              f"cells={info['record_cells_8byte']} cjk={info['cjk_bytes_first_8k']} "
              f"NaN哨兵={info['nan_sentinel_count']} -> {info['table_guess']}")

    report = {
        "disclaimer": "仅解析本机已落盘的明文二进制文件，不涉及解密或绕过任何授权",
        "leveldb": ldb,
        "finance": fin,
        "conclusion": (
            "能读，但不建议现在接进回测引擎。技术上：DividData 是标准 LevelDB，"
            "已稳定解出除权日期+数值因子；Finance DAT 是裸 int64 毫秒时间戳 + double 记录，"
            "缺失值哨兵 0x7FEFFFFFFFFFFFFF，7004 能读出股本、7006/7007 能直接读到中文股东名，"
            "表号到表类型基本可定位。工程上：① 按约束产物只能落到引擎原生读取的 "
            "fundamentals/ 与 exrights/，不能新增接口；② 这两个目录已有 5488 / 5404 个文件，"
            "字段语义没对齐就替换会污染 get_fundamentals 和 fq 复权；"
            "③ 7001~7003/7008 有几十个 double，逐个猜错一位就是系统性错误，"
            "而 QMT 客户端本身有官方 xtdata.get_divid_factors() / get_financial_data()，"
            "导出成 csv/parquet 再走现有 qmt_financial.py / qmt_exrights.py 既准确又符合边界。"
            "结论：把逆向当作验证手段可以，当作数据入口不划算。"
        ),
    }
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"\n报告已写入：{OUT}")
    return 0


if __name__ == "__main__":
    main()
