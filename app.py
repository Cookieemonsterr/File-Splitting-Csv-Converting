import streamlit as st
import pandas as pd
import zipfile
import io
import re

st.set_page_config(page_title="Outlet Splitter", layout="centered")
st.title("Outlet Splitter & CSV Converter")

SUPPORTED_TYPES = ["csv", "tsv", "txt", "xlsx", "xls", "json"]

# ---------------- Helpers ----------------

def clean_price_column_only(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return text
    s = str(text)
    safe_chars = []
    for char in s:
        code = ord(char)
        if 32 <= code <= 126:
            safe_chars.append(char)
        elif char in ["\n", "\r", "\t"]:
            safe_chars.append(" ")
    s = "".join(safe_chars)
    s = " ".join(s.split())
    return s.strip()

def normalize_numeric_like_columns(df: pd.DataFrame) -> pd.DataFrame:
    def extract_clean_number(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return x
        s = clean_price_column_only(x)
        if not s:
            return ""
        m = re.search(r"[-+]?\d+(?:[.,]\d+)?", s)
        if not m:
            return s
        return m.group(0).replace(",", ".")

    key_cols = []
    for c in df.columns:
        lc = str(c).lower().strip()
        if any(k in lc for k in ["price", "cost", "amount", "value", "rate", "rsp"]):
            key_cols.append(c)

    for c in key_cols:
        if c in df.columns:
            df[c] = df[c].apply(extract_clean_number)

    return df

def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    # never drop rows; only drop fully empty columns
    df = df.dropna(axis=1, how="all")

    # clean column names (ASCII-safe)
    cleaned_cols = []
    for c in df.columns:
        col_str = str(c)
        safe_chars = []
        for char in col_str:
            code = ord(char)
            if 32 <= code <= 126:
                safe_chars.append(char)
            elif char in [" ", "\t"]:
                safe_chars.append(" ")
        cleaned = "".join(safe_chars).strip()
        cleaned = " ".join(cleaned.split())
        cleaned_cols.append(cleaned if cleaned else "Unnamed")
    df.columns = cleaned_cols

    # trim strings
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].apply(lambda x: x.strip() if isinstance(x, str) else x)

    df = normalize_numeric_like_columns(df)
    return df

def safe_name(s: str) -> str:
    s = str(s)
    s = s.replace("/", "-").replace("\\", "-")
    s = re.sub(r'[<>:"|?*]', "-", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120] if s else "UNKNOWN"

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    # UTF-8 with BOM: best for Excel + Sheets
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

def to_xlsx_bytes_single_sheet(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=str(sheet_name)[:31] or "Sheet1")
    return bio.getvalue()

def to_xlsx_bytes_multi_sheet(sheets: dict) -> bytes:
    """
    sheets: {sheet_name: dataframe}
    """
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        for sh, df in sheets.items():
            df.to_excel(writer, index=False, sheet_name=str(sh)[:31] or "Sheet")
    return bio.getvalue()

def is_numeric_header(col) -> bool:
    return bool(re.fullmatch(r"\d{5,}", str(col).strip()))

def detect_outlet_columns(df: pd.DataFrame):
    return [c for c in df.columns if is_numeric_header(c)]

def detect_outlet_row_column(df: pd.DataFrame):
    priority_patterns = [
        r"\boutlet\s*id\b", r"\bstore\s*id\b", r"\bbranch\s*id\b",
        r"\boutlet\b", r"\bbranch\b", r"\bstore\b",
        r"\bsite\s*no\b", r"\bsite\b", r"\blocation\b",
    ]
    cols = list(df.columns)
    for pat in priority_patterns:
        for c in cols:
            lc = str(c).lower().strip()
            if re.search(pat, lc):
                series = df[c].dropna()
                if series.empty:
                    continue
                nun = series.nunique()
                ratio = nun / max(1, len(series))
                if nun >= 2 and ratio < 0.7:
                    return c
    return None

def detect_outlet_row_column_smart(df: pd.DataFrame):
    col = detect_outlet_row_column(df)
    if col:
        return col
    if df is None or df.empty:
        return None

    bad_keywords = ["upc", "barcode", "gtin", "sku", "item", "product", "price",
                    "qty", "quantity", "stock", "name", "description", "plu"]
    best = None

    for c in df.columns[:25]:
        lc = str(c).lower().strip()
        if any(k in lc for k in bad_keywords):
            continue

        series = df[c].dropna()
        if series.empty:
            continue

        nun = series.nunique()
        total = len(series)
        ratio = nun / max(1, total)

        if nun < 2 or ratio > 0.7:
            continue

        sample = series.astype(str).head(40)
        looks_id = sample.apply(lambda x: bool(re.fullmatch(r"\d{3,}", x.strip()))).mean()
        score = (1 - ratio) * 3 + looks_id * 5

        if best is None or score > best[0]:
            best = (score, c)

    return best[1] if best else None

def apply_combined_outlet_key_if_possible(df: pd.DataFrame):
    def norm(s): return str(s).lower().strip()

    site_col = None
    outletid_col = None

    for c in df.columns:
        lc = norm(c)
        if site_col is None and re.search(r"\bsite\s*no\b", lc):
            site_col = c
        if outletid_col is None and re.search(r"\boutlet\s*id\b", lc):
            outletid_col = c

    if site_col and outletid_col:
        df2 = df.copy()
        df2["_outlet_key"] = (
            df2[site_col].astype(str).fillna("UNKNOWN") + " - " +
            df2[outletid_col].astype(str).fillna("UNKNOWN")
        )
        return df2, "_outlet_key"

    return df, None

# ---------------- Safe header detection ----------------

HEADER_TOKENS = [
    "upc", "barcode", "gtin", "sku",
    "category", "sub_category", "subcategory",
    "item", "product", "name", "description",
    "price", "rsp", "stock", "qty", "quantity",
    "outlet", "store", "branch", "site", "plu",
]

def detect_header_row_from_preview(preview_df: pd.DataFrame, max_rows: int = 30) -> int:
    best_row = 0
    best_hits = -1
    rows_to_check = min(max_rows, len(preview_df))
    for r in range(rows_to_check):
        row = preview_df.iloc[r].tolist()
        cells = [str(x).strip().lower() for x in row if pd.notna(x)]
        hits = sum(1 for c in cells if any(t in c for t in HEADER_TOKENS))
        if hits > best_hits:
            best_hits = hits
            best_row = r

    if best_hits < 2:
        return 0
    return min(best_row, 5)

def read_excel_sheet_smart_from_bytes(excel_bytes: io.BytesIO, sheet_name: str, engine: str,
                                     auto_header: bool, manual_header: int | None):
    excel_bytes.seek(0)

    if manual_header is not None:
        header_row = manual_header
    elif auto_header:
        preview = pd.read_excel(
            excel_bytes, sheet_name=sheet_name, header=None, nrows=40,
            dtype=object, engine=engine, keep_default_na=False, na_filter=False
        )
        header_row = detect_header_row_from_preview(preview)
    else:
        header_row = 0

    excel_bytes.seek(0)
    df = pd.read_excel(
        excel_bytes, sheet_name=sheet_name, header=header_row,
        dtype=object, engine=engine, keep_default_na=False, na_filter=False
    )

    before_rows = len(df)
    df = clean_df(df)
    after_rows = len(df)
    return df, header_row, before_rows, after_rows

# ---------------- Fake XLS fallback (keeps rows) ----------------

def looks_like_utf16_text(sample: bytes) -> bool:
    if sample.startswith(b"\xff\xfe") or sample.startswith(b"\xfe\xff"):
        return True
    return b"\x00" in sample

def try_read_delimited_bytes_as_df(file_bytes: bytes) -> tuple[pd.DataFrame, str]:
    """
    Robust fake-xls reader:
    1) Try UTF-16 TSV
    2) If row count seems too low, strip null bytes and parse TSV
    """
    def parse_bytes(enc, sep, b):
        bio = io.BytesIO(b)
        df = pd.read_csv(
            bio,
            sep=sep,
            encoding=enc,
            dtype=object,
            keep_default_na=False,
            na_filter=False,
            engine="python"
        )
        return clean_df(df)

    approx_lines = file_bytes.count(b"\n") + file_bytes.count(b"\n\x00")
    if approx_lines < 10:
        approx_lines = None

    attempts = [("utf-16", "\t"), ("utf-16-le", "\t"), ("utf-16-be", "\t")]

    last_err = None
    for enc, sep in attempts:
        try:
            df = parse_bytes(enc, sep, file_bytes)
            if df is not None and not df.empty and len(df.columns) > 1:
                # if suspiciously low vs raw newline estimate -> try null-strip
                if approx_lines and len(df) < int(approx_lines * 0.8):
                    break
                return df, f"Parsed as delimited text (enc={enc}, sep=TAB)"
        except Exception as e:
            last_err = e

    # null-strip fallback
    try:
        b2 = file_bytes.replace(b"\x00", b"")
        df2 = parse_bytes("latin1", "\t", b2)
        if df2 is not None and not df2.empty and len(df2.columns) > 1:
            return df2, "Parsed as NULL-stripped TSV (best for fake .XLS)"
    except Exception as e:
        last_err = e

    raise last_err or ValueError("Could not parse as delimited text")

def read_text_table_with_fallback(file_like, sep: str) -> tuple[pd.DataFrame, str]:
    encodings_to_try = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None
    for enc in encodings_to_try:
        try:
            file_like.seek(0)
            df = pd.read_csv(
                file_like,
                sep=sep,
                dtype=object,
                encoding=enc,
                keep_default_na=False,
                na_filter=False,
                engine="python"
            )
            return clean_df(df), enc
        except Exception as e:
            last_err = e
    raise last_err

def read_any_file(uploaded, auto_header: bool, manual_header: int | None):
    name = uploaded.name.lower()

    # Excel
    if name.endswith(("xlsx", "xls")):
        file_bytes = uploaded.getvalue()
        excel_bytes = io.BytesIO(file_bytes)
        engines_to_try = ["openpyxl", "xlrd"]
        last_err = None

        for engine in engines_to_try:
            try:
                excel_bytes.seek(0)
                xls = pd.ExcelFile(excel_bytes, engine=engine)

                cleaned = {}
                header_rows = {}
                row_report = {}

                for sh in xls.sheet_names:
                    df_sh, header_row, before_rows, after_rows = read_excel_sheet_smart_from_bytes(
                        excel_bytes, sh, engine=engine,
                        auto_header=auto_header, manual_header=manual_header
                    )
                    if df_sh is None or df_sh.empty:
                        continue
                    cleaned[str(sh).strip()] = df_sh
                    header_rows[str(sh).strip()] = header_row
                    row_report[str(sh).strip()] = (before_rows, after_rows)

                return {
                    "type": "excel",
                    "sheets": cleaned,
                    "header_rows": header_rows,
                    "row_report": row_report,
                    "excel_engine": engine,
                }
            except Exception as e:
                last_err = e

        # fake XLS fallback
        try:
            sample = file_bytes[:4000]
            if looks_like_utf16_text(sample):
                df, note = try_read_delimited_bytes_as_df(file_bytes)
                return {"type": "table", "df": df, "note": note}
        except Exception:
            pass

        raise last_err

    # JSON
    if name.endswith("json"):
        uploaded.seek(0)
        df = pd.read_json(uploaded, dtype=object)
        df = clean_df(df)
        return {"type": "table", "df": df}

    # Text tables
    sep = "\t" if name.endswith("tsv") else ","
    df, used_encoding = read_text_table_with_fallback(uploaded, sep=sep)
    return {"type": "table", "df": df, "encoding": used_encoding}

# ---------------- UI ----------------

uploaded_files = st.file_uploader("Upload file(s)", type=SUPPORTED_TYPES, accept_multiple_files=True)

mode = st.radio("What do you want to do?", ["Auto split + convert", "Convert only"], index=0)

output_format = st.radio(
    "Output format",
    ["CSV", "XLSX"],
    index=0
)

keep_outlet_id_only_in_filename = st.checkbox(
    "When splitting, keep outlet id only in the filename",
    value=True
)

st.markdown("---")
auto_header = st.checkbox("Auto detect header row", value=True)
manual_header = None
if st.checkbox("Manual header row override", value=False):
    manual_header = st.number_input("Header row index (0 = first row)", min_value=0, max_value=200, value=0, step=1)

def write_file(z: zipfile.ZipFile, path_no_ext: str, df: pd.DataFrame, sheet_name: str = "Sheet1"):
    if output_format.startswith("XLSX"):
        z.writestr(f"{path_no_ext}.xlsx", to_xlsx_bytes_single_sheet(df, sheet_name=sheet_name))
    else:
        z.writestr(f"{path_no_ext}.csv", to_csv_bytes(df))

def write_workbook_convert_only(z: zipfile.ZipFile, out_path_no_ext: str, sheets: dict):
    # for convert-only + XLSX, keep one file same name, multi-sheet
    z.writestr(f"{out_path_no_ext}.xlsx", to_xlsx_bytes_multi_sheet(sheets))

if uploaded_files:
    big_zip = io.BytesIO()

    with zipfile.ZipFile(big_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for uploaded in uploaded_files:
            folder = safe_name(uploaded.name)
            base = safe_name(uploaded.name.rsplit(".", 1)[0])

            try:
                result = read_any_file(uploaded, auto_header=auto_header, manual_header=manual_header)
            except Exception as e:
                z.writestr(f"{folder}/ERROR.txt", f"Failed to read file: {uploaded.name}\n\n{repr(e)}")
                continue

            # info files
            if result.get("excel_engine"):
                z.writestr(f"{folder}/INFO_excel_engine.txt", f"Excel engine used: {result['excel_engine']}")
            if result.get("encoding"):
                z.writestr(f"{folder}/INFO_encoding.txt", f"Read using encoding: {result['encoding']}")
            if result.get("note"):
                z.writestr(f"{folder}/INFO_note.txt", result["note"])
            if result.get("row_report"):
                lines = [f"{sh}: rows_read={b} | rows_after_clean={a}" for sh, (b, a) in result["row_report"].items()]
                z.writestr(f"{folder}/INFO_rows_report.txt", "\n".join(lines))

           # ---------------- Convert only = ONLY convert (NO outlet splitting) ----------------
if mode == "Convert only (no splitting)":
    if result["type"] == "excel":
        sheets = result["sheets"] or {}
        if not sheets:
            z.writestr(f"{folder}/ERROR.txt", "No readable data found in this Excel file.")
            continue

        if output_format.startswith("XLSX"):
            # ✅ Keep ALL sheets, ONE workbook, same name
            z.writestr(f"{folder}/{base}.xlsx", to_xlsx_bytes_multi_sheet(sheets))

        else:
            # ✅ CSV cannot store multiple sheets, so make ONE CSV by stacking
            frames = []
            for sh, df_sh in sheets.items():
                out = df_sh.copy()
                out.insert(0, "_sheet", sh)  # keeps sheet origin
                frames.append(out)
            merged = pd.concat(frames, ignore_index=True)

            write_file(z, f"{folder}/{base}", merged, sheet_name=base)

    else:
        # table/text/fake-xls -> ONE file, same name
        df = result["df"]
        if df is None or df.empty:
            z.writestr(f"{folder}/ERROR.txt", "No readable rows found.")
            continue

        write_file(z, f"{folder}/{base}", df, sheet_name=base)

    z.writestr(f"{folder}/INFO.txt", "Convert-only mode → converted ONLY (no outlet splitting).")
    continue

            # ---------------- Auto split + convert (NO combined output) ----------------
            if result["type"] == "excel":
                sheets = result["sheets"] or {}
                if not sheets:
                    z.writestr(f"{folder}/ERROR.txt", "No readable data found in this Excel file.")
                    continue

                # multiple sheets => treat each sheet as outlet, filename = sheet only (no outlet_)
                if len(sheets) > 1:
                    combined_frames = []
                    for sh, df_sh in sheets.items():
                        out = df_sh.copy()
                        if not keep_outlet_id_only_in_filename:
                            out.insert(0, "outlet_id", sh)
                        out.insert(0 if keep_outlet_id_only_in_filename else 1, "_sheet", sh)
                        combined_frames.append(out)

                        # ✅ filename = sheet name only
                        write_file(z, f"{folder}/{safe_name(sh)}", out, sheet_name=sh)

                    # keep long_format only (no combined)
                    long_df = pd.concat(combined_frames, ignore_index=True)
                    write_file(z, f"{folder}/long_format", long_df, sheet_name="long_format")
                    z.writestr(f"{folder}/INFO.txt", "Detected multiple sheets → split per sheet/outlet. (No combined output)")
                    continue

                df = list(sheets.values())[0]
            else:
                df = result["df"]

            if df is None or df.empty:
                z.writestr(f"{folder}/ERROR.txt", "No readable rows found.")
                continue

            # outlets as columns
            outlet_cols = detect_outlet_columns(df)
            if outlet_cols:
                base_cols = [c for c in df.columns if c not in outlet_cols]

                long_df = df.melt(
                    id_vars=base_cols,
                    value_vars=outlet_cols,
                    var_name="outlet_id",
                    value_name="outlet_value"
                )

                if exclude_zero:
                    v = long_df["outlet_value"].astype(str).str.strip()
                    long_df = long_df[~v.isin(["", "0", "0.0"])]

                write_file(z, f"{folder}/long_format", long_df, sheet_name="long_format")

                for oc in outlet_cols:
                    out_df = df[base_cols + [oc]].copy().rename(columns={oc: "outlet_value"})
                    if exclude_zero:
                        v = out_df["outlet_value"].astype(str).str.strip()
                        out_df = out_df[~v.isin(["", "0", "0.0"])]

                    if not keep_outlet_id_only_in_filename:
                        out_df.insert(0, "outlet_id", oc)

                    # ✅ filename = outlet id only
                    write_file(z, f"{folder}/{safe_name(oc)}", out_df, sheet_name=str(oc))

                z.writestr(f"{folder}/INFO.txt", "Detected outlets as COLUMNS → split per outlet column. (No combined output)")
                continue

            # outlets as rows
            df2, combined_key = apply_combined_outlet_key_if_possible(df)
            if combined_key:
                outlet_row_col = combined_key
                df = df2
            else:
                outlet_row_col = detect_outlet_row_column_smart(df)

            if outlet_row_col:
                z.writestr(f"{folder}/INFO_outlet_column.txt", f"Outlet column detected: {outlet_row_col}")

                for outlet, grp in df.groupby(outlet_row_col, dropna=False):
                    grp = grp.copy()
                    if not keep_outlet_id_only_in_filename:
                        grp.insert(0, "outlet_id", outlet)

                    # ✅ filename = outlet id only
                    write_file(z, f"{folder}/{safe_name(outlet)}", grp, sheet_name=str(outlet))

                long_df = df.copy()
                long_df.insert(0, "outlet_id", long_df[outlet_row_col])
                write_file(z, f"{folder}/long_format", long_df, sheet_name="long_format")

                z.writestr(f"{folder}/INFO.txt", f"Detected outlets as ROWS using column: {outlet_row_col}. (No combined output)")
                continue

            # fallback (no outlet detected) — just export original name
            write_file(z, f"{folder}/{base}", df, sheet_name=base)
            z.writestr(f"{folder}/INFO.txt", "No outlet detected → exported single file with original name (no combined).")

    st.success("Processed files successfully ✅")
    st.download_button(
        "⬇️ Download results (ZIP)",
        big_zip.getvalue(),
        file_name="outlet_outputs.zip",
        mime="application/zip",
    )
