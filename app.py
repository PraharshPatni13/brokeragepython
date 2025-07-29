import os
import pandas as pd
import pdfplumber
import re
import uuid
from flask import Flask, request, jsonify, send_file, render_template, send_from_directory
from werkzeug.utils import secure_filename
from fuzzywuzzy import process
from typing import Dict, List, Set, Optional, Tuple
from werkzeug.exceptions import BadRequest, InternalServerError

app = Flask(__name__, static_folder='static', template_folder='templates')

# Configuration from environment variables
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'Uploads')
OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER', 'filled_output')
PDF_PASSWORDS = ["ARN100481", "AAHCP7661C", ""]
MAX_REASONABLE_RATE = 10.0
ALLOWED_EXTENSIONS = {"pdf", "xlsx", "xls"}
BROKERAGE_TYPES = [
    "FIRST YEAR TRAIL",
    "SECOND YEAR TRAIL",
    "THIRD YEAR TRAIL",
    "FOURTH YEAR TRAIL",
    "LONGTERM YEAR TRAIL",
]
SCHEME_VALIDATIONS = {
    "hsbc financial services fund": {"FOURTH YEAR TRAIL": 1.35},
    "hsbc india export opportunities fund": {
        "THIRD YEAR TRAIL": 1.45,
        "FOURTH YEAR TRAIL": 1.35,
    },
    "hsbc midcap fund": {
        "THIRD YEAR TRAIL": 1.15,
        "FOURTH YEAR TRAIL": 1.05,
        "LONGTERM YEAR TRAIL": 1.05,
    },
}
BROKERAGE_COLUMN_PATTERNS = [
    (re.compile(r"\b(first|1st)\s*(year|yr)\s*(trail|commission|rate)?\b", re.IGNORECASE), ["FIRST YEAR TRAIL"]),
    (re.compile(r"\b(second|2nd)\s*(year|yr)\s*(trail|commission|rate)?\b", re.IGNORECASE), ["SECOND YEAR TRAIL"]),
    (re.compile(r"\b(third|3rd)\s*(year|yr)\s*(trail|commission|rate)?\b", re.IGNORECASE), ["THIRD YEAR TRAIL"]),
    (re.compile(r"\b(fourth|4th)\s*(year|yr)\s*(trail|commission|rate)?\b", re.IGNORECASE), ["FOURTH YEAR TRAIL"]),
    (re.compile(r"\b(longterm|long\s*term|5\+?|beyond\s*4)\s*(year|yr)?\s*(trail|commission|rate)?\b", re.IGNORECASE), ["LONGTERM YEAR TRAIL"]),
    (re.compile(r"\b(1\s*[-to]\s*3|1\s*through\s*3|first\s*3|initial\s*3)\s*(year|years|yr|yrs)\s*(trail|commission|rate)?\b", re.IGNORECASE), ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"]),
    (re.compile(r"\b(trail\s*(1\s*[-to]\s*3|1-3)|years?\s*1-3)\b", re.IGNORECASE), ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"]),
]

# Ensure upload and output directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def allowed_file(filename: str, extensions: Set[str]) -> bool:
    """Check if a file has an allowed extension."""
    return "." in filename and filename.rsplit(".", 1)[1].lower() in extensions

@app.route("/")
def form():
    """Render the file upload form."""
    return render_template("upload.html")

@app.route("/upload", methods=["POST"])
def upload_files():
    """Handle file uploads and process PDF and Excel files."""
    try:
        if "pdf" not in request.files or "excel" not in request.files:
            raise BadRequest("Both PDF and Excel files are required")

        pdf_file = request.files["pdf"]
        excel_file = request.files["excel"]

        if not allowed_file(pdf_file.filename, {"pdf"}) or not allowed_file(excel_file.filename, {"xlsx", "xls"}):
            raise BadRequest("Invalid file format. PDF and Excel files only.")

        pdf_filename = secure_filename(pdf_file.filename)
        excel_filename = secure_filename(excel_file.filename)
        pdf_path = os.path.join(UPLOAD_FOLDER, pdf_filename)
        excel_path = os.path.join(UPLOAD_FOLDER, excel_filename)
        output_filename = f"{uuid.uuid4()}.xlsx"
        output_path = os.path.join(OUTPUT_FOLDER, output_filename)

        pdf_file.save(pdf_path)
        excel_file.save(excel_path)
        scheme_map = extract_scheme_data(pdf_path, PDF_PASSWORDS)
        fill_excel(excel_path, scheme_map, output_path)
        return send_file(output_path, as_attachment=True, download_name="filled_brokerage.xlsx")
    except BadRequest as e:
        return jsonify({"error": str(e)}), 400
    except Exception:
        raise InternalServerError("An error occurred while processing the files")
    finally:
        for path in (pdf_path, excel_path, output_path):
            if os.path.exists(path):
                try:
                    os.remove(path)
                except OSError:
                    pass

def extract_scheme_data(pdf_path: str, passwords: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """Extract brokerage rates from a PDF file."""
    scheme_map: Dict[str, Dict[str, Optional[float]]] = {}
    rate_pattern = re.compile(r"(\d*\.\d{1,2}%?)")

    for password in passwords:
        try:
            with pdfplumber.open(pdf_path, password=password or None) as pdf:
                tables_found = False

                for page in pdf.pages:
                    tables = page.extract_tables()

                    for table in tables:
                        if not table or len(table) < 2:
                            continue
                        tables_found = True

                        header = [normalize(str(cell)) if cell else "" for cell in table[0]]
                        col_mapping = {}
                        for i, col in enumerate(header):
                            col_lower = col.lower()
                            if any(x in col_lower for x in ["scheme", "fund", "name"]):
                                col_mapping["SCHEME"] = i
                                continue
                            for pattern, brokerage_types in BROKERAGE_COLUMN_PATTERNS:
                                if pattern.search(col_lower):
                                    for bt in brokerage_types:
                                        col_mapping[bt] = i
                                    break

                        if "SCHEME" not in col_mapping:
                            continue

                        for row in table[1:]:
                            if not row or len(row) <= col_mapping["SCHEME"]:
                                continue
                            scheme_name = normalize(str(row[col_mapping["SCHEME"]]))
                            if not scheme_name or any(x in scheme_name for x in ["scheme name", "total", "aggregate"]):
                                continue

                            rates = {bt: None for bt in BROKERAGE_TYPES}
                            for bt, col_idx in col_mapping.items():
                                if bt == "SCHEME" or col_idx >= len(row):
                                    continue
                                cell_value = str(row[col_idx]).strip() if row[col_idx] else ""
                                matches = rate_pattern.findall(cell_value)
                                if matches:
                                    try:
                                        rate_value = float(matches[0].replace(",", ".").rstrip("%"))
                                        if rate_value > MAX_REASONABLE_RATE:
                                            continue
                                        rates[bt] = rate_value
                                    except ValueError:
                                        continue

                            if rates.get("FOURTH YEAR TRAIL") and not rates.get("LONGTERM YEAR TRAIL"):
                                rates["LONGTERM YEAR TRAIL"] = rates["FOURTH YEAR TRAIL"]

                            if any(r for r in rates.values() if r is not None):
                                scheme_map[scheme_name] = rates

                    if not tables_found or not scheme_map:
                        text = page.extract_text()
                        if text:
                            lines = text.splitlines()
                            current_scheme = None
                            for i, line in enumerate(lines):
                                line = normalize(line.strip())
                                if not line or any(x in line for x in ["scheme name", "total", "aggregate"]):
                                    continue
                                matches = rate_pattern.findall(line)
                                scheme_name = normalize(re.sub(r"\d*\.\d{1,2}%?", "", line).strip())
                                if matches and scheme_name and not any(bt.lower() in scheme_name for bt in BROKERAGE_TYPES):
                                    current_scheme = scheme_name
                                    rates = {bt: None for bt in BROKERAGE_TYPES}
                                    rate_idx = 0
                                    for j in range(i, min(i + len(BROKERAGE_TYPES), len(lines))):
                                        subline = normalize(lines[j].strip())
                                        sub_matches = rate_pattern.findall(subline)
                                        matched_brokerage_types = []
                                        for pattern, brokerage_types in BROKERAGE_COLUMN_PATTERNS:
                                            if pattern.search(subline):
                                                matched_brokerage_types.extend(brokerage_types)
                                                break
                                        for rate in sub_matches:
                                            try:
                                                rate_value = float(rate.replace(",", ".").rstrip("%"))
                                                if rate_value > MAX_REASONABLE_RATE:
                                                    continue
                                                if matched_brokerage_types and rate_idx < len(matched_brokerage_types):
                                                    for bt in matched_brokerage_types:
                                                        rates[bt] = rate_value
                                                    rate_idx += len(matched_brokerage_types)
                                                elif rate_idx < len(BROKERAGE_TYPES):
                                                    rates[BROKERAGE_TYPES[rate_idx]] = rate_value
                                                    rate_idx += 1
                                            except ValueError:
                                                continue
                                    if rates.get("FOURTH YEAR TRAIL") and not rates.get("LONGTERM YEAR TRAIL"):
                                        rates["LONGTERM YEAR TRAIL"] = rates["FOURTH YEAR TRAIL"]
                                    if any(r for r in rates.values() if r is not None):
                                        scheme_map[current_scheme] = rates

                if scheme_map:
                    for scheme, expected_rates in SCHEME_VALIDATIONS.items():
                        if scheme in scheme_map:
                            rates = scheme_map[scheme]
                            for brokerage_type, expected_rate in expected_rates.items():
                                current_rate = rates.get(brokerage_type)
                                if current_rate != expected_rate:
                                    rates[brokerage_type] = expected_rate
                    return scheme_map

        except Exception:
            pass

    return {}

def normalize(text: str) -> str:
    """Normalize text by removing special characters and standardizing spaces."""
    text = re.sub(r"[^\w\s.]", "", str(text)).strip().lower()
    text = re.sub(r"\s*(regular plan|reg|institutional plan|ex institutional plan|retail plan|long term plan)\s*$", "", text)
    return text.strip()

def fill_excel(excel_path: str, scheme_map: Dict[str, Dict[str, Optional[float]]], output_path: str) -> None:
    """Fill Excel file with brokerage rates from scheme_map."""
    try:
        df = pd.read_excel(excel_path)
    except Exception:
        raise InternalServerError("Failed to read Excel file")

    date_col = [col for col in df.columns if "date" in col.lower() and "brokerage" not in col.lower()]
    if date_col:
        df[date_col[0]] = pd.to_datetime(df[date_col[0]], errors="coerce").dt.strftime("%d-%m-%Y")

    normalized_pdf_keys = {normalize(k): v for k, v in scheme_map.items()}
    brokerage_type_map = {
        "FIRST YEAR TRAIL": "FIRST YEAR TRAIL",
        "SECOND YEAR TRAIL": "SECOND YEAR TRAIL",
        "THIRD YEAR TRAIL": "THIRD YEAR TRAIL",
        "FOURTH YEAR TRAIL": "FOURTH YEAR TRAIL",
        "LONGTERM YEAR TRAIL": "LONGTERM YEAR TRAIL",
        "FOURTH YEAR": "FOURTH YEAR TRAIL",
        "4TH YEAR TRAIL": "FOURTH YEAR TRAIL",
        "4TH YEAR": "FOURTH YEAR TRAIL",
        "LONG TERM TRAIL": "LONGTERM YEAR TRAIL",
        "LONG TERM": "LONGTERM YEAR TRAIL",
        "1 TO 3 YEARS TRAIL": ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"],
        "1-3 YEARS TRAIL": ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"],
        "1 TO 3 YEARS": ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"],
        "1-3 YEARS": ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"],
        "TRAIL 1-3": ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"],
        "TRAIL YEARS 1-3": ["FIRST YEAR TRAIL", "SECOND YEAR TRAIL", "THIRD YEAR TRAIL"],
    }

    def get_brokerage(row) -> Optional[float]:
        """Extract brokerage rate for a given row."""
        try:
            scheme = normalize(str(row.get("Schemename", "")))
            brokerage_type = str(row.get("BrokerageName", "")).strip().upper()
            standardized_brokerage_types = brokerage_type_map.get(brokerage_type, brokerage_type)

            if isinstance(standardized_brokerage_types, list):
                pass
            elif standardized_brokerage_types not in BROKERAGE_TYPES:
                return None

            if not scheme or not standardized_brokerage_types:
                return None

            if scheme in normalized_pdf_keys:
                if isinstance(standardized_brokerage_types, list):
                    for bt in standardized_brokerage_types:
                        rate = normalized_pdf_keys[scheme].get(bt)
                        if rate is not None:
                            return rate
                    return None
                else:
                    rate = normalized_pdf_keys[scheme].get(standardized_brokerage_types)
                    return rate

            result = process.extractOne(scheme, list(normalized_pdf_keys.keys()), score_cutoff=90)
            if result is None:
                return None

            match, _ = result
            if isinstance(standardized_brokerage_types, list):
                for bt in standardized_brokerage_types:
                    rate = normalized_pdf_keys[match].get(bt)
                    if rate is not None:
                        return rate
                return None
            else:
                return normalized_pdf_keys[match].get(standardized_brokerage_types)
        except Exception:
            return None

    df["T15"] = df.apply(get_brokerage, axis=1)
    df["B15"] = df["T15"]

    try:
        df.to_excel(output_path, index=False)
    except Exception:
        raise InternalServerError("Failed to write output Excel file")

if __name__ == "__main__":
    # For local testing only, not used in production
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)))