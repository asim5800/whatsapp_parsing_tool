"""
Utilities to parse WhatsApp chat exports and extract message details into
structured JSON and Excel files.  The parser reads a typical WhatsApp
export file contained within a ZIP archive, extracts messages and
attachments, and optionally performs OCR on attached image files to
retrieve any text contained within the images.

To parse a chat export, call the :func:`parse_chat` function with the
path to a ZIP file and the directory where output files should be
written.  The function returns the paths to the generated JSON and
Excel files.

.. note::
    OCR functionality requires the :mod:`pytesseract` library and a
    local installation of the Tesseract OCR engine.  If either of
    these components is missing the parser will still run but image
    attachments will not have OCR text extracted.

Example
-------

>>> from parse_whatsapp import parse_chat
>>> json_path, excel_path = parse_chat('export.zip', 'out')
>>> print('JSON written to', json_path)
>>> print('Excel written to', excel_path)

The resulting JSON has the following structure::

    {
      "messages": [
        {
          "date": "2025-09-09",
          "time": "17:58",
          "sender": "John Doe",
          "text": "Hello world",
          "attachments": [
            {
              "filename": "IMG-20250909-WA0059.jpg",
              "ocr_text": "...text extracted from image..."
            },
            ...
          ]
        },
        ...
      ]
    }

The Excel file contains one row per message with columns for the
timestamp, sender, textual content, attachment filenames and
extracted OCR text.
"""

from __future__ import annotations

import os
import re
import json
import zipfile
import tempfile
from datetime import datetime
from typing import List, Dict, Tuple, Optional

import pandas as pd

try:
    from PIL import Image
except ImportError:
    Image = None  # type: ignore

try:
    import pytesseract  # type: ignore
except ImportError:
    pytesseract = None  # type: ignore


def _convert_date(date_str: str) -> str:
    """Convert an exported date (e.g. ``9/8/25``) into ISO format.

    WhatsApp exports dates using a two-digit year; this helper
    interprets the year relative to 2000 and returns a full four
    digit year in ``YYYY-MM-DD`` format.

    Parameters
    ----------
    date_str:
        The raw date string from the export.

    Returns
    -------
    str
        The date in ISO ``YYYY-MM-DD`` format.
    """
    # WhatsApp uses m/d/yy; Python handles both 1 or 2 digit month/day
    try:
        dt = datetime.strptime(date_str, "%m/%d/%y")
    except ValueError:
        # Fallback: try with d/m/yy in case export is in other locale
        dt = datetime.strptime(date_str, "%d/%m/%y")
    return dt.strftime("%Y-%m-%d")


def _convert_time(time_str: str) -> str:
    """Convert a 12‑hour time string (e.g. ``1:32 PM``) to 24‑hour ``HH:MM``.

    The export may include narrow no-break spaces between the time and
    period; this helper removes any extraneous characters before
    conversion.
    """
    # Normalise any unicode spaces and uppercase AM/PM
    cleaned = time_str.replace("\u202f", " ").strip().upper()
    dt = datetime.strptime(cleaned, "%I:%M %p")
    return dt.strftime("%H:%M")


def _perform_ocr(image_path: str) -> str:
    """Attempt to extract text from an image using Tesseract OCR.

    If either the Pillow or pytesseract libraries are unavailable or
    OCR fails for any reason, this function returns an empty string.
    """
    if pytesseract is None or Image is None:
        return ""
    try:
        with Image.open(image_path) as img:
            # Convert to RGB to avoid unsupported mode errors
            if img.mode != 'RGB':
                img = img.convert('RGB')
            text = pytesseract.image_to_string(img)
            return text.strip()
    except Exception:
        return ""


def parse_chat(zip_path: str, output_dir: str) -> Tuple[str, str]:
    """Parse a WhatsApp chat export ZIP file into JSON and Excel files.

    Parameters
    ----------
    zip_path:
        The path to the WhatsApp export ZIP file.
    output_dir:
        Directory where the generated ``chat_data.json`` and
        ``chat_data.xlsx`` files will be stored.  The directory
        structure will be created if it does not already exist.

    Returns
    -------
    Tuple[str, str]
        A two‑element tuple containing the path to the JSON file and
        the path to the Excel file.
    """
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")
    os.makedirs(output_dir, exist_ok=True)
    # Extract all contents of the ZIP into a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(tmpdir)
        # Locate the chat text file (.txt extension)
        txt_files = [f for f in os.listdir(tmpdir) if f.lower().endswith('.txt')]
        if not txt_files:
            raise RuntimeError("No .txt chat file found in the provided ZIP archive.")
        chat_file = os.path.join(tmpdir, txt_files[0])
        attachments_dir = tmpdir
        # Prepare regex for parsing message lines.
        # There are two possible formats for exported lines:
        # 1. Regular messages: ``date, time - sender: message``
        # 2. System messages (no sender): ``date, time - message``
        # We compile two patterns and attempt to match the more specific one first.
        regular_re = re.compile(
            r'^(\d{1,2}/\d{1,2}/\d{2}),\s+(\d{1,2}:\d{2}\s*[APap][Mm])\s+-\s+([^:]+?):\s*(.*)$'
        )
        system_re = re.compile(
            r'^(\d{1,2}/\d{1,2}/\d{2}),\s+(\d{1,2}:\d{2}\s*[APap][Mm])\s+-\s+(.*)$'
        )
        messages: List[Dict[str, object]] = []
        current_msg: Optional[Dict[str, object]] = None
        with open(chat_file, 'r', encoding='utf-8', errors='ignore') as fh:
            for raw_line in fh:
                line = raw_line.rstrip('\n')
                # Try to match a regular message first
                m_regular = regular_re.match(line)
                if m_regular:
                    if current_msg is not None:
                        messages.append(current_msg)
                    date_str, time_str, sender, message_text = m_regular.groups()
                    date_iso = _convert_date(date_str)
                    time_iso = _convert_time(time_str)
                    current_msg = {
                        'date': date_iso,
                        'time': time_iso,
                        'sender': sender.strip(),
                        'text': message_text.strip(),
                        'attachments': []  # type: ignore
                    }
                    continue
                # Try system message format (no sender)
                m_system = system_re.match(line)
                if m_system:
                    if current_msg is not None:
                        messages.append(current_msg)
                    date_str, time_str, message_text = m_system.groups()
                    date_iso = _convert_date(date_str)
                    time_iso = _convert_time(time_str)
                    current_msg = {
                        'date': date_iso,
                        'time': time_iso,
                        'sender': 'System',
                        'text': message_text.strip(),
                        'attachments': []  # type: ignore
                    }
                    continue
                # Continuation of previous message (no date/time at start)
                if current_msg is not None:
                    current_msg['text'] += '\n' + line
            # Append the last message after reading all lines
            if current_msg is not None:
                messages.append(current_msg)
        # Identify attachments within the message text
        attach_re = re.compile(r'([A-Za-z0-9_\-]+\.(?:jpg|jpeg|png|gif|bmp|webp|heic|opus|mp4))', re.IGNORECASE)
        for msg in messages:
            text = msg.get('text', '') or ''
            attachments: List[Dict[str, str]] = []
            for m in attach_re.finditer(text):
                fname = m.group(1)
                fpath = os.path.join(attachments_dir, fname)
                ocr_text = ''
                # Only attempt OCR on supported image types
                if os.path.isfile(fpath) and fname.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.heic')):
                    ocr_text = _perform_ocr(fpath)
                attachments.append({'filename': fname, 'ocr_text': ocr_text})
            if attachments:
                msg['attachments'] = attachments
        # Build a DataFrame for tabular output
        rows: List[Dict[str, str]] = []
        # Helper to extract collection/loan details from a message's text
        def extract_details(text: str) -> Dict[str, str]:
            """Extract predefined collection/loan details from a message.

            Only key–value pairs that correspond to recognised loan/collection
            fields are extracted.  Keys are normalised by removing
            non‑alphabet characters and comparing against known
            patterns such as ``loan_num``, ``name`` or ``phone_number``.
            Unrecognised key–value pairs are ignored so that unrelated
            colon‑separated messages (e.g. links) do not populate
            collection detail columns.
            """
            details: Dict[str, str] = {}
            for segment in text.split('\n'):
                if ':' not in segment:
                    continue
                key_part, value_part = segment.split(':', 1)
                key_raw = key_part.strip()
                value_raw = value_part.strip().lstrip('-').strip()
                if not key_raw:
                    continue
                # Normalise key: lowercase and remove non‑alphabetical characters
                key_norm = re.sub(r'[^a-z]', '', key_raw.lower())
                canonical: Optional[str] = None
                if 'loan' in key_norm and 'num' in key_norm:
                    canonical = 'loan_num'
                elif 'loan' in key_norm and 'no' in key_norm:
                    canonical = 'loan_num'
                elif 'name' in key_norm:
                    canonical = 'name'
                elif 'phone' in key_norm or 'mobile' in key_norm:
                    canonical = 'phone_number'
                elif 'loan' in key_norm and 'amount' in key_norm:
                    canonical = 'loan_amount'
                elif 'disbursal' in key_norm and 'date' in key_norm:
                    canonical = 'disbursal_date'
                elif 'repayment' in key_norm and 'amt' in key_norm:
                    canonical = 'repayment_amt'
                elif 'repayment' in key_norm and 'amount' in key_norm:
                    canonical = 'repayment_amt'
                elif 'repayment' in key_norm and 'date' in key_norm:
                    canonical = 'repayment_date'
                elif 'receive' in key_norm and 'amt' in key_norm:
                    canonical = 'receive_amt'
                elif 'receive' in key_norm and 'amount' in key_norm:
                    canonical = 'receive_amt'
                elif 'receive' in key_norm and 'date' in key_norm:
                    canonical = 'receive_date'
                elif 'status' in key_norm:
                    canonical = 'status'
                elif 'reloan' in key_norm:
                    canonical = 'reloan'
                # If we recognised the key, record its value
                if canonical:
                    details[canonical] = value_raw
            return details

        for msg in messages:
            att_names = ', '.join(att['filename'] for att in msg.get('attachments', []))
            att_texts = ' || '.join(att['ocr_text'] for att in msg.get('attachments', []) if att['ocr_text'])
            row: Dict[str, str] = {
                'date': msg['date'],
                'time': msg['time'],
                'sender': msg['sender'],
                'text': msg['text'],
                'attachments': att_names,
                'ocr_text': att_texts,
            }
            # Extract key–value details from the message and prefix keys
            details = extract_details(msg['text'])
            for key, val in details.items():
                row[f'collection_details_{key}'] = val
            rows.append(row)
        df = pd.DataFrame(rows)
        # Write JSON
        json_data = {'messages': messages}
        json_path = os.path.join(output_dir, 'chat_data.json')
        with open(json_path, 'w', encoding='utf-8') as jf:
            json.dump(json_data, jf, ensure_ascii=False, indent=2)
        # Write Excel
        excel_path = os.path.join(output_dir, 'chat_data.xlsx')
        # Ensure there is at least one row to avoid Excel writing errors
        if df.empty:
            df = pd.DataFrame([{'date': '', 'time': '', 'sender': '', 'text': '', 'attachments': '', 'ocr_text': ''}])
        df.to_excel(excel_path, index=False)
        return json_path, excel_path
