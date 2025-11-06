"""
A simple Streamlit web interface for processing WhatsApp chat exports.

The application accepts a ZIP file exported from WhatsApp (obtained via
``Export chat``), parses the messages and attachments, performs OCR on
image attachments when possible, and provides the results as
downloadable JSON and Excel files.  This portal can be deployed to
platforms such as GitHub Pages (via Streamlit sharing) or any other
Python hosting environment supported by Streamlit.

To run locally, install the required dependencies::

    pip install streamlit pandas pillow pytesseract

Ensure that the Tesseract OCR engine is installed on your system if you
would like to extract text from images.  On Debian/Ubuntu systems
Tesseract can be installed via ``apt-get install tesseract-ocr``.

Run the app with::

    streamlit run app.py

Then open the URL displayed in your terminal.
"""
import os


import tempfile

import streamlit as st

from parse_whatsapp import parse_chat


def main() -> None:
    st.set_page_config(page_title="WhatsApp Chat Export Processor")
    st.title("WhatsApp Chat Export Processor")
    st.write(
        "Upload a ZIP file exported from WhatsApp (with media) and this portal\n"
        "will parse the messages, extract attachments, perform OCR on image\n"
        "files when possible and provide both a nested JSON and an Excel file\n"
        "for download."
    )
    uploaded_file = st.file_uploader("Select a WhatsApp export ZIP file", type=["zip"])
    if uploaded_file is not None:
        with st.spinner("Processing the chat export. This may take a few moments..."):
            # Save the uploaded zip to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip:
                temp_zip.write(uploaded_file.read())
                temp_zip_path = temp_zip.name
            # Use a temporary directory for output
            output_dir = tempfile.mkdtemp()
            try:
                json_path, excel_path = parse_chat(temp_zip_path, output_dir)
            except Exception as exc:
                st.error(f"An error occurred while parsing the chat: {exc}")
                # Clean up temp files
                os.unlink(temp_zip_path)
                return
            # Provide download buttons for both files
            st.success("Processing complete! Download your files below.")
            with open(json_path, "rb") as jf:
                st.download_button(
                    label="Download JSON",
                    data=jf,
                    file_name="chat_data.json",
                    mime="application/json",
                )
            with open(excel_path, "rb") as ef:
                st.download_button(
                    label="Download Excel",
                    data=ef,
                    file_name="chat_data.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
            # Clean up the temporary zip file after processing
            os.unlink(temp_zip_path)



if __name__ == "__main__":
    main()
