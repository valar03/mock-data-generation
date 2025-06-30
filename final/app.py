import streamlit as st
import pandas as pd
import os
import tempfile

from main import run_pipeline  # your main logic as a callable

st.set_page_config(page_title="Mock Data Generator", layout="centered")
st.title("📄 Mock Data Generator")
st.write("Upload a raw data file and get realistic mock data based on patterns learned.")

uploaded_file = st.file_uploader("Upload a raw .csv/.dat/.txt file", type=["csv", "dat", "txt"])

records = st.slider("Number of mock records to generate", 100, 5000, step=100, value=500)

if uploaded_file:
    st.success("✅ File uploaded successfully.")

    if st.button("Generate Mock Data"):
        with st.spinner("⏳ Processing..."):
            # Save uploaded file to temp location
            tmp_dir = tempfile.TemporaryDirectory()
            input_path = os.path.join(tmp_dir.name, uploaded_file.name)
            with open(input_path, "wb") as f:
                f.write(uploaded_file.read())

            # Set output path
            output_path = os.path.join(tmp_dir.name, "mock_output.csv")

            # Run main logic
            try:
                run_pipeline(input_path, output_path, records)
                st.success("✅ Mock data generated!")

                # Show preview
                df = pd.read_csv(output_path)
                st.dataframe(df.head(10))

                # Download link
                st.download_button("📥 Download CSV", data=df.to_csv(index=False),
                                   file_name="mock_data.csv", mime="text/csv")
            except Exception as e:
                st.error(f"❌ Error: {e}")
            finally:
                tmp_dir.cleanup()
