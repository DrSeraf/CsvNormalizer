import pandas as pd
from core.pipeline.runner import run_pipeline

in_csv = "test_input.csv"
out_csv = "test_output.csv"
log = "test_log.txt"
run_pipeline(
    input_csv=in_csv,
    output_csv=out_csv,
    config_yaml="configs/profiles/uni.yaml",
    log_txt=log,
    chunksize=1000,
    delimiter_override=",",
    encoding_override="utf-8",
)
print("DONE")
print(pd.read_csv(out_csv).to_string(index=False))

