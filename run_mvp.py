from core.pipeline.runner import run_pipeline

run_pipeline(
    input_csv="examples/sample_input_small.csv",
    output_csv="examples/out.csv",
    config_yaml="configs/profiles/minimal_email.yaml",
    log_txt="examples/out_log.txt",
    chunksize=100000,
)
