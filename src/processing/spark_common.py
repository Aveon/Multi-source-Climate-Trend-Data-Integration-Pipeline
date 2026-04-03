import os
import sys
import tempfile

from pyspark.sql import SparkSession


NOISY_SPARK_STARTUP_PREFIXES = (
    "WARNING: Using incubator modules:",
    "Using Spark's default log4j profile:",
    "Setting default log level to",
    "To adjust logging level use",
)
NOISY_SPARK_STARTUP_SNIPPETS = (
    "WARN Utils: Your hostname",
    "Set SPARK_LOCAL_IP if you need to bind to another address",
    "WARN Utils: Service 'SparkUI' could not bind on port",
    "WARN NativeCodeLoader: Unable to load native-hadoop library for your platform",
    "WARN SparkConf: Note that spark.local.dir will be overridden",
    "INFO: Closing down clientserver connection",
)


def _is_noisy_spark_startup_line(line: str) -> bool:
    """Identify Spark/JVM startup lines that add noise without helping operators."""
    stripped = line.strip()
    if not stripped:
        return True
    if stripped.startswith(NOISY_SPARK_STARTUP_PREFIXES):
        return True
    return any(snippet in stripped for snippet in NOISY_SPARK_STARTUP_SNIPPETS)


def _emit_filtered_startup_output(captured_output: str) -> None:
    """Replay only the Spark startup lines that are still worth showing."""
    for line in captured_output.splitlines():
        if _is_noisy_spark_startup_line(line):
            continue
        print(line, file=sys.stderr)


def _create_spark_session(builder) -> SparkSession:
    """Create the Spark session while filtering known startup noise from stdout/stderr."""
    stdout_fd = sys.stdout.fileno()
    stderr_fd = sys.stderr.fileno()
    saved_stdout_fd = os.dup(stdout_fd)
    saved_stderr_fd = os.dup(stderr_fd)

    with tempfile.TemporaryFile(mode="w+b") as capture_file:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
            os.dup2(capture_file.fileno(), stdout_fd)
            os.dup2(capture_file.fileno(), stderr_fd)
            spark = builder.getOrCreate()
        finally:
            sys.stdout.flush()
            sys.stderr.flush()
            os.dup2(saved_stdout_fd, stdout_fd)
            os.dup2(saved_stderr_fd, stderr_fd)
            os.close(saved_stdout_fd)
            os.close(saved_stderr_fd)
            capture_file.seek(0)
            captured_output = capture_file.read().decode("utf-8", errors="replace")

    _emit_filtered_startup_output(captured_output)
    return spark


def get_spark(app_name: str) -> SparkSession:
    """Create or reuse the project's local Spark session."""
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
        .config("spark.sql.parquet.output.committer.class", "org.apache.parquet.hadoop.ParquetOutputCommitter")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.driver.memory", "2g")
    )
    spark = _create_spark_session(builder)
    spark.sparkContext.setLogLevel("ERROR")
    return spark
