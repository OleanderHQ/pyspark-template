#!/usr/bin/env bash

set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"
ICEBERG_SPARK_RUNTIME_PACKAGE="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1"
ICEBERG_AWS_BUNDLE_PACKAGE="org.apache.iceberg:iceberg-aws-bundle:1.10.1"
OLEANDER_ICEBERG_REST_URI="https://iceberg.oleander.dev/catalog"
S3TABLES_REST_URI_TEMPLATE='https://s3tables.{region}.amazonaws.com/iceberg'
ICEBERG_EXTENSION="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

packages=()
extensions=()
user_args=()

die() {
  printf '%s: %s\n' "$SCRIPT_NAME" "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

append_unique_package() {
  local package="$1"
  local existing

  [[ -n "$package" ]] || return 0
  for existing in "${packages[@]}"; do
    [[ "$existing" == "$package" ]] && return 0
  done
  packages+=("$package")
}

append_unique_extension() {
  local extension="$1"
  local existing

  [[ -n "$extension" ]] || return 0
  for existing in "${extensions[@]}"; do
    [[ "$existing" == "$extension" ]] && return 0
  done
  extensions+=("$extension")
}

add_package_csv() {
  local rest="$1"
  local item

  while [[ "$rest" == *,* ]]; do
    item="$(trim "${rest%%,*}")"
    append_unique_package "$item"
    rest="${rest#*,}"
  done

  item="$(trim "$rest")"
  append_unique_package "$item"
}

add_extension_csv() {
  local rest="$1"
  local item

  while [[ "$rest" == *,* ]]; do
    item="$(trim "${rest%%,*}")"
    append_unique_extension "$item"
    rest="${rest#*,}"
  done

  item="$(trim "$rest")"
  append_unique_extension "$item"
}

join_by_comma() {
  local result=""
  local item

  for item in "$@"; do
    if [[ -z "$result" ]]; then
      result="$item"
    else
      result="${result},${item}"
    fi
  done

  printf '%s' "$result"
}

spark_option_requires_value() {
  case "$1" in
    --archives|--class|--conf|--deploy-mode|--driver-class-path|--driver-java-options|--driver-library-path|--driver-memory|--exclude-packages|--executor-cores|--executor-memory|--files|--jars|--keytab|--kill|--master|--name|--num-executors|--packages|--principal|--properties-file|--proxy-user|--py-files|--queue|--remote|--repositories|--status|--total-executor-cores)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

parse_user_args() {
  local arg
  local value

  while (($# > 0)); do
    arg="$1"
    case "$arg" in
      --)
        user_args+=("$@")
        return 0
        ;;
      --packages)
        (($# >= 2)) || die "--packages requires a value"
        add_package_csv "$2"
        shift 2
        ;;
      --packages=*)
        add_package_csv "${arg#--packages=}"
        shift
        ;;
      --conf)
        (($# >= 2)) || die "--conf requires a value"
        value="$2"
        if [[ "$value" == spark.sql.extensions=* ]]; then
          add_extension_csv "${value#spark.sql.extensions=}"
        else
          user_args+=("$arg" "$value")
        fi
        shift 2
        ;;
      --conf=*)
        value="${arg#--conf=}"
        if [[ "$value" == spark.sql.extensions=* ]]; then
          add_extension_csv "${value#spark.sql.extensions=}"
        else
          user_args+=("$arg")
        fi
        shift
        ;;
      --*=*)
        user_args+=("$arg")
        shift
        ;;
      --*)
        if spark_option_requires_value "$arg"; then
          (($# >= 2)) || die "$arg requires a value"
          user_args+=("$arg" "$2")
          shift 2
        else
          user_args+=("$arg")
          shift
        fi
        ;;
      -*)
        user_args+=("$arg")
        shift
        ;;
      *)
        user_args+=("$@")
        return 0
        ;;
    esac
  done
}

generate_oleander_conf_args() {
  python3 - "$SCRIPT_NAME" "$OLEANDER_ICEBERG_REST_URI" "$S3TABLES_REST_URI_TEMPLATE" <<'PY'
import json
import subprocess
import sys

wrapper_name = sys.argv[1]
oleander_rest_uri = sys.argv[2]
s3tables_uri_template = sys.argv[3]


def fail(message):
    print(f"{wrapper_name}: {message}", file=sys.stderr)
    sys.exit(1)


def run_json(args):
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip()
        suffix = f": {detail}" if detail else ""
        fail(f"{' '.join(args)} failed{suffix}")

    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        fail(f"{' '.join(args)} returned invalid JSON: {exc}")


def required(mapping, key, context):
    value = mapping.get(key)
    if value is None or value == "":
        fail(f"{context} is missing {key}")
    return str(value)


def emit_arg(value):
    if "\0" in value:
        fail("generated Spark argument contains a NUL byte")
    sys.stdout.buffer.write(value.encode("utf-8"))
    sys.stdout.buffer.write(b"\0")


def emit_conf(key, value):
    emit_arg("--conf")
    emit_arg(f"{key}={value}")


def catalog_prefix(name):
    return f"spark.sql.catalog.{name}"


def emit_rest_catalog(name, warehouse, token):
    prefix = catalog_prefix(name)
    emit_conf(prefix, "org.apache.iceberg.spark.SparkCatalog")
    emit_conf(f"{prefix}.type", "rest")
    emit_conf(f"{prefix}.uri", oleander_rest_uri)
    emit_conf(f"{prefix}.warehouse", warehouse)
    emit_conf(f"{prefix}.rest.auth.type", "oauth2")
    emit_conf(f"{prefix}.token", token)
    emit_conf(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")


def emit_s3tables_catalog(name, properties, credentials):
    context = f"catalog {name}"
    region = required(properties, "region", context)
    account_id = required(properties, "accountId", context)
    bucket = required(properties, "bucket", context)
    access_key_id = required(credentials, "accessKeyId", f"{context} credentials")
    secret_access_key = required(credentials, "secretAccessKey", f"{context} credentials")
    session_token = credentials.get("sessionToken")
    table_bucket_arn = (
        properties.get("tableBucketArn")
        or properties.get("bucketArn")
        or f"arn:aws:s3tables:{region}:{account_id}:bucket/{bucket}"
    )

    try:
        uri = s3tables_uri_template.format(region=region)
    except Exception as exc:
        fail(f"invalid S3 Tables REST URI template: {exc}")

    prefix = catalog_prefix(name)
    emit_conf(prefix, "org.apache.iceberg.spark.SparkCatalog")
    emit_conf(f"{prefix}.type", "rest")
    emit_conf(f"{prefix}.uri", uri)
    emit_conf(f"{prefix}.warehouse", table_bucket_arn)
    emit_conf(f"{prefix}.rest.sigv4-enabled", "true")
    emit_conf(f"{prefix}.rest.signing-name", "s3tables")
    emit_conf(f"{prefix}.rest.signing-region", region)
    emit_conf(f"{prefix}.rest.access-key-id", access_key_id)
    emit_conf(f"{prefix}.rest.secret-access-key", secret_access_key)
    if session_token:
        emit_conf(f"{prefix}.rest.session-token", str(session_token))
    emit_conf(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    emit_conf(f"{prefix}.client.region", region)
    emit_conf(f"{prefix}.s3.access-key-id", access_key_id)
    emit_conf(f"{prefix}.s3.secret-access-key", secret_access_key)
    if session_token:
        emit_conf(f"{prefix}.s3.session-token", str(session_token))


catalogs_doc = run_json(["oleander", "catalogs", "list", "--json"])
catalogs = catalogs_doc.get("catalogs") if isinstance(catalogs_doc, dict) else catalogs_doc
if catalogs is None:
    catalogs = []
if not isinstance(catalogs, list):
    fail("oleander catalogs list --json returned an unexpected shape")

for catalog in catalogs:
    if not isinstance(catalog, dict):
        fail("oleander catalogs list --json returned a non-object catalog entry")

    name = required(catalog, "name", "catalog entry")
    catalog_type = str(catalog.get("type", "")).lower()
    properties = catalog.get("properties") or {}
    if not isinstance(properties, dict):
        fail(f"catalog {name} properties must be an object")

    credentials = run_json(["oleander", "catalogs", "credentials", name, "--json"])
    if not isinstance(credentials, dict):
        fail(f"oleander credentials for catalog {name} returned an unexpected shape")

    if catalog_type == "lakekeeper":
        warehouse = required(properties, "warehouse", f"catalog {name}")
        token = required(credentials, "token", f"catalog {name} credentials")
        emit_rest_catalog(name, warehouse, token)
    elif catalog_type == "s3tables":
        emit_s3tables_catalog(name, properties, credentials)
    else:
        print(f"{wrapper_name}: skipping unsupported catalog {name} of type {catalog_type}", file=sys.stderr)
PY
}

for arg in "$@"; do
  case "$arg" in
    -h|--help|--version)
      exec "$SPARK_SUBMIT_BIN" "$@"
      ;;
  esac
done

require_command "$SPARK_SUBMIT_BIN"
require_command oleander
require_command python3

append_unique_package "$ICEBERG_SPARK_RUNTIME_PACKAGE"
append_unique_package "$ICEBERG_AWS_BUNDLE_PACKAGE"
append_unique_extension "$ICEBERG_EXTENSION"
parse_user_args "$@"

tmp_file="$(mktemp "/tmp/oleander-spark-submit-conf.XXXXXX")"
trap 'rm -f "$tmp_file"' EXIT

generate_oleander_conf_args >"$tmp_file"

catalog_args=()
if [[ -s "$tmp_file" ]]; then
  while IFS= read -r -d '' arg; do
    catalog_args+=("$arg")
  done <"$tmp_file"
fi

packages_csv="$(join_by_comma "${packages[@]}")"
extensions_csv="$(join_by_comma "${extensions[@]}")"

wrapper_args=(
  --packages "$packages_csv"
  --conf "spark.sql.extensions=${extensions_csv}"
)

exec "$SPARK_SUBMIT_BIN" "${wrapper_args[@]}" "${catalog_args[@]}" "${user_args[@]}"
