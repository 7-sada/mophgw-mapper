import pandas as pd
import httpx
import json

pd.options.mode.use_inf_as_na = True
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def get_process(url, headers, topic, **any):
  url = url + f"?topic={topic}"
  with httpx.Client() as client:
    r = client.get(url=url, headers=headers)
    json = r.json()
  if json["ok"] == True and len(json["rows"]) != 0:
    return json["rows"]
  else:
    return {}


def set_process(url, headers, process_id, status, **any):
  url = url + "/status"
  headers["Content-Type"] = "application/x-www-form-urlencoded"
  data = f"processId={process_id}&status={status}"
  with httpx.Client() as client:
    r = client.put(url=url, headers=headers, data=data)
    json = r.json()
  if (json["ok"] == True):
    return json
  else:
    return {}


def read_csv(minio_config: dict, path: str = "", filename: str = "", prefix_type: str = None):
  df = pd.read_csv(f"{path}/{filename}", engine="c",
                   dtype=pd.StringDtype(), storage_options=minio_config)
  prefix = filename.removesuffix(".csv.gz")
  if prefix_type != "":
    df = df.add_prefix(add_prefix(prefix, prefix_type))
  return df


def add_prefix(name: str = None, prefix_type: str = None):
  if prefix_type == "local":
    return f"{name}_local"
  elif prefix_type == "std":
    return f"{name}_"
  else:
    return ""


def merge_codes(minio_config: dict, main_df: pd.DataFrame, pathname: str, prefix_type: str, merge_config: dict) -> pd.DataFrame:
  main_cols = main_df.columns
  for k, v in merge_config.items():
    if v[0] in main_cols:
      ldf = read_csv(minio_config, pathname, k + ".csv.gz", prefix_type)
      ldf = ldf.drop_duplicates(subset=[add_prefix(k, prefix_type) + v[1]])
      main_df = pd.merge(main_df, ldf, how="left",
                         left_on=v[0], right_on=add_prefix(k, prefix_type) + v[1])
  return main_df


def rename_columns(df: pd.DataFrame, cols_rename: dict[str, str]) -> pd.DataFrame:
    cols = df.columns.to_list()
    for k, v in cols_rename.items():
        for i, col in enumerate(cols):
            if v in col:
                cols[i] = col.replace(v, k)
    df.columns = cols
    return df

def set_package(df: pd.DataFrame, cols_select: list, add_cols: list, value="") -> pd.DataFrame:
  df = df[cols_select].copy()
  df.loc[:, (add_cols)] = value
  return df


def upload_minio(minio_config: dict, df: pd.DataFrame, object_key: str, **api_process) -> bool:
  try:
    df.to_csv(f"{object_key}", index=False, lineterminator="\n",
              storage_options=minio_config, encoding="utf-8", compression="gzip")
    # set_process(status="SUCCESS", **api_process)
    return True
  except Exception as ex:
    # set_process(status="ERROR", **api_process)
    print(ex)
    return False
