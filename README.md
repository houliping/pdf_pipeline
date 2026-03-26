# V2_InterleaveDataPool 分析/采样/章节切分（模块化实现）

这个项目按阶段处理 `V2_InterleaveDataPool` 的 `jsonl_res/data_*.jsonl`：

1. `analyze`：抽样子集做统计+可视化，并对关键门控（目前支持 `token_total_min`）做 sweep，帮助你调阈值。
2. `select`：基于门控过滤 + `pdf_md5` 粒度去重（通过 `selected_manifest` 的方式实现抽样资格）+ `category_code` 配额 reservoir（近似）。
3. `emit`：再次扫描全量，只把命中的行写到新目录；并对输出做一次 `pdf_md5` 去重，保证“同一 pdf_md5 只采一份”。
4. `chunk`：对 `emit` 产出的样本按 `chapter_level` 真切分（读取 `meta_info.address.s3_parse_address` 下的 `middle.json`，用 adapter 推断 title->interleave slot 映射；返回 chunks，切 `texts/images` 并重算可用统计字段）。

## 数据约定

- 输入目录（本地挂载）：`<root>/<dataset_name>/<jsonl_res>`（默认 `jsonl_res`）
- 文件命名：`data_XXXX.jsonl`
- 每行 json：
  - `pdf_md5`
  - `meta_info.statistics_info.clean_image_num`
  - `meta_info.statistics_info.clean_total_token`
  - `meta_info.origin_pdf_layout_score.{decision,score}`
  - `meta_info.origin_pdf_nsfw_score.{decision,score}`
  - `meta_info.language_fasttext.language`（可选）
  - `meta_info.category_cls_v1.3.{category_name,category_code,score}`
  - `meta_info.chapter_info`（用于 `chapter_level` 过滤/切分）

## 安装/依赖

- 代码会优先使用 `orjson`（不可用则 fallback `json`）
- 可视化使用 `matplotlib`（不可用则只输出 json 统计文件）
- `chunk` 阶段需要 `middle.json` 可访问（本地路径 or 通过 `moxing` 读取 `s3://`/`obs://`）。

## 命令行

### 1) analyze（统计+可视化+token阈值sweep）

```bash
python scripts/v2_interleave_cli.py analyze \
  --dataset_name YOUR_DATASET \
  --root /data/bucket-8107/hlp/V2_InterleaveDataPool \
  --jsonl_dir_name jsonl_res \
  --out_dir /tmp/v2_run1 \
  --viz_reservoir_size 200000 \
  --sweep_token_total_min_list "0,200,500,1000,2000"
```

输出：
- `/tmp/v2_run1/analysis/reservoir_stats.json`
- `/tmp/v2_run1/analysis/*.png`
- `/tmp/v2_run1/analysis/sweep_token_total_min.json`

`category_code.txt` 默认在仓库根目录，会用于把 `category_code` 显示为中文名（`bar_main_category_cn.png`）。

### 2) select（生成 selected_manifest）

```bash
python scripts/v2_interleave_cli.py select \
  --dataset_name YOUR_DATASET \
  --root /data/bucket-8107/hlp/V2_InterleaveDataPool \
  --jsonl_dir_name jsonl_res \
  --output_dir /tmp/v2_run1 \
  --sample_total 100000 \
  --category_ratio_json '{"0205":0.4,"0201":0.6}' \
  --min_image_num 1 \
  --token_total_min 200 \
  --token_total_max 5000 \
  --layout_score_min 50 \
  --nsfw_score_max 0.1 \
  --num_workers 8
```

输出：
- `/tmp/v2_run1/selected_manifest.json`

### 3) emit（输出采样后的 jsonl 行 + pdf_md5 去重）

```bash
python scripts/v2_interleave_cli.py emit \
  --dataset_name YOUR_DATASET \
  --root /data/bucket-8107/hlp/V2_InterleaveDataPool \
  --jsonl_dir_name jsonl_res \
  --selected_manifest /tmp/v2_run1/selected_manifest.json \
  --output_dir /tmp/v2_run1 \
  --num_workers 8
```

输出：
- `/tmp/v2_run1/jsonl_res_sample_raw/data_XXXX.jsonl`（中间产物）
- `/tmp/v2_run1/jsonl_res_sample/data_XXXX.jsonl`（最终去重后）

### 4) chunk（true_chunk：按 chapter_level 切分）

```bash
python scripts/v2_interleave_cli.py chunk \
  --dataset_name YOUR_DATASET \
  --root /data/bucket-8107/hlp/V2_InterleaveDataPool \
  --input_dir /tmp/v2_run1 \
  --output_dir /tmp/v2_run1_chunk \
  --chapter_level 1 \
  --image_token_guess 0 \
  --num_workers 8
```

输出：
- `/tmp/v2_run1_chunk/chunked_jsonl_res/data_XXXX.jsonl`

## chapter 切分 adapter（可优化点）

当前 `MinerU2MiddleAdapter` 是启发式实现：它会在 `middle.json` 中寻找“block/paragraph/items 列表”，并尝试提取每个块的 `title_idx`（字段名通过候选 key 列表匹配）。如果映射失败：

```bash
... chunk ... --debug
```

会直接抛出异常并附带 `middle.json` 的 keys 片段，方便你对 adapter 做针对性优化。

