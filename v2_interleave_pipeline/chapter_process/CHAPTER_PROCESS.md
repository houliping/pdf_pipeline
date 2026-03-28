# chapter_process：章节切分与 Pangu 行生成

## 你在描述里的目标（已实现 / 部分实现）

1. **输入**：jsonl 行内 `meta_info.address` 的 `s3_parse_address`（原始 middle）、`s3_clean_address`（清洗后 middle），以及 `chapter_info`（标题 tid → level）。
2. **读取**：同时打开原始 middle、清洗后 middle、同目录下 `{pdf_name}.md`；用**清洗后** `pdf_info` 做切块与 markdown 生成；用**原始** middle 做部分校验与图片类型策略。
3. **切块**：按 `chapter_info` 重标 `level` → 递归按页跨度切分 → 合并过短/缺图块。
4. **输出**：每块 `content2panguml` → `texts` / `images` / 计数；`read_panguml` 模式下将**整条源记录** `deepcopy` 后 `update(切块字段)`，便于继承 `meta_info` 等键。

## 代码审查结论

### 已修复的问题

| 问题 | 说明 |
|------|------|
| `title_utils_v3.py` 误与 `utils/mineru_utils.py` 整文件重复 | 已替换为真正的 `TitleUtils` + `print_filter_res`，按文档顺序为 title 块分配 tid。 |
| 验证段使用 `final_item["text"]` | `content2panguml` 返回的是 **`texts`**，已改正。 |
| `chapter_info` JSON key 为字符串 | 读入时转为 **`int` → `int`**，与 `title_info_dict` 对齐。 |
| 强依赖 `moxing` / `loguru` | `moxing` 可选，本地用 `open`；`loguru` 缺失时回退标准 `logging`。 |
| 包导入 | 统一为 `v2_interleave_pipeline.chapter_process.*`，并补 `__init__.py`。 |

### 仍待完善（对接「最终目标」时建议做）

1. **写输出 jsonl**：`process_main` 里已构造 `final_items`，但**尚未写入** `local_output_dir` 下 jsonl；需按池子相对路径落盘（与 README「筛选+切分」一致）。
2. **TitleUtils 的 tid 与线上 chapter_info**：若历史数据 tit 编号规则不是「按文档顺序递增 title」，需与数据方约定同一套 id。
3. **验证3**：`merge_block_pairs` 与 `page_location` 仅在源行有 `page_location` 时比较；若全库无该字段可删验证。
4. **性能**：按行 `mox` 读大文件、多 shard 时宜加并行或流式写。
5. **`read_model` 路径**：`id` 仍为 `伪造_{idx}`，与正式池无关；正式用 `read_panguml`。

## 如何运行

在仓库根目录（`pdf_pipeline`）：

```bash
python -c "from v2_interleave_pipeline.chapter_process.slice_by_chapter import process_main; process_main('read_panguml', 'input.jsonl', '/tmp/out')"
```

需本机已安装：`PIL` 等；远程路径需 `moxing`。
