from misc_utils.utils import build_markdown_table_from_dicts

if __name__ == "__main__":
    data = [{f"{k}": 0.2 * k / (1 + n) for k in range(3)} for n in range(9)]
    print(f"{data=}")
    print(build_markdown_table_from_dicts(data, col_title="column_title"))
