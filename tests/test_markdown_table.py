from misc_utils.utils import build_markdown_table_from_dicts

if __name__ == "__main__":
    data = [
        {
            f"{'row_title' if c_idx == 0 else f'col-{c_idx}'}": f"row-{r_idx}"
            if c_idx == 0
            else 0.2 * c_idx / (1 + r_idx)
            for c_idx in range(3)
        }
        for r_idx in range(9)
    ]
    print(f"{data=}")
    print(build_markdown_table_from_dicts(data, col_title="column_title"))
