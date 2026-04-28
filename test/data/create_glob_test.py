#!/usr/bin/env python3

import os
import shutil
from pathlib import Path

import h5py
import numpy as np


SCRIPT_DIR = Path(__file__).resolve().parent
GLOB_DIR = SCRIPT_DIR / "glob"
NESTED_DIR = GLOB_DIR / "nested"
EQUIV_DIR = GLOB_DIR / "equiv"
EQUIV_NESTED_DIR = EQUIV_DIR / "nested"
PUSHDOWN_DIR = SCRIPT_DIR / "glob_pushdown"
PUSHDOWN_NESTED_DIR = PUSHDOWN_DIR / "nested"
GLOB_SYMLINK_DIR = SCRIPT_DIR / "glob_symlink"
GLOB_ORDER_DIR = SCRIPT_DIR / "glob_order"
GLOB_DEEP_DIR = SCRIPT_DIR / "glob_deep"
GLOB_HIDDEN_DIR = SCRIPT_DIR / "glob_hidden"
GLOB_LITERAL_META_DIR = SCRIPT_DIR / "glob_literal_meta"
LARGE_GLOB_DIR = SCRIPT_DIR / "glob_large"
MANY_SMALL_DIR = SCRIPT_DIR / "glob_many_small"
SOURCE_RSE_FILE = SCRIPT_DIR / "multithreading_test.h5"
SOURCE_LARGE_FILE = SCRIPT_DIR / "large" / "large_multithreading.h5"
SOURCE_PUSHDOWN_FILE = SCRIPT_DIR / "pushdown_test.h5"
SOURCE_WITH_ATTRS_FILE = SCRIPT_DIR / "with_attrs.h5"
SOURCE_ATTRS_EDGE_CASES_FILE = SCRIPT_DIR / "attrs_edge_cases.h5"


def write_matching_file(path: Path, values: list[int], more: list[int], child: list[int], scale: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as h5:
        h5.create_dataset("values", data=np.array(values, dtype=np.int32))
        h5.create_dataset("more", data=np.array(more, dtype=np.int32))
        h5.create_dataset("scale", data=np.int32(scale))
        group = h5.create_group("group")
        group.create_dataset("child", data=np.array(child, dtype=np.int32))


def write_mismatch_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as h5:
        h5.create_dataset("values", data=np.array([1.5, 2.5], dtype=np.float64))
        h5.create_dataset("more", data=np.array([100, 200], dtype=np.int32))
        h5.create_dataset("scale", data=np.int32(999))
        group = h5.create_group("group")
        group.create_dataset("child", data=np.array([9], dtype=np.int32))


def write_string_variant_files(ascii_path: Path, utf8_path: Path) -> None:
    ascii_path.parent.mkdir(parents=True, exist_ok=True)
    utf8_path.parent.mkdir(parents=True, exist_ok=True)

    fixed_ascii = h5py.string_dtype(encoding="ascii", length=8)
    variable_utf8 = h5py.string_dtype(encoding="utf-8")

    with h5py.File(ascii_path, "w") as h5:
        h5.create_dataset("names", data=np.array(["alpha"], dtype=fixed_ascii), dtype=fixed_ascii)
        h5.create_dataset("tag", data=np.array("ascii", dtype=fixed_ascii), dtype=fixed_ascii)

    with h5py.File(utf8_path, "w") as h5:
        h5.create_dataset("names", data=np.array(["åä"], dtype=object), dtype=variable_utf8)
        h5.create_dataset("tag", data=np.array("utf8", dtype=object), dtype=variable_utf8)


def write_order_file(path: Path, value: int, marker_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as h5:
        h5.create_dataset("values", data=np.array([value], dtype=np.int32))
        h5.create_dataset(marker_name, data=np.array([value], dtype=np.int32))


def write_attribute_schema_file(path: Path, *, second_name: str = "b", second_value=np.int64(2)) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as h5:
        dataset = h5.create_dataset("target", data=np.array([1], dtype=np.int32))
        dataset.attrs["a"] = np.int32(1)
        dataset.attrs[second_name] = second_value


def write_large_order_file(path: Path, base_value: int, num_rows: int = 50_000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(path, "w") as h5:
        h5.create_dataset("values", data=np.arange(num_rows, dtype=np.int32) + np.int32(base_value))


def write_equivalence_file(
    path: Path,
    values: list[int],
    more: list[int],
    scale: int,
    tags: list[str],
    status_run_starts: list[int],
    status_values: list[int],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    utf8 = h5py.string_dtype(encoding="utf-8")
    with h5py.File(path, "w") as h5:
        h5.create_dataset("values", data=np.array(values, dtype=np.int32))
        h5.create_dataset("more", data=np.array(more, dtype=np.int32))
        h5.create_dataset("scale", data=np.int32(scale))
        h5.create_dataset("tag", data=np.array(tags, dtype=object), dtype=utf8)
        h5.create_dataset("status_run_starts", data=np.array(status_run_starts, dtype=np.int64))
        h5.create_dataset("status_values", data=np.array(status_values, dtype=np.int32))


def write_many_small_fixtures(directory: Path, num_files: int = 1000, rows_per_file: int = 3) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for path in directory.glob("part_*.h5"):
        path.unlink()

    combined_values = np.empty(num_files * rows_per_file, dtype=np.int32)
    for file_idx in range(num_files):
        start = file_idx * rows_per_file
        values = np.arange(start, start + rows_per_file, dtype=np.int32)
        part_path = directory / f"part_{file_idx + 1:04d}.h5"
        with h5py.File(part_path, "w") as h5:
            dataset = h5.create_dataset("values", data=values)
            dataset.attrs["file_index"] = np.int32(file_idx + 1)
            dataset.attrs["first_value"] = np.int32(start)
            dataset.attrs["rows_per_file"] = np.int32(rows_per_file)
        combined_values[start : start + rows_per_file] = values

    with h5py.File(directory / "combined.h5", "w") as h5:
        h5.create_dataset("values", data=combined_values)


def link_or_copy_file(source: Path, target: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(f"Missing source fixture for glob test generation: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    try:
        os.link(source, target)
    except OSError:
        shutil.copyfile(source, target)


def remove_existing_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def create_symlink(link_target: str, link_path: Path, *, target_is_directory: bool = False) -> None:
    link_path.parent.mkdir(parents=True, exist_ok=True)
    remove_existing_path(link_path)
    try:
        os.symlink(link_target, link_path, target_is_directory=target_is_directory)
    except (NotImplementedError, OSError) as exc:
        raise RuntimeError("Creating glob symlink fixtures requires symlink support") from exc


def main() -> None:
    GLOB_DIR.mkdir(parents=True, exist_ok=True)
    NESTED_DIR.mkdir(parents=True, exist_ok=True)
    EQUIV_DIR.mkdir(parents=True, exist_ok=True)
    EQUIV_NESTED_DIR.mkdir(parents=True, exist_ok=True)
    PUSHDOWN_DIR.mkdir(parents=True, exist_ok=True)
    PUSHDOWN_NESTED_DIR.mkdir(parents=True, exist_ok=True)
    GLOB_SYMLINK_DIR.mkdir(parents=True, exist_ok=True)
    GLOB_ORDER_DIR.mkdir(parents=True, exist_ok=True)
    GLOB_DEEP_DIR.mkdir(parents=True, exist_ok=True)
    GLOB_HIDDEN_DIR.mkdir(parents=True, exist_ok=True)
    GLOB_LITERAL_META_DIR.mkdir(parents=True, exist_ok=True)
    LARGE_GLOB_DIR.mkdir(parents=True, exist_ok=True)
    MANY_SMALL_DIR.mkdir(parents=True, exist_ok=True)

    write_matching_file(GLOB_DIR / "glob_same_1.h5", [1, 2, 3], [10, 20, 30], [5], 100)
    write_matching_file(GLOB_DIR / "glob_same_2.h5", [4, 5], [40, 50], [6, 7], 200)
    write_matching_file(NESTED_DIR / "glob_same_3.h5", [6], [60], [8], 300)
    write_mismatch_file(GLOB_DIR / "glob_mismatch.h5")
    write_string_variant_files(GLOB_DIR / "glob_strings_1_ascii.h5", GLOB_DIR / "glob_strings_2_utf8.h5")

    link_or_copy_file(SOURCE_RSE_FILE, GLOB_DIR / "rse_same_1.h5")
    link_or_copy_file(SOURCE_RSE_FILE, GLOB_DIR / "rse_same_2.h5")
    link_or_copy_file(SOURCE_PUSHDOWN_FILE, PUSHDOWN_DIR / "pushdown_1.h5")
    link_or_copy_file(SOURCE_PUSHDOWN_FILE, PUSHDOWN_DIR / "pushdown_2.h5")
    link_or_copy_file(SOURCE_PUSHDOWN_FILE, PUSHDOWN_NESTED_DIR / "pushdown_3.h5")
    link_or_copy_file(SOURCE_WITH_ATTRS_FILE, GLOB_DIR / "glob_with_attrs_1.h5")
    link_or_copy_file(SOURCE_WITH_ATTRS_FILE, GLOB_DIR / "glob_with_attrs_2.h5")
    link_or_copy_file(SOURCE_ATTRS_EDGE_CASES_FILE, GLOB_DIR / "glob_attrs_edge_1.h5")
    link_or_copy_file(SOURCE_ATTRS_EDGE_CASES_FILE, GLOB_DIR / "glob_attrs_edge_2.h5")
    write_equivalence_file(
        EQUIV_DIR / "equiv_1.h5",
        values=[1, 4, 7, 10],
        more=[11, 14, 17, 20],
        scale=5,
        tags=["aa", "bb", "cc", "dd"],
        status_run_starts=[0, 2],
        status_values=[3, 7],
    )
    write_equivalence_file(
        EQUIV_DIR / "equiv_2.h5",
        values=[2, 5, 8],
        more=[12, 15, 18],
        scale=9,
        tags=["åx", "ux", "vx"],
        status_run_starts=[0, 1],
        status_values=[4, 8],
    )
    write_equivalence_file(
        EQUIV_NESTED_DIR / "equiv_3.h5",
        values=[3, 6],
        more=[13, 16],
        scale=12,
        tags=["ny", "zz"],
        status_run_starts=[0],
        status_values=[9],
    )

    link_or_copy_file(GLOB_DIR / "glob_same_1.h5", GLOB_SYMLINK_DIR / "real" / "nested.h5")
    link_or_copy_file(GLOB_DIR / "glob_same_2.h5", GLOB_SYMLINK_DIR / "root_file.h5")
    create_symlink("real/nested.h5", GLOB_SYMLINK_DIR / "link_file.h5")
    remove_existing_path(GLOB_SYMLINK_DIR / "broken_link.h5")
    if os.name != "nt":
        create_symlink("missing.h5", GLOB_SYMLINK_DIR / "broken_link.h5")
    create_symlink("real", GLOB_SYMLINK_DIR / "link_dir", target_is_directory=True)

    write_order_file(GLOB_ORDER_DIR / "order_2.h5", 2, "marker_2")
    write_order_file(GLOB_ORDER_DIR / "order_10.h5", 10, "marker_10")
    write_order_file(GLOB_ORDER_DIR / "order_1.h5", 1, "marker_1")
    write_order_file(GLOB_ORDER_DIR / "nested" / "order_0.h5", 0, "marker_0")
    write_order_file(GLOB_ORDER_DIR / "z" / "order_99.h5", 99, "marker_99")

    write_order_file(GLOB_DEEP_DIR / "deep_0.h5", 0, "marker_0")
    write_order_file(GLOB_DEEP_DIR / "alpha" / "deep_1.h5", 1, "marker_1")
    write_order_file(GLOB_DEEP_DIR / "alpha" / "beta" / "gamma" / "deep_2.h5", 2, "marker_2")
    write_order_file(
        GLOB_DEEP_DIR / "alpha" / "beta" / "gamma" / "delta" / "epsilon" / "deep_3.h5",
        3,
        "marker_3",
    )

    write_order_file(GLOB_HIDDEN_DIR / "visible.h5", 1, "marker_1")
    write_order_file(GLOB_HIDDEN_DIR / ".hidden_root.h5", 2, "marker_2")
    write_order_file(GLOB_HIDDEN_DIR / ".hidden_dir" / "in_hidden_dir.h5", 10, "marker_10")
    write_order_file(GLOB_HIDDEN_DIR / "visible_dir" / "inside_visible.h5", 20, "marker_20")
    write_order_file(GLOB_HIDDEN_DIR / "visible_dir" / ".nested_hidden" / "in_nested_hidden.h5", 30, "marker_30")
    write_order_file(GLOB_HIDDEN_DIR / ".hidden_parent" / "visible_child" / "in_hidden_parent.h5", 40, "marker_40")
    write_order_file(GLOB_LITERAL_META_DIR / "literal[1].h5", 1, "marker_1")
    write_order_file(GLOB_LITERAL_META_DIR / "dir[1]" / "nested.h5", 2, "marker_2")
    write_attribute_schema_file(GLOB_DIR / "attr_schema_1.h5")
    write_attribute_schema_file(GLOB_DIR / "attr_schema_2.h5")
    write_attribute_schema_file(GLOB_DIR / "attr_schema_name_mismatch.h5", second_name="c")
    write_attribute_schema_file(GLOB_DIR / "attr_schema_type_mismatch.h5", second_value=np.float64(2.5))

    link_or_copy_file(SOURCE_LARGE_FILE, LARGE_GLOB_DIR / "large_same_1.h5")
    link_or_copy_file(SOURCE_LARGE_FILE, LARGE_GLOB_DIR / "large_same_2.h5")
    write_large_order_file(LARGE_GLOB_DIR / "large_order_2.h5", 200_000)
    write_large_order_file(LARGE_GLOB_DIR / "large_order_10.h5", 1_000_000)
    write_large_order_file(LARGE_GLOB_DIR / "large_order_1.h5", 100_000)
    write_many_small_fixtures(MANY_SMALL_DIR)


if __name__ == "__main__":
    main()
