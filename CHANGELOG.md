# Changelog

## 0.4.0 - 2023 May 25
- Changed `from_pipelines` to accept an iterator instead of vector
- Add `Pipeline::from_rows()`
- Add `Pipeline::collect_into_rows()`

## 0.3.1 - 2023 Feb 17
- Fix `Pipeline::from_path` panic

## 0.3.0 - 2023 Jan 18
- Add Pipeline `select` method
- Add Pipeline `filter` & `filter_col` methods
- Add Pipeline `from_pipelines` constructor for merging pipelines together
- Add `count` transformer
- Remember row order in transform_into
- Include source index in errors

## 0.2.0 - 2023 Jan 11
- Add `Target` struct helper for creating targets, and hide the targets in the `target` module.
- Publish `Transform` trait
- Add `sum` transformer

## 0.1.0 - 2023 Jan 11
- Initial release
