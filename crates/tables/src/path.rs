// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! S3 object-key path helpers for the `.rustfs-tables/` hidden prefix.
//!
//! Layout under each warehouse bucket:
//! ```
//! .rustfs-tables/
//!   {ns}/
//!     .namespace.json              ← namespace properties
//!     {table}/
//!       version-hint.text          ← current version number (plain integer text)
//!       metadata/
//!         v{N}.metadata.json       ← Iceberg TableMetadata JSON
//! ```
//! Data files (Parquet, Avro manifests) are written by query engines directly
//! under `s3://{warehouse}/{ns}/{table}/` — the catalog does not manage them.

/// The hidden prefix used for all catalog metadata within a warehouse bucket.
pub const TABLES_META_PREFIX: &str = ".rustfs-tables";

/// Key for namespace properties JSON.
///
/// e.g. `.rustfs-tables/analytics/.namespace.json`
pub fn namespace_meta_key(ns: &[String]) -> String {
    format!("{}/{}/{}/.namespace.json", TABLES_META_PREFIX, ns.join("/"), "")
        .replace("//", "/")
        .trim_end_matches('/')
        .to_string()
        + "/.namespace.json"
}

/// Simpler version: key for namespace JSON given the joined path string.
pub fn namespace_meta_key_str(ns_path: &str) -> String {
    format!("{TABLES_META_PREFIX}/{ns_path}/.namespace.json")
}

/// S3 prefix under which all metadata for a given namespace lives.
///
/// e.g. `.rustfs-tables/analytics/`
pub fn namespace_prefix(ns: &[String]) -> String {
    format!("{}/{}/", TABLES_META_PREFIX, ns.join("/"))
}

/// S3 prefix under which all metadata for a given table lives.
///
/// e.g. `.rustfs-tables/analytics/orders/`
pub fn table_prefix(ns: &[String], table: &str) -> String {
    format!("{}/{}/{}/", TABLES_META_PREFIX, ns.join("/"), table)
}

/// Key for the version-hint file for a table.
///
/// e.g. `.rustfs-tables/analytics/orders/version-hint.text`
pub fn version_hint_key(ns: &[String], table: &str) -> String {
    format!("{}/{}/{}/version-hint.text", TABLES_META_PREFIX, ns.join("/"), table)
}

/// Key for a specific versioned metadata JSON file.
///
/// e.g. `.rustfs-tables/analytics/orders/metadata/v3.metadata.json`
pub fn table_metadata_key(ns: &[String], table: &str, version: u64) -> String {
    format!("{}/{}/{}/metadata/v{}.metadata.json", TABLES_META_PREFIX, ns.join("/"), table, version)
}

/// The `location` field in the returned `TableMetadata` — where engines write data files.
/// This is the non-hidden path: `s3://{warehouse}/{ns}/{table}`.
pub fn table_data_location(warehouse: &str, ns: &[String], table: &str) -> String {
    format!("s3://{}/{}/{}", warehouse, ns.join("/"), table)
}

/// The `metadata-location` field returned to clients after create/load/commit.
pub fn metadata_location(warehouse: &str, ns: &[String], table: &str, version: u64) -> String {
    format!(
        "s3://{}/{}",
        warehouse,
        table_metadata_key(ns, table, version)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_meta_key() {
        let ns = vec!["analytics".to_string()];
        assert_eq!(namespace_meta_key_str("analytics"), ".rustfs-tables/analytics/.namespace.json");
        let _ = namespace_meta_key(&ns); // just verify it doesn't panic
    }

    #[test]
    fn test_namespace_prefix() {
        let ns = vec!["data_science".to_string()];
        assert_eq!(namespace_prefix(&ns), ".rustfs-tables/data_science/");
    }

    #[test]
    fn test_table_prefix() {
        let ns = vec!["analytics".to_string()];
        assert_eq!(table_prefix(&ns, "orders"), ".rustfs-tables/analytics/orders/");
    }

    #[test]
    fn test_version_hint_key() {
        let ns = vec!["analytics".to_string()];
        assert_eq!(version_hint_key(&ns, "orders"), ".rustfs-tables/analytics/orders/version-hint.text");
    }

    #[test]
    fn test_table_metadata_key() {
        let ns = vec!["analytics".to_string()];
        assert_eq!(
            table_metadata_key(&ns, "orders", 3),
            ".rustfs-tables/analytics/orders/metadata/v3.metadata.json"
        );
    }

    #[test]
    fn test_table_data_location() {
        let ns = vec!["analytics".to_string()];
        assert_eq!(
            table_data_location("my-warehouse", &ns, "orders"),
            "s3://my-warehouse/analytics/orders"
        );
    }

    #[test]
    fn test_metadata_location() {
        let ns = vec!["analytics".to_string()];
        assert_eq!(
            metadata_location("my-warehouse", &ns, "orders", 4),
            "s3://my-warehouse/.rustfs-tables/analytics/orders/metadata/v4.metadata.json"
        );
    }

    #[test]
    fn test_multi_level_namespace() {
        let ns = vec!["a".to_string(), "b".to_string()];
        assert_eq!(namespace_prefix(&ns), ".rustfs-tables/a/b/");
        assert_eq!(version_hint_key(&ns, "t"), ".rustfs-tables/a/b/t/version-hint.text");
    }
}
