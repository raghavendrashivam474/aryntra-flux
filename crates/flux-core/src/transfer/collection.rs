//! Transfer collection model for multi-file transfers (S1.6).
//!
//! A [`TransferPlan`] represents an ordered sequence of files to transfer
//! sequentially over a single established session. Each file is represented
//! by a [`TransferItem`] that carries the source path and the relative
//! destination path used for reconstruction on the receiver side.
//!
//! # Design constraints
//!
//! - S1.6 is **sequential only** — no parallelism.
//! - Path sanitization is a **security requirement** (§9 of the S1.6 brief).
//! - Directory enumeration produces **deterministic ordering** (§10).

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can occur while building a [`TransferPlan`].
#[derive(Debug)]
pub enum CollectionError {
    /// An I/O error occurred during enumeration.
    Io(std::io::Error),
    /// A relative path attempts directory traversal (e.g. `../../evil.txt`).
    PathTraversal(PathBuf),
    /// A supplied path is absolute when a relative path was expected.
    AbsolutePath(PathBuf),
    /// A supplied path does not exist on disk.
    NotFound(PathBuf),
    /// The resulting plan would contain zero items.
    EmptyCollection,
}

impl fmt::Display for CollectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error during enumeration: {e}"),
            Self::PathTraversal(p) => {
                write!(f, "path traversal detected: {}", p.display())
            }
            Self::AbsolutePath(p) => {
                write!(f, "absolute path not allowed here: {}", p.display())
            }
            Self::NotFound(p) => write!(f, "path not found: {}", p.display()),
            Self::EmptyCollection => write!(f, "transfer plan is empty"),
        }
    }
}

impl std::error::Error for CollectionError {}

impl From<std::io::Error> for CollectionError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ---------------------------------------------------------------------------
// Path sanitization
// ---------------------------------------------------------------------------

/// Validate and normalize a relative path for safe receiver-side reconstruction.
///
/// # Rejects
///
/// - Absolute paths (`/etc/passwd`, `C:\Windows\...`)
/// - Directory traversal (`../../evil.txt`, `foo/../../../bar`)
/// - Empty paths
///
/// # Normalizes
///
/// - Backslashes to forward slashes (for cross-platform consistency)
/// - Redundant `.` components
pub fn sanitize_relative_path(path: &Path) -> Result<PathBuf, CollectionError> {
    // Reject absolute paths immediately.
    if path.is_absolute() {
        return Err(CollectionError::AbsolutePath(path.to_path_buf()));
    }

    let mut normalized = PathBuf::new();
    let mut depth: i32 = 0;

    for component in path.components() {
        use std::path::Component;
        match component {
            Component::Normal(name) => {
                normalized.push(name);
                depth += 1;
            }
            Component::CurDir => {
                // Skip `.` — it's a no-op.
            }
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(CollectionError::PathTraversal(path.to_path_buf()));
                }
                normalized.pop();
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(CollectionError::AbsolutePath(path.to_path_buf()));
            }
        }
    }

    if normalized.as_os_str().is_empty() {
        return Err(CollectionError::PathTraversal(path.to_path_buf()));
    }

    Ok(normalized)
}

// ---------------------------------------------------------------------------
// TransferItem
// ---------------------------------------------------------------------------

/// A single file within a multi-file transfer collection.
#[derive(Debug, Clone)]
pub struct TransferItem {
    /// Absolute path to the source file on the sender's filesystem.
    pub source_path: PathBuf,

    /// Relative path for reconstruction on the receiver side.
    ///
    /// - For single-file transfers: typically just the file name.
    /// - For directory transfers: preserves the directory structure
    ///   relative to the root of the transferred directory.
    ///
    /// This path is always relative and must be sanitized by the receiver
    /// before use (existing S1.4 path-safety rules apply).
    pub relative_path: PathBuf,
}

impl TransferItem {
    /// Create a transfer item for a single file.
    ///
    /// The relative path defaults to the file name component of `source_path`.
    /// Returns `None` if the path has no file name (e.g. a root directory).
    pub fn from_file(source_path: PathBuf) -> Option<Self> {
        let relative_path = source_path.file_name()?.into();
        Some(Self {
            source_path,
            relative_path,
        })
    }

    /// Create a transfer item with an explicit relative path.
    ///
    /// Used when building directory transfer plans where the relative
    /// path must preserve the internal directory structure.
    pub fn with_relative_path(source_path: PathBuf, relative_path: PathBuf) -> Self {
        Self {
            source_path,
            relative_path,
        }
    }
}

// ---------------------------------------------------------------------------
// TransferPlan
// ---------------------------------------------------------------------------

/// An ordered plan of files to transfer sequentially over one session.
///
/// The plan is intentionally sequential — S1.6 does not introduce
/// parallelism. Files are transferred in the order they appear in `items`.
///
/// # Deterministic ordering
///
/// When constructed from a directory, items are sorted by normalized
/// relative path to ensure reproducible transfers and predictable
/// resume behavior (see S1.6 brief §10).
#[derive(Debug, Clone)]
pub struct TransferPlan {
    /// Ordered list of transfer items.
    pub items: Vec<TransferItem>,
}

impl TransferPlan {
    /// Create an empty transfer plan.
    pub fn empty() -> Self {
        Self { items: Vec::new() }
    }

    /// Create a plan from a single file path.
    ///
    /// Returns `None` if the path has no file name component.
    pub fn single_file(path: PathBuf) -> Option<Self> {
        let item = TransferItem::from_file(path)?;
        Some(Self { items: vec![item] })
    }

    /// Create a plan from multiple file paths.
    ///
    /// Each file's relative path defaults to its file name.
    /// Paths that lack a file name component are silently skipped.
    pub fn from_files(paths: Vec<PathBuf>) -> Self {
        let items = paths
            .into_iter()
            .filter_map(TransferItem::from_file)
            .collect();
        Self { items }
    }

    /// Create a plan from pre-built transfer items.
    ///
    /// Use this when you need explicit control over relative paths,
    /// e.g. when enumerating a directory tree.
    pub fn from_items(items: Vec<TransferItem>) -> Self {
        Self { items }
    }

    /// Number of items in the plan.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the plan contains no items.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Sort items by relative path for deterministic ordering.
    ///
    /// Call this after directory enumeration to ensure reproducible
    /// transfer order across runs and platforms.
    pub fn sort_by_relative_path(&mut self) {
        self.items
            .sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    }

    // -----------------------------------------------------------------------
    // S1.6: Unified plan builder
    // -----------------------------------------------------------------------

    /// Build a transfer plan from a list of user-supplied paths.
    ///
    /// Each path may be:
    /// - A **file** → added as a single item.
    /// - A **directory** → recursively enumerated; relative paths preserve
    ///   the internal directory structure.
    ///
    /// The resulting plan is sorted by relative path for deterministic
    /// ordering. Returns an error if any path does not exist, contains
    /// traversal sequences, or if the final plan would be empty.
    pub fn from_paths(paths: &[PathBuf]) -> Result<Self, CollectionError> {
        let mut items = Vec::new();

        for path in paths {
            if !path.exists() {
                return Err(CollectionError::NotFound(path.clone()));
            }

            if path.is_file() {
                let item = TransferItem::from_file(path.clone())
                    .ok_or_else(|| CollectionError::NotFound(path.clone()))?;
                items.push(item);
            } else if path.is_dir() {
                let dir_items = enumerate_directory(path, path)?;
                items.extend(dir_items);
            }
        }

        if items.is_empty() {
            return Err(CollectionError::EmptyCollection);
        }

        let mut plan = Self { items };
        plan.sort_by_relative_path();
        Ok(plan)
    }
}

// ---------------------------------------------------------------------------
// Directory enumeration (private)
// ---------------------------------------------------------------------------

/// Recursively enumerate a directory into transfer items.
///
/// `base` is the root directory used to compute relative paths.
/// All relative paths are validated through [`sanitize_relative_path`].
fn enumerate_directory(dir: &Path, base: &Path) -> Result<Vec<TransferItem>, CollectionError> {
    let mut items = Vec::new();

    let entries = fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            let sub_items = enumerate_directory(&path, base)?;
            items.extend(sub_items);
        } else if path.is_file() {
            let relative = path
                .strip_prefix(base)
                .map_err(|_| CollectionError::PathTraversal(path.clone()))?;

            // Security: validate the relative path.
            let safe_relative = sanitize_relative_path(relative)?;

            items.push(TransferItem::with_relative_path(
                path.clone(),
                safe_relative,
            ));
        }
    }

    Ok(items)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // -- Existing data-model tests ------------------------------------------

    #[test]
    fn single_file_plan_has_one_item() {
        let plan = TransferPlan::single_file(PathBuf::from("/data/report.pdf")).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.items[0].relative_path, PathBuf::from("report.pdf"));
    }

    #[test]
    fn multi_file_plan_preserves_order() {
        let paths = vec![
            PathBuf::from("/a/first.txt"),
            PathBuf::from("/b/second.txt"),
            PathBuf::from("/c/third.txt"),
        ];
        let plan = TransferPlan::from_files(paths);
        assert_eq!(plan.len(), 3);
        assert_eq!(plan.items[0].relative_path, PathBuf::from("first.txt"));
        assert_eq!(plan.items[2].relative_path, PathBuf::from("third.txt"));
    }

    #[test]
    fn sort_by_relative_path_is_deterministic() {
        let items = vec![
            TransferItem::with_relative_path(PathBuf::from("/src/z.rs"), PathBuf::from("src/z.rs")),
            TransferItem::with_relative_path(PathBuf::from("/src/a.rs"), PathBuf::from("src/a.rs")),
            TransferItem::with_relative_path(
                PathBuf::from("/README.md"),
                PathBuf::from("README.md"),
            ),
        ];
        let mut plan = TransferPlan::from_items(items);
        plan.sort_by_relative_path();

        assert_eq!(plan.items[0].relative_path, PathBuf::from("README.md"));
        assert_eq!(plan.items[1].relative_path, PathBuf::from("src/a.rs"));
        assert_eq!(plan.items[2].relative_path, PathBuf::from("src/z.rs"));
    }

    #[test]
    fn empty_plan_is_empty() {
        let plan = TransferPlan::empty();
        assert!(plan.is_empty());
        assert_eq!(plan.len(), 0);
    }

    #[test]
    fn from_file_rejects_root_path() {
        let result = TransferItem::from_file(PathBuf::from("/"));
        assert!(result.is_none());
    }

    // -- Path sanitization tests --------------------------------------------

    #[test]
    fn sanitize_accepts_normal_relative_path() {
        let result = sanitize_relative_path(Path::new("docs/design.md"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("docs/design.md"));
    }

    #[test]
    fn sanitize_rejects_parent_traversal() {
        let result = sanitize_relative_path(Path::new("../../evil.txt"));
        assert!(matches!(result, Err(CollectionError::PathTraversal(_))));
    }

    #[test]
    fn sanitize_rejects_embedded_traversal() {
        let result = sanitize_relative_path(Path::new("foo/../../../bar"));
        assert!(matches!(result, Err(CollectionError::PathTraversal(_))));
    }

    #[test]
    fn sanitize_rejects_absolute_unix_path() {
        let result = sanitize_relative_path(Path::new("/etc/passwd"));
        assert!(matches!(result, Err(CollectionError::AbsolutePath(_))));
    }

    #[test]
    fn sanitize_strips_dot_components() {
        let result = sanitize_relative_path(Path::new("./src/./main.rs"));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), PathBuf::from("src/main.rs"));
    }

    // -- from_paths integration tests (filesystem) --------------------------

    #[test]
    fn from_paths_single_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("hello.txt");
        fs::write(&file, "hello").unwrap();

        let plan = TransferPlan::from_paths(&[file]).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.items[0].relative_path, PathBuf::from("hello.txt"));
    }

    #[test]
    fn from_paths_directory_sorted() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        // Create a small tree: z.txt, a.txt, sub/m.txt
        fs::write(root.join("z.txt"), "z").unwrap();
        fs::write(root.join("a.txt"), "a").unwrap();
        fs::create_dir(root.join("sub")).unwrap();
        fs::write(root.join("sub/m.txt"), "m").unwrap();

        let plan = TransferPlan::from_paths(&[root.to_path_buf()]).unwrap();
        assert_eq!(plan.len(), 3);

        // Must be sorted by relative path.
        assert_eq!(plan.items[0].relative_path, PathBuf::from("a.txt"));
        assert_eq!(plan.items[1].relative_path, PathBuf::from("sub/m.txt"));
        assert_eq!(plan.items[2].relative_path, PathBuf::from("z.txt"));
    }

    #[test]
    fn from_paths_rejects_nonexistent() {
        let result = TransferPlan::from_paths(&[PathBuf::from("/no/such/file.xyz")]);
        assert!(matches!(result, Err(CollectionError::NotFound(_))));
    }

    #[test]
    fn from_paths_empty_dir_is_error() {
        let dir = tempfile::tempdir().unwrap();
        let result = TransferPlan::from_paths(&[dir.path().to_path_buf()]);
        assert!(matches!(result, Err(CollectionError::EmptyCollection)));
    }
}
