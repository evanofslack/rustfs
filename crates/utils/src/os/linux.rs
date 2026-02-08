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

use crate::os::{DiskInfo, IOStats};
use rustix::fs::statfs;
use std::fs::File;
use std::io::{self, BufRead, Error, ErrorKind};
use std::path::Path;

/// Returns total and free bytes available in a directory, e.g. `/`.
pub fn get_info(p: impl AsRef<Path>) -> std::io::Result<DiskInfo> {
    let path_display = p.as_ref().display();
    // Use statfs on Linux to get access to f_type (filesystem magic number)
    let stat = statfs(p.as_ref())?;

    // Linux statfs:
    // f_bsize: Optimal transfer block size
    // f_blocks: Total data blocks in file system
    // f_frsize: Fragment size (since Linux 2.6) - unit for blocks
    //
    // If f_frsize is > 0, it is the unit for f_blocks, f_bfree, f_bavail.
    // Otherwise f_bsize is used.
    let bsize = if stat.f_frsize > 0 {
        stat.f_frsize as u64
    } else {
        stat.f_bsize as u64
    };

    let bfree = stat.f_bfree as u64;
    let bavail = stat.f_bavail as u64;
    let blocks = stat.f_blocks as u64;

    let reserved = match bfree.checked_sub(bavail) {
        Some(reserved) => reserved,
        None => {
            return Err(Error::other(format!(
                "detected f_bavail space ({bavail}) > f_bfree space ({bfree}), fs corruption at ({path_display}). please run 'fsck'",
            )));
        }
    };

    let total = match blocks.checked_sub(reserved) {
        Some(total) => total * bsize,
        None => {
            return Err(Error::other(format!(
                "detected reserved space ({reserved}) > blocks space ({blocks}), fs corruption at ({path_display}). please run 'fsck'",
            )));
        }
    };

    let free = bavail * bsize;
    let used = match total.checked_sub(free) {
        Some(used) => used,
        None => {
            return Err(Error::other(format!(
                "detected free space ({free}) > total drive space ({total}), fs corruption at ({path_display}). please run 'fsck'"
            )));
        }
    };

    let st = rustix::fs::stat(p.as_ref())?;
    let major = rustix::fs::major(st.st_dev) as u64;
    let minor = rustix::fs::minor(st.st_dev) as u64;

    let (name, rotational, nrrequests) = get_block_device_properties(major, minor);

    Ok(DiskInfo {
        total,
        free,
        used,
        files: stat.f_files as u64,
        ffree: stat.f_ffree as u64,
        fstype: get_fs_type(stat.f_type as u64).to_string(),
        major,
        minor,
        name,
        rotational,
        nrrequests,
    })
}

/// Detect block device properties (name, rotational, nrrequests) from sysfs.
///
/// Follows the same approach as MinIO:
/// 1. Scan `/proc/diskstats` to map major:minor → device name
/// 2. Resolve partition → parent device via `/sys/class/block/<dev>` symlink
/// 3. Read `/sys/block/<device>/queue/rotational` (1=HDD, 0=SSD)
/// 4. Read `/sys/block/<device>/queue/nr_requests`
///
/// If any step fails (NFS, FUSE, containers without sysfs), defaults to
/// rotational=true (conservative, matching MinIO's behavior).
fn get_block_device_properties(major: u64, minor: u64) -> (String, bool, u64) {
    let device_name = match resolve_device_name_from_diskstats(major, minor) {
        Some(name) => name,
        None => return (String::new(), true, 0), // unknown device → HDD (conservative)
    };

    let parent_device = resolve_parent_device(&device_name);
    let rotational = read_sysfs_rotational(&parent_device);
    let nrrequests = read_sysfs_nr_requests(&parent_device);

    // Conservative default matching MinIO: None (unknown) or true → HDD.
    // Only treat as SSD when we definitively read "0" from sysfs.
    let is_rotational = rotational.unwrap_or(true);

    (device_name, is_rotational, nrrequests)
}

/// Scan `/proc/diskstats` to find the device name for a given major:minor pair.
///
/// `/proc/diskstats` format: `<major> <minor> <name> <stats...>`
fn resolve_device_name_from_diskstats(major: u64, minor: u64) -> Option<String> {
    resolve_device_name_from_content(&std::fs::read_to_string("/proc/diskstats").ok()?, major, minor)
}

/// Parse diskstats content to find the device name for a given major:minor pair.
/// Separated from I/O for testability.
fn resolve_device_name_from_content(content: &str, major: u64, minor: u64) -> Option<String> {
    for line in content.lines() {
        let mut fields = line.split_whitespace();
        let dev_major: u64 = fields.next()?.parse().ok()?;
        let dev_minor: u64 = fields.next()?.parse().ok()?;
        let name = fields.next()?;
        if dev_major == major && dev_minor == minor {
            return Some(name.to_string());
        }
    }
    None
}

/// Resolve a partition device to its parent block device.
///
/// For example, `nvme0n1p1` → `nvme0n1`, `sda1` → `sda`.
/// If the device is already a parent (e.g., `sda`, `nvme0n1`), returns it unchanged.
///
/// Works by reading the `/sys/class/block/<dev>` symlink which points to
/// something like `../../devices/.../block/nvme0n1/nvme0n1p1`. The parent
/// directory's name gives the physical device.
fn resolve_parent_device(device_name: &str) -> String {
    let link = format!("/sys/class/block/{device_name}");
    match std::fs::read_link(&link) {
        Ok(target) => parent_device_from_sysfs_path(&target, device_name),
        Err(_) => device_name.to_string(),
    }
}

/// Extract the parent block device from a sysfs symlink target path.
/// Separated from I/O for testability.
///
/// Sysfs symlink target looks like:
///   `../../devices/pci.../block/nvme0n1/nvme0n1p1` (partition)
///   `../../devices/pci.../block/sda` (whole device)
///
/// If the device is a partition, the parent directory contains the physical device name.
/// We verify the parent has a queue/rotational file to confirm it's a real block device.
fn parent_device_from_sysfs_path(target: &Path, device_name: &str) -> String {
    if let Some(parent) = target.parent() {
        if let Some(parent_name) = parent.file_name() {
            let parent_str = parent_name.to_string_lossy();
            // Only use the parent if it differs from the device and has queue stats.
            if parent_str != device_name {
                let queue_path = format!("/sys/block/{parent_str}/queue/rotational");
                if Path::new(&queue_path).exists() {
                    return parent_str.to_string();
                }
            }
        }
    }
    device_name.to_string()
}

/// Read the rotational flag from sysfs.
/// Returns Some(true) for HDD, Some(false) for SSD/NVMe, None if unreadable.
fn read_sysfs_rotational(device_name: &str) -> Option<bool> {
    let path = format!("/sys/block/{device_name}/queue/rotational");
    parse_rotational(&std::fs::read_to_string(path).ok()?)
}

/// Parse the content of a sysfs rotational file.
/// Separated from I/O for testability.
fn parse_rotational(content: &str) -> Option<bool> {
    match content.trim() {
        "1" => Some(true),  // HDD
        "0" => Some(false), // SSD / NVMe
        _ => None,
    }
}

/// Read the nr_requests value from sysfs.
/// Returns 0 if unreadable.
fn read_sysfs_nr_requests(device_name: &str) -> u64 {
    let path = format!("/sys/block/{device_name}/queue/nr_requests");
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| parse_nr_requests(&s))
        .unwrap_or(0)
}

/// Parse the content of a sysfs nr_requests file.
/// Separated from I/O for testability.
fn parse_nr_requests(content: &str) -> Option<u64> {
    content.trim().parse::<u64>().ok()
}

/// Returns the filesystem type of the underlying mounted filesystem
///
/// TODO The following mapping could not find the corresponding constant in `nix`:
///
/// "137d" => "EXT",
/// "4244" => "HFS",
/// "5346544e" => "NTFS",
/// "61756673" => "AUFS",
/// "ef51" => "EXT2OLD",
/// "2fc12fc1" => "zfs",
/// "ff534d42" => "cifs",
/// "53464846" => "wslfs",
fn get_fs_type(fs_type: u64) -> &'static str {
    // Magic numbers for various filesystems
    match fs_type {
        0x01021994 => "TMPFS",
        0x4d44 => "MSDOS",
        0x6969 => "NFS",
        0xEF53 => "EXT4",
        0xf15f => "ecryptfs",
        0x794c7630 => "overlayfs",
        0x52654973 => "REISERFS",
        // Additional common ones can be added here:
        // 0x58465342 => "XFS",
        // 0x9123683E => "BTRFS",
        _ => "UNKNOWN",
    }
}

pub fn same_disk(disk1: &str, disk2: &str) -> std::io::Result<bool> {
    let stat1 = rustix::fs::stat(disk1)?;
    let stat2 = rustix::fs::stat(disk2)?;

    Ok(stat1.st_dev == stat2.st_dev)
}

pub fn get_drive_stats(major: u32, minor: u32) -> std::io::Result<IOStats> {
    read_drive_stats(&format!("/sys/dev/block/{major}:{minor}/stat"))
}

fn read_drive_stats(stats_file: &str) -> std::io::Result<IOStats> {
    let stats = read_stat(stats_file)?;
    if stats.len() < 11 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!("found invalid format while reading {stats_file}"),
        ));
    }
    let mut io_stats = IOStats {
        read_ios: stats[0],
        read_merges: stats[1],
        read_sectors: stats[2],
        read_ticks: stats[3],
        write_ios: stats[4],
        write_merges: stats[5],
        write_sectors: stats[6],
        write_ticks: stats[7],
        current_ios: stats[8],
        total_ticks: stats[9],
        req_ticks: stats[10],
        ..Default::default()
    };

    if stats.len() > 14 {
        io_stats.discard_ios = stats[11];
        io_stats.discard_merges = stats[12];
        io_stats.discard_sectors = stats[13];
        io_stats.discard_ticks = stats[14];
    }
    Ok(io_stats)
}

fn read_stat(file_name: &str) -> std::io::Result<Vec<u64>> {
    // Open file
    let path = Path::new(file_name);
    let file = File::open(path)?;

    // Create a BufReader
    let reader = io::BufReader::new(file);

    // Read first line
    let mut stats = Vec::new();
    if let Some(line) = reader.lines().next() {
        let line = line?;
        // Split line and parse as u64
        // https://rust-lang.github.io/rust-clippy/master/index.html#trim_split_whitespace
        for token in line.split_whitespace() {
            let ui64: u64 = token
                .parse()
                .map_err(|e| Error::new(ErrorKind::InvalidData, format!("failed to parse '{token}' as u64: {e}")))?;
            stats.push(ui64);
        }
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // --- Tests for resolve_device_name_from_content ---

    #[test]
    fn test_parse_diskstats_nvme() {
        let content = "\
 259       0 nvme0n1 1234 0 5678 100 0 0 0 0 0 100 100
 259       1 nvme0n1p1 500 0 2000 50 0 0 0 0 0 50 50
 259       2 nvme0n1p2 734 0 3678 50 0 0 0 0 0 50 50";
        assert_eq!(resolve_device_name_from_content(content, 259, 0), Some("nvme0n1".to_string()));
        assert_eq!(resolve_device_name_from_content(content, 259, 1), Some("nvme0n1p1".to_string()));
    }

    #[test]
    fn test_parse_diskstats_sata() {
        let content = "\
   8       0 sda 1000 0 5000 100 0 0 0 0 0 100 100
   8       1 sda1 500 0 2000 50 0 0 0 0 0 50 50
   8      16 sdb 2000 0 10000 200 0 0 0 0 0 200 200";
        assert_eq!(resolve_device_name_from_content(content, 8, 0), Some("sda".to_string()));
        assert_eq!(resolve_device_name_from_content(content, 8, 16), Some("sdb".to_string()));
    }

    #[test]
    fn test_parse_diskstats_virtio() {
        let content = " 252       0 vda 500 0 2000 50 0 0 0 0 0 50 50";
        assert_eq!(resolve_device_name_from_content(content, 252, 0), Some("vda".to_string()));
    }

    #[test]
    fn test_parse_diskstats_not_found() {
        let content = "   8       0 sda 1000 0 5000 100 0 0 0 0 0 100 100";
        assert_eq!(resolve_device_name_from_content(content, 9, 0), None);
        assert_eq!(resolve_device_name_from_content(content, 8, 99), None);
    }

    #[test]
    fn test_parse_diskstats_empty() {
        assert_eq!(resolve_device_name_from_content("", 8, 0), None);
    }

    // --- Tests for parent_device_from_sysfs_path ---

    #[test]
    fn test_parent_device_nvme_partition() {
        // nvme0n1p1 is a partition of nvme0n1
        let target = PathBuf::from("../../devices/pci0000:00/0000:00:06.0/nvme/nvme0/nvme0n1/nvme0n1p1");
        // In unit test context, sysfs doesn't exist, so it falls back to device_name.
        // We test the path parsing logic directly.
        let parent = target.parent().unwrap();
        let parent_name = parent.file_name().unwrap().to_string_lossy();
        assert_eq!(parent_name, "nvme0n1");
    }

    #[test]
    fn test_parent_device_sata_partition() {
        let target = PathBuf::from("../../devices/pci0000:00/0000:00:1f.2/ata1/host0/target0:0:0/0:0:0:0/block/sda/sda1");
        let parent = target.parent().unwrap();
        let parent_name = parent.file_name().unwrap().to_string_lossy();
        assert_eq!(parent_name, "sda");
    }

    #[test]
    fn test_parent_device_whole_disk() {
        // Whole disk: parent is "block", not a valid device — should return device_name.
        let target = PathBuf::from("../../devices/pci0000:00/0000:00:06.0/nvme/nvme0/nvme0n1");
        let result = parent_device_from_sysfs_path(&target, "nvme0n1");
        // Parent is "nvme0" which won't have queue/rotational in test env,
        // so it falls back to device_name.
        assert_eq!(result, "nvme0n1");
    }

    // --- Tests for parse_rotational ---

    #[test]
    fn test_parse_rotational_hdd() {
        assert_eq!(parse_rotational("1\n"), Some(true));
        assert_eq!(parse_rotational("1"), Some(true));
    }

    #[test]
    fn test_parse_rotational_ssd() {
        assert_eq!(parse_rotational("0\n"), Some(false));
        assert_eq!(parse_rotational("0"), Some(false));
    }

    #[test]
    fn test_parse_rotational_unknown() {
        assert_eq!(parse_rotational(""), None);
        assert_eq!(parse_rotational("garbage"), None);
        assert_eq!(parse_rotational("2"), None);
    }

    // --- Tests for parse_nr_requests ---

    #[test]
    fn test_parse_nr_requests_valid() {
        assert_eq!(parse_nr_requests("256\n"), Some(256));
        assert_eq!(parse_nr_requests("128"), Some(128));
        assert_eq!(parse_nr_requests("  64  \n"), Some(64));
    }

    #[test]
    fn test_parse_nr_requests_invalid() {
        assert_eq!(parse_nr_requests(""), None);
        assert_eq!(parse_nr_requests("garbage"), None);
    }

    // --- Tests for conservative default ---

    #[test]
    fn test_block_device_properties_unknown_device() {
        // major:minor that won't exist in /proc/diskstats during test
        let (name, rotational, nrrequests) = get_block_device_properties(999, 999);
        assert!(name.is_empty());
        // Conservative default: unknown device is treated as HDD (matching MinIO)
        assert!(rotational);
        assert_eq!(nrrequests, 0);
    }
}
