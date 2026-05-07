#!/usr/bin/env node
/**
 * pack.js — bundle the extension into a ZIP suitable for "Load unpacked"
 * sharing or for uploading to the Chrome Web Store later.
 *
 * Why this exists:
 *  - Chrome's "Load unpacked" wants a folder, not a zip — so the recipient
 *    has to unzip first. This script keeps the unzipped layout clean (just
 *    drag-and-drop into chrome://extensions after extracting).
 *  - It excludes everything that isn't part of the runtime extension:
 *    docs, the packer itself, OS junk files, the dist/ folder.
 *
 * Output: <ext>/dist/ocoi-extension-v{manifest.version}.zip
 *
 * Cross-platform: uses Node's built-in zlib + manual ZIP writer, so it
 * runs on Windows without requiring a `zip` binary or PowerShell.
 */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

const ROOT = __dirname;
const DIST = path.join(ROOT, "dist");
const manifest = JSON.parse(
  fs.readFileSync(path.join(ROOT, "manifest.json"), "utf8")
);
const VERSION = manifest.version || "0.0.0";
const OUT = path.join(DIST, `ocoi-extension-v${VERSION}.zip`);

const EXCLUDE_DIRS = new Set(["dist", "node_modules", ".git"]);
const EXCLUDE_FILES = new Set([
  "pack.js",
  ".DS_Store",
  "Thumbs.db",
]);
// All Markdown docs are repo-side artifacts — never part of the runtime
// extension. Excluding them as a class avoids the trap of forgetting to
// add a new doc file to EXCLUDE_FILES whenever one is created.
const EXCLUDE_EXT = new Set([".md"]);

function walk(dir, base = "") {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    if (entry.isDirectory()) {
      if (EXCLUDE_DIRS.has(entry.name)) continue;
      out.push(...walk(path.join(dir, entry.name), path.join(base, entry.name)));
    } else if (entry.isFile()) {
      if (EXCLUDE_FILES.has(entry.name)) continue;
      const ext = path.extname(entry.name).toLowerCase();
      if (EXCLUDE_EXT.has(ext)) continue;
      out.push({
        absPath: path.join(dir, entry.name),
        zipPath: path.join(base, entry.name).replace(/\\/g, "/"),
      });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Minimal ZIP writer (store + deflate). Spec: PKZIP APPNOTE 6.3.x.

function crc32Table() {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[i] = c >>> 0;
  }
  return t;
}
const CRC_TABLE = crc32Table();
function crc32(buf) {
  let c = 0xffffffff;
  for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return (c ^ 0xffffffff) >>> 0;
}

function dosTime(d) {
  const t = ((d.getHours() & 0x1f) << 11) |
            ((d.getMinutes() & 0x3f) << 5) |
            ((Math.floor(d.getSeconds() / 2)) & 0x1f);
  return t & 0xffff;
}
function dosDate(d) {
  const v = (((d.getFullYear() - 1980) & 0x7f) << 9) |
            (((d.getMonth() + 1) & 0x0f) << 5) |
            (d.getDate() & 0x1f);
  return v & 0xffff;
}

function buildZip(files) {
  const now = new Date();
  const time = dosTime(now);
  const date = dosDate(now);

  const localChunks = [];
  const centralChunks = [];
  let offset = 0;

  for (const f of files) {
    const raw = fs.readFileSync(f.absPath);
    const nameBuf = Buffer.from(f.zipPath, "utf8");
    const compressed = zlib.deflateRawSync(raw, { level: 9 });

    // Pick whichever is smaller — store (method 0) or deflate (method 8).
    const useDeflate = compressed.length < raw.length;
    const data = useDeflate ? compressed : raw;
    const method = useDeflate ? 8 : 0;
    const crc = crc32(raw);

    // Local file header
    const lfh = Buffer.alloc(30);
    lfh.writeUInt32LE(0x04034b50, 0);
    lfh.writeUInt16LE(20, 4);
    lfh.writeUInt16LE(0x0800, 6); // bit 11 = utf-8 filename
    lfh.writeUInt16LE(method, 8);
    lfh.writeUInt16LE(time, 10);
    lfh.writeUInt16LE(date, 12);
    lfh.writeUInt32LE(crc, 14);
    lfh.writeUInt32LE(data.length, 18);
    lfh.writeUInt32LE(raw.length, 22);
    lfh.writeUInt16LE(nameBuf.length, 26);
    lfh.writeUInt16LE(0, 28);

    localChunks.push(lfh, nameBuf, data);

    // Central directory header
    const cdh = Buffer.alloc(46);
    cdh.writeUInt32LE(0x02014b50, 0);
    cdh.writeUInt16LE(20, 4);
    cdh.writeUInt16LE(20, 6);
    cdh.writeUInt16LE(0x0800, 8);
    cdh.writeUInt16LE(method, 10);
    cdh.writeUInt16LE(time, 12);
    cdh.writeUInt16LE(date, 14);
    cdh.writeUInt32LE(crc, 16);
    cdh.writeUInt32LE(data.length, 20);
    cdh.writeUInt32LE(raw.length, 24);
    cdh.writeUInt16LE(nameBuf.length, 28);
    cdh.writeUInt16LE(0, 30);
    cdh.writeUInt16LE(0, 32);
    cdh.writeUInt16LE(0, 34);
    cdh.writeUInt16LE(0, 36);
    cdh.writeUInt32LE(0, 38);
    cdh.writeUInt32LE(offset, 42);

    centralChunks.push(cdh, nameBuf);

    offset += lfh.length + nameBuf.length + data.length;
  }

  const localBuf = Buffer.concat(localChunks);
  const centralBuf = Buffer.concat(centralChunks);

  // End of central directory record
  const eocd = Buffer.alloc(22);
  eocd.writeUInt32LE(0x06054b50, 0);
  eocd.writeUInt16LE(0, 4);
  eocd.writeUInt16LE(0, 6);
  eocd.writeUInt16LE(files.length, 8);
  eocd.writeUInt16LE(files.length, 10);
  eocd.writeUInt32LE(centralBuf.length, 12);
  eocd.writeUInt32LE(localBuf.length, 16);
  eocd.writeUInt16LE(0, 20);

  return Buffer.concat([localBuf, centralBuf, eocd]);
}

// ---------------------------------------------------------------------------

function main() {
  if (!fs.existsSync(DIST)) fs.mkdirSync(DIST, { recursive: true });

  const files = walk(ROOT)
    .filter((f) => !f.zipPath.startsWith("dist/"))
    .sort((a, b) => a.zipPath.localeCompare(b.zipPath));

  if (files.length === 0) {
    console.error("No files to pack — bailing.");
    process.exit(1);
  }

  const zip = buildZip(files);
  fs.writeFileSync(OUT, zip);

  const sizeKb = (zip.length / 1024).toFixed(1);
  console.log(`Wrote ${OUT}`);
  console.log(`  ${files.length} files, ${sizeKb} KB`);
  console.log("");
  console.log("To install on another machine:");
  console.log("  1. Unzip the archive somewhere stable.");
  console.log("  2. chrome://extensions → Developer mode → Load unpacked");
  console.log("  3. Pick the unzipped folder (the one with manifest.json).");
}

main();
