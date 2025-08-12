# coding: utf-8
from __future__ import print_function, unicode_literals

import os
import stat
import tempfile
import threading
from queue import Queue

from .authsrv import AuthSrv
from .bos import bos
from .sutil import StreamArc, errdesc
from .util import Daemon, fsenc, min_ex

if True:  # pylint: disable=using-constant-test
    from typing import Any, Generator, Optional

    from .util import NamedLogger

try:
    import py7zr
    HAS_PY7ZR = True
except ImportError:
    HAS_PY7ZR = False


class QFile7z(object):
    """file-like object which buffers writes into a queue for 7z streaming"""

    def __init__(self) -> None:
        self.q: Queue[Optional[bytes]] = Queue(64)
        self.bq: list[bytes] = []
        self.nq = 0
        self.closed = False

    def write(self, buf: Optional[bytes]) -> None:
        if self.closed:
            return
            
        if buf is None or self.nq >= 240 * 1024:
            if self.bq:
                self.q.put(b"".join(self.bq))
                self.bq = []
                self.nq = 0

        if buf is None:
            self.q.put(None)
            self.closed = True
        else:
            self.bq.append(buf)
            self.nq += len(buf)

    def close(self) -> None:
        self.write(None)

    def flush(self) -> None:
        pass


class Stream7z(StreamArc):
    """construct in-memory 7z file from the given path"""

    def __init__(
        self,
        log: "NamedLogger",
        asrv: AuthSrv,
        fgen: Generator[dict[str, Any], None, None],
        cmp: str = "",
        **kwargs: Any
    ):
        if not HAS_PY7ZR:
            raise Exception("py7zr library not available. Install with: pip install py7zr")
            
        super(Stream7z, self).__init__(log, asrv, fgen)

        self.ci = 0
        self.co = 0
        self.qfile = QFile7z()
        self.errf: dict[str, Any] = {}
        self._temp_files: list[str] = []
        self._lock = threading.Lock()

        # parse compression level
        try:
            if ":" in cmp or "," in cmp:
                cmp, zs = cmp.replace(":", ",").split(",", 1)
                self.compression_level = int(zs)
            else:
                self.compression_level = 5  # default compression level
        except (ValueError, IndexError):
            self.compression_level = 5

        # clamp compression level to valid range
        self.compression_level = max(0, min(9, self.compression_level))

        Daemon(self._gen, "s7z-gen")

    def gen(self) -> Generator[Optional[bytes], None, None]:
        """generator that yields 7z archive data"""
        buf = b""
        try:
            while True:
                buf = self.qfile.q.get()
                if not buf:
                    break

                self.co += len(buf)
                yield buf

            yield None
        finally:
            # clean up temporary files
            while buf:
                try:
                    buf = self.qfile.q.get_nowait()
                except:
                    break

            self._cleanup_temp_files()

            if self.errf:
                bos.unlink(self.errf["ap"])

    def _cleanup_temp_files(self) -> None:
        """clean up temp files created during archive generation"""
        with self._lock:
            for temp_path in self._temp_files:
                try:
                    bos.unlink(temp_path)
                except:
                    pass
            self._temp_files.clear()

    def _create_temp_file(self, src_path: str, vp: str) -> str:
        """create a temp file for py7zr processing"""
        # create temporary file
        fd, temp_path = tempfile.mkstemp(prefix="copyparty-7z-")
        os.close(fd)
        
        # copy source file to temp location (py7zr expects file paths and not file objects)
        with open(fsenc(src_path), "rb") as src, open(temp_path, "wb") as dst:
            while True:
                chunk = src.read(64 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
        
        with self._lock:
            self._temp_files.append(temp_path)
        
        return temp_path

    def ser(self, f: dict[str, Any]) -> None:
        """serialize a single file into the archive"""
        name = f["vp"]
        src = f["ap"]
        fsi = f["st"]

        if stat.S_ISDIR(fsi.st_mode):
            return

        self.ci += fsi.st_size
        
        # create temporary file for py7zr
        temp_path = self._create_temp_file(src, name)
        
        # store info for later
        f["temp_path"] = temp_path

    def _gen(self) -> None:
        """background thread that generates the 7z archive"""
        errors = []
        file_map = {}  # map of archive names to temp file paths
        
        try:
            for f in self.fgen:
                if "err" in f:
                    errors.append((f["vp"], f["err"]))
                    continue

                if self.stopped:
                    break

                try:
                    self.ser(f)
                    if "temp_path" in f:
                        file_map[f["vp"]] = f["temp_path"]
                except Exception as ex:
                    ex_str = min_ex(5, True).replace("\n", "\n-- ")
                    errors.append((f["vp"], ex_str))

            if errors:
                self.errf, txt = errdesc(self.asrv.vfs, errors)
                self.log("\n".join(([repr(self.errf)] + txt[1:])))
                # add error file to the archive
                error_temp = self._create_temp_file(self.errf["ap"], self.errf["vp"])
                file_map[self.errf["vp"]] = error_temp

            # create the 7z archive
            if file_map:
                self._create_7z_archive(file_map)
            
        except Exception as ex:
            self.log("error creating 7z archive: {}".format(ex))
        finally:
            self.qfile.close()

    def _create_7z_archive(self, file_map: dict[str, str]) -> None:
        """create the actual 7z archive using py7zr"""
        # create a temporary 7z file
        fd, temp_7z_path = tempfile.mkstemp(suffix=".7z", prefix="copyparty-")
        os.close(fd)
        
        try:
            # create filter based on compression level
            filters = None
            if self.compression_level > 0:
                # use LZMA2 with preset based on compression level
                # map compression level 0-9 to py7zr presets
                preset_map = {
                    0: 0,           # no compression
                    1: 1,           # fast
                    2: 2,
                    3: 3,
                    4: 4,
                    5: 5,           # default
                    6: 6,
                    7: 7,
                    8: 8,
                    9: 9,           # maximum
                }
                preset = preset_map.get(self.compression_level, 5)
                if preset > 0:
                    filters = [{"id": py7zr.FILTER_LZMA2, "preset": preset}]
            
            # actually create 7z archive
            with py7zr.SevenZipFile(temp_7z_path, 'w', filters=filters) as archive:
                for archive_name, temp_file_path in file_map.items():
                    archive.write(temp_file_path, archive_name)

            # stream the archive
            with open(temp_7z_path, 'rb') as f:
                while True:
                    chunk = f.read(64 * 1024)  # 64KB chunks
                    if not chunk:
                        break
                    self.qfile.write(chunk)
                    
        except Exception as ex:
            self.log("Error writing 7z archive: {}".format(ex))
            raise
        finally:
            # clean up the temporary 7z file
            try:
                bos.unlink(temp_7z_path)
            except:
                pass
