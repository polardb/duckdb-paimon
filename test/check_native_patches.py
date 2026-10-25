#!/usr/bin/env python3
# Copyright (c) 2026, Alibaba Group Holding Limited
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise the actual ExternalProject patch step on pinned, offline Git fixtures."""

import argparse
import pathlib
import re
import shutil
import subprocess
import tempfile


def run(args, cwd=None):
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=pathlib.Path, required=True)
    parser.add_argument("--cmake", default="cmake")
    args = parser.parse_args()
    root = args.source_root.resolve()
    native = root / "third_party/paimon-cpp"
    text = (root / "CMakeLists.txt").read_text()
    recipe = text[text.index("set(PAIMON_CPP_PATCHES") : text.index("set(PAIMON_CPP_TOOLCHAIN_ARGS)")]
    names = re.findall(r"\$\{CMAKE_CURRENT_SOURCE_DIR\}/(patches/[^\s)]+)", recipe)
    assert len(names) >= 2, "Expected the native installation and planner patches"
    patch_command = re.search(r"^  PATCH_COMMAND (.+)$", text, re.MULTILINE).group(1)
    files = set()
    for name in names:
        files.update(re.findall(r"^--- a/(.+)$", (root / name).read_text(), re.MULTILINE))
    assert files, "Missing patch input inventory"
    for broken in (None, 0, len(names) - 1):
        with tempfile.TemporaryDirectory(prefix="paimon patch controls ") as directory:
            work = pathlib.Path(directory)
            project = work / "project"
            checkout = work / "native fixture"
            checkout.mkdir()
            result = run(["git", "init", "-q", str(checkout)])
            assert result.returncode == 0, result.stderr
            for name in files:
                data = subprocess.check_output(["git", "show", "HEAD:" + name], cwd=native)
                target = checkout / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
            for index, name in enumerate(names):
                target = project / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(root / name, target)
                if index == broken:
                    target.write_text("deliberately invalid native patch\n")
            build = work / "build"
            (project / "CMakeLists.txt").write_text(
                'cmake_minimum_required(VERSION 3.20)\nproject(patch_control NONE)\ninclude(ExternalProject)\n'
                'set(PAIMON_CPP_SOURCE_DIR "'
                + checkout.as_posix()
                + '")\n'
                + recipe
                + '\nfile(WRITE "${CMAKE_CURRENT_BINARY_DIR}/patch-path" "${PAIMON_CPP_PATCH_FILE}")\n'
                'ExternalProject_Add(native_patch SOURCE_DIR "${PAIMON_CPP_SOURCE_DIR}"\n'
                'DOWNLOAD_COMMAND "" UPDATE_COMMAND ""\nPATCH_COMMAND '
                + patch_command
                + '\nCONFIGURE_COMMAND "" BUILD_COMMAND "" INSTALL_COMMAND "")\n'
            )
            generator = "Ninja" if shutil.which("ninja") else "Unix Makefiles"
            result = run([args.cmake, "-S", str(project), "-B", str(build), "-G", generator])
            assert result.returncode == 0, result.stderr
            result = run([args.cmake, "--build", str(build)])
            assert (result.returncode == 0) == (broken is None), (
                broken,
                result.returncode,
                result.stdout,
                result.stderr,
            )
            for index, name in enumerate(names):
                applied = run(["git", "apply", "--reverse", "--check", str(root / name)], cwd=checkout)
                assert (applied.returncode == 0) == (broken is None or index < broken), (broken, name)
            if broken is None:
                result = run(["sh", (build / "patch-path").read_text()])
                assert result.returncode == 0, result.stderr
            print("PASS", "apply and idempotence" if broken is None else "failure at patch " + str(broken))


if __name__ == "__main__":
    main()
