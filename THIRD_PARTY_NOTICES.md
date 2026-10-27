# Third-party notices and references

ParaVault's original source is provided under the MIT License in `LICENSE`.
External components retain their own terms. The build locates SQLite and
Zstandard through system packages; their source is not bundled here.

## SQLite

Project: https://www.sqlite.org/
Copyright statement: https://www.sqlite.org/copyright.html

The SQLite library's deliverable code has been dedicated to the public domain
by its authors. The statement above describes the scope of that dedication
and the separately licensed build tools.

## Zstandard

Project: https://github.com/facebook/zstd
License: BSD 3-Clause (the BSD licensing option is used here).

The following notice is reproduced from the installed Zstandard 1.5.7 package.
If distributing a different version or bundling the library in a binary
release, include the license supplied with that version.

```text
BSD License

For Zstandard software

Copyright (c) Meta Platforms, Inc. and affiliates. All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

 * Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

 * Neither the name Facebook, nor Meta, nor the names of its contributors may
   be used to endorse or promote products derived from this software without
   specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

## BLAKE3 algorithm

`src/hashing/blake3.cpp` is the project's compact implementation of unkeyed
BLAKE3-256, following the published algorithm. The official BLAKE3 library is
not a build dependency and is not vendored in this repository.

Specification and design rationale:
https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf

Official implementation and licensing information:
https://github.com/BLAKE3-team/BLAKE3#licenses

BLAKE3 was designed by Jack O'Connor, Samuel Neves, Jean-Philippe Aumasson,
and Zooko. The upstream implementation offers CC0 1.0, Apache 2.0, and
Apache 2.0 with LLVM exceptions as licensing options. These references do
not claim that ParaVault authored the BLAKE3 algorithm.
