# hermeneutic

## Building with vcpkg

Some build targets fetch dependencies via
[vcpkg](https://github.com/microsoft/vcpkg) in manifest mode (`vcpkg.json`,
pinned via `builtin-baseline`). vcpkg itself is not vendored in this repo;
clone it once and point `VCPKG_ROOT` at it:

```sh
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
~/vcpkg/bootstrap-vcpkg.sh
export VCPKG_ROOT=~/vcpkg

cmake --preset vcpkg
cmake --build build-vcpkg
```

The first configure after adding a dependency to `vcpkg.json` builds it from
source, which can take a while; vcpkg's binary cache
(`~/.cache/vcpkg/archives` by default) speeds up subsequent configures and
other checkouts on the same machine.

The default build (no preset) needs no vcpkg and only builds
`hermeneutic_tests` as before.
