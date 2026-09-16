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
```

The default build (no preset) needs no vcpkg and only builds
`hermeneutic_tests` as before.

## Building the gRPC-based service targets

The service build (`HERMENEUTIC_BUILD_SERVICE=ON`, set by the `vcpkg` preset)
fetches gRPC and its dependencies via vcpkg:

```sh
cmake --preset vcpkg
```

The first configure builds gRPC/protobuf/abseil from source, which takes a
while; vcpkg's binary cache (`~/.cache/vcpkg/archives` by default) speeds up
subsequent configures and other checkouts on the same machine.

### Adding a `.proto`

Every file under `proto/` is picked up automatically (`cmake --build` alone
regenerates and recompiles after you add, edit, or remove one). Put each
`.proto` at a path that mirrors its `package`, e.g. `package bobby.hermeneutic.foo`
goes in `proto/bobby/hermeneutic/foo/*.proto`, matching the generated headers under
`generated/bobby/hermeneutic/foo/`. This keeps the C++ namespace, the file layout,
and the generated include paths consistent, and avoids filename collisions
between packages.
