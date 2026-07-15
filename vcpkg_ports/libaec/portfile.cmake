# Use GitHub for this overlay to avoid intermittent 429 responses from
# gitlab.dkrz.de when vcpkg downloads the source archive in CI.
#
# This intentionally mirrors the libaec 1.1.3#1 port from the vcpkg registry
# commit used by DuckDB extension CI. Remove or update this overlay when the
# CI vcpkg commit moves to a newer libaec port; otherwise this local port keeps
# taking precedence.
vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO Deutsches-Klimarechenzentrum/libaec
    REF v1.1.3
    SHA512 b64d10f8dd1f8d4c08dcbb5025550c790b01c9138714131456632e37cb58b60f40a94015644db727489fb0365dfc1e7ef0494f890639c8f306f2c90c09299136
    PATCHES
        static-shared.patch
        cmake-config.patch
)

vcpkg_cmake_configure(
    SOURCE_PATH "${SOURCE_PATH}"
    OPTIONS
        -DBUILD_TESTING=OFF
)
vcpkg_cmake_install()
vcpkg_copy_pdbs()
vcpkg_cmake_config_fixup(CONFIG_PATH "cmake")

file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL "${CURRENT_PORT_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")
vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE.txt")
