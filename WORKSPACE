workspace(name = "com_github_googleforgames_triton")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Go rules

git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go",
    commit = "8e6cfa5ef437aca79a987ad87e093c2065d71394",
    shallow_since = "1588715450 -0400",
)

git_repository(
    name = "bazel_gazelle",
    remote = "https://github.com/bazelbuild/bazel-gazelle",
    commit = "7ef160e5f9aad352418421941377e5df0716e047",
    shallow_since = "1588182354 -0400",
)

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")

go_rules_dependencies()

go_register_toolchains()

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

# Protobuf

git_repository(
    name = "com_google_protobuf",
    commit = "d0bfd5221182da1a7cc280f3337b5e41a89539cf",
    remote = "https://github.com/protocolbuffers/protobuf",
    shallow_since = "1581711200 -0800",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

# gRPC

http_archive(
    name = "com_github_grpc_grpc",
    urls = [
        "https://github.com/grpc/grpc/archive/v1.28.1.tar.gz",
    ],
    strip_prefix = "grpc-1.28.1",
    sha256 = "4cbce7f708917b6e58b631c24c59fe720acc8fef5f959df9a58cdf9558d0a79b",
)

load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

# googleapis

git_repository(
    name = "com_google_googleapis",
    remote = "https://github.com/googleapis/googleapis",
    commit = "4bef0001ac7040431ea24b6187424fdec9c08b1b",
    shallow_since = "1588704700 -0700",
)

load("@com_google_googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
    grpc = True,
    cc = True,
    go = True,
)

# Buildtools

http_archive(
    name = "com_github_bazelbuild_buildtools",
    strip_prefix = "buildtools-master",
    url = "https://github.com/bazelbuild/buildtools/archive/master.zip",
)

load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

buildifier_dependencies()

# Let gazelle handle Go dependencies
# gazelle:repository_macro go_repositories.bzl%go_repositories
load("//:go_repositories.bzl", "go_repositories")

go_repositories()
