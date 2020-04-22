workspace(name = "com_github_googleforgames_triton")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "7b9bbe3ea1fccb46dcfa6c3f3e29ba7ec740d8733370e21cdc8937467b4a4349",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.22.4/rules_go-v0.22.4.tar.gz",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.22.4/rules_go-v0.22.4.tar.gz",
    ],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains()

http_archive(
    name = "bazel_gazelle",
    sha256 = "d8c45ee70ec39a57e7a05e5027c32b1576cc7f16d9dd37135b0eddde45cf1b10",
    urls = [
        "https://storage.googleapis.com/bazel-mirror/github.com/bazelbuild/bazel-gazelle/releases/download/v0.20.0/bazel-gazelle-v0.20.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.20.0/bazel-gazelle-v0.20.0.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

gazelle_dependencies()

# Use gazelle to declare Go dependencies in Bazel.
# gazelle:repository_macro repositories.bzl%go_repositories

load("//:repositories.bzl", "go_repositories")

go_repositories()

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_google_protobuf",
    commit = "d0bfd5221182da1a7cc280f3337b5e41a89539cf",
    remote = "https://github.com/protocolbuffers/protobuf",
    shallow_since = "1581711200 -0800",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

protobuf_deps()

http_archive(
    name = "com_github_bazelbuild_buildtools",
    strip_prefix = "buildtools-master",
    url = "https://github.com/bazelbuild/buildtools/archive/master.zip",
)

load("@com_github_bazelbuild_buildtools//buildifier:deps.bzl", "buildifier_dependencies")

buildifier_dependencies()

go_repository(
    name = "com_github_alecthomas_template",
    importpath = "github.com/alecthomas/template",
    sum = "h1:cAKDfWh5VpdgMhJosfJnn5/FoN2SRZ4p7fJNX58YPaU=",
    version = "v0.0.0-20160405071501-a0175ee3bccc",
)

go_repository(
    name = "com_github_alecthomas_units",
    importpath = "github.com/alecthomas/units",
    sum = "h1:qet1QNfXsQxTZqLG4oE62mJzwPIB8+Tee4RNCL9ulrY=",
    version = "v0.0.0-20151022065526-2efee857e7cf",
)

go_repository(
    name = "com_github_armon_consul_api",
    importpath = "github.com/armon/consul-api",
    sum = "h1:G1bPvciwNyF7IUmKXNt9Ak3m6u9DE1rF+RmtIkBpVdA=",
    version = "v0.0.0-20180202201655-eb2c6b5be1b6",
)

go_repository(
    name = "com_github_beorn7_perks",
    importpath = "github.com/beorn7/perks",
    sum = "h1:HWo1m869IqiPhD389kmkxeTalrjNbbJTC8LXupb+sl0=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_cespare_xxhash",
    importpath = "github.com/cespare/xxhash",
    sum = "h1:a6HrQnmkObjyL+Gs60czilIUGqrzKutQD6XZog3p+ko=",
    version = "v1.1.0",
)

go_repository(
    name = "com_github_coreos_bbolt",
    importpath = "github.com/coreos/bbolt",
    sum = "h1:wZwiHHUieZCquLkDL0B8UhzreNWsPHooDAG3q34zk0s=",
    version = "v1.3.2",
)

go_repository(
    name = "com_github_coreos_etcd",
    importpath = "github.com/coreos/etcd",
    sum = "h1:8F3hqu9fGYLBifCmRCJsicFqDx/D68Rt3q1JMazcgBQ=",
    version = "v3.3.13+incompatible",
)

go_repository(
    name = "com_github_coreos_go_semver",
    importpath = "github.com/coreos/go-semver",
    sum = "h1:3Jm3tLmsgAYcjC+4Up7hJrFBPr+n7rAqYeSw/SZazuY=",
    version = "v0.2.0",
)

go_repository(
    name = "com_github_coreos_go_systemd",
    importpath = "github.com/coreos/go-systemd",
    sum = "h1:Wf6HqHfScWJN9/ZjdUKyjop4mf3Qdd+1TvvltAvM3m8=",
    version = "v0.0.0-20190321100706-95778dfbb74e",
)

go_repository(
    name = "com_github_coreos_pkg",
    importpath = "github.com/coreos/pkg",
    sum = "h1:lBNOc5arjvs8E5mO2tbpBpLoyyu8B6e44T7hJy6potg=",
    version = "v0.0.0-20180928190104-399ea9e2e55f",
)

go_repository(
    name = "com_github_davecgh_go_spew",
    importpath = "github.com/davecgh/go-spew",
    sum = "h1:vj9j/u1bqnvCEfJOwUhtlOARqs3+rkHYY13jYWTU97c=",
    version = "v1.1.1",
)

go_repository(
    name = "com_github_dgrijalva_jwt_go",
    importpath = "github.com/dgrijalva/jwt-go",
    sum = "h1:7qlOGliEKZXTDg6OTjfoBKDXWrumCAMpl/TFQ4/5kLM=",
    version = "v3.2.0+incompatible",
)

go_repository(
    name = "com_github_dgryski_go_sip13",
    importpath = "github.com/dgryski/go-sip13",
    sum = "h1:RMLoZVzv4GliuWafOuPuQDKSm1SJph7uCRnnS61JAn4=",
    version = "v0.0.0-20181026042036-e10d5fee7954",
)

go_repository(
    name = "com_github_fsnotify_fsnotify",
    importpath = "github.com/fsnotify/fsnotify",
    sum = "h1:hsms1Qyu0jgnwNXIxa+/V/PDsU6CfLf6CNO8H7IWoS4=",
    version = "v1.4.9",
)

go_repository(
    name = "com_github_go_kit_kit",
    importpath = "github.com/go-kit/kit",
    sum = "h1:Wz+5lgoB0kkuqLEc6NVmwRknTKP6dTGbSqvhZtBI/j0=",
    version = "v0.8.0",
)

go_repository(
    name = "com_github_go_logfmt_logfmt",
    importpath = "github.com/go-logfmt/logfmt",
    sum = "h1:MP4Eh7ZCb31lleYCFuwm0oe4/YGak+5l1vA2NOE80nA=",
    version = "v0.4.0",
)

go_repository(
    name = "com_github_go_stack_stack",
    importpath = "github.com/go-stack/stack",
    sum = "h1:5SgMzNM5HxrEjV0ww2lTmX6E2Izsfxas4+YHWRs3Lsk=",
    version = "v1.8.0",
)

go_repository(
    name = "com_github_gogo_protobuf",
    importpath = "github.com/gogo/protobuf",
    sum = "h1:/s5zKNz0uPFCZ5hddgPdo2TK2TVrUNMn0OOX8/aZMTE=",
    version = "v1.2.1",
)

go_repository(
    name = "com_github_golang_groupcache",
    importpath = "github.com/golang/groupcache",
    sum = "h1:veQD95Isof8w9/WXiA+pa3tz3fJXkt5B7QaRBrM62gk=",
    version = "v0.0.0-20190129154638-5b532d6fd5ef",
)

go_repository(
    name = "com_github_google_btree",
    importpath = "github.com/google/btree",
    sum = "h1:0udJVsspx3VBr5FwtLhQQtuAsVc79tTq0ocGIPAU6qo=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_google_gofuzz",
    importpath = "github.com/google/gofuzz",
    sum = "h1:A8PeW59pxE9IoFRqBp37U+mSNaQoZ46F1f0f863XSXw=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_google_uuid",
    importpath = "github.com/google/uuid",
    sum = "h1:Gkbcsh/GbpXz7lPftLA3P6TYMwjCLYm83jiFQZF/3gY=",
    version = "v1.1.1",
)

go_repository(
    name = "com_github_gopherjs_gopherjs",
    importpath = "github.com/gopherjs/gopherjs",
    sum = "h1:EGx4pi6eqNxGaHF6qqu48+N2wcFQ5qg5FXgOdqsJ5d8=",
    version = "v0.0.0-20181017120253-0766667cb4d1",
)

go_repository(
    name = "com_github_gorilla_websocket",
    importpath = "github.com/gorilla/websocket",
    sum = "h1:WDFjx/TMzVgy9VdMMQi2K2Emtwi2QcUQsztZ/zLaH/Q=",
    version = "v1.4.0",
)

go_repository(
    name = "com_github_grpc_ecosystem_go_grpc_middleware",
    importpath = "github.com/grpc-ecosystem/go-grpc-middleware",
    sum = "h1:Iju5GlWwrvL6UBg4zJJt3btmonfrMlCDdsejg4CZE7c=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_grpc_ecosystem_go_grpc_prometheus",
    importpath = "github.com/grpc-ecosystem/go-grpc-prometheus",
    sum = "h1:Ovs26xHkKqVztRpIrF/92BcuyuQ/YW4NSIpoGtfXNho=",
    version = "v1.2.0",
)

go_repository(
    name = "com_github_hashicorp_hcl",
    importpath = "github.com/hashicorp/hcl",
    sum = "h1:0Anlzjpi4vEasTeNFn2mLJgTSwt0+6sfsiTG8qcWGx4=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_jonboulle_clockwork",
    importpath = "github.com/jonboulle/clockwork",
    sum = "h1:VKV+ZcuP6l3yW9doeqz6ziZGgcynBVQO+obU0+0hcPo=",
    version = "v0.1.0",
)

go_repository(
    name = "com_github_json_iterator_go",
    importpath = "github.com/json-iterator/go",
    sum = "h1:9yzud/Ht36ygwatGx56VwCZtlI/2AD15T1X2sjSuGns=",
    version = "v1.1.9",
)

go_repository(
    name = "com_github_jtolds_gls",
    importpath = "github.com/jtolds/gls",
    sum = "h1:xdiiI2gbIgH/gLH7ADydsJ1uDOEzR8yvV7C0MuV77Wo=",
    version = "v4.20.0+incompatible",
)

go_repository(
    name = "com_github_julienschmidt_httprouter",
    importpath = "github.com/julienschmidt/httprouter",
    sum = "h1:TDTW5Yz1mjftljbcKqRcrYhd4XeOoI98t+9HbQbYf7g=",
    version = "v1.2.0",
)

go_repository(
    name = "com_github_kisielk_errcheck",
    importpath = "github.com/kisielk/errcheck",
    sum = "h1:ZqfnKyx9KGpRcW04j5nnPDgRgoXUeLh2YFBeFzphcA0=",
    version = "v1.1.0",
)

go_repository(
    name = "com_github_kisielk_gotool",
    importpath = "github.com/kisielk/gotool",
    sum = "h1:AV2c/EiW3KqPNT9ZKl07ehoAGi4C5/01Cfbblndcapg=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_konsorten_go_windows_terminal_sequences",
    importpath = "github.com/konsorten/go-windows-terminal-sequences",
    sum = "h1:mweAR1A6xJ3oS2pRaGiHgQ4OO8tzTaLawm8vnODuwDk=",
    version = "v1.0.1",
)

go_repository(
    name = "com_github_kr_logfmt",
    importpath = "github.com/kr/logfmt",
    sum = "h1:T+h1c/A9Gawja4Y9mFVWj2vyii2bbUNDw3kt9VxK2EY=",
    version = "v0.0.0-20140226030751-b84e30acd515",
)

go_repository(
    name = "com_github_kr_pretty",
    importpath = "github.com/kr/pretty",
    sum = "h1:L/CwN0zerZDmRFUapSPitk6f+Q3+0za1rQkzVuMiMFI=",
    version = "v0.1.0",
)

go_repository(
    name = "com_github_kr_pty",
    importpath = "github.com/kr/pty",
    sum = "h1:VkoXIwSboBpnk99O/KFauAEILuNHv5DVFKZMBN/gUgw=",
    version = "v1.1.1",
)

go_repository(
    name = "com_github_kr_text",
    importpath = "github.com/kr/text",
    sum = "h1:45sCR5RtlFHMR4UwH9sdQ5TC8v0qDQCHnXt+kaKSTVE=",
    version = "v0.1.0",
)

go_repository(
    name = "com_github_magiconair_properties",
    importpath = "github.com/magiconair/properties",
    sum = "h1:ZC2Vc7/ZFkGmsVC9KvOjumD+G5lXy2RtTKyzRKO2BQ4=",
    version = "v1.8.1",
)

go_repository(
    name = "com_github_matttproud_golang_protobuf_extensions",
    importpath = "github.com/matttproud/golang_protobuf_extensions",
    sum = "h1:4hp9jkHxhMHkqkrB3Ix0jegS5sx/RkqARlsWZ6pIwiU=",
    version = "v1.0.1",
)

go_repository(
    name = "com_github_mitchellh_mapstructure",
    importpath = "github.com/mitchellh/mapstructure",
    sum = "h1:fmNYVwqnSfB9mZU6OS2O6GsXM+wcskZDuKQzvN1EDeE=",
    version = "v1.1.2",
)

go_repository(
    name = "com_github_modern_go_concurrent",
    importpath = "github.com/modern-go/concurrent",
    sum = "h1:ZqeYNhU3OHLH3mGKHDcjJRFFRrJa6eAM5H+CtDdOsPc=",
    version = "v0.0.0-20180228061459-e0a39a4cb421",
)

go_repository(
    name = "com_github_modern_go_reflect2",
    importpath = "github.com/modern-go/reflect2",
    sum = "h1:9f412s+6RmYXLWZSEzVVgPGK7C2PphHj5RJrvfx9AWI=",
    version = "v1.0.1",
)

go_repository(
    name = "com_github_mwitkow_go_conntrack",
    importpath = "github.com/mwitkow/go-conntrack",
    sum = "h1:F9x/1yl3T2AeKLr2AMdilSD8+f9bvMnNN8VS5iDtovc=",
    version = "v0.0.0-20161129095857-cc309e4a2223",
)

go_repository(
    name = "com_github_oklog_ulid",
    importpath = "github.com/oklog/ulid",
    sum = "h1:EGfNDEx6MqHz8B3uNV6QAib1UR2Lm97sHi3ocA6ESJ4=",
    version = "v1.3.1",
)

go_repository(
    name = "com_github_oneofone_xxhash",
    importpath = "github.com/OneOfOne/xxhash",
    sum = "h1:KMrpdQIwFcEqXDklaen+P1axHaj9BSKzvpUUfnHldSE=",
    version = "v1.2.2",
)

go_repository(
    name = "com_github_pelletier_go_toml",
    importpath = "github.com/pelletier/go-toml",
    sum = "h1:T5zMGML61Wp+FlcbWjRDT7yAxhJNAiPPLOFECq181zc=",
    version = "v1.2.0",
)

go_repository(
    name = "com_github_pkg_errors",
    importpath = "github.com/pkg/errors",
    sum = "h1:WdK/asTD0HN+q6hsWO3/vpuAkAr+tw6aNJNDFFf0+qw=",
    version = "v0.8.0",
)

go_repository(
    name = "com_github_pmezard_go_difflib",
    importpath = "github.com/pmezard/go-difflib",
    sum = "h1:4DBwDE0NGyQoBHbLQYPwSUPoCMWR5BEzIk/f1lZbAQM=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_prometheus_client_golang",
    importpath = "github.com/prometheus/client_golang",
    sum = "h1:9iH4JKXLzFbOAdtqv/a+j8aewx2Y8lAjAydhbaScPF8=",
    version = "v0.9.3",
)

go_repository(
    name = "com_github_prometheus_common",
    importpath = "github.com/prometheus/common",
    sum = "h1:7etb9YClo3a6HjLzfl6rIQaU+FDfi0VSX39io3aQ+DM=",
    version = "v0.4.0",
)

go_repository(
    name = "com_github_prometheus_procfs",
    importpath = "github.com/prometheus/procfs",
    sum = "h1:sofwID9zm4tzrgykg80hfFph1mryUeLRsUfoocVVmRY=",
    version = "v0.0.0-20190507164030-5867b95ac084",
)

go_repository(
    name = "com_github_prometheus_tsdb",
    importpath = "github.com/prometheus/tsdb",
    sum = "h1:YZcsG11NqnK4czYLrWd9mpEuAJIHVQLwdrleYfszMAA=",
    version = "v0.7.1",
)

go_repository(
    name = "com_github_sirupsen_logrus",
    importpath = "github.com/sirupsen/logrus",
    sum = "h1:1N5EYkVAPEywqZRJd7cwnRtCb6xJx7NH3T3WUTF980Q=",
    version = "v1.5.0",
)

go_repository(
    name = "com_github_smartystreets_assertions",
    importpath = "github.com/smartystreets/assertions",
    sum = "h1:zE9ykElWQ6/NYmHa3jpm/yHnI4xSofP+UP6SpjHcSeM=",
    version = "v0.0.0-20180927180507-b2de0cb4f26d",
)

go_repository(
    name = "com_github_smartystreets_goconvey",
    importpath = "github.com/smartystreets/goconvey",
    sum = "h1:fv0U8FUIMPNf1L9lnHLvLhgicrIVChEkdzIKYqbNC9s=",
    version = "v1.6.4",
)

go_repository(
    name = "com_github_soheilhy_cmux",
    importpath = "github.com/soheilhy/cmux",
    sum = "h1:0HKaf1o97UwFjHH9o5XsHUOF+tqmdA7KEzXLpiyaw0E=",
    version = "v0.1.4",
)

go_repository(
    name = "com_github_spaolacci_murmur3",
    importpath = "github.com/spaolacci/murmur3",
    sum = "h1:qLC7fQah7D6K1B0ujays3HV9gkFtllcxhzImRR7ArPQ=",
    version = "v0.0.0-20180118202830-f09979ecbc72",
)

go_repository(
    name = "com_github_spf13_afero",
    importpath = "github.com/spf13/afero",
    sum = "h1:m8/z1t7/fwjysjQRYbP0RD+bUIF/8tJwPdEZsI83ACI=",
    version = "v1.1.2",
)

go_repository(
    name = "com_github_spf13_cast",
    importpath = "github.com/spf13/cast",
    sum = "h1:oget//CVOEoFewqQxwr0Ej5yjygnqGkvggSE/gB35Q8=",
    version = "v1.3.0",
)

go_repository(
    name = "com_github_spf13_jwalterweatherman",
    importpath = "github.com/spf13/jwalterweatherman",
    sum = "h1:XHEdyB+EcvlqZamSM4ZOMGlc93t6AcsBEu9Gc1vn7yk=",
    version = "v1.0.0",
)

go_repository(
    name = "com_github_spf13_pflag",
    importpath = "github.com/spf13/pflag",
    sum = "h1:zPAT6CGy6wXeQ7NtTnaTerfKOsV6V6F8agHXFiazDkg=",
    version = "v1.0.3",
)

go_repository(
    name = "com_github_spf13_viper",
    importpath = "github.com/spf13/viper",
    sum = "h1:pDDu1OyEDTKzpJwdq4TiuLyMsUgRa/BT5cn5O62NoHs=",
    version = "v1.6.3",
)

go_repository(
    name = "com_github_stretchr_objx",
    importpath = "github.com/stretchr/objx",
    sum = "h1:2vfRuCMp5sSVIDSqO8oNnWJq7mPa6KVP3iPIwFBuy8A=",
    version = "v0.1.1",
)

go_repository(
    name = "com_github_stretchr_testify",
    importpath = "github.com/stretchr/testify",
    sum = "h1:TivCn/peBQ7UY8ooIcPgZFpTNSz0Q2U6UrFlUfqbe0Q=",
    version = "v1.3.0",
)

go_repository(
    name = "com_github_subosito_gotenv",
    importpath = "github.com/subosito/gotenv",
    sum = "h1:Slr1R9HxAlEKefgq5jn9U+DnETlIUa6HfgEzj0g5d7s=",
    version = "v1.2.0",
)

go_repository(
    name = "com_github_tmc_grpc_websocket_proxy",
    importpath = "github.com/tmc/grpc-websocket-proxy",
    sum = "h1:LnC5Kc/wtumK+WB441p7ynQJzVuNRJiqddSIE3IlSEQ=",
    version = "v0.0.0-20190109142713-0ad062ec5ee5",
)

go_repository(
    name = "com_github_tv4_logrus_stackdriver_formatter",
    importpath = "github.com/TV4/logrus-stackdriver-formatter",
    sum = "h1:nFea8RiX7ecTnWPM+9FIqwZYJdcGo58CHMGIVdYzMXg=",
    version = "v0.1.0",
)

go_repository(
    name = "com_github_xiang90_probing",
    importpath = "github.com/xiang90/probing",
    sum = "h1:eY9dn8+vbi4tKz5Qo6v2eYzo7kUS51QINcR5jNpbZS8=",
    version = "v0.0.0-20190116061207-43a291ad63a2",
)

go_repository(
    name = "com_github_xordataexchange_crypt",
    importpath = "github.com/xordataexchange/crypt",
    sum = "h1:ESFSdwYZvkeru3RtdrYueztKhOBCSAAzS4Gf+k0tEow=",
    version = "v0.0.3-0.20170626215501-b2862e3d0a77",
)

go_repository(
    name = "in_gopkg_alecthomas_kingpin_v2",
    importpath = "gopkg.in/alecthomas/kingpin.v2",
    sum = "h1:jMFz6MfLP0/4fUyZle81rXUoxOBFi19VUFKVDOQfozc=",
    version = "v2.2.6",
)

go_repository(
    name = "in_gopkg_ini_v1",
    importpath = "gopkg.in/ini.v1",
    sum = "h1:AQvPpx3LzTDM0AjnIRlVFwFFGC+npRopjZxLJj6gdno=",
    version = "v1.51.0",
)

go_repository(
    name = "in_gopkg_resty_v1",
    importpath = "gopkg.in/resty.v1",
    sum = "h1:CuXP0Pjfw9rOuY6EP+UvtNvt5DSqHpIxILZKT/quCZI=",
    version = "v1.12.0",
)

go_repository(
    name = "io_etcd_go_bbolt",
    importpath = "go.etcd.io/bbolt",
    sum = "h1:Z/90sZLPOeCy2PwprqkFa25PdkusRzaj9P8zm/KNyvk=",
    version = "v1.3.2",
)

go_repository(
    name = "org_golang_x_time",
    importpath = "golang.org/x/time",
    sum = "h1:SvFZT6jyqRaOeXpc5h/JSfZenJ2O330aBsf7JfSUXmQ=",
    version = "v0.0.0-20190308202827-9d24e82272b4",
)

go_repository(
    name = "org_uber_go_atomic",
    importpath = "go.uber.org/atomic",
    sum = "h1:cxzIVoETapQEqDhQu3QfnvXAV4AlzcvUCxkVUFw3+EU=",
    version = "v1.4.0",
)

go_repository(
    name = "org_uber_go_multierr",
    importpath = "go.uber.org/multierr",
    sum = "h1:HoEmRHQPVSqub6w2z2d2EOVs2fjyFRGyofhKuyDq0QI=",
    version = "v1.1.0",
)

go_repository(
    name = "org_uber_go_zap",
    importpath = "go.uber.org/zap",
    sum = "h1:ORx85nbTijNz8ljznvCMR1ZBIPKFn3jQrag10X2AsuM=",
    version = "v1.10.0",
)

go_repository(
    name = "com_github_google_martian",
    importpath = "github.com/google/martian",
    sum = "h1:/CP5g8u/VJHijgedC/Legn3BAbAaWPgecwXBIDzw5no=",
    version = "v2.1.0+incompatible",
)

go_repository(
    name = "com_github_google_pprof",
    importpath = "github.com/google/pprof",
    sum = "h1:eqyIo2HjKhKe/mJzTG8n4VqvLXIOEG+SLdDqX7xGtkY=",
    version = "v0.0.0-20181206194817-3ea8567a2e57",
)

go_repository(
    name = "com_github_googleapis_gax_go_v2",
    importpath = "github.com/googleapis/gax-go/v2",
    sum = "h1:sjZBwGj9Jlw33ImPtvFviGYvseOtDM7hkSKB7+Tv3SM=",
    version = "v2.0.5",
)

go_repository(
    name = "com_github_hashicorp_golang_lru",
    importpath = "github.com/hashicorp/golang-lru",
    sum = "h1:0hERBMJE1eitiLkihrMvRVBYAkpHzc/J3QdDN+dAcgU=",
    version = "v0.5.1",
)

go_repository(
    name = "com_github_jstemmer_go_junit_report",
    importpath = "github.com/jstemmer/go-junit-report",
    sum = "h1:rBMNdlhTLzJjJSDIjNEXX1Pz3Hmwmz91v+zycvx9PJc=",
    version = "v0.0.0-20190106144839-af01ea7f8024",
)

go_repository(
    name = "io_opencensus_go",
    importpath = "go.opencensus.io",
    sum = "h1:mU6zScU4U1YAFPHEHYk+3JC4SY7JxgkqS10ZOSyksNg=",
    version = "v0.21.0",
)

go_repository(
    name = "org_golang_google_api",
    importpath = "google.golang.org/api",
    sum = "h1:J1Pl9P2lnmYFSJvgs70DKELqHNh8CNWXPbud4njEE2s=",
    version = "v0.22.0",
)
