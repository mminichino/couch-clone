rootProject.name = "cbclone"
include("couchbase2")
include("core")
include("couchbase3")

sourceControl {
    gitRepository(java.net.URI.create("https://github.com/mminichino/restfull-core.git")) {
        producesModule("com.codelry.util:restfull-core")
    }
}
