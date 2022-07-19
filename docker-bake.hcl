variable "BUNDLES_OUTPUT" {
  default = "./bundles"
}
variable "DOCKER_CROSSPLATFORMS" {
  default = ""
}

// Special target: https://github.com/docker/metadata-action#bake-definition
target "meta-helper" {
  tags = ["dockereng/moby-bin:local"]
}

target "_common" {
  args = {
    BUILDKIT_CONTEXT_KEEP_GIT_DIR = 1
    APT_MIRROR = "cdn-fastly.deb.debian.org"
  }
}

group "default" {
  targets = ["binary"]
}

target "binary" {
  inherits = ["_common"]
  target = "binary"
  output = [BUNDLES_OUTPUT]
}

target "dynbinary" {
  inherits = ["binary"]
  target = "dynbinary"
}

target "cross" {
  inherits = ["binary"]
  args = {
    CROSS = "true"
    DOCKER_CROSSPLATFORMS = DOCKER_CROSSPLATFORMS
  }
  target = "cross"
}

target "pkg" {
  inherits = ["meta-helper"]
  target = "pkg"
  contexts = {
    # we use a named context because cross build does not work with buildkit
    # platforms field atm (would be fixed by https://github.com/moby/moby/pull/43529).
    # so you have to run "cross" target first to make bundles available for
    # this named context.
    bundles = BUNDLES_OUTPUT
  }
  platforms = [
    "linux/amd64",
    "linux/arm/v5",
    "linux/arm/v6",
    "linux/arm/v7",
    "linux/arm64",
    "linux/ppc64le",
    "linux/s390x",
    "windows/amd64",
    "windows/arm64"
  ]
}
