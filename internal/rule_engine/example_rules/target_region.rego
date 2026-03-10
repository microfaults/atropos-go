package target_region

import rego.v1

default match := false

# Match EU or APAC regions.
match if {
    input.region in {"eu-west-1", "eu-central-1", "ap-southeast-1", "ap-northeast-1"}
}
