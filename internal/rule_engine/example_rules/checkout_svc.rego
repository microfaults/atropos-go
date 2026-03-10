package checkout_svc

import rego.v1

default match := false

match if {
    input.service == "checkout"
}
