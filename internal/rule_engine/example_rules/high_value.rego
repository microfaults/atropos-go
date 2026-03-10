package high_value

import rego.v1

default match := false

match if {
    input.total_amount.units > 100
}
