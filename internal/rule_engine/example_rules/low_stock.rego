package low_stock

import rego.v1

default match := false

# Match when the item's available quantity is below the threshold.
match if {
    input.stock_quantity < 10
}
