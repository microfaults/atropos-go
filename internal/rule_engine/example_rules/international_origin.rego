package international_origin

import rego.v1

default match := false

# Match if the shipping country is NOT "US" or "CA" (i.e. international).
match if {
    input.shipping_country != "US"
    input.shipping_country != "CA"
}
