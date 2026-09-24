package wire

// NetworkResolver resolves a target into listen and upstream addresses for
// network fault proxies. Supplied via atropos.Config.NetworkResolver.
type NetworkResolver func(target string) (listen, upstream string, err error)
