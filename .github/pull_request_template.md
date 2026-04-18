## What

<!-- Brief description of the change. -->

## Why

<!-- Motivation, linked issue/ticket. -->

## How

<!-- Implementation notes worth flagging. -->

## Verification

- [ ] `go vet ./...`
- [ ] `go test -race ./...`
- [ ] `govulncheck ./...`
- [ ] Manually exercised with `example/` (if hot-path change)

## Security

- [ ] No new secrets, credentials, or tokens introduced
- [ ] No new network listeners exposed by default beyond loopback
- [ ] If TLS / SASL / auth surface changed, `SECURITY.md` was updated
