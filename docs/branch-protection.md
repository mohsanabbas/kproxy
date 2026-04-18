# Branch protection on this repository

GitHub Free does not enforce **branch protection rules** or **rulesets** on
private repositories. The protections you would normally configure in
*Settings → Branches* are silently ignored on this account tier.

To regain server-side enforcement, choose one of:

1. **Upgrade the account to GitHub Pro / Team.** Both rulesets and classic
   branch protection start enforcing immediately on private repos.
2. **Move the repo to a paid organisation.** Same effect; also unlocks
   CODEOWNERS-required reviews.
3. **Make the repository public.** Branch protection becomes free.

Until then, this repo enforces the same gates **client-side** via a Git
pre-push hook in `.githooks/pre-push`.

## Enable the local hook

Run once per clone:

```sh
git config core.hooksPath .githooks
chmod +x .githooks/pre-push
```

The hook runs only on pushes that update `refs/heads/main` and executes:

- `go vet ./...`
- `go test -race -count=1 -timeout 120s ./...`
- `gosec` (if installed)
- `govulncheck` (if installed)

A failing gate aborts the push. To bypass for a single push (discouraged):

```sh
git push --no-verify
```

## What is enforced server-side today

- The CI workflow (`.github/workflows/ci.yml`) runs vet/build/test/race +
  govulncheck on every push and PR. Failures are reported but **not blocking**
  on Free.
- The security workflow (`.github/workflows/security.yml`) runs gitleaks,
  gosec, and govulncheck weekly and on push.
- `.github/CODEOWNERS` flags reviewers automatically once collaborators are
  added; reviews are requested but **not required** on Free.
- Dependabot is enabled (`.github/dependabot.yml`) for `gomod` (root +
  `/example`) and `github-actions`.

## When upgrading

After moving to a paid plan, run:

```sh
gh api -X PUT repos/<owner>/<repo>/branches/main/protection \
  -H "Accept: application/vnd.github+json" \
  --input .github/branch-protection.main.json
```

The desired protection definition is checked in at
[.github/branch-protection.main.json](.github/branch-protection.main.json).
