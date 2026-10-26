# Releasing

A release is one git tag. Pushing a tag like `v0.1.0` makes GitHub Actions:

1. run the tests (`go test -race ./...`),
2. build `rediska` for macOS, Linux and Windows, each for Intel and ARM,
3. publish a [GitHub Release](https://github.com/vinayhpandya/rediska/releases)
   with the archives, `checksums.txt` and a changelog of the commits since the
   last tag,
4. update the Homebrew cask in
   [vinayhpandya/homebrew-tap](https://github.com/vinayhpandya/homebrew-tap),
   so `brew install vinayhpandya/tap/rediska` gets the new version.

The workflow is `.github/workflows/release.yml`, and what gets built is in
`.goreleaser.yaml`. [GoReleaser](https://goreleaser.com) does steps 2–4.

## One-time setup

1. **Create the tap repository**: a new public repo named exactly
   `homebrew-tap` under your account, with a README so it isn't empty.
   Homebrew finds `vinayhpandya/tap` by that name.
2. **Create a token for it**: on GitHub, *Settings → Developer settings →
   Fine-grained tokens → Generate new token*. Give it access to **only**
   `homebrew-tap`, with **Contents: Read and write**.
3. **Store the token in this repo**: *Settings → Secrets and variables →
   Actions → New repository secret*, named `HOMEBREW_TAP_GITHUB_TOKEN`.

The workflow's built-in `GITHUB_TOKEN` can only write to this repo, which is
why the tap needs its own token. Fine-grained tokens expire, so renew it
when GitHub emails you.

## Making a release

```bash
git checkout main && git pull
go test -race ./...            # the workflow does this too, but fail fast
git tag v0.1.0
git push origin v0.1.0
```

Then watch the **release** workflow on the Actions tab. When it finishes,
check the release page, and on a Mac:

```bash
brew update
brew install vinayhpandya/tap/rediska     # or: brew upgrade rediska
rediska --version
```

Tags follow [semantic versioning](https://semver.org): bump the last number
for fixes (`v0.1.1`), the middle one for new features (`v0.2.0`). A tag with
a suffix, like `v0.2.0-rc1`, is published as a pre-release.

## Trying it without publishing

[Install GoReleaser](https://goreleaser.com/install/), then:

```bash
goreleaser check                         # is .goreleaser.yaml valid?
goreleaser release --snapshot --clean    # build everything into dist/
```

A snapshot builds every archive and the cask file into `dist/` (ignored by
git), and publishes nothing. `dist/homebrew/Casks/rediska.rb` is the cask
that would go to the tap.

## If something goes wrong

- **The workflow fails at the Homebrew step**: the GitHub Release is already
  published. Fix the token, then re-run the failed job from the Actions tab.
- **A bad release went out**: delete the release and the tag on GitHub, then
  delete the tag locally (`git tag -d v0.1.0`), fix, and tag again. If the
  tap was already updated, the next release overwrites it.
- **macOS says the binary can't be opened**: the binary isn't signed by
  Apple. The cask removes macOS's quarantine flag after installing, which
  needs a recent Homebrew (`brew update`). For a downloaded archive, run
  `xattr -d com.apple.quarantine rediska` once.
