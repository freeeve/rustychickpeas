# Remove GITHUB_TOKEN from bench.yml remote URL

bench.yml embedded the token in the git remote URL (set-url with
x-access-token), which can leak into logs on push failure. checkout@v4
already persists a scoped credential, so the rewrite was unnecessary.
Also gated the commit step to push events (it previously ran on
pull_request, where pushing always failed silently) and added a
pull --rebase to reduce race failures against concurrent pushes.
