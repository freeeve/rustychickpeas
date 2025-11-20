# Image Conflicts Between Local and GitHub Actions

## Current Behavior

Images are named by benchmark only, not by machine:
- `builder_add_nodes_10000_time.svg`
- `builder_add_nodes_10000_time_badge.svg`

This means:
- **Local images** and **GitHub Actions images** use the **same filenames**
- When GitHub Actions runs, it will **overwrite** any local images you committed
- The last commit wins (usually GitHub Actions)

## Options

### Option 1: Keep Current Behavior (Recommended)
- Local images are temporary/for testing
- GitHub Actions images are the "source of truth"
- Local images get overwritten when CI runs (which is fine)

### Option 2: Machine-Specific Images
If you want to keep both local and GitHub images, we can modify the plotter to include machine name:
- `builder_add_nodes_10000_time_local.svg`
- `builder_add_nodes_10000_time_github.svg`

This would require updating the plotter and any README embeds.

### Option 3: Separate Directories
- `bench/img/local/` for local images
- `bench/img/github/` for CI images

## Recommendation

**Keep current behavior** - it's simpler and GitHub Actions will maintain the canonical images. Local images are just for preview/testing before committing.

If you want to preserve local images, we can implement Option 2 or 3.

