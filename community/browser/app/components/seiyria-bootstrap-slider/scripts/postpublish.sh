#!/bin/bash

echo "Running postpublish script"

# Version bump (patch)
grunt bump-only:patch
# Generate new dist
grunt prod
# Generate new index.html page
grunt template
# Commit new release tag
grunt bump-commit
# Push commits/tags to master branch on remote 'origin'
git push origin master:master && git push --tags

# Push new source code to gh-pages branch
git checkout -B gh-pages origin/gh-pages
git pull -r origin gh-pages
git checkout master -- js/bootstrap-slider.js index.html css/bootstrap-slider.css
git commit -m "updates"
git push origin gh-pages:gh-pages -f

# Switch back to master branch
git checkout master