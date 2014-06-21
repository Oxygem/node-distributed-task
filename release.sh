#!/bin/sh

VERSION=`cat package.json | grep -oEi '[0-9]+.[0-9]+.[0-9]+'`

echo "# node-distributed-task"
echo "# Releasing: v$VERSION"

git tag -a "v$VERSION" -m "v$VERSION"
git push --tags
npm publish

echo "# Done!"
