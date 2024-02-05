#!/bin/bash
while [[ $# > 1 ]]
do
key="$1"

case $key in
    -r|--releaseVersion)
    RELEASEVERSION="$2"
    shift # past argument
    ;;
    -d|--developmentVersion)
    DEVELOPMENTVERSION="$2"
    shift # past argument
    ;;
    *)
    # unknown option
    ;;
esac
shift # past argument or value
done

if [ -n "$RELEASEVERSION" ]; then
mvn versions:set -DnewVersion=$RELEASEVERSION -DgroupId='*' -DartifactId='*' -DprocessAllModules -DgenerateBackupPoms=false -P versions

git commit -a -m "prepare release $RELEASEVERSION"
git tag -a v$RELEASEVERSION -m "LinkedFactory POD release $RELEASEVERSION"
fi

if [ -n "$DEVELOPMENTVERSION" ]; then
mvn versions:set -DnewVersion=$DEVELOPMENTVERSION -DgroupId='*' -DartifactId='*' -DprocessAllModules -DgenerateBackupPoms=false -P versions

git commit -a -m "prepare for next development iteration"
fi