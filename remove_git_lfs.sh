for f in $(git lfs ls-files); do
    git rm --cached $f
done
