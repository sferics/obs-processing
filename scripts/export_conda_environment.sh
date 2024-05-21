# https://python-bloggers.com/2021/09/creating-and-replicating-an-anaconda-environment-from-a-yaml-file/
conda env export | grep -v "^prefix:$" | sed -e '/^variables:$/,$d' > config/environment.yml
