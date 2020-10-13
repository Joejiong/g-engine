set -ex
set -o pipefail
# 配合覆盖率平台，本地结构和发布结构需要一致，导致本地代码结构层次较深
# 这个脚本用于准备本地软链目录，方便开发

rm -rf engine builtin
ln -s src/joewu/graph/engine engine
ln -s src/joewu/graph/builtin builtin
