mkdir -p target

# PDF toolchain using FOP
a2x -a docinfo -k -v -d book -f pdf --fop -D target --xsltproc-opts "--stringparam toc.section.depth 1 --stringparam admon.graphics 1" main/index.txt

# output as chunked html
#a2x -v -a docinfo --attribute icons -d book -f chunked -D target --xsltproc-opts "--stringparam admon.graphics 1" main/index.txt

# output in text format
#a2x -a docinfo -d book -f text -D target main/index.txt

# another pdf toolchain - images won't show up in the pdf
# at the moment, may need some configuration
#a2x -v -a docinfo -d book -k -f pdf -D target --dblatex-opts "--param toc.section.depth=1" main/index.txt

