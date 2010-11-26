# PDF toolchain using FOP
a2x -a docinfo -k -v -d book -f pdf --fop -D target --xsltproc-opts "--stringparam toc.section.depth 1" index.txt

# output as chunked html
#a2x -a docinfo -d book -f chunked -D target index.txt

# output in text format
#a2x -a docinfo -d book -f text -D target index.txt

# another pdf toolchain - images won't show up in the pdf
# at the moment, may need some configuration
#a2x -v -a docinfo -d book -k -f pdf -D target --dblatex-opts "--param toc.section.depth=1" index.txt

