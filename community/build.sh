rm -rf target
mkdir target

# PDF toolchain using FOP
mkdir -p target/fop
a2x -v -k -f pdf --fop -D target/fop --conf-file=conf/fop.conf --xsl-file=conf/fo.xsl --xsltproc-opts "--stringparam toc.section.depth 1 --stringparam admon.graphics 1" index.txt

# output as chunked html
#a2x -v -k -f chunked -D target --conf-file=conf/chunked.conf --xsltproc-opts "--stringparam admon.graphics 1" index.txt

# output as single html file, using the xhtml11 backend
#mkdir -p target/html/images
#cp -R images/* target/html/images
#asciidoc -v -a docinfo -a icons -d book -o target/html/index.html --conf-file=conf/asciidoc.conf index.txt

# output in text format
#mkdir target/text
#a2x -v -k -f text -D target/text --xsl-file=conf/text.xsl --conf-file=conf/text.conf index.txt

# PDF toolchain using latex
#mkdir -p target/dblatex
#a2x -v -k -f pdf -D target/dblatex --conf-file=conf/dblatex.conf index.txt

