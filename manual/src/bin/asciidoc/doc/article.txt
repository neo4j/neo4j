The Article Title
=================
Author's Name <authors@email.address>
v1.0, 2003-12


This is the optional preamble (an untitled section body). Useful for
writing simple sectionless documents consisting only of a preamble.

NOTE: The abstract, preface, appendix, bibliography, glossary and
index section titles are significant ('specialsections').


:numbered!:
[abstract]
Example Abstract
----------------
The optional abstract (one or more paragraphs) goes here.

This document is an AsciiDoc article skeleton containing briefly
annotated element placeholders plus a couple of example index entries
and footnotes.

:numbered:

The First Section
-----------------
Article sections start at level 1 and can be nested up to four levels
deep.
footnote:[An example footnote.]
indexterm:[Example index entry]

And now for something completely different: ((monkeys)), lions and
tigers (Bengal and Siberian) using the alternative syntax index
entries.
(((Big cats,Lions)))
(((Big cats,Tigers,Bengal Tiger)))
(((Big cats,Tigers,Siberian Tiger)))
Note that multi-entry terms generate separate index entries.

Here are a couple of image examples: an image:images/smallnew.png[]
example inline image followed by an example block image:

.Tiger block image
image::images/tiger.png[Tiger image]

Followed by an example table:

.An example table
[width="60%",options="header"]
|==============================================
| Option          | Description
| -a 'USER GROUP' | Add 'USER' to 'GROUP'.
| -R 'GROUP'      | Disables access to 'GROUP'.
|==============================================

.An example example
===============================================
Lorum ipum...
===============================================

[[X1]]
Sub-section with Anchor
~~~~~~~~~~~~~~~~~~~~~~~
Sub-section at level 2.

A Nested Sub-section
^^^^^^^^^^^^^^^^^^^^
Sub-section at level 3.

Yet another nested Sub-section
++++++++++++++++++++++++++++++
Sub-section at level 4.

This is the maximum sub-section depth supported by the distributed
AsciiDoc configuration.
footnote:[A second example footnote.]


The Second Section
------------------
Article sections are at level 1 and can contain sub-sections nested up
to four deep.

An example link to anchor at start of the <<X1,first sub-section>>.
indexterm:[Second example index entry]

An example link to a bibliography entry <<taoup>>.


:numbered!:

[appendix]
Example Appendix
----------------
AsciiDoc article appendices are just just article sections with
'specialsection' titles.

Appendix Sub-section
~~~~~~~~~~~~~~~~~~~~
Appendix sub-section at level 2.


[bibliography]
Example Bibliography
--------------------
The bibliography list is a style of AsciiDoc bulleted list.

[bibliography]
- [[[taoup]]] Eric Steven Raymond. 'The Art of Unix
  Programming'. Addison-Wesley. ISBN 0-13-142901-9.
- [[[walsh-muellner]]] Norman Walsh & Leonard Muellner.
  'DocBook - The Definitive Guide'. O'Reilly & Associates. 1999.
  ISBN 1-56592-580-7.


[glossary]
Example Glossary
----------------
Glossaries are optional. Glossaries entries are an example of a style
of AsciiDoc labeled lists.

[glossary]
A glossary term::
  The corresponding (indented) definition.

A second glossary term::
  The corresponding (indented) definition.


ifdef::backend-docbook[]
[index]
Example Index
-------------
////////////////////////////////////////////////////////////////
The index is normally left completely empty, it's contents being
generated automatically by the DocBook toolchain.
////////////////////////////////////////////////////////////////
endif::backend-docbook[]
