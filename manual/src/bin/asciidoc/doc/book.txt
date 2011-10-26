Book Title Goes Here
====================
Author's Name
v1.0, 2003-12
:doctype: book


[dedication]
Example Dedication
------------------
Optional dedication.

This document is an AsciiDoc book skeleton containing briefly
annotated example elements plus a couple of example index entries and
footnotes.

Books are normally used to generate DocBook markup and the titles of
the preface, appendix, bibliography, glossary and index sections are
significant ('specialsections').


[preface]
Example Preface
---------------
Optional preface.

Preface Sub-section
~~~~~~~~~~~~~~~~~~~
Preface sub-section body.


The First Chapter
-----------------
Chapters can contain sub-sections nested up to three deep.
footnote:[An example footnote.]
indexterm:[Example index entry]

Chapters can have their own bibliography, glossary and index.

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

Chapter Sub-section
^^^^^^^^^^^^^^^^^^^
Sub-section at level 3.

Chapter Sub-section
+++++++++++++++++++
Sub-section at level 4.

This is the maximum sub-section depth supported by the distributed
AsciiDoc configuration.
footnote:[A second example footnote.]


The Second Chapter
------------------
An example link to anchor at start of the <<X1,first sub-section>>.
indexterm:[Second example index entry]

An example link to a bibliography entry <<taoup>>.


The Third Chapter
-----------------
Book chapters are at level 1 and can contain sub-sections.


:numbered!:

[appendix]
Example Appendix
----------------
One or more optional appendixes go here at section level 1.

Appendix Sub-section
~~~~~~~~~~~~~~~~~~~
Sub-section body.


[bibliography]
Example Bibliography
--------------------
The bibliography list is a style of AsciiDoc bulleted list.

[bibliography]
.Books
- [[[taoup]]] Eric Steven Raymond. 'The Art of Unix
  Programming'. Addison-Wesley. ISBN 0-13-142901-9.
- [[[walsh-muellner]]] Norman Walsh & Leonard Muellner.
  'DocBook - The Definitive Guide'. O'Reilly & Associates. 1999.
  ISBN 1-56592-580-7.

[bibliography]
.Articles
- [[[abc2003]]] Gall Anonim. 'An article', Whatever. 2003.


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


[colophon]
Example Colophon
----------------
Text at the end of a book describing facts about its production.


[index]
Example Index
-------------
////////////////////////////////////////////////////////////////
The index is normally left completely empty, it's contents being
generated automatically by the DocBook toolchain.
////////////////////////////////////////////////////////////////
