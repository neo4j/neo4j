
size = 16x16
context = mimetypes

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =					\
	application-certificate.png		\
	application-x-executable.png		\
	audio-x-generic.png			\
	font-x-generic.png	\
	image-x-generic.png			\
	package-x-generic.png			\
	text-html.png				\
	text-x-generic.png			\
	text-x-generic-template.png			\
	text-x-script.png			\
	video-x-generic.png			\
	x-office-address-book.png		\
	x-office-calendar.png	\
	x-office-document.png		\
	x-office-document-template.png		\
	x-office-drawing.png		\
	x-office-drawing-template.png		\
	x-office-presentation.png	\
	x-office-presentation-template.png	\
	x-office-spreadsheet.png		\
	x-office-spreadsheet-template.png		

EXTRA_DIST =					\
	$(icons_DATA)

install-data-local: install-iconsDATA
	(cd $(DESTDIR)$(themedir)/$(size) && $(ICONMAP) -c $(context))
