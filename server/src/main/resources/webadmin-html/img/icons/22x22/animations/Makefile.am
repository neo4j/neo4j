
size = 22x22
context = animations

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =						\
	process-working.png

EXTRA_DIST =						\
	$(icons_DATA)

install-data-local: install-iconsDATA
	(cd $(DESTDIR)$(themedir)/$(size) && $(ICONMAP) -c $(context))
