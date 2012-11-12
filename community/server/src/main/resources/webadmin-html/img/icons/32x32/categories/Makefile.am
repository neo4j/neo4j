
size = 32x32
context = categories

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =				\
	applications-accessories.png	\
	applications-development.png	\
	applications-games.png	\
	applications-graphics.png	\
	applications-internet.png	\
	applications-multimedia.png	\
	applications-office.png	\
	applications-other.png	\
	applications-system.png	\
	preferences-desktop.png	\
	preferences-desktop-peripherals.png	\
	preferences-system.png

EXTRA_DIST =				\
	$(icons_DATA)

install-data-local: install-iconsDATA
	(cd $(DESTDIR)$(themedir)/$(size) && $(ICONMAP) -c $(context))
