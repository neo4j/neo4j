
size = 22x22
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
if ENABLE_ICON_FRAMING
	$(mkinstalldirs) $(DESTDIR)$(themedir)/24x24/$(context)
	(cd $(DESTDIR)$(themedir) && \
	for icon in $(icons_DATA); do \
		$(CONVERT) $(size)/$(context)/$$icon 24x24/$(context)/$$icon; \
	done)
	(cd $(DESTDIR)$(themedir)/24x24 && $(ICONMAP) -c $(context))
endif

uninstall-local:
	rm -rf $(themedir)/24x24/$(context)
