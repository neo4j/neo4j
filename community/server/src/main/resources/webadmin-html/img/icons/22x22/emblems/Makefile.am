
size = 22x22
context = emblems

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =				\
	emblem-favorite.png		\
	emblem-important.png		\
	emblem-photos.png		\
	emblem-readonly.png		\
	emblem-symbolic-link.png	\
	emblem-system.png		\
	emblem-unreadable.png

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
