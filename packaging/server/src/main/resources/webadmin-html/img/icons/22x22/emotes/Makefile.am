
size = 22x22
context = emotes

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =			\
	face-angel.png		\
	face-crying.png		\
	face-devilish.png	\
	face-glasses.png	\
	face-grin.png		\
	face-kiss.png		\
	face-monkey.png	\
	face-plain.png		\
	face-sad.png		\
	face-smile.png		\
	face-smile-big.png	\
	face-surprise.png	\
	face-wink.png

EXTRA_DIST =			\
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
