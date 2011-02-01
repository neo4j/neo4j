
size = 22x22
context = devices

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =				\
	audio-card.png			\
	audio-input-microphone.png	\
	battery.png			\
	camera-photo.png		\
	camera-video.png		\
	computer.png			\
	drive-optical.png		\
	drive-harddisk.png		\
	drive-removable-media.png	\
	input-gaming.png		\
	input-keyboard.png		\
	input-mouse.png			\
	media-optical.png		\
	media-floppy.png		\
	media-flash.png			\
	multimedia-player.png		\
	network-wired.png		\
	network-wireless.png		\
	printer.png			\
	video-display.png

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
