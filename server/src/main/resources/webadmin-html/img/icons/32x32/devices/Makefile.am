
size = 32x32
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
