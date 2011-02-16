
size = 32x32
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
