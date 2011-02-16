
size = 32x32
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
