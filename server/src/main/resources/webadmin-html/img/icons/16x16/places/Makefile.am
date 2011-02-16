
size = 16x16
context = places

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =					\
	folder.png				\
	folder-remote.png			\
	folder-saved-search.png	\
	network-server.png			\
	network-workgroup.png			\
	start-here.png	\
	user-desktop.png			\
	user-home.png				\
	user-trash.png

EXTRA_DIST =					\
	$(icons_DATA)

install-data-local: install-iconsDATA
	(cd $(DESTDIR)$(themedir)/$(size) && $(ICONMAP) -c $(context))
