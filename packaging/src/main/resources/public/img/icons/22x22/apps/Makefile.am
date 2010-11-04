
size = 22x22
context = apps

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =						\
	accessories-calculator.png			\
	accessories-character-map.png			\
	accessories-text-editor.png			\
	help-browser.png				\
	internet-group-chat.png				\
	internet-mail.png				\
	internet-news-reader.png			\
	internet-web-browser.png			\
	office-calendar.png				\
	preferences-desktop-accessibility.png		\
	preferences-desktop-assistive-technology.png	\
	preferences-desktop-font.png			\
	preferences-desktop-keyboard-shortcuts.png	\
	preferences-desktop-locale.png	\
	preferences-desktop-multimedia.png			\
	preferences-desktop-remote-desktop.png		\
	preferences-desktop-screensaver.png		\
	preferences-desktop-theme.png			\
	preferences-desktop-wallpaper.png		\
	preferences-system-network-proxy.png		\
	preferences-system-session.png			\
	preferences-system-windows.png			\
	system-file-manager.png				\
	system-installer.png				\
	system-software-update.png			\
	system-users.png				\
	utilities-system-monitor.png			\
	utilities-terminal.png

EXTRA_DIST =						\
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
