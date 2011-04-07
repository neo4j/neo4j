
size = 22x22
context = actions

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =				\
	address-book-new.png		\
	appointment-new.png		\
	bookmark-new.png		\
	contact-new.png			\
	document-new.png		\
	document-open.png		\
	document-print.png		\
	document-print-preview.png	\
	document-properties.png		\
	document-save.png		\
	document-save-as.png		\
	edit-clear.png			\
	edit-copy.png			\
	edit-cut.png			\
	edit-delete.png	\
	edit-find.png			\
	edit-find-replace.png			\
	edit-paste.png			\
	edit-redo.png			\
	edit-select-all.png	\
	edit-undo.png			\
	folder-new.png			\
	format-indent-less.png		\
	format-indent-more.png		\
	format-justify-center.png	\
	format-justify-fill.png		\
	format-justify-left.png		\
	format-justify-right.png	\
	format-text-bold.png		\
	format-text-italic.png		\
	format-text-strikethrough.png	\
	format-text-underline.png	\
	go-bottom.png			\
	go-down.png			\
	go-first.png			\
	go-home.png			\
	go-jump.png			\
	go-last.png			\
	go-next.png			\
	go-previous.png			\
	go-top.png			\
	go-up.png			\
	list-add.png	\
	list-remove.png	\
	mail-forward.png	\
	mail-message-new.png		\
	mail-mark-junk.png	\
	mail-mark-not-junk.png	\
	mail-reply-all.png	\
	mail-reply-sender.png	\
	mail-send-receive.png	\
	media-eject.png	\
	media-playback-pause.png	\
	media-playback-start.png	\
	media-playback-stop.png		\
	media-record.png		\
	media-seek-backward.png		\
	media-seek-forward.png		\
	media-skip-backward.png		\
	media-skip-forward.png		\
	process-stop.png		\
	system-lock-screen.png		\
	system-log-out.png		\
	system-search.png		\
	system-shutdown.png		\
	tab-new.png			\
	view-fullscreen.png		\
	view-refresh.png 		\
	window-new.png	

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
