
size = 16x16
context = status

iconsdir = $(themedir)/$(size)/$(context)

icons_DATA =				\
	audio-volume-high.png		\
	audio-volume-low.png		\
	audio-volume-medium.png		\
	audio-volume-muted.png		\
	battery-caution.png		\
	dialog-error.png		\
	dialog-information.png		\
	dialog-warning.png		\
	folder-drag-accept.png		\
	folder-open.png			\
	folder-visiting.png		\
	image-loading.png		\
	image-missing.png		\
	mail-attachment.png 		\
	network-error.png		\
	network-idle.png		\
	network-offline.png		\
	network-receive.png		\
	network-transmit.png		\
	network-transmit-receive.png	\
	network-wireless-encrypted.png	\
	printer-error.png		\
	software-update-available.png	\
	software-update-urgent.png	\
	user-trash-full.png		\
	weather-clear.png		\
	weather-clear-night.png		\
	weather-few-clouds.png		\
	weather-few-clouds-night.png	\
	weather-overcast.png		\
	weather-severe-alert.png	\
	weather-showers.png		\
	weather-showers-scattered.png	\
	weather-snow.png		\
	weather-storm.png

EXTRA_DIST =				\
	$(icons_DATA)

install-data-local: install-iconsDATA
	(cd $(DESTDIR)$(themedir)/$(size) && $(ICONMAP) -c $(context))
