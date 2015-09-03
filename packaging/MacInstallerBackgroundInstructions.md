_**Adding a background to the Mac installer:**_

1 - Create a DMG using install4j.

2 - Using Disk Utilities, convert the DMG to read/write. Remember to eject it beforehand.

3 - Still using Disk Utilities, resize the DMG so it has enough space for the picture.

4 - Mount the DMG.

5 - Create a folder for the picture in the DMG, preferably hidden.

    mkdir /Volumes/<DMG>/.background

6 - Copy the background picture to the folder that was just created.

    cp <picturename> /Volumes/<DMG>/.background/<picturename>

7 - Create a symbolic link so that Neo can easily be installed by dragging it into Applications.

    ln -s /Applications "Applications"

8 - Now, open two finder windows. One showing the .background folder, the other the DMG top-level folder. Open View Options for the latter and select Picture in the Background radio box. Then, **without** changing the focus of the View Options window, drag the background picture from .background to the "drag image here" box.
![](https://s3-eu-west-1.amazonaws.com/build-service.neo4j.org/tutorial/dmgstyling.png)

9 - Adjust the icons to your liking.

10 - Copy the .DS_Store file from the DMG top-level folder and place it somewhere else. It is recommended to change the name to make it not hidden and to avoid confusion with the actual .DS_Store file for the folder you'll be placing it into.

    cp .DS_Store <wherever>/DS_Store

11 - We no longer need the DMG we've been working with. Delete **and** remove it from the trash. This is **very** important because:
>At this point, our work with the read/write DMG is finished. We should now delete it and also remove it from the Trash. If we don't do this, subsequent tests will automatically mount this DMG again. This is due to the "alias" feature in Mac OS X. The .DS_Store contains an alias to the configured background image and as long as the original DMG still exists somewhere, it will open it from the template DMG instead of from the newly generated DMG. 

12 - Open your install4j file. Under Media -> <NameOfYourMacInstaller> -> Edit Media File -> Installer Options -> Additional Files in DMG add the DS_Store file as .DS_Store, the background picture as .background/<picturename> (the name **must** be the same as the one in the read/write dmg) and the Applications symlink.
![](https://s3-eu-west-1.amazonaws.com/build-service.neo4j.org/tutorial/additionalfiles.png)

13 - Save your install4j project and create the DMG. If everything went well, it should be all fancy now =)

Reference - [Styling Of DMGs On Mac OS X](http://resources.ej-technologies.com/install4j/help/doc/index.html)
