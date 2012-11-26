# Allocation Tracker

A fork of the java-allocation-instrumenter project from google code. This version contains some improvements
to avoid class loading errors related to instrumenting native classes.

This project contains a java agent that rewrites all classes on the classpath, such that it can track allocation
of all of them.

## How to use this

When the project builds, it produces a jarfile with the classifier "premain". This jar file contains all dependencies
for the project, as well as the correct jar manifests for the JVM to load it. It is hard coded into the jar that
the JVM will be able to find it by looking for "./allocation.jar", which means that you need to rename the jar,
and put it in the root folder where you will be running your jvm from.

If you are building the performance-framework project, build this with mvn package, and then rename and move the
resulting jar into performance-framework/src/main/resources for it to be picked up.