package org.neo4j.release.it.std.exec;

/**
 */
public class HelloWorldApp {

    public static void main(String[] args) {
        final String subject = ((args.length > 0) ? args[0] : "world");
        System.out.println("hello " + subject);
    }
}
