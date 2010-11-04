/**
 * Event handler for webadmin.
 */
wa.events = new neo4j.Events();

/**
 * Quick access to {@link wa.events#trigger}
 */
wa.trigger = neo4j.proxy(wa.events.trigger, wa.events);

/**
 * Quick access to {@link wa.events#bind}
 */
wa.bind = neo4j.proxy(wa.events.bind, wa.events);