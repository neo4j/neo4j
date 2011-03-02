package org.neo4j.server.ext.visualization.gwt.client;

import com.vaadin.terminal.gwt.client.ApplicationConnection;
import com.vaadin.terminal.gwt.client.Paintable;
import com.vaadin.terminal.gwt.client.UIDL;
import com.google.gwt.dom.client.Document;
import com.google.gwt.user.client.ui.Widget;

public class VGraphComponent extends Widget implements Paintable {

    /** Set the CSS class name to allow styling. */
    public static final String CLASSNAME = "v-mycomponent";

    /** The client side widget identifier */
    protected String paintableId;

    /** Reference to the server connection object. */
    ApplicationConnection client;

    /**
     * The constructor should first call super() to initialize the component and
     * then handle any initialization relevant to Vaadin.
     */
    public VGraphComponent() {
        // TODO Example code is extending GWT Widget so it must set a root element.
        // Change to proper element or remove if extending another widget
        setElement(Document.get().createDivElement());
        
        // This method call of the Paintable interface sets the component
        // style name in DOM tree
        setStyleName(CLASSNAME);
    }

    /**
     * Called whenever an update is received from the server 
     */
    public void updateFromUIDL(UIDL uidl, ApplicationConnection client) {
        // This call should be made first. 
        // It handles sizes, captions, tooltips, etc. automatically.
        if (client.updateComponent(this, uidl, true)) {
            // If client.updateComponent returns true there has been no changes and we
            // do not need to update anything.
            return;
        }

        // Save reference to server connection object to be able to send
        // user interaction later
        this.client = client;

        // Save the client side identifier (paintable id) for the widget
        paintableId = uidl.getId();

        // TODO replace dummy code with actual component logic
        getElement().setInnerHTML("Look! A pretty graph!");
        
    }

}
