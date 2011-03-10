package org.neo4j.server.ext.visualization.gwt.client;

import java.util.HashSet;
import java.util.Set;

import org.vaadin.gwtgraphics.client.DrawingArea;
import org.vaadin.gwtgraphics.client.VectorObject;

import com.google.gwt.dom.client.Style;
import com.google.gwt.dom.client.Style.Position;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Random;
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.RootPanel;
import com.vaadin.terminal.gwt.client.ApplicationConnection;
import com.vaadin.terminal.gwt.client.Paintable;
import com.vaadin.terminal.gwt.client.UIDL;
import com.vaadin.terminal.gwt.client.Util;

public class VGraphComponent extends Composite implements Paintable,
        ClickHandler {

    /** Set the CSS class name to allow styling. */
    public static final String CLASSNAME = "v-mycomponent";

    /** The client side widget identifier */
    protected String paintableId;

    /** Reference to the server connection object. */
    ApplicationConnection client;

    private final Panel root = new AbsolutePanel();
    private final DrawingArea canvas = new DrawingArea(0, 0);
    private final Set<Node> nodes = new HashSet<Node>();

    /**
     * The constructor should first call super() to initialize the component and
     * then handle any initialization relevant to Vaadin.
     */
    public VGraphComponent() {
        initWidget(root);
        RootPanel.getBodyElement().getStyle().setBackgroundColor("green");
        Style canvasStyle = canvas.getElement().getStyle();
        canvasStyle.setPosition(Position.ABSOLUTE);
        canvasStyle.setBackgroundColor("white");
        root.add(canvas);
        root.getElement().getStyle().setPosition(Position.ABSOLUTE);
        canvas.addClickHandler(this);

        // This method call of the Paintable interface sets the component
        // style name in DOM tree
        setStyleName(CLASSNAME);
    }

    public void setWidth(String width) {
        Util.setWidthExcludingPaddingAndBorder(this, width, 0);
        canvas.setWidth(getOffsetWidth());
        updateNodes();
    }

    public void setHeight(String height) {
        Util.setHeightExcludingPaddingAndBorder(this, height, 0);
        canvas.setHeight(getOffsetHeight());
        updateNodes();
    }

    private Node createRandomNode() {
        return createNode(Random.nextDouble() * canvas.getWidth(),
                Random.nextDouble() * canvas.getHeight());
    }

    private Node createNode(double x, double y) {
        Node node = new Node(this, "Node " + nodes.size(), x, y);
        nodes.add(node);
        return node;
    }

    public void onClick(ClickEvent event) {
        System.out.println("Click " + event.getNativeButton());
        if (event.isShiftKeyDown()) {
            Node[] list = nodes.toArray(new Node[nodes.size()]);
            Node other = list[Random.nextInt(list.length)];
            Node node = createNode(event.getRelativeX(canvas.getElement()),
                    event.getRelativeY(canvas.getElement()));
            createRandomRelationships(node, other);
            event.preventDefault();
        }
    }

    private void createRandomRelationships(Node node1, Node node2) {
        int choice = Random.nextInt(3);
        if (choice == 0 || choice == 2) {
            createRelationship(node1, node2);
        }
        if (choice == 1 || choice == 2) {
            createRelationship(node2, node1);
        }
    }

    /**
     * Called whenever an update is received from the server
     */
    public void updateFromUIDL(UIDL uidl, ApplicationConnection client) {
        // This call should be made first.
        // It handles sizes, captions, tooltips, etc. automatically.
        if (client.updateComponent(this, uidl, true)) {
            // If client.updateComponent returns true there has been no changes
            // and we
            // do not need to update anything.
            return;
        }

        // Save reference to server connection object to be able to send
        // user interaction later
        this.client = client;

        // Save the client side identifier (paintable id) for the widget
        paintableId = uidl.getId();

        Node node1 = createRandomNode();
        Node node2 = createRandomNode();

        createRelationship(node1, node2);
        createRelationship(node2, node1);
    }

    private Relationship createRelationship(Node from, Node to) {
        String type = new String[] { "LIKES", "EATS", "FEARS" }[Random
                .nextInt(3)];
        Relationship edge = new Relationship(this, from, to, type);

        from.addOutgoing(edge);
        to.addIncoming(edge);
        return edge;
    }

    public void updateNodes() {
        for (Node node : nodes) {
            node.update();
        }
    }

    void add(HTML widget) {
        root.add(widget);
    }

    void add(VectorObject widget) {
        canvas.add(widget);
    }

    void addNeighborTo(Node node) {
        Node other = createRandomNode();
        createRandomRelationships(node, other);
    }
}
