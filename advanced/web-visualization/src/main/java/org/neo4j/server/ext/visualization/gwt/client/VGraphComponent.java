package org.neo4j.server.ext.visualization.gwt.client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.vaadin.gwtgraphics.client.DrawingArea;
import org.vaadin.gwtgraphics.client.Line;
import org.vaadin.gwtgraphics.client.VectorObject;

import com.google.gwt.dom.client.Style;
import com.google.gwt.dom.client.Style.Position;
import com.google.gwt.dom.client.Style.Unit;
import com.google.gwt.event.dom.client.ClickEvent;
import com.google.gwt.event.dom.client.ClickHandler;
import com.google.gwt.user.client.Element;
import com.google.gwt.user.client.Random;
import com.google.gwt.user.client.ui.AbsolutePanel;
import com.google.gwt.user.client.ui.Composite;
import com.google.gwt.user.client.ui.HTML;
import com.google.gwt.user.client.ui.Panel;
import com.google.gwt.user.client.ui.RootPanel;
import com.google.gwt.user.client.ui.Widget;
import com.vaadin.terminal.gwt.client.ApplicationConnection;
import com.vaadin.terminal.gwt.client.Paintable;
import com.vaadin.terminal.gwt.client.UIDL;
import com.vaadin.terminal.gwt.client.Util;

public class VGraphComponent extends Composite implements Paintable,
        ClickHandler {

    /** Set the CSS class name to allow styling. */
    public static final String CLASSNAME = "v-mycomponent";

    private static final int NODE_SIZE = 30;

    /** The client side widget identifier */
    protected String paintableId;

    /** Reference to the server connection object. */
    ApplicationConnection client;

    private Panel root = new AbsolutePanel();
    private DrawingArea canvas = new DrawingArea(0, 0);
    private Set<Widget> nodes = new HashSet<Widget>();
    private Map<Widget, Set<Relationship>> edgesFrom = new HashMap<Widget, Set<Relationship>>();
    private Map<Widget, Set<Relationship>> edgesTo = new HashMap<Widget, Set<Relationship>>();

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
        constrainNodes();
    }

    public void setHeight(String height) {
        Util.setHeightExcludingPaddingAndBorder(this, height, 0);
        canvas.setHeight(getOffsetHeight());
        constrainNodes();
    }

    private Widget createRandomNode() {
        return createNode(Math.random() * canvas.getWidth(), Math.random()
                * canvas.getHeight());
    }

    private Widget createNode(double x, double y) {
        HTML node = new HTML("<div style='text-align:center'>Node "
                + nodes.size() + "</div>");
        Style style = node.getElement().getStyle();
        style.setPosition(Position.ABSOLUTE);
        style.setLeft(x, Unit.PX);
        style.setTop(y, Unit.PX);
        style.setWidth(NODE_SIZE, Unit.PX);
        style.setHeight(NODE_SIZE, Unit.PX);
        style.setBackgroundColor("lightblue");
        style.setFontSize(12, Unit.PX);
        root.add(node);
        nodes.add(node);
        new NodeHandler(node, this);
        return node;
    }

    public void onClick(ClickEvent event) {
        System.out.println("Click " + event.getNativeButton());
        if (event.isShiftKeyDown()) {
            Widget[] list = nodes.toArray(new Widget[nodes.size()]);
            Widget other = list[Random.nextInt(list.length)];
            Widget node = createNode(event.getRelativeX(canvas.getElement()),
                    event.getRelativeY(canvas.getElement()));
            int choice = Random.nextInt(3);
            if (choice == 0 || choice == 2) {
                createRelationship(node, other);
            } 
            if (choice == 1 || choice == 2) {
                createRelationship(other, node);
            }
            event.preventDefault();
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

        Widget node1 = createRandomNode();
        Widget node2 = createRandomNode();

        createRelationship(node1, node2);
        createRelationship(node2, node1);
    }

    private Relationship createRelationship(Widget node1, Widget node2) {
        String type = new String[] { "LIKES", "EATS", "FEARS" }[Random
                .nextInt(3)];
        Relationship edge = new Relationship(this, node1, node2, type);

        mapRelationship(node1, edge, edgesFrom);
        mapRelationship(node2, edge, edgesTo);
        return edge;
    }

    private static void mapRelationship(Widget node, Relationship edge,
            Map<Widget, Set<Relationship>> map) {
        Set<Relationship> edges = map.get(node);
        if (edges == null) {
            edges = new HashSet<Relationship>();
            map.put(node, edges);
        }
        edges.add(edge);
    }

    void updateRelationshipsFor(Widget node) {
        update(edgesFrom.get(node));
        update(edgesTo.get(node));
    }

    private void update(Set<Relationship> edges) {
        if (edges != null) {
            for (Relationship edge : edges) {
                edge.update();
            }
        }
    }

    public void constrainNodes() {
        for (Widget node : nodes) {
            Element element = node.getElement();
            reposition(node, element.getOffsetLeft(), element.getOffsetTop());
        }
    }

    void reposition(Widget node, int newX, int newY) {
        Style style = node.getElement().getStyle();
        style.setLeft(limit(0, newX, getOffsetWidth() - node.getOffsetWidth()),
                Unit.PX);
        style.setTop(
                limit(0, newY, getOffsetHeight() - node.getOffsetHeight()),
                Unit.PX);
        updateRelationshipsFor(node);
    }

    private static int limit(int min, int value, int max) {
        return Math.min(Math.max(min, value), max);
    }

    void add(HTML widget) {
        root.add(widget);
    }

    void add(VectorObject widget) {
        canvas.add(widget);
    }
}
