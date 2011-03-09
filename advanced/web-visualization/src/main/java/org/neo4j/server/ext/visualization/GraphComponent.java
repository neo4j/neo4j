package org.neo4j.server.ext.visualization;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.ext.visualization.graph.VisualGraphDatabase;
import org.neo4j.server.ext.visualization.gwt.client.VGraphComponent;

import com.vaadin.terminal.PaintException;
import com.vaadin.terminal.PaintTarget;
import com.vaadin.ui.AbstractComponent;
import com.vaadin.ui.ClientWidget;

/**
 * Server side component for the VGraphComponent widget.
 */
@ClientWidget(VGraphComponent.class)
public class GraphComponent extends AbstractComponent {

    private VisualGraphDatabase visualGraph;

    public GraphComponent(GraphDatabaseService graphDb) {
        this.visualGraph = new VisualGraphDatabase(graphDb);
        setWidth("100%");
        setHeight("100%");
    }

    @Override
    public void paintContent(PaintTarget target) throws PaintException {
        super.paintContent(target);

        // TODO Paint any component specific content by setting attributes
        // These attributes can be read in updateFromUIDL in the widget.
    }

}
