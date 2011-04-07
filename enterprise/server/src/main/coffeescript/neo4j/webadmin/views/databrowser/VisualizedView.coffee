###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/visualization/VisualGraph'
   'neo4j/webadmin/data/ItemUrlResolver'
   'neo4j/webadmin/views/databrowser/VisualizationSettingsDialog'
   'neo4j/webadmin/views/View'
   'neo4j/webadmin/security/HtmlEscaper'
   'neo4j/webadmin/templates/databrowser/visualization'
   'lib/backbone'], 
  (VisualGraph, ItemUrlResolver, VisualizationSettingsDialog, View, HtmlEscaper, template) ->

    class VisualizedView extends View

      events : 
        'click #visualization-show-settings' : "showSettingsDialog"
        'click #visualization-reflow' : "reflowGraphLayout"


      initialize : (options)->
        @server = options.server
        @appState = options.appState
        @dataModel = options.dataModel
      
        @settings = @appState.getVisualizationSettings()
        @settings.bind("change", @settingsChanged)

      render : =>
        if @browserHasRequiredFeatures()
          if @vizEl? then @getViz().detach()
          
          $(@el).html(template())

          @vizEl = $("#visualization", @el)
          @getViz().attach(@vizEl)

          switch @dataModel.get("type")
            when "node"
              @visualizeFromNode @dataModel.getData().getItem()
            when "relationship"
              @visualizeFromRelationships [@dataModel.getData().getItem()]
            when "relationshipList"
              @visualizeFromRelationships @dataModel.getData().getRawRelationships()
        else 
          @showBrowserNotSupportedMessage()

        return this

      visualizeFromNode : (node) ->
        @getViz().setNode(node)

      visualizeFromRelationships : (rels) ->

        MAX = 10 # Temporary limit, should be replaced by filtering

        nodeDownloadChecklist = {}
        nodePromises = []
        
        for i in [0...rels.length]
          rel = rels[i]
          if i >= MAX
            alert "Only showing the first ten in the set, to avoid crashing the visualization. We're working on adding filtering here!"
            break
          if not nodeDownloadChecklist[rel.getStartNodeUrl()]?
            nodeDownloadChecklist[rel.getStartNodeUrl()] = true
            nodePromises.push rel.getStartNode()

          if not nodeDownloadChecklist[rel.getEndNodeUrl()]?
            nodeDownloadChecklist[rel.getStartNodeUrl()] = true
            nodePromises.push rel.getEndNode()

        allNodes = neo4j.Promise.join.apply(this, nodePromises)
        allNodes.then (nodes) =>
          @getViz().setNodes nodes

      settingsChanged : () =>
        if @viz?
          @viz.setLabelProperties(@settings.getLabelProperties())
      
      getViz : () =>
        width = $(document).width() - 40;
        height = $(document).height() - 160;
        @viz ?= new VisualGraph(@server,width,height)
        @settingsChanged()
        return @viz


      showSettingsDialog : =>
        if @settingsDialog?
          @hideSettingsDialog()
        else
          button = $("#visualization-show-settings")
          button.addClass("selected")
          @settingsDialog = new VisualizationSettingsDialog(
            appState : @appState
            baseElement : button
            closeCallback : @hideSettingsDialog)

      hideSettingsDialog : =>
        if @settingsDialog?
          @settingsDialog.remove()
          delete(@settingsDialog)
          $("#visualization-show-settings").removeClass("selected")

      browserHasRequiredFeatures : ->
        Object.prototype.__defineGetter__?

      showBrowserNotSupportedMessage : ->
        $(@el).html("<div class='pad'>
          <h1>I currently do not support visualization in this browser :(</h1>
          <p>I can't find the __defineGetter__ API method, which the visualization lib I use, Arbor.js, needs.</p>
          <p>If you really want to use visualization (it's pretty awesome), please consider using Google Chrome, Firefox or Safari.</p>
          </div>")

      reflowGraphLayout : () =>
        if @viz != null
          @viz.reflow()

      remove : =>
        if @browserHasRequiredFeatures()
          @dataModel.unbind("change:data", @render)
          @hideSettingsDialog()
          @getViz().stop()
        super()

      detach : =>
        if @browserHasRequiredFeatures()
          @dataModel.unbind("change:data", @render)
          @hideSettingsDialog()
          @getViz().stop()
        super()

      attach : (parent) =>
        super(parent)
        if @browserHasRequiredFeatures() and @vizEl?
          @getViz().start()
          @dataModel.bind("change:data", @render)

)
