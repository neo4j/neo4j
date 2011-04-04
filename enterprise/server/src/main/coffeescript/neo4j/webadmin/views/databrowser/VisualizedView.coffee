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


      initialize : (options)->

        @server = options.server
        @appState = options.appState
        @settings = @appState.getVisualizationSettings()
        @dataModel = options.dataModel

        @settings.bind("change", @settingsChanged)

      render : =>

        if @vizEl? then @getViz().detach()
        
        $(@el).html(template())

        @vizEl = $("#visualization", @el)
        @getViz().attach(@vizEl)

        switch @dataModel.get("type")
          when "node"
            node = @dataModel.getData().getItem()
            @getViz().setNode(node)

      settingsChanged : () =>
        if @viz?
          @viz.getNodeStyler().setLabelProperties(@settings.getLabelProperties())
      
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


      remove : =>
        @dataModel.unbind("change:data", @render)
        @getViz().stop()
        super()

      detach : =>
        @dataModel.unbind("change:data", @render)
        @getViz().stop()
        super()

      attach : (parent) =>
        super(parent)
        if @vizEl?
          @getViz().start()
        @dataModel.bind("change:data", @render)

)
