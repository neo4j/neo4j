###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['neo4j/webadmin/utils/ItemUrlResolver'
   'ribcage/ui/Dialog'
   'ribcage/ui/FilterList'
   './nodeFilterDialogTemplate'
   'lib/backbone'], 
  (ItemUrlResolver, Dialog, FilterList, template) ->
  
    class NodeFilterDialog extends Dialog
      
      events : 
        'dblclick .selectWrap' : 'w'
        'click .complete'  : 'complete'
        'click .selectAll' : 'selectAll'
        'click .cancel'    : 'cancel'

      w : ->
        alert "BLURG!"

      initialize : (opts) ->
        @completeCallback = opts.completeCallback
        @urlResolver = new ItemUrlResolver()

        labelProperties = opts.labelProperties or []

        @nodes = opts.nodes
        filterableItems = for node in @nodes
          id = @urlResolver.extractNodeId(node.getSelf())
          label = id
          for labelProp in labelProperties
            if node.hasProperty(labelProp)
              label = "#{id}: " + JSON.stringify node.getProperty(labelProp)
          { node : node, key : node.getSelf(), label : label }

        @filterList = new FilterList(filterableItems)
        super()

      render : () ->
        $(@el).html(template())
        @filterList.attach $(".filter", @el)
        @filterList.render()

        wrapHeight = $(@el).height()
        @filterList.height(wrapHeight - 80)
        

      wrapperClicked : (ev) =>
        if ev.originalTarget is ev.currentTarget
          @cancel()

      complete : () =>
        nodes = for item in @filterList.getFilteredItems()
          item.node
        @completeCallback(nodes, this)

      selectAll : () ->
        @completeCallback(@nodes, this)

      cancel : () ->
        @completeCallback([], this)


)
