###
Copyright (c) 2002-2018 "Neo Technology,"
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
  ['./base',
   './DashboardInfoView',
   './DashboardChartsView',
   'ribcage/View',
   'lib/amd/jQuery'],
  (template, DashboardInfoView, DashboardChartsView, View, $) ->

    class DashboardView extends View

      template : template

      initialize : (@opts) =>
        @appState = opts.state
        @server = @appState.getServer()
        @kernelBean = opts.kernelBean

        @kernelBean.bind "change", @render

      render : =>
        kernelVersion = if @kernelBean.get("kernel")? then @kernelBean.get("kernel").KernelVersion else "N/A"
        $(@el).html @template(
          server : { url : @server.url, version : kernelVersion } )

        if @infoView?
          @infoView.remove()
        if @chartsView?
          @chartsView.remove()

        @infoView = new DashboardInfoView(@opts)
        @chartsView = new DashboardChartsView(@opts)
        @infoView.attach $("#dashboard-info", @el)
        @chartsView.attach $("#dashboard-charts", @el)
        @infoView.render()
        @chartsView.render()

        return this

      remove : =>
        @kernelBean.unbind("change",@render)
        @infoView.remove()
        @chartsView.remove()
        super()

      detach : =>
        @chartsView.unbind()
        super()

)
