define([ "flight/lib/component", "flight/lib/utils", "slickgrid", "moment" ], //
function(defineComponent, utils, Slick, moment) {
	return defineComponent(function() {
		this.attributes({
			item : null,
			properties : "auto",
			initialData : {}
		});

		this.after('initialize', function() {
			this.on("grid-setDomain", this.updateDomain);
			this.on("grid-setProperties", this.updateProperties);

			var self = this;

			this.item = this.attr.item;
			this.properties = this.attr.properties;
			this.autoProperties = this.properties == "auto";
			this.domain = {
				from : undefined,
				to : undefined
			};

            var that = this;
			this.createColumns = {
                create: function(){
                    var columns = that.properties.map(function(p) {
                        return {
                            name : $.uri(p).localPart(),
                            field : p,
                            width : 150
                        };
                    });
                    columns.unshift({
                        name : "datetime",
                        field : "lf:datetime",
                        width : 200,
                        formatter : function(row, cell, value, columnDef, dataContext) {
                            if (value == null) {
                                return "";
                            } else {
                                return moment(value).format('lll');
                            }
                        }
                    });
                    return columns;
                }
            }

			var columns = this.createColumns.create();

			var options = {
				rowHeight : 30,
				editable : false,
				enableAddRow : false,
				enableCellNavigation : false,
				enableColumnReorder : false
			};

			this.gridData = [];

			this.grid = new Slick.Grid(this.node, this.gridData, columns, options);
			this.grid.onScroll.subscribe(function(e, args) {
				this.triggerViewUpdated();
			}.bind(this));

			this.reloader = {
				currentReload : null,
				reloadRange : function(from, to) {
					if (this.currentReload) {
						this.currentReload.cancel();
					}
					var canceled = false;
					var blockSize = 5000;
					var fetchedData = {};

					var limit = undefined;
					if (from == undefined || to == undefined) {
						limit = blockSize * 2;
					}

					var fetchItems = [ self.item ];
					var fetchProperties = self.properties;

					function loadSlices() {
						var result = self.loadData(fetchItems, fetchProperties, from, to, blockSize);
						$.when(result).then(function(loadedData) {
							to = 0;
							var fetchMoreItems = [];
							$.each(fetchItems, function(index1, item) {
								var newItemData = loadedData[item];
								if (!newItemData) {
									return;
								}
								fetchedData[item] = fetchedData[item] || {};
								var loadOlderItemData = false;
								$.each(fetchProperties, function(index2, property) {
									var newPropertyData = newItemData[property];
									if (!newPropertyData) {
										return;
									}

									var fetchedPropertyData = fetchedData[item][property] || [];
									if (newPropertyData.length == blockSize) {
										// fetch until time (to - 1)
										to = Math.max(newPropertyData[0].time - 1, to);
										loadOlderItemData = limit == undefined || fetchedPropertyData.length + newPropertyData.length < limit;
									}

									fetchedData[item][property] = newPropertyData.map(function(d) {
										return [ d.time, d.value ];
									}).concat(fetchedPropertyData);
								});
								if (loadOlderItemData) {
									fetchMoreItems.push(item);
								}
							});

							if (!canceled) {
								// TODO update grid data
								if (fetchMoreItems.length > 0) {
									fetchItems = fetchMoreItems;
									loadSlices();
								} else {
									self.updateGrid(fetchedData);
								}
							}
						});
					}
					loadSlices();
					this.currentReload = {
						cancel : function() {
							canceled = true;
						}
					};
				},
				start : function() {
					// no data-loader mixin registered
					if (!self.loadData) {
						return;
					}
					this.reloadRange(self.domain.from, self.domain.to);
				}
			};
			this.reloader.start();
		});

		this.updateGrid = function(data) {
			var dataByTime = {};
			var itemData = data[this.item];
			Object.keys(itemData).forEach(function(property) {
				var propertyData = itemData[property];
				propertyData.forEach(function(row) {
					var time = row[0];
					var value = row[1];

					dataByTime[time] = dataByTime[time] || {
						'lf:datetime' : time
					};
					dataByTime[time][property] = JSON.stringify(value);
				});
			});
			var self = this;
			this.gridData.length = 0;
			Object.keys(dataByTime).forEach(function(t) {
				self.gridData.push(dataByTime[t]);
			});
			this.gridData.sort(function(a, b) {
				// sort descending
				return b['lf:datetime'] - a['lf:datetime'];
			});
			if (this.gridData.length) {
				this.domain = {
					from : this.gridData[this.gridData.length - 1]['lf:datetime'],
					to : this.gridData[0]['lf:datetime']
				};
				this.trigger("grid-domain-updated", this.domain);
			} else {
				this.domain = {
					from : undefined,
					to : undefined
				};
			}
			this.grid.invalidate();
			this.triggerViewUpdated();
		}

		/**
		 * Updates the domain of the grid.
		 * 
		 * @param data -
		 *            The new domain data in the form of { from: ..., to: ... }
		 *            where 'to' is optional.
		 */
		this.updateDomain = function(evt, data) {
			var from = data.from;
			var to = data.to;
			var domain = this.domain;
			if (to === undefined) {
				to = domain.to;
			}
			if (from === undefined) {
				from = domain.from;
			}
			this.domain = {
				from : from,
				to : to
			};
			this.reloader.start();
		};

		/**
		 * Updates the property columns of the grid.
		 * 
		 * @param data -
		 *            The new properties as an object { properties: [prop1,
		 *            prop2, ...] }.
		 */
		this.updateProperties = function(evt, data) {
			console.log("update " + data);
			this.properties = data.properties || [];
			this.reloader.start();
			this.grid.setColumns(this.createColumns.create());
		};

		this.triggerViewUpdated = function() {
			var viewport = this.grid.getViewport();
			this.trigger("grid-view-updated", {
				first : viewport.top,
				// use Math.min since bottom is sometimes greater then length
				last : Math.min(viewport.bottom, this.gridData.length - 1),
				itemCount : this.gridData.length
			});
		};
	});
});