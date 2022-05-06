define([ "flight/lib/component", "flight/lib/utils", "d3", "simplify" ], function(defineComponent, utils, d3, simplify) {
	return defineComponent(function() {
		this.attributes({
			items : "auto",
			properties : "auto",
			initialData : {},
			width : 0,
			height : 0,
			animated : true
		});

		this.mapValue = function(value) {
			if (typeof value === "boolean") {
				return value ? 1 : 0;
			} else if (typeof value === "object") {
				return 0;
			}
			return value;
		};

		this.after('initialize', function() {
			var self = this;

			this.items = this.attr.items;
			this.autoItems = this.items == "auto";
			this.properties = this.attr.properties;
			this.autoProperties = this.properties == "auto";

			if (this.autoItems) {
				self.items = Object.keys(this.attr.initialData);
			}
			this.data = {};
			$.each(self.items, function(index1, item) {
				var newData = {};
				self.data[item] = newData;
				var itemData = self.attr.initialData[item] || {};
				$.each(self.autoProperties ? Object.keys(itemData) : self.properties, function(index2, property) {
					var propertyData = itemData[property] || [];
					propertyData.reverse();
					newData[property] = propertyData.map(function(d) {
						return [ d.time, self.mapValue(d.value) ];
					});
				});
			});

			var width = this.attr.width ? this.attr.width : this.$node.width();
			var height = this.attr.height ? this.attr.height : this.$node.height();
			if (height == 0) {
				height = 3 / 4 * width;
			}
			var margin = {
				top : 20,
				right : 20,
				bottom : 40,
				left : 40
			};
			width = width - margin.left - margin.right;
			height = height - margin.top - margin.bottom;

			var maxTime = Number.MIN_VALUE;
			var hasData = false;
			$.each(self.data, function(item, itemData) {
				$.each(itemData, function(property, propertyData) {
					if (propertyData.length) {
						maxTime = Math.max(propertyData[propertyData.length - 1][0], maxTime);
						hasData = true;
					}
				});
			});
			
			var n = 100, duration = 1000, now = Date.now();
			var animated = this.attr.animated;
			
			var xBegin = now - (n - 2) * duration;
			var xEnd = now + 4 * duration;
			var lastVisible = xEnd - (xEnd - xBegin) / 2;
			if (hasData) {
				if (maxTime <= lastVisible) {
					animated = false;
				}
				now = maxTime;
				xBegin = now - (n - 2) * duration;
				xEnd = now + 4 * duration;
			}
			var x = d3.scaleTime().domain([ xBegin, xEnd ]).range([ 0, width ]);
			// the current transform of the x axis
			var xTransform = null;
			// the zoomed x axis
			var xz = x.copy();

			xz.assign = other => {
				xz.domain(other.domain());
				xz.range(other.range());
			};
			var y = d3.scaleLinear().domain([ -1, 1 ]).range([ height, 0 ]);

			var xAxis = d3.axisBottom().scale(xz);
			var yAxis = d3.axisLeft().scale(y);

			var widget = d3.select(self.node);
			var fullWidth = width + margin.left + margin.right;
			var fullHeight = height + margin.top + margin.bottom;
			var aspect = width / height;

			var svg = widget.append("svg").attr("width", fullWidth).attr("height", fullHeight);
			svg.attr("viewBox", "0 0 " + fullWidth + " " + fullHeight).attr("preserveAspectRatio", "xMidYMid");
			function resize() {
				var targetWidth = $(svg.node()).parent().width();
				svg.attr("width", targetWidth);
				svg.attr("height", targetWidth / aspect);
			}
			this.on(window, "resize", resize);
			resize();

			var group = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

			group.append("defs").append("clipPath").attr("id", "clip").append("rect").attr("width", width).attr("height", height);
			var xAxisSvg = group.append("g").attr("class", "x axis").attr("transform", "translate(0," + height + ")").call(xAxis);
			var yAxisSvg = group.append("g").attr("class", "y axis").call(yAxis);
			var paths = group.append("g").attr("clip-path", "url(#clip)");

			var line = d3.line().x(function(d, i) {
				return d.x;
			}).y(function(d, i) {
				return d.y;
			});
			updateYDomain(false);

			var autoUpdate = true;
			var reloader = {
				currentReload : null,
				reloadRange : function(from, to) {
					if (this.currentReload) {
						this.currentReload.cancel();
					}
					var canceled = false;
					var blockSize = 5000;
					var fetchedData = {};

					var allProperties = {};
					var fetchItems = Object.keys(self.data);
					$.each(fetchItems, function(index1, item) {
						var itemData = self.data[item] || {};
						$.each(Object.keys(itemData), function(index2, property) {
							allProperties[property] = true;
						});
					});
					var fetchProperties = Object.keys(allProperties);
					function loadBlocks() {
						var result = self.loadData(fetchItems, fetchProperties, from, to, blockSize);
						$.when(result).then(function(loadedData) {
							to = 0;
							var loadOlderData = false;
							$.each(fetchItems, function(index1, item) {
								var newItemData = loadedData[item];
								if (!newItemData) {
									return;
								}
								fetchedData[item] = fetchedData[item] || {};
								$.each(fetchProperties, function(index2, property) {
									var newPropertyData = newItemData[property];
									if (!newPropertyData) {
										return;
									}

									if (newPropertyData.length == blockSize) {
										// fetch until time (to - 1)
										to = Math.max(newPropertyData[0].time - 1, to);
										loadOlderData = true;
									}

									var fetchedPropertyData = fetchedData[item][property] || [];
									fetchedData[item][property] = newPropertyData.map(function(d) {
										return [ d.time, self.mapValue(d.value) ];
									}).concat(fetchedPropertyData);

									// TODO merge
									self.data[item] = fetchedData[item];
								});
							});

							if (!canceled) {
								if (loadOlderData) {
									updatePaths(false);
									loadBlocks();
								} else {
									// do this only once to avoid flickering due
									// to changing y-positions
									updateYDomain(false);
								}
							}
						});
					}
					loadBlocks();
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
					var from = xz.invert(0).getTime();
					var to = xz.invert(width).getTime();
					this.reloadRange(from, to);
				}
			};
			var zoom = d3.zoom().scaleExtent([ 0.001, 10 ]).on('zoom', function(e) {
				autoUpdate = false;

				// created transformed x-scale and copy result to xz
				xTransform = e.transform;
				xz.assign(xTransform.rescaleX(x));

				xAxisSvg.call(xAxis);
				updatePaths(false);

				var from = xz.invert(0).getTime();
				var to = xz.invert(width).getTime();
				// if (data && data.length > 0 && data[0][0] <= from &&
				// data[data.length - 1][0] >= to) {
				// no reloading of data required
				// TODO always cache already loaded data
				// return;
				// }
				self.trigger("graph-domain-updated", {
					from : from,
					to : to
				});
				reloader.start();
			}) ;
			widget.call(zoom);

			function updatePaths(slide) {
				function identity(p) {
					return p;
				}

				// project and simplify line
				function preparePoints(points) {
					// compute projected points
					var projectedPoints = points.map(function(v) {
						return {
							x : xz(v[0]),
							y : y(v[1])
						}
					});
					// use tolerance in px to simplify the line
					var simplified = simplify(projectedPoints, 3);
					return simplified;
				}

				function createPath(target, item, property) {
					var points = preparePoints(self.data[item][property]);

					var path = target.append("path").attr("class", "line").attr("data-item", item).attr("data-property", property).datum(points).attr("d", line);
					self.trigger("graph-path-created", {
						item : item,
						property : property,
						path : path.node()
					});
				}

				function updateLines(item) {
					var itemData = self.data[item];
					var lines = d3.select(this).selectAll("g").data(Object.keys(itemData), identity);
					lines.enter().append("g").each(function(property) {
						createPath(d3.select(this), item, property);
					});
					lines.exit().remove();
					d3.select(this).selectAll("g").each(function(property) {
						// redraw the line
						var path = d3.select(this).select("path");
						path.datum(preparePoints(self.data[item][property])).attr("d", line);
						// slide it to the left
						if (slide) {
							path.attr("transform", null).transition().duration(duration).ease(d3.easeLinear) //
							.attr("transform", "translate(" + xz(now - (n - 1) * duration) + ",0)");
						}
					});
				}

				var itemPaths = paths.selectAll("g").data(Object.keys(self.data), identity);
				itemPaths.enter().append("g").each(updateLines);
				itemPaths.exit().remove();
				itemPaths.each(updateLines);
			}

			function updateYDomain(slide) {
				var min = Number.POSITIVE_INFINITY;
				var max = Number.NEGATIVE_INFINITY;
				var doUpdate = false;
				$.each(self.data, function(item, itemData) {
					$.each(itemData, function(property, propertyData) {
						if (propertyData.length > 0) {
							min = Math.min(d3.min(propertyData, function(d) {
								return d[1];
							}), min);
							max = Math.max(d3.max(propertyData, function(d) {
								return d[1];
							}), max);
							doUpdate = true;
						}
					});
				});
				if (doUpdate) {
					var padding = Math.abs(y.invert(0) - y.invert(10));
					y.domain([ min - padding, max + padding ]);

					// update the y-axis
					// yAxisSvg.transition().duration(duration).ease("linear").call(yAxis);
					yAxisSvg.call(yAxis);
					updatePaths(slide);
				}
			}

			if (animated) {
				tick();
			}
			
			var lastTick = undefined;
			function tick() {
				if (!autoUpdate) {
					return;
				}

				var diffToLastTick = lastTick ? Date.now() - lastTick : 0;
				lastTick = Date.now();
				
				// update the domains
				now = now + diffToLastTick;
				var xBegin = now - (n - 2) * duration;
				var xEnd = now + 4 * duration;

				// ensure that at least half of the graph is visible
				// var lastVisible = now - (now - xBegin) / 2;
				// if (data.length && data[data.length - 1][0] <= lastVisible) {
				// animated = false;
				// return;
				// }
				x.domain([ xBegin, xEnd ]);
				xz.assign(xTransform ? xTransform.rescaleX(x) : x);
				updateYDomain(true);

				// slide the x-axis left
				var xAnim = xAxisSvg.transition().duration(duration).ease(d3.easeLinear);
				xAnim.call(xAxis);

				// TODO use requestAnimationFrame
				setTimeout(tick, duration);
			}

			this.on("graph-update", function(evt, newData) {
				var begin = xz.domain()[0];
				$.each(self.autoItems ? Object.keys(newData) : self.items, function(index1, item) {
					var newItemData = newData[item] || {};
					$.each(self.autoProperties ? Object.keys(newItemData) : self.properties, function(index2, property) {
						var newPropertyData = newItemData[property] || [];
						if (newPropertyData.constructor !== Array) {
							newPropertyData = [ newPropertyData ];
						}
						if (newPropertyData.length > 1 && newPropertyData[0].time > newPropertyData[1].time) {
							// reverse time-descending sorted data
							newPropertyData.reverse();
						}
						if (!autoUpdate && newPropertyData.length > 0) {
							var visibleMaxTime = xz.invert(width).getTime();
							if (newPropertyData[newPropertyData.length - 1].time > visibleMaxTime) {
								// ensure that no gaps are created between
								// loaded values
								return;
							}
						}
						if (!self.data[item]) {
							self.data[item] = {};
						}
						var propertyData = self.data[item][property];
						if (!propertyData) {
							propertyData = self.data[item][property] = [];
						}
						newPropertyData.forEach(function(d) {
							propertyData.push([ d.time, self.mapValue(d.value) ]);
						});

						// pop the old data points off the front
						// do this only if a data-loader mixin is registered
						if (self.loadData !== undefined) {
							while (propertyData.length && propertyData[0][0] < begin) {
								propertyData.shift();
							}
						}
					});
				});
				if (!autoUpdate) {
					updateYDomain(false);
				} else if (!animated) {
					tick();
				}
			});

			this.on("graph-clear", function(evt, newData) {
				self.data = {};
				//hide graph
				this.node.style.visibility = "hidden";
				// simply remove all paths
				updatePaths(false);
			});

			var widget = self.$node.closest(".widget");
			this.on(widget, "graph-setItems", function(evt, data) {
				self.items = data.items;
				reloader.start();
			});

			this.on(widget, "graph-setProperties", function(evt, data) {
                if (this.node.style.visibility == "hidden") {
                    this.node.style.visibility = "visible";
                }
		        self.properties = data.properties;
                var newData = {};
                $.each(Object.keys(self.data), function(index1, item) {
                    newData[item] = {};
                    $.each(self.properties, function(index2, property) {
                        newData[item][property] = [];
                    });
                });
                self.data = newData;
                reloader.start();
			});

			/**
			 * Updates the domain of the graph's x-axis.
			 * 
			 * @param data -
			 *            The new domain data in the form of { from: ..., to:
			 *            ... } where 'to' is optional.
			 */
			this.on(widget, "graph-setDomain", function(evt, data) {
				var from = data.from;
				var to = data.to;
				autoUpdate = false;
				var xDomain = x.domain();
				if (to === undefined) {
					to = from + (xDomain[1] - xDomain[0]);
				}
				if (from === undefined) {
					from = to - (xDomain[1] - xDomain[0]);
				}
				x.domain([ from, to ]);
				xz.assign(xTransform ? xTransform.rescaleX(x) : x);

				xAxisSvg.call(xAxis);
				updateYDomain(false);
				reloader.start();
			});
		});
	});
});