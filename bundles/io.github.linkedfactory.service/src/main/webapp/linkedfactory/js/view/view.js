$(function() {
	$("[data-tab]").each(function() {
		var self = $(this);
		self.click(function() {
			var li = self.closest("li");
			var active = li.closest("ul").find(".active");
			active.removeClass("active");
			li.addClass("active");
			$(active.find("a").attr("data-tab")).hide();
			$(self.attr("data-tab")).show();
		});
	});
});

var initialData = {};
$(document).on("stream-init", function(evt, data) {
	initialData = data;
});

$(function() {
	var item = $('.uri').text().trim();
	var itemProperties = []
	var selectedProperties = [];
	$.ajax({
		dataType : "json",
		url : "/linkedfactory/properties",
		data : {
			item : item,
		},
		success : function(properties) {
			properties = (properties || []).map(function(p) {
				return p["@id"];
			});
			itemProperties = properties;

			// console.log(item);
			// console.log(properties);

			var container = $("#graph-properties");
			container.empty();
			if (properties.length < 5) {
				var list = $(
						'<ul id="graph-properties" class="nav nav-pills"></ul>')
						.appendTo(container);
				$.each(properties, function(index, p) {
					var btn = $(
							'<li><a href="javascript:void(0)">'
									+ $.uri(p).localPart()
									+ '</a></li>').attr(
							"data-property", p);
					if (index == 0) {
						btn.addClass("active");
					}
					btn.appendTo(list);
				});
				require([ "view/navbutton" ], function(navbtn) {
					navbtn.attachTo(list.find("li"));
				});
			} else {
				var list = $('<select class="form-control"></select>')
						.appendTo(container);
				$.each(properties, function(index, p) {
					var option = $('<option>').attr("value", p).text(
							$.uri(p).localPart());
					if (index == 0) {
						option.attr("selected", true);
					}
					option.appendTo(list);
				});
				require([ "select2" ], function(select2) {
					list.select2();
					list.on("select2:select", function(e) {
						var property = list.val();
						list.trigger("graph-setProperties", {
							properties : [ property ]
						});
					});
				});
			}

			if (properties.length) {
				selectedProperties = [ properties[0] ];
				require([ "view/graph", "view/valuestore" ], function(
						graph, valuestore) {
					graph.mixin(valuestore).attachTo(
							"#graph-sensordata", {
								items : [ item ],
								initialData : initialData,
								properties : selectedProperties
							});
				});

				require([ "view/grid", "view/valuestore" ], function(
						grid, valuestore) {
					grid.mixin(valuestore).attachTo("#grid-sensordata",
							{
								item : item,
								initialData : initialData,
								properties : properties
							});
				});

				require([ "select2" ], function(select2) {
					var gridProperties = $("#grid-properties");
					$.each(properties, function(index, p) {
						var option = $('<option>').attr("value", p)
								.text($.uri(p).localPart());
						option.attr("selected", true);
						option.appendTo(gridProperties);
					});
					gridProperties.select2();
					gridProperties.on(
							"select2:select select2:unselect",
							function(e) {
								$("#grid-sensordata").trigger(
										"grid-setProperties",
										{
											properties : gridProperties
													.val()
													|| []
										});
							});
				});
			}

			$(".widget").on("graph-setProperties grid-setProperties", function(evt, data) {
				selectedProperties = data.properties;
			});
		}
	});

	$(document).on("stream-update", function(evt, data) {
		setTimeout(function() {
			$("#graph-sensordata").trigger("graph-update", data);
		}, 0);
	});
	
	var pickerFrom = $('#picker-from');
	var pickerTo = $('#picker-to');
	var ignoreDateChange = false;
	require([ "datetimepicker" ], function(picker) {
		var locale = window.navigator.userLanguage
				|| window.navigator.language;
		pickerFrom.datetimepicker({
			locale : locale
		});
		pickerTo.datetimepicker({
			locale : locale
		});
		pickerFrom.on("dp.change", function(evt) {
			if (!ignoreDateChange) {
				$("#graph-sensordata").trigger("graph-setDomain", {
					from : evt.date.toDate().getTime()
				});
				$("#grid-sensordata").trigger("grid-setDomain", {
					from : evt.date.toDate().getTime()
				});
			}
		});
		pickerTo.on("dp.change", function(evt) {
			if (!ignoreDateChange) {
				$("#graph-sensordata").trigger("graph-setDomain", {
					to : evt.date.toDate().getTime()
				});
				$("#grid-sensordata").trigger("grid-setDomain", {
					to : evt.date.toDate().getTime()
				});
			}
		});
	});
	
	pickerTo.closest(".widget").on("graph-domain-updated grid-domain-updated",
			function(evt, data) {
				ignoreDateChange = true;
				pickerFrom.data("DateTimePicker").date(new Date(data.from));
				pickerTo.data("DateTimePicker").date(new Date(data.to));
				ignoreDateChange = false;
			});
	
	$("#grid-sensordata").on("grid-view-updated", function(evt, data) {
		$("#grid-info").text((data.first + 1) + " to " + (data.last + 1) + " of " + data.itemCount + " items");
	});
	
	$(".export-csv-button").on("click", function() {
		var from = pickerFrom.data("DateTimePicker").date().toDate().getTime();
		var to = pickerTo.data("DateTimePicker").date().toDate().getTime();
		var items = item;
		var properties = selectedProperties.join(" ");
		var params = {
			items : items,
			properties : properties,
			from : from,
			to : to,
			type : "text/csv",
			dateformat : "yyyy-MM-dd HH:mm:ss.SSS",
			timezoneoffset : new Date().getTimezoneOffset()
		};
		if ($(".relative-time-checkbox").is(":checked")) {
			params["relativetime"] = true;
		}
		params = enilink.encodeParams(params);
		window.location.href = "/linkedfactory/values?" + params;
		console.log("/linkedfactory/values?" + params);
	});
});