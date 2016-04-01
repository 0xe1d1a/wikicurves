$(document).ready(function() {
    var diameter = 500;
    var color = d3.scale.category20c();

    var bubble = d3.layout.force()
        .size([diameter, diameter])
        .distance(150)
        .charge(-100);

    var svg = d3.select("#bubbles").append("svg")
        .attr("width", diameter)
        .attr("height", diameter)
        .attr("class", "bubble");

    d3.csv("dataset_wikicurves.csv", function(error, data) {
        bubble.nodes(data).start();

        var node = svg.selectAll(".node")
            .data(data)
        .enter().append("g")
            .attr("class", "node");

        node.append("circle")
            .attr("x", function(d) { return d.x; })
            .attr("y", function(d) { return d.y; })
            .attr("r", 30)
            .style("fill", function(d) { return color(d.cluster_id); });

        node.append("text")
            .style("text-anchor", "middle")
            .text(function(d) { return d.page_name; });

        bubble.on("tick", function() {
            node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
        });

        node.on("mouseover", function(d) {
            $("#interpolations").show();
            render_chart([d.v1, d.v2, d.v3, d.v4, d.v5, d.v6,
                          d.v7, d.v8, d.v9, d.v10, d.v11, d.v12]);
        });

        node.on("mouseout", function(d) {
            $("#interpolations").hide();
            $("#interpolations").html("");
        });
    });

    function render_chart(values) {
        var margin = {top: 20, right: 20, bottom: 30, left: 50};
        var width = 500 - margin.left - margin.right;
        var height = 500 - margin.top - margin.bottom;

        var x = d3.scale.linear()
            .range([0, width]);

        var y = d3.scale.linear()
            .range([height, 0]);

        var xAxis = d3.svg.axis()
            .scale(x)
            .orient("bottom");

        var yAxis = d3.svg.axis()
            .scale(y)
            .orient("left");

        var line = d3.svg.line()
            .interpolate("basis")
            .x(function(d, i) { return x(i + 1); })
            .y(function(d) { return y(d); });

        var svg = d3.select("#interpolations").append("svg")
            .attr("width", width + margin.left + margin.right)
            .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        x.domain([1, 12]);
        y.domain([0, 15]);

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(xAxis);

        svg.append("g")
            .attr("class", "y axis")
            .call(yAxis)
        .append("text")
            .attr("transform", "rotate(-90)")
            .attr("y", 6)
            .attr("dy", ".71em")
            .style("text-anchor", "end")
            .text("Page hits");

        svg.append("path")
            .datum(values)
            .attr("class", "line")
            .attr("d", line);
    }
});
