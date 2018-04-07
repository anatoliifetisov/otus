var tabsEnum = Object.freeze({"stats": "stats", "settings": "settings"});
var firstRun = true;

retrieveSummary();
setInterval(retrieveSummary, 3000);

function openTab(evt, tabName) {
    var i, tabcontent, tablinks;

    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }

    tablinks = document.getElementsByClassName("tablinks");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].className = tablinks[i].className.replace(" active", "");
    }

    document.getElementById(tabName).style.display = "block";
    evt.currentTarget.className += " active";
}

function buildBarplot(data) {

    var svg = d3.select("#barplot");
    svg.selectAll("g").remove();

    var margin = {
        top: 20,
        right: 20,
        bottom: 200,
        left: 40
    };

    var width = +svg.attr("width") - margin.left - margin.right;
    var height = +svg.attr("height") - margin.top - margin.bottom;
    var g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var x = d3.scaleBand()
        .rangeRound([0, width])
        .padding(0.05)
        .align(0.1);

    var y = d3.scaleLinear()
        .rangeRound([height, 0]);

    var z = d3.scaleOrdinal()
        .range(["#ff0000", "#c0c0c0", "#7fe500"]);

    var make_label = function (tag, mean) {
        return tag + ", mean: " + d3.format(".2f")(mean);
    };

    var keys = ["negative", "neutral", "positive"];

    if (data.length === 0)
        return;

    data.sort(function (a, b) {
        return b.total - a.total;
    });
    x.domain(data.map(function (d) {
        return make_label(d.tag, d.mean);
    }));
    y.domain([0, d3.max(data, function (d) {
        return d.total;
    })]).nice();
    z.domain(keys);

    g.append("g")
        .selectAll("g")
        .data(d3.stack().keys(keys)(data))
        .enter().append("g")
        .attr("fill", function (d) {
            return z(d.key);
        })
        .selectAll("rect")
        .data(function (d) {
            return d;
        })
        .enter().append("rect")
        .attr("x", function (d) {
            return x(make_label(d.data.tag, d.data.mean));
        })
        .attr("y", function (d) {
            return y(d[1]);
        })
        .attr("height", function (d) {
            return y(d[0]) - y(d[1]);
        })
        .attr("width", x.bandwidth());

    g.append("g")
        .attr("class", "axis")
        .attr("transform", "translate(0," + height + ")")
        .call(d3.axisBottom(x))
        .selectAll("text")
        .attr("y", 0)
        .attr("x", 9)
        .attr("dy", ".35em")
        .attr("transform", "rotate(45)")
        .style("text-anchor", "start");

    g.append("g")
        .attr("class", "axis")
        .call(d3.axisLeft(y).ticks(null, "s"))
        .append("text")
        .attr("x", 2)
        .attr("y", y(y.ticks().pop()) + 0.5)
        .attr("dy", "0.32em")
        .attr("fill", "#000")
        .attr("font-weight", "bold")
        .attr("text-anchor", "start")
        .text("Tweets");

    var legend = g.append("g")
        .attr("font-family", "sans-serif")
        .attr("font-size", 10)
        .attr("text-anchor", "end")
        .selectAll("g")
        .data(keys.slice().reverse())
        .enter().append("g")
        .attr("transform", function (d, i) {
            return "translate(0," + i * 20 + ")";
        });

    legend.append("rect")
        .attr("x", width - 19)
        .attr("width", 19)
        .attr("height", 19)
        .attr("fill", z);

    legend.append("text")
        .attr("x", width - 24)
        .attr("y", 9.5)
        .attr("dy", "0.32em")
        .text(function (d) {
            return d;
        });
}

function retrieveSummary() {
    var address = location.protocol+'//'+location.hostname+(location.port ? ':'+location.port: '');
    httpGetAsync(address + "/latestSummary", function (summary) {

        var data = JSON.parse(summary);

        buildBarplot(data);

        if (firstRun && data.length > 0){
            var currentSettings = data.map(function (x) {
                return x.tag;
            });

            for (var i = 0; i < currentSettings.length; i++) {
                insertRow(currentSettings[i])
            }
            firstRun = false;
        }
    });
}

function httpGetAsync(theUrl, callback) {
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.onreadystatechange = function () {
        if (xmlHttp.readyState === 4 && xmlHttp.status === 200)
            callback(xmlHttp.responseText);
    };
    xmlHttp.open("GET", theUrl, true); // true for asynchronous
    xmlHttp.send(null);
}

function httpPostAsync(url, params) {
    var data = JSON.stringify(params);

    $.ajax({
        type: "POST",
        url: url,
        data: data,
        contentType: "application/json; charset=utf-8"
    });
}

function deleteRow(row) {
    var i = row.parentNode.parentNode.rowIndex;
    document.getElementById("hashtagsTable").deleteRow(i);
}


function insertRow(s) {
    var table = document.getElementById("hashtagsTable");
    if (typeof s !== "undefined" && table.rows[1].cells[1].getElementsByTagName("input")[0].value === "") {
        table.rows[1].cells[1].getElementsByTagName("input")[0].value = s;
        return;
    }

    var new_row = table.rows[1].cloneNode(true);
    var len = table.rows.length;

    var inp1 = new_row.cells[1].getElementsByTagName('input')[0];
    inp1.id += len;
    if (typeof s !== "undefined")
        inp1.value = s;
    else
        inp1.value = "";
    table.appendChild(new_row);
}

function saveSettings() {
    var table = document.getElementById("hashtagsTable");
    var tags = [];
    for (var i = 1; i < table.rows.length; i++) {
        var tag = table.rows[i].cells[1].getElementsByTagName("input")[0].value;
        if (tag !== "")
            tags.push(tag)
    }

    var address = location.protocol+'//'+location.hostname+(location.port ? ':'+location.port: '');
    httpPostAsync(address + "/settings", tags)
}