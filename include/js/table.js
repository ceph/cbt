var keys = d3.keys(dataSet[0]);

var thead = d3.select("#view > thead")
var th = thead.selectAll("th")
        .data(keys)
        .enter()
        .append('th')
        .text(function(d){ return d })

var tbody = d3.select("#view > tbody");

var tr = tbody.selectAll("tr")
    .data(dataSet)
    .enter()
    .append('tr')
        .selectAll('td')
        .data(function (row) {
            return d3.entries(row);
        })
        .enter()
        .append('td')
        .append('div')
        .style({
            "background-color": function(d, i){
                if(i < 3) return "lightblue";
                return makecolor(d.value, 20, 181);
            },
        })
        .text(function(d){
            return d.value
        });

function makecolor(val, min, max) {
    var red = 255;
    var green = 255;
    if(val < min) {
        green = 0;
    } else if(val < min+((max-min)/2.0)) {
        green = Math.round(((val-min)/((max-min)/2.0)) * 255);
    } else if(val < max) {
        red = Math.round(((max-val)/((max-min)/2.0)) * 255);
    } else {
        red = 0;
    } 
    return "#" + rgb2hex(red,green,0);
}

function rgb2hex(r,g,b) {
    if (g !== undefined) 
        return Number(0x1000000 + r*0x10000 + g*0x100 + b).toString(16).substring(1);
    else 
        return Number(0x1000000 + r[0]*0x10000 + r[1]*0x100 + r[2]).toString(16).substring(1);
}

