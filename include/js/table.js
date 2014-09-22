var keys = d3.keys(dataSet[0]);

var mins = {}
var maxes = {}
dataSet.forEach(function(item) {
  var mean = d3.mean(d3.values(item).slice(3));
  var deviation = d3.deviation(d3.values(item).slice(3));
  var minmax_key = d3.values(item).slice(0,3).join("");
//  console.log(minmax_key);
  mins[minmax_key] = mean-deviation;
  maxes[minmax_key] = mean+deviation;
});
//console.log(mins);
//console.log(maxes);

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
            key = d3.values(row).slice(0,3).join("")
            dataArray = d3.entries(row);
            dataArray.forEach(function(data) {
              data["min"] = mins[key];
              data["max"] = maxes[key];
            });
//            console.log(dataArray);
            return dataArray;

       })
        .enter()
        .append('td')
        .append('div')
        .style({
            "background-color": function(d, i){
                if(i < 3) return "lightblue";
                console.log(d);
                if (d.min === 0 && d.max === 0) {
                   return "lightgrey";
                }
                return makecolor(d.value, d.min, d.max);
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

