// Example POST method implementation:
// Modified from https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch
async function postData(url = '', data = {}) {

    // Default options are marked with *
    const response = await fetch(url, {
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        method: 'POST', // *GET, POST, PUT, DELETE, etc.
        mode: 'cors', // no-cors, *cors, same-origin
        cache: 'no-cache', // *default, no-cache, reload, force-cache, only-if-cached
        credentials: 'same-origin', // include, *same-origin, omit
        headers: {
            'Content-Type': 'application/json'
            // 'Content-Type': 'application/x-www-form-urlencoded',
        },
        redirect: 'follow', // manual, *follow, error
        referrerPolicy: 'no-referrer', // no-referrer, *client
        body: JSON.stringify(data) // body data type must match "Content-Type" header
    });
    return await response.json(); // parses JSON response into native JavaScript objects
}

// displays login/error message
function setError(message) {
    let error = document.getElementById('error-msg');
    error.classList.add(...'ui red message'.split(' '));
    error.innerHTML = message;
}

// register user
function register() {

    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;

    if (username && password) {

        postData('/register', { username: username, password: password })
            .then((data) => {
                if (data.status == 0) {
                    setError(data.message);
                } else {
                    window.location.href = '/dashboard';
                }
            });

    } else {
        setError('Please provide a username and password.');
    }

}

// login user
function login() {

    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;

    if (username && password) {

        postData('/login', { username: username, password: password })
            .then((data) => {
                if (data.status == 0) {
                    setError(data.message);
                } else {
                    window.location.href = '/dashboard';
                }
            });

    } else {
        setError('Please provide a username and password.');
    }

}

// get ranking from API
function ranking() {

    // start dimmer
    document.getElementById('loading-dimmer').classList.add('active');

    const years = document.getElementById('years').value || 1;
    const months = document.getElementById('months').value || 0;
    const days = document.getElementById('days').value || 0;
    const limit = document.getElementById('limit').value || 100;
    const er = document.getElementById('er').checked || false;

    postData('/ranking', {
        years: years,
        months: months,
        days: days,
        limit: limit,
        er: er
    })
    .then((data) => {
        if (data.status == 0) {
            setError(data.message);
            // end dimmer
            document.getElementById('loading-dimmer').classList.remove('active');
        } else {
            setRankTable(data.rows, er);
        }
    });

}

// set table ranking
function setRankTable(data, isEr) {

    let rankColumn = document.getElementById('rank-column')
    if(isEr){
        rankColumn.innerHTML = 'ER Influence'
    } else {
        rankColumn.innerHTML = 'Volatility'
    }

    let table = document.getElementById('ranking-table');

    // clear previous 
    table.innerHTML = '';

    data.forEach((rowData) => {
        table.appendChild(createRow(rowData));
    });

    // init graph first time
    getGraph(data[0].symbol);
}

// create table row
function createRow(rowData) {

    // create row
    let row = document.createElement('tr');
    row.id = rowData.symbol;
    row.onclick = () => getGraph(rowData.symbol);
    row.classList.add('row-element');

    // create cells
    let symbol = document.createElement('td');
    let name = document.createElement('td');
    let type = document.createElement('td');
    let region = document.createElement('td');
    let currency = document.createElement('td');
    let volatility = document.createElement('td');

    // populate cells
    symbol.innerHTML = rowData.symbol;
    name.innerHTML = rowData.name;
    type.innerHTML = rowData.type;
    region.innerHTML = rowData.region;
    currency.innerHTML = rowData.currency;
    volatility.innerHTML = rowData.volatility;

    // add to row
    row.appendChild(symbol);
    row.appendChild(name);
    row.appendChild(type);
    row.appendChild(region);
    row.appendChild(currency);
    row.appendChild(volatility);

    return row;
}

// get graph data
function getGraph(id) {

    // start dimmer
    document.getElementById('loading-dimmer').classList.add('active');

    // unselect previous
    let allRows = document.getElementsByClassName('row-element');
    for (let i = 0; i < allRows.length; i++) {
        let row = allRows[i];
        row.classList.remove('selected');
    }

    // select current
    document.getElementById(id).classList.add('selected');

    // grab params
    const years = document.getElementById('years').value || 1;
    const months = document.getElementById('months').value || 0;
    const days = document.getElementById('days').value || 0;
    const symbol = id;

    postData('/graphData', {
        years: years,
        months: months,
        days: days,
        symbol: symbol
    })
    .then((data) => {
        if (data.status == 0) {
            setError(data.message);
            // end dimmer
            document.getElementById('loading-dimmer').classList.remove('active');
        } else {
            drawGraphs(data.rows);
        }
    });
}

// draws the graphs
function drawGraphs(rows) {

    // clear previous charts if any
    let volGraph = document.getElementById('v-graph');
    let avgGraph = document.getElementById('a-graph');
    volGraph.innerHTML = '';
    avgGraph.innerHTML = '';

    google.charts.load('current', { 'packages': ['corechart'] });
    google.charts.setOnLoadCallback(drawCharts);

    // Find ER Dates
    function erd(date){
        if(([12, 3, 6, 9].indexOf(date.getMonth() + 1) > -1 && date.getDate() > 15) ||
            ([1, 4, 7, 10].indexOf(date.getMonth() + 1) > -1 && date.getDate() <= 15)){
            return true;
        } else {
            return false;
        }
    }

    function drawCharts() {
        let maxDev = 0;
        // find max
        rows.forEach((element) => {
            if(parseFloat(element.price_deviation) > maxDev){
                maxDev = element.price_deviation;
            }
        });

        const volData = new google.visualization.DataTable();
        volData.addColumn('date', 'Date');
        volData.addColumn('number', 'Price Deviation');
        volData.addColumn('number', 'Earnings Season');

        rows.map((element) => {
            return [new Date(element.median_date), parseFloat(element.price_deviation),
                erd(new Date(element.median_date)) ? parseFloat(maxDev) * 1.15 : null]
        }).forEach((row) => {
            volData.addRow(row);
        })

        const volOptions = {
            title: 'Volatility (2 week standard deviation window)',
            curveType: 'function',
            legend: { position: 'bottom' },
            vAxis: {
                viewWindowMode:'explicit',
                viewWindow: {
                    max: parseFloat(maxDev) * 1.15,
                    min: -parseFloat(maxDev) * 0.15
                }
            },
            series: {
                0: { color: '#e2431e', type: 'line' },
                1: { color: '#FFD700', type: 'area' }
            }
        };
        
        const avgData = google.visualization.arrayToDataTable([
            ['Date', 'Average Price']
        ].concat(rows.map((element) => {
            return [new Date(element.median_date), parseFloat(element.average_price)];
        })));
        const avgOptions = {
            title: 'Average Price (2 week window average)',
            curveType: 'function',
            legend: { position: 'bottom' },
            series: {
                0: { color: '#1c91c0' }
            }
        };

        const volChart = new google.visualization.ComboChart(volGraph);
        const avgChart = new google.visualization.LineChart(avgGraph);

        volChart.draw(volData, volOptions);
        avgChart.draw(avgData, avgOptions);

        // end dimmer
        document.getElementById('loading-dimmer').classList.remove('active');
    }
}

document.addEventListener("DOMContentLoaded", function (event) {

    // load data
    if (window.location.pathname.indexOf('/dashboard') > -1) {
        ranking();
    } else if(window.location.pathname.indexOf('/login') > -1){
        document.getElementById('username').onkeydown = function(event) {
            if (event.keyCode === 13) {
                login();
            }
        };
        document.getElementById('password').onkeydown = function(event) {
            if (event.keyCode === 13) {
                login();
            }
        };
    } else if(window.location.pathname.indexOf('/register') > -1){
        document.getElementById('username').onkeydown = function(event) {
            if (event.keyCode === 13) {
                register();
            }
        };
        document.getElementById('password').onkeydown = function(event) {
            if (event.keyCode === 13) {
                register();
            }
        };
    }

});
