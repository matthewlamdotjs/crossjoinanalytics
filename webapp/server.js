const express = require('express');
const session = require('express-session');
const redis = require('redis');
const redisStore = require('connect-redis')(session);
const bodyParser = require('body-parser');
const bcrypt = require('bcrypt');
const pg = require('pg');
const path = require('path');

// Init App and Router and redisClient
const router = express.Router();
const app = express();
app.use(express.static(path.join(__dirname, 'views')));
app.engine('html', require('ejs').renderFile);
const client = redis.createClient();

/* ENV Config */

const DB_URL = process.env['CJ_DB_URL'];
const DB_PORT = process.env['CJ_DB_PORT'];
const DB_UN = process.env['CJ_DB_UN'];
const DB_PW = process.env['CJ_DB_PW'];
const SESSION_SECRET = process.env['SESSION_SECRET'];

if (!DB_URL || !DB_PORT || !DB_UN || !DB_PW || !SESSION_SECRET) {
    console.log('Invalid Credentials. Check ENV Variables');
    process.exit(1);
}

/* DB CONFIG */

const dbConfig = {
    user: DB_UN,
    password: DB_PW,
    database: 'postgres',
    host: DB_URL,
    port: DB_PORT,
    max: 100, // 100 clients at a any given time
    idleTimeoutMillis: 10000,
}

// init db
const db = new pg.Pool(dbConfig);
db.on('error', function (err) {
    console.error('idle client error' + err.message + err.stack);
});


/* AUTH/SESH CONFIG */

app.use(session({
    secret: SESSION_SECRET,
    store: new redisStore({
        host: 'localhost',
        port: 6379,
        client: client,
        ttl: 260
    }),
    saveUninitialized: false,
    resave: false
}));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));


/* ROUTER */

router.get('/', function (req, res) {
    if (req.session.key) {
        res.render('dashboard.html', {
            username: req.session.key['username']
        });
    }
    res.render('index.html');
});

router.get('/dashboard', function (req, res) {
    if (req.session.key) {
        res.render('dashboard.html', {
            username: req.session.key['username']
        });
    } else {
        res.redirect('/login');
    }
});

router.get('/login', function (req, res) {
    res.render('login.html');
});

router.post('/login', function (req, res) {
    // grab credentials
    let username = req.body.username;
    let password = req.body.password;

    db.query('SELECT id, username, password, type FROM users WHERE username=$1', [username], (error, result) => {
        if (result.rows.length > 0) {
            bcrypt.compare(password, result.rows[0].password, function (err, check) {
                if (err) {
                    console.error('Error while checking password: ' + err);
                } else if (check) {
                    req.session.key = {
                        username: result.rows[0].username
                    };
                    return res.json({
                        status: 1,
                        message: 'Success.'
                    });
                }
                else {
                    return res.json({
                        status: 0,
                        message: 'Incorrect login details.'
                    });
                }
            });
        } else if (error) {
            return res.json({
                status: 0,
                message: error
            });
        } else {
            return res.json({
                status: 0,
                message: 'No user found.'
            });
        }
    });
});

router.get('/register', function (req, res) {
    res.render('register.html');
});

router.post('/register', async function (req, res) {

    let username = req.body.username;
    let password = await bcrypt.hash(req.body.password, 5);

    db.query('SELECT 1 FROM users WHERE username=$1;', [username], (error, result) => {
        if (result.rows.length > 0) {
            return res.json({
                status: 0,
                message: 'Username already exists.'
            });
        } else if (error) {
            return res.json({
                status: 0,
                message: error
            });
        } else {
            db.query('INSERT INTO users(username, password) VALUES ($1, $2);', [username, password], (error, result) => {
                if (error) {
                    return res.json({
                        status: 0,
                        message: error
                    });
                } else {
                    req.session.key = {
                        username: username
                    };
                    return res.json({
                        status: 1,
                        message: `Success, user ${username} created.`
                    });
                }
            });
        }
    });
});

router.get('/logout', function (req, res) {
    if (req.session.key) {
        req.session.destroy(function () {
            res.redirect('/');
        });
    } else {
        res.redirect('/');
    }
});

router.post('/ranking', function (req, res) {

    // ranking window defaults
    let years = req.body.years || 1;
    let months = req.body.months || 0;
    let days = req.body.days || 0;

    // default result limit to 100
    let limit = req.body.limit || 100;

    if (req.session.key) {
        db.query(`
            SELECT
                VTbl.symbol,
                STbl.name,
                STbl.type,
                STbl.region,
                STbl.currency,
                CAST(VTbl.volatility AS numeric(8,2))
            FROM
                (
                    SELECT
                        symbol,
                        AVG(price_deviation) AS volatility
                    FROM
                        volatility_aggregation_tbl AS VTbl
                    WHERE
                        end_date <= current_date AND
                        start_date > (
                            current_date
                            - interval '${years} year'
                            - interval '${months} month'
                            - interval '${days} day'
                        )
                    GROUP BY
                        VTbl.symbol
                    ORDER BY
                        volatility DESC
                    LIMIT ${limit}
                ) AS VTbl
            LEFT JOIN
                symbol_master_tbl AS STbl
            ON
                STbl.symbol = VTbl.symbol;
        `, [], (error, result) => {
            if (result.rows.length > 0) {
                return res.json({
                    status: 1,
                    rows: result.rows
                });
            } else if (error) {
                return res.json({
                    status: 0,
                    message: error
                });
            } else {
                return res.json({
                    status: 0,
                    message: 'No results available.'
                });
            }
        });
    } else {
        res.redirect('/login');
    }
});

router.post('/graphData', function (req, res) {

    // ranking window defaults
    let years = req.body.years || 1;
    let months = req.body.months || 0;
    let days = req.body.days || 0;
    let symbol = req.body.symbol;

    if (req.session.key && symbol) {
        db.query(`
            SELECT
                symbol,
                start_date,
                end_date,
                price_deviation,
                average_price
            FROM
                volatility_aggregation_tbl
            WHERE
                end_date <= current_date AND
                start_date > (
                    current_date
                    - interval '${years} year'
                    - interval '${months} month'
                    - interval '${days} day'
                ) AND
                symbol=$1
            ORDER BY
                end_date DESC;
            `, [symbol], (error, result) => {
            if (result.rows.length > 0) {
                return res.json({
                    status: 1,
                    rows: result.rows
                });
            } else if (error) {
                return res.json({
                    status: 0,
                    message: error
                });
            } else {
                return res.json({
                    status: 0,
                    message: 'No results available.'
                });
            }
        });
    } else if (!symbol) {
        return res.json({
            status: 0,
            message: 'Please provide symbol.'
        });
    } else {
        res.redirect('/login');
    }
});

app.use('/', router);

// HTTP
app.listen(process.env.PORT || 80, () => {
    console.log(`App Started on PORT ${process.env.PORT || 80}`);
});
