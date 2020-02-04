const express = require('express');
const session = require('express-session');
const https = require('https');
const bodyParser = require('body-parser');
const fs = require('fs');
const pg = require('pg');

// Init App and Router
const router = express.Router();
const app = express();

// ENV Config
const DB_URL = process.env['CJ_DB_URL'];
const DB_PORT = process.env['CJ_DB_PORT'];
const DB_UN = process.env['CJ_DB_UN'];
const DB_PW = process.env['CJ_DB_PW'];
const SESSION_SECRET = process.env['SESSION_SECRET'];

if(!(DB_URL || DB_PORT || DB_UN || DB_PW || SESSION_SECRET)){
    console.log('Invalid Credentials. Check ENV Variables');
    process.exit(1);
}

// DB Config
const dbConfig = {
    user: config.db.user,
    password: config.db.password,
    database: config.db.database,
    host: config.db.host,
    port: config.db.port,
    max: config.db.max,
    idleTimeoutMillis: config.db.idleTimeoutMillis,
}
  
const db = new pg.Pool(dbConfig);
db.on('error', function (err) {
    console.error('idle client error', err.message, err.stack)
});

const query = (text, params, callback) => {
    return pool.query(text, params, callback)
}

app.use(session({secret: 'ssshhhhh', saveUninitialized: true, resave: true}));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));
app.use(express.static(__dirname + '/views'));

router.get('/',(req,res) => {
    const sess = req.session;
    if(sess.email) {
        return res.redirect('/dashboard');
    }
    res.sendFile('index.html');
});

router.post('/login',(req,res) => {
    const sess = req.session;
    sess.email = req.body.email;
    res.end('done');
});

router.get('/admin',(req,res) => {
    const sess = req.session;
    if(sess.email) {
        res.write(`<h1>Hello ${sess.email} </h1><br>`);
        res.end('<a href='+'/logout'+'>Logout</a>');
    }
    else {
        res.write('<h1>Please login first.</h1>');
        res.end('<a href='+'/'+'>Login</a>');
    }
});

router.get('/logout',(req,res) => {
    req.session.destroy((err) => {
        if(err) {
            return console.log(err);
        }
        res.redirect('/');
    });

});

app.use('/', router);

// HTTPS
https.createServer({
    key: fs.readFileSync('server.key'),
    cert: fs.readFileSync('server.cert')
  }, app)
  .listen(process.env.PORT || 443, function () {
    console.log(`Node HTTPS Server Started on PORT ${process.env.PORT || 443}`);
  });