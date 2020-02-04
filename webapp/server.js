const express = require('express');
const session = require('express-session');
const redis   = require("redis");
const redisStore = require('connect-redis')(session);
const bodyParser = require('body-parser');
const bcrypt = require('bcrypt');
const pg = require('pg');
const path = require('path'); 

// Init App and Router and redisClient
const router = express.Router();
const app = express();
const client  = redis.createClient();

/* ENV Config */

const DB_URL = process.env['CJ_DB_URL'];
const DB_PORT = process.env['CJ_DB_PORT'];
const DB_UN = process.env['CJ_DB_UN'];
const DB_PW = process.env['CJ_DB_PW'];
const SESSION_SECRET = process.env['SESSION_SECRET'];

if (!DB_URL || !DB_PORT || !DB_UN || !DB_PW || !SESSION_SECRET) {
    console.log(DB_URL);
    console.log(DB_PORT);
    console.log(DB_UN);
    console.log(DB_PW);
    console.log(SESSION_SECRET);

    console.log(!DB_URL);
    console.log(!DB_PORT);
    console.log(!DB_UN);
    console.log(!DB_PW);
    console.log(!SESSION_SECRET);
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
    secret: 'ssshhhhh',
    store: new redisStore({
        host: 'localhost',
        port: 6379,
        client: client,
        ttl :  260
    }),
    saveUninitialized: false,
    resave: false
}));
app.use(bodyParser.json());      
app.use(bodyParser.urlencoded({extended: true}));


/* ROUTER */

router.get('/',function(req,res){
    res.render('index.html');
});

router.get('/dashboard',function(req,res){
    if(req.session.key) {
        res.render('dashboard.html',{ email : req.session.key['username']});
    } else {
        res.redirect('/login');
    }
});

router.get('/login', function(req,res){
    res.render('login.html');
});

router.post('/login',function(req, res){
    // grab credentials
    let username = req.param('username');
    let password = req.param('password');

    db.query('SELECT id, username, password, type FROM users WHERE username=$1', [username], (error, result) => {
        if(result.rows.length > 0) {
            bcrypt.compare(password, result.rows[0].password, function (err, check) {
                if(err) {
                    console.error('Error while checking password: ' + err);
                } else if(check){
                    req.session.key = {
                        username: result.rows[0].username
                    };
                    return res.json({
                        status: 1,
                        message: 'Success.'
                    });
                }
                else{
                    return res.json({
                        status: 0,
                        message: 'Incorrect login details.'
                    });
                }
            });
        } else if(error){
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

router.get('/register', function(req,res){
    res.render('register.html');
});

router.post('/register',function(req,res){
    
});

router.get('/logout',function(req,res){
    if(req.session.key) {
        req.session.destroy(function(){
        res.redirect('/');
        });
    } else {
        res.redirect('/');
    }
});


app.use('/', router);
app.use(express.static(path.join(__dirname, 'src')));

// HTTP
app.listen(process.env.PORT || 80,() => {
    console.log(`App Started on PORT ${process.env.PORT || 80}`);
});