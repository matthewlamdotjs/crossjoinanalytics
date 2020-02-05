// Example POST method implementation:
// Modified from https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch
async function postData(url = '', data = {}) {

    // Default options are marked with *
    const response = await fetch(url, {
        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
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
function setError(message){
    let error = document.getElementById('error-msg');
    error.classList.add(...'ui red message'.split(' '));
    error.innerHTML = message;
}

// register user
function register() {

    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;

    if(username && password){

        postData('/register', { username: username, password: password })
        .then((data) => {
            if(data.status == 0){
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

    if(username && password){

        postData('/login', { username: username, password: password })
        .then((data) => {
            if(data.status == 0){
                setError(data.message);
            } else {
                window.location.href = '/dashboard';
            }
        });
        
    } else {
        setError('Please provide a username and password.');
    }

}