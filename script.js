// scripts.js

// Function to display an alert when the page loads
document.addEventListener('DOMContentLoaded', (event) => {
    alert('Welcome to the Weather Data Analysis Report!');
});

// Example of a simple function to change the background color of a table on button click
function changeTableBackgroundColor() {
    let tables = document.querySelectorAll('table');
    tables.forEach(table => {
        table.style.backgroundColor = '#e0f7fa'; // Light cyan
    });
}

// Example of a function to add a button that changes table background color
function addChangeColorButton() {
    let button = document.createElement('button');
    button.textContent = 'Change Table Background Color';
    button.onclick = changeTableBackgroundColor;
    document.body.insertBefore(button, document.body.firstChild);
}

// Call the function to add the button
addChangeColorButton();
