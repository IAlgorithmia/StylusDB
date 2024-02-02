#!/usr/bin/env node

const readline = require('readline');
const { executeSELECTQuery, executeINSERTQuery, executeDELETEQuery } = require('./queryExecutor');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

rl.setPrompt('SQL> ');
console.log('SQL Query Engine CLI. Enter your SQL commands, or type "exit" to quit.');

rl.prompt();

rl.on('input', async (input) => {
    if (input.toLowerCase() === 'exit') {
        rl.close();
        return;
    }

    try {
        if (input.toLowerCase().startsWith('select')) {

            const result = await executeSELECTQuery(input);
            console.log('Result:', result);

        } else if (input.toLowerCase().startsWith('insert into')) {

            const result = await executeINSERTQuery(input);
            console.log(result.message);

        } else if (input.toLowerCase().startsWith('delete from')) {

            const result = await executeDELETEQuery(input);
            console.log(result.message);

        } else {
            console.log('Unsupported command');
        }
    } catch (error) {

        console.error('Error:', error.message);
        
    }

    rl.prompt();
}).on('close', () => {
    console.log('Exiting SQL CLI');
    process.exit(0);
});