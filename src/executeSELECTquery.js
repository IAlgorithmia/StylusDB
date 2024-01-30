const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses } = parseQuery(query);
    const data = await readCSV(`${table}.csv`);

    // Filtering based on WHERE clause
    const filteredData = whereClauses
        ? data.filter(row => whereClauses.every(clause => {
            // You can expand this to handle different operators
            return evaluateCondition(row, clause);
        }))
        : data;

    // Selecting the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = executeSELECTQuery;