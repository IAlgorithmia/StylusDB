const { _, parseQuery } = require('./queryParser');
const readCSV = require('./csvReader');

// Helper functions for different JOIN types
function performInnerJoin(data, joinData, joinCondition, fields, table) {
    return data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}

function createResultRow(mainRow, joinRow, fields, table, includeAllMainFields) {
    const resultRow = {};

    if (includeAllMainFields) {
        // Include all fields from the main table
        Object.keys(mainRow || {}).forEach(key => {
            const prefixedKey = `${table}.${key}`;
            resultRow[prefixedKey] = mainRow ? mainRow[key] : null;
        });
    }

    // Now, add or overwrite with the fields specified in the query
    fields.forEach(field => {
        const [tableName, fieldName] = field.includes('.') ? field.split('.') : [table, field];
        resultRow[field] = tableName === table && mainRow ? mainRow[fieldName] : joinRow ? joinRow[fieldName] : null;
    });

    return resultRow;
}

function getValueFromRow(row, compoundFieldName) {
    const [tableName, fieldName] = compoundFieldName.split('.');
    return row[`${tableName}.${fieldName}`] || row[fieldName];
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
    return data.flatMap(mainRow => {
        const matchingJoinRows = joinData.filter(joinRow => {
            const mainValue = getValueFromRow(mainRow, joinCondition.left);
            const joinValue = getValueFromRow(joinRow, joinCondition.right);
            return mainValue === joinValue;
        });

        if (matchingJoinRows.length === 0) {
            return [createResultRow(mainRow, null, fields, table, true)];
        }

        return matchingJoinRows.map(joinRow => createResultRow(mainRow, joinRow, fields, table, true));
    });
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
    // Cache the structure of a main table row (keys only)
    const mainTableRowStructure = data.length > 0 ? Object.keys(data[0]).reduce((acc, key) => {
        acc[key] = null; // Set all values to null initially
        return acc;
    }, {}) : {};

    return joinData.map(joinRow => {
        const mainRowMatch = data.find(mainRow => {
            const mainValue = getValueFromRow(mainRow, joinCondition.left);
            const joinValue = getValueFromRow(joinRow, joinCondition.right);
            return mainValue === joinValue;
        });

        // Use the cached structure if no match is found
        const mainRowToUse = mainRowMatch || mainTableRowStructure;

        // Include all necessary fields from the 'student' table
        return createResultRow(mainRowToUse, joinRow, fields, table, true);
    });
}

async function executeSELECTQuery(query) {
    try {
        const { fields, table, whereClauses, joinType, joinTable, joinCondition, groupByFields, orderByFields, limit, isDistinct } = parseQuery(query);
        let data = await readCSV(`${table}.csv`);
        // Logic for applying JOINs
        if (joinTable && joinCondition) {
            const joinData = await readCSV(`${joinTable}.csv`);
            switch (joinType.toUpperCase()) {
                case 'INNER':
                    data = performInnerJoin(data, joinData, joinCondition, fields, table);
                    break;
                case 'LEFT':
                    data = performLeftJoin(data, joinData, joinCondition, fields, table);
                    break;
                case 'RIGHT':
                    data = performRightJoin(data, joinData, joinCondition, fields, table);
                    break;
                default: throw new Error(`Unsupported join type: ${joinType}`);
            }
        }
        data = whereClauses
            ? data.filter(row => whereClauses.every(clause => {
                // You can expand this to handle different operators
                return evaluateCondition(row, clause);
            }))
            : data;
        // Selecting the specified fields
        if (groupByFields) {
            data = applyGroupBy(data, groupByFields, fields);
        }

        if (orderByFields) {
            data.sort((a, b) => {
                for (let { fieldName, order } of orderByFields) {
                    if (a[fieldName] < b[fieldName]) return order === 'ASC' ? -1 : 1;
                    if (a[fieldName] > b[fieldName]) return order === 'ASC' ? 1 : -1;
                }
                return 0;
            });
        }

        if (limit !== null) {
            data = data.slice(0, limit);
        }

        if (isDistinct) {
            data = [...new Map(data.map(item => [fields.map(field => item[field]).join('|'), item])).values()];
        }       

        return data.map(row => {
            const selectedRow = {};
            fields.forEach(field => {
                selectedRow[field] = row[field];
            });
            return selectedRow;
        });
    }
    catch (error) {
        throw new Error(`Error executing query: ${error.message}`);
    }
}

function applyGroupBy(data, groupByFields, aggregateFunctions) {
    const groupResults = {};
    data.forEach(row => {
        // Generate a key for the group
        const groupKey = groupByFields.map(field => row[field]).join('-');

        // Initialize group in results if it doesn't exist
        if (!groupResults[groupKey]) {
            groupResults[groupKey] = { count: 0, sums: {}, mins: {}, maxes: {} };
            groupByFields.forEach(field => groupResults[groupKey][field] = row[field]);
        }

        // Aggregate calculations
        groupResults[groupKey].count += 1;
        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\w+)\)/.exec(func);
            if (match) {
                const [, aggFunc, aggField] = match;
                const value = parseFloat(row[aggField]);

                switch (aggFunc.toUpperCase()) {
                    case 'SUM':
                        groupResults[groupKey].sums[aggField] = (groupResults[groupKey].sums[aggField] || 0) + value;
                        break;
                    case 'MIN':
                        groupResults[groupKey].mins[aggField] = Math.min(groupResults[groupKey].mins[aggField] || value, value);
                        break;
                    case 'MAX':
                        groupResults[groupKey].maxes[aggField] = Math.max(groupResults[groupKey].maxes[aggField] || value, value);
                        break;
                    // Additional aggregate functions can be added here
                }
            }
        });
    });

    // Convert grouped results into an array format
    return Object.values(groupResults).map(group => {
        // Construct the final grouped object based on required fields
        const finalGroup = {};
        groupByFields.forEach(field => finalGroup[field] = group[field]);
        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\*|\w+)\)/.exec(func);
            if (match) {
                const [, aggFunc, aggField] = match;
                switch (aggFunc.toUpperCase()) {
                    case 'SUM':
                        finalGroup[func] = group.sums[aggField];
                        break;
                    case 'MIN':
                        finalGroup[func] = group.mins[aggField];
                        break;
                    case 'MAX':
                        finalGroup[func] = group.maxes[aggField];
                        break;
                    case 'COUNT':
                        finalGroup[func] = group.count;
                        break;
                    // Additional aggregate functions can be handled here
                }
            }
        });

        return finalGroup;
    });
}

function evaluateCondition(row, clause) {
    let { field, operator, value } = clause;
    const rowValue = parseValue(row[field]);
    let conditionValue = parseValue(value);
    switch (operator) {
        case '=': return rowValue === conditionValue;
        case '!=': return rowValue !== conditionValue;
        case '>': return rowValue > conditionValue;
        case '<': return rowValue < conditionValue;
        case '>=': return rowValue >= conditionValue;
        case '<=': return rowValue <= conditionValue;
        case 'LIKE': {
            const regexPattern = '^' + clause.value.replace(/%/g, '.*') + '$';
            return new RegExp(regexPattern, 'i').test(row[clause.field]);
        }
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

function parseValue(value) {

    // Return null or undefined as is
    if (value === null || value === undefined) {
        return value;
    }

    // If the value is a string enclosed in single or double quotes, remove them
    if (typeof value === 'string' && ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith('"') && value.endsWith('"')))) {
        value = value.substring(1, value.length - 1);
    }

    // Check if value is a number
    if (!isNaN(value) && value.trim() !== '') {
        return Number(value);
    }
    // Assume value is a string if not a number
    return value;
}

module.exports = executeSELECTQuery;