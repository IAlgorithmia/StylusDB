const { _, parseQuery, parseINSERTquery, parseDELETEquery } = require('./queryParser');
const { readCSV, writeCSV } = require('./csvStorage');

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
        Object.keys(mainRow || {}).forEach(key => {
            const prefixedKey = `${table}.${key}`;
            resultRow[prefixedKey] = mainRow ? mainRow[key] : null;
        });
    }
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
    const mainTableRowStructure = data.length > 0 ? Object.keys(data[0]).reduce((acc, key) => {
        acc[key] = null;
        return acc;
    }, {}) : {};

    return joinData.map(joinRow => {
        const mainRowMatch = data.find(mainRow => {
            const mainValue = getValueFromRow(mainRow, joinCondition.left);
            const joinValue = getValueFromRow(joinRow, joinCondition.right);
            return mainValue === joinValue;
        });


        const mainRowToUse = mainRowMatch || mainTableRowStructure;


        return createResultRow(mainRowToUse, joinRow, fields, table, true);
    });
}

async function executeSELECTQuery(query) {
    try {
        const { fields, table, whereClauses, joinType, joinTable, joinCondition, groupByFields, hasAggregateWithoutGroupBy, orderByFields, limit, isDistinct } = parseQuery(query);
        let data = await readCSV(`${table}.csv`);

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

                return evaluateCondition(row, clause);
            }))
            : data;

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

        if (hasAggregateWithoutGroupBy) {

            const result = {};

            fields.forEach(field => {
                const match = /(\w+)\((\*|\w+)\)/.exec(field);
                if (match) {
                    const [, aggFunc, aggField] = match;
                    switch (aggFunc.toUpperCase()) {
                        case 'COUNT':
                            result[field] = data.length;
                            break;
                        case 'SUM':
                            result[field] = data.reduce((acc, row) => acc + parseFloat(row[aggField]), 0);
                            break;
                        case 'AVG':
                            result[field] = data.reduce((acc, row) => acc + parseFloat(row[aggField]), 0) / data.length;
                            break;
                        case 'MIN':
                            result[field] = Math.min(...data.map(row => parseFloat(row[aggField])));
                            break;
                        case 'MAX':
                            result[field] = Math.max(...data.map(row => parseFloat(row[aggField])));
                            break;

                    }
                }
            });

            return [result];

        } else if (groupByFields) {
            groupResults = applyGroupBy(data, groupByFields, fields);
            return groupResults;
        } else {

            return data.map(row => {
                const selectedRow = {};
                fields.forEach(field => {

                    selectedRow[field] = row[field];
                });
                return selectedRow;
            });
        }
    }
    catch (error) {
        throw new Error(`Error executing query: ${error.message}`);
    }
}

async function executeINSERTQuery(query) {
    const { table, columns, values } = parseINSERTquery(query);
    const data = await readCSV(`${table}.csv`);


    const newRow = {};
    columns.forEach((column, index) => {

        let value = values[index];
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length - 1);
        }
        newRow[column] = value;
    });


    data.push(newRow);


    await writeCSV(`${table}.csv`, data);

    return { message: "Row inserted successfully." };
}

function applyGroupBy(data, groupByFields, aggregateFunctions) {
    const groupResults = new Map();

    data.forEach(row => {
        const groupKey = groupByFields.map(field => row[field]).join('-');

        if (!groupResults.has(groupKey)) {
            groupResults.set(groupKey, { count: 0, sums: {}, mins: {}, maxes: {} });
            groupByFields.forEach(field => groupResults.get(groupKey)[field] = row[field]);
        }

        const group = groupResults.get(groupKey);
        group.count += 1;

        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\w+)\)/.exec(func);
            if (match) {
                const [, aggFunc, aggField] = match;
                const value = parseFloat(row[aggField]);

                switch (aggFunc.toUpperCase()) {
                    case 'SUM':
                        group.sums[aggField] = (group.sums[aggField] || 0) + value;
                        break;
                    case 'MIN':
                        group.mins[aggField] = Math.min(group.mins[aggField] || value, value);
                        break;
                    case 'MAX':
                        group.maxes[aggField] = Math.max(group.maxes[aggField] || value, value);
                        break;
                }
            }
        });
    });

    return Array.from(groupResults.values()).map(group => {
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


    if (value === null || value === undefined) {
        return value;
    }


    if (typeof value === 'string' && ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith('"') && value.endsWith('"')))) {
        value = value.substring(1, value.length - 1);
    }


    if (!isNaN(value) && value.trim() !== '') {
        return Number(value);
    }

    return value;
}

async function executeDELETEQuery(query) {
    const { table, whereClauses } = parseDELETEquery(query);
    let data = await readCSV(`${table}.csv`);
    console.log(table, whereClauses);

    if (whereClauses.length > 0) {
        data = data.filter(row => !whereClauses.every(clause => evaluateCondition(row, clause)));
    } else {
        data = [];
    }
    await writeCSV(`${table}.csv`, data);

    return { message: "Rows deleted successfully." };
}

module.exports = { executeSELECTQuery, executeINSERTQuery, executeDELETEQuery };