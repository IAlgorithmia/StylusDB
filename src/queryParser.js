function parseQuery(query) {
    try {
        
        query = query.trim();
        let isDistinct = false;
        if (query.includes('INSERT INTO'))
        {
            const insertInfo = parseINSERTquery(query);
        }

        if (query.toUpperCase().includes('SELECT DISTINCT')) {
            isDistinct = true;
            query = query.replace('SELECT DISTINCT', 'SELECT');
        }

        const limitRegex = /\sLIMIT\s(\d+)/i;
        const limitMatch = query.match(limitRegex);

        let limit = null;
        if (limitMatch) {
            limit = parseInt(limitMatch[1]);
            query = query.substring(0, limitMatch.index);
        }

        const orderByRegex = /\sORDER BY\s(.+)/i;
        const orderByMatch = query.match(orderByRegex);

        let orderByFields = null;
        if (orderByMatch) {
            query = query.substring(0, orderByMatch.index);
            orderByFields = orderByMatch[1].split(',').map(field => {
                const [fieldName, order] = field.trim().split(/\s+/);
                return { fieldName, order: order ? order.toUpperCase() : 'ASC' };
            });
        }
        const groupByRegex = /\sGROUP BY\s(.+)/i;
        const groupByMatch = query.match(groupByRegex);

        let groupByFields = null;
        if (groupByMatch) {
            groupByFields = groupByMatch[1].split(',').map(field => field.trim());
        }
        
        const whereSplit = query.split(/\sWHERE\s/i);
        query = whereSplit[0].trim();

        const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;
        const joinInfo = parseJoinClause(query);
        const selectRegex = /^SELECT\s(.+?)\sFROM\s(\w+)/i;
        
        let selectMatch = query.match(selectRegex);
        if (!selectMatch) {
            throw new Error('Invalid SELECT format');
        }
        selectMatch = selectMatch[0];
        let joinTable = null, joinCondition = null, joinType = null;
        if (joinInfo.joinPresent) {
            joinType = joinInfo.joinType;
            joinTable = joinInfo.joinTable;
            joinCondition = joinInfo.joinCondition;
        }

        const match = query.match(selectRegex);
        if (match) {
            let [, fields, table] = match;
            let hasAggregate = false;
            let hasAggregateWithoutGroupBy = false;
            fields = fields.split(',').map(field => field.trim().toLowerCase())
            for (let i = 0; i < fields.length; i++) {
                const keywords = ['COUNT', 'AVG', 'MIN', 'MAX', 'SUM'];
                if (keywords.some(keyword => fields[i].toLowerCase().includes(keyword.toLowerCase()))) {
                    const [agrfunc, aggregate] = fields[i].split(/(?<=\b(?:COUNT|AVG|MIN|MAX|SUM)\b)/i);
                    fields[i] = (agrfunc ? agrfunc.toUpperCase() : '') + aggregate;
                    hasAggregate = true;
                }
            }
            if (hasAggregate && !(groupByFields)) {
                hasAggregateWithoutGroupBy = true;
            }
            const whereClauses = whereClause ? parseWhereClause(whereClause) : [];
            
            return {
                fields,
                table: table.trim().toLowerCase(),
                whereClauses: whereClauses.length > 0 ? whereClauses : [],
                joinType,
                isDistinct,
                orderByFields,
                joinTable,
                limit,
                hasAggregateWithoutGroupBy,
                joinCondition,
                groupByFields
            };

        } else {
            throw new Error('Invalid SELECT format');
        }
    }
    catch (error) {
        throw new Error(`Query parsing error: ${error.message}`);
    }
}

function parseWhereClause(whereString) {

    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    const match = whereString.match(conditionRegex);
    const includesLIKE = whereString.includes(' LIKE ');
    if (!match && !includesLIKE) {
        throw new Error('Invalid WHERE clause format');
    }

    const conditions = whereString.split(/ AND | OR /i);
    const proper_conditions = [];
    conditions.forEach(element => {
        proper_conditions.push(element.trim());
    });

    return proper_conditions.map(condition => {
        let [field, operator, value] = condition.split(/\s+/);
        if (operator == 'LIKE' && ((typeof value === 'string') && ((value.startsWith("'") && value.endsWith("'")) || (value.startsWith('"') && value.endsWith('"'))))) {
            value = value.substring(1, value.length - 1);
        }
        return { field, operator, value };
    });
}

function parseINSERTquery(query)
{
    const insertReg = /INSERT INTO (\w+) \(([^)]+)\) VALUES \(([^)]+)\)/;
    const insertMatch = query.match(insertReg);
    if (insertMatch)
    {
        return {
            type: 'INSERT',
            table: insertMatch[1],
            columns: insertMatch[2].split(',').map(field => field.trim()),
            values: insertMatch[3].split(',').map(field => field.trim())
        };
    }
}

function parseDELETEquery(query) {
    const deleteReg = /^DELETE FROM (\w+) WHERE (.+)$/i;
    const deleteMatch = query.match(deleteReg);
    if (deleteMatch) {
        return {
            type: 'DELETE',
            table: deleteMatch[1],
            whereClauses: parseWhereClause(deleteMatch[2])
        };
    }
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);
    if (joinMatch) {
        return {
            joinPresent: (joinMatch != null),
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            groupByFields: null,
            isDistinct: false,
            orderByFields: null,
            limit: null,
            hasAggregateWithoutGroupBy: false,
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinPresent: (joinMatch != null),
        joinType: null,
        joinTable: null,
        orderByFields: null,
        isDistinct: false,
        groupByFields: null,
        limit: null,
        hasAggregateWithoutGroupBy: false,
        joinCondition: null
    };
}

module.exports = { parseJoinClause, parseQuery, parseINSERTquery, parseDELETEquery};