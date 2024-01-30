function parseQuery(query) {
    const selectRegex = /SELECT (.+) FROM (\S+)( WHERE (.+))?/i;
    const match = query.match(selectRegex);
    if (match) {
        const [, fields, table, , whereString] = match;
        const whereClauses = whereString ? parseWhereClause(whereString) : null;
        console.log(fields, '-space-', table, '-space-',  whereClauses)
        return {
            fields: fields.split(',').map(field => field.trim().toLowerCase()),
            table: table.trim().toLowerCase(),
            whereClauses: whereClauses ? whereClauses : null
        };
    } else {
        throw new Error('Invalid query format');
    }
}

function parseWhereClause(whereString) {
    const conditions = whereString.split(/ AND | OR /i);
    const proper_conditions = []
    conditions.forEach(element => {
        proper_conditions.push(element.trim())
    });
    return proper_conditions.map(condition => {
        const [field, operator, value] = condition.split(/\s+/);
        return { field, operator, value };
    });
}

module.exports = parseQuery;