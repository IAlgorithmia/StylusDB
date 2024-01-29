function parseQuery(query) {
    const selectRegex = /SELECT (.+) FROM (\S+)( WHERE (.+))?/i;
    const match = query.match(selectRegex);
    if (match) {
        const [, fields, table, , whereClause] = match;
        console.log(fields, '-space-', table, '-space-', whereClause)
        return {
            fields: fields.split(',').map(field => field.trim().toLowerCase()),
            table: table.trim().toLowerCase(),
            whereClause: whereClause ? whereClause.trim().toLowerCase() : null
        };
    } else {
        throw new Error('Invalid query format');
    }
}

module.exports = parseQuery;