var database = require('database')
var mustache = require('leonardodelfino/thrustjs-mustache')

var CONSTANTS = {
  limitBind: '_limit_',
  offsetBind: '_offset_'
}

function buildColumns(columns) {
  columns = columns || []

  if (columns.length > 0) {
    return columns.join(', ')
  } else {
    return '*'
  }
}

function buildWhere(where) {
  var whereClause = where.map(function (value, idx) {
    return value[0] + ' ' + value[1] + ' :' + value[0]
  }).join(' AND ')

  if (whereClause.length > 0) {
    return ' WHERE ' + whereClause
  } else {
    return ' '
  }
}

function buildOrderBy(order) {
  order = order || {}
  var orderClause = order.map(function (value, idx) {
    return value[0] + ' ' + value[1]
  }).join(', ')

  if (orderClause.length > 0) {
    return ' ORDER BY ' + orderClause
  } else {
    return ''
  }
}

function buildLimit(limit) {
  return limit ? (' LIMIT :' + CONSTANTS.limitBind) : ''
}

function buildOffset(offset) {
  return offset ? (' OFFSET :' + CONSTANTS.offsetBind) : ''
}

function buildClause(table, columns, where, orderby, limit, offset) {
  return 'SELECT ' + buildColumns(columns) +
    ' FROM ' + table +
    buildWhere(where) +
    buildOrderBy(orderby) +
    buildLimit(limit) +
    buildOffset(offset)
}

function select(table) {
  var columns = []
  var where = []
  var limit = null
  var offset = null
  var orderby = []
  var binds = {}

  var setColumns = function () {
    columns = columns.concat([].slice.call(arguments))
    return this
  }

  var setCondition = function (field, value, operator) {
    binds[field] = (operator === 'ilike') ? ('%' + value + '%') : value
    where.push([field, operator])
  }

  var setEquals = function (field, value) {
    setCondition(field, value, '=')
    return this
  }

  var setNotEquals = function (field, value) {
    setCondition(field, value, '<>')
    return this
  }

  var setLike = function (field, value) {
    setCondition(field, value, 'ilike')
    return this
  }

  var setLimit = function (value) {
    binds[CONSTANTS.limitBind] = value
    limit = true
    return this
  }

  var setOffset = function (value) {
    binds[CONSTANTS.offsetBind] = value
    offset = true
    return this
  }

  var setOrderBy = function (column, order) {
    order = order || 'ASC'
    orderby.push([column, order])
    return this
  }

  var execute = function () {
    var sql = buildClause(table, columns, where, orderby, limit, offset)
    // console.log('------------------------------------------------------------------------------------------------------------------------\n',
    // sql, '\n', 'PARAMS: ', binds)
    return this.db.select(sql, binds)
  }

  return {
    columns: setColumns,
    eq: setEquals,
    neq: setNotEquals,
    like: setLike,
    limit: setLimit,
    offset: setOffset,
    orderBy: setOrderBy,
    execute: execute.bind(this)
  }
}

function imprimirConsultaLog(name, params) {
  var sql = mustache.render(_SYSSQL[name].trim(), params).split('\n')
  var sqlPrint = ''
  var sep = ''
  for (var i = 0; i < sql.length; i++) {
    if (sql[i].trim().length > 0) {
      sqlPrint += sep + sql[i]
      sep = '\n'
    }
  }
  console.log('------------------------------------------------------------------------------------------------------------------------\n',
    sqlPrint, '\n', 'PARAMS: ', params)
}

/**
 * Executa uma consulta com base em um arquivo
 *
 * @param {String} name Nome da consulta
 * @param {Object} params Bindings
 * @returns
 */
function namedQuery(name, params) {
  if (!_SYSSQL[name]) {
    throw 'Consulta não encontrada'
  }
  var consulta = mustache.render(_SYSSQL[name].trim(), params).replace(/(\n|\t)/g, ' ')

  imprimirConsultaLog(name, params)

  return this.db.execute(consulta, params)
}

function createDbInstance(options) {
  var db = database.createDbInstance(options)
  var ctx = {
    db: db
  }
  return {
    getInfoColumns: db.getInfoColumns,
    insert: db.insert,
    select: select.bind(ctx),
    namedQuery: namedQuery.bind(ctx),
    update: db.update,
    delete: db.delete,
    execute: db.execute,
    executeInSingleTransaction: db.executeInSingleTransaction
  }
}

exports = {
  createDbInstance: createDbInstance
}