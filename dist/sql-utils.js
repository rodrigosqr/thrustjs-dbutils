var fs = require('filesystem')
var mustache = require('leonardodelfino/thrustjs-mustache')

function pesquisarSqls(diretorio, consultas) {
  var dir = new File(diretorio);
  var arquivosDiretorio = dir.listFiles();

  if (arquivosDiretorio !== null) {
    Java.from(arquivosDiretorio).forEach(function (arquivo) {
      if (arquivo.isFile()) {
        if (arquivo.getName().endsWith('.sql')) {
          consultas[arquivo.getName().slice(0, -4)] = fs.readAll(arquivo.getPath(), "UTF-8");
        }
      } else if (arquivo.isDirectory()) {
        pesquisarSqls(arquivo.getPath(), consultas);
      }
    });
  }
}

function carregarConsultas() {
  // Carregando o arquivo de consultas
  var consultas = {}
  pesquisarSqls(getConfig().parametrosSistema.sqlPath, consultas)
  dangerouslyLoadToGlobal('_SYSSQL', consultas)
}

function carregarConsulta(name, params) {
    return mustache.render(_SYSSQL[name].trim(), params).replace(/(\n|\t)/g, " ")
}

exports = {
  carregarConsultas: carregarConsultas,
  carregarConsulta: carregarConsulta
}
