'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = Number(item['$'])
	});
	return stats;
}

function rowToMap1(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = item['$']
	});
	return stats;
}

//hclient.table('stephanieramos_occupations').scan({ maxVersions: 1}, (err,rows) => {
	//console.info(rows)
//})

//hclient.table('stephanieramos_metroareas').scan({ maxVersions: 1}, (err,rows) => {
	//console.info(rows)
//})

//hclient.table('stephanieramos_batch').scan({
	//filter: {type : "PrefixFilter",
		      //value: "1018000-0000"},
	//maxVersions: 1},
	//(err, value) => {
	  //console.info(value)
	//})

//hclient.table('stephanieramos_top10_batch').scan({
		//filter: {type : "PrefixFilter",
			//value: "2019"},
		//maxVersions: 1},
	//(err, value) => {
		//console.info(value)
	//})


app.use(express.static('public'));



app.get('/employment-stats.html', function (req, res) {
	hclient.table('stephanieramos_metroareas').scan({ maxVersions: 1}, (metro_err,metro_rows) => {
		hclient.table('stephanieramos_occupations').scan({ maxVersions: 1}, (occ_err,occ_rows) => {
			var template = filesystem.readFileSync("employment-stats.mustache").toString();
			var html = mustache.render(template, {
				occupations : occ_rows,
				metroareas: metro_rows
			});
			res.send(html)
		})
	})
});

app.get('/employment-stats.html', function (req, res) {
	hclient.table('stephanieramos_occupations').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("employment-stats.mustache").toString();
		var html = mustache.render(template, {
			occupations : rows
		});
		res.send(html)
	})
});

app.get('/employment-top.html', function (req, res) {
	hclient.table('stephanieramos_years').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("top10.mustache").toString();
		var html = mustache.render(template, {
			years : rows
		});
		res.send(html)
	})
});

app.get('/employment-worse.html', function (req, res) {
	hclient.table('stephanieramos_years').scan({ maxVersions: 1}, (err,rows) => {
		var template = filesystem.readFileSync("worse10.mustache").toString();
		var html = mustache.render(template, {
			years : rows
		});
		res.send(html)
	})
});

function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}


app.get('/employment-stats-results.html',function (req, res) {
	const occupation=req.query['metroarea'] + req.query['job'];
	const metroarea=req.query['metroarea']
	const job = req.query['job']
	console.log(occupation);
	function processYearRecord(yearRecord) {
		if (yearRecord == undefined){
			var result = { year : '-'}
		} else {
		var result = { year : yearRecord['year']};
			result['msa_total_emp'] = yearRecord['msa_total_emp']
		    result['n_tot_emp'] = yearRecord['n_tot_emp']
		    result['msa_a_mean'] = yearRecord['msa_a_mean']
			result['wage'] = '$' + result['msa_a_mean']
		    result['n_a_mean'] = yearRecord['n_a_mean']
		    result['unemp_rate'] = yearRecord['unemp_rate'] == undefined ? "-" : yearRecord['unemp_rate'].toFixed(1)+'%'
		    result['lag_msa_a_mean'] = yearRecord['lag_msa_a_mean']
		    result['lag_n_a_mean'] = yearRecord['lag_n_a_mean']
			result['labor_force'] = yearRecord['labor_force']
			result['labor'] = (result['msa_a_mean'] / result['labor_force']).toFixed(2)+'%'
		    result["pct"] = result['n_tot_emp'] == 0 ? "-" : (100 * result['msa_total_emp']/result['n_tot_emp']).toFixed(2)+'%'
		    result['dif'] = (100 * (result['msa_a_mean'] - result['n_a_mean'])/result['n_a_mean']).toFixed(2)+'%'
		    result['growth_msa'] = result['lag_msa_a_mean'] == undefined ? "-" : (100 * (result['msa_a_mean'] - result['lag_msa_a_mean'])/result['lag_msa_a_mean']).toFixed(2)+'%'
		    result['growth_n'] = result['lag_n_a_mean'] == undefined ? "-" : (100 * (result['n_a_mean'] - result['lag_n_a_mean'])/result['lag_n_a_mean']).toFixed(2)+'%'
		    result['pred_wage'] = yearRecord['pred_wage'] == undefined ? "-" : '$' + yearRecord['pred_wage'].toFixed(0)
		    }
		console.log(result);
		return result;
	}
	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var year = Number(removePrefix(cell['key'], occupation))
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year ) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { year: year }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = Number(cell['$'])
		})
		result.push(processYearRecord(yearRecord))
		return result;
	}


	hclient.table('stephanieramos_batch_new').scan({
			filter: {type : "PrefixFilter",
				value: occupation},
			maxVersions: 1},
		(err, cells) => {
			hclient.table('stephanieramos_metroareas').row(metroarea).get((error, metro) => {
				hclient.table('stephanieramos_occupations').row(job).get((error, occ) => {
			var ai = airlineInfo(cells);
			var template = filesystem.readFileSync("employment-result.mustache").toString();
			var html = mustache.render(template, {
				occInfo : ai,
				occupation : occupation,
				metro:  metro[0]['$'],
				occ:  occ[0]['$'],
				bool: ai[0]['year'] == '-' ? false : true
			});
			res.send(html)
			})
		})

	})
});

////new
app.get('/employment-top-results.html',function (req, res) {
	const year=req.query['year'];
	console.log(year);
	function processYearRecord(yearRecord) {
		var result = { occ_code : yearRecord['occ_code']};
		result['occ_title'] = yearRecord['occ_title']
		result['growth'] = (100 * Number(yearRecord['growth'])).toFixed(2)+'%'
		result['a_mean'] = yearRecord['a_mean']== undefined ? "-" : '$' + yearRecord['a_mean']
		console.log(result)
		return result;
	}
	function yearInfo(cells) {
		var result = [];
		console.log(result)
		var yearRecord;
		cells.forEach(function(cell) {
			var occ_code = String(removePrefix(cell['key'], year))
			if(yearRecord === undefined)  {
				yearRecord = { occ_code: occ_code }
			} else if (yearRecord['occ_code'] != occ_code ) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { occ_code: occ_code }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = cell['$']
		})
		result.push(processYearRecord(yearRecord))
		console.info(result)
		return result;
	}

	hclient.table('stephanieramos_top10_batch').scan({
			filter: {type : "PrefixFilter",
				value: year},
			maxVersions: 1},
		(err, cells) => {
			var ai = yearInfo(cells);
			var template = filesystem.readFileSync("top10-result.mustache").toString();
			var html = mustache.render(template, {
				yearInfo : ai,
				year : year,
			});
			res.send(html)

		})
});

app.get('/employment-worse-results.html',function (req, res) {
	const year=req.query['year'];
	console.log(year);
	function processYearRecord(yearRecord) {
		var result = { occ_code : yearRecord['occ_code']};
		result['occ_title'] = yearRecord['occ_title']
		result['growth'] = (100 * Number(yearRecord['growth'])).toFixed(2)+'%'
		result['a_mean'] = yearRecord['a_mean']== undefined ? "-" : '$' + yearRecord['a_mean']
		console.log(result)
		return result;
	}
	function yearInfo(cells) {
		var result = [];
		console.log(result)
		var yearRecord;
		cells.forEach(function(cell) {
			var occ_code = String(removePrefix(cell['key'], year))
			if(yearRecord === undefined)  {
				yearRecord = { occ_code: occ_code }
			} else if (yearRecord['occ_code'] != occ_code ) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { occ_code: occ_code }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = cell['$']
		})
		result.push(processYearRecord(yearRecord))
		console.info(result)
		return result;
	}

	hclient.table('stephanieramos_worse10_batch').scan({
			filter: {type : "PrefixFilter",
				value: year},
			maxVersions: 1},
		(err, cells) => {
			var ai = yearInfo(cells);
			var template = filesystem.readFileSync("worse10-result.mustache").toString();
			var html = mustache.render(template, {
				yearInfo : ai,
				year : year,
			});
			res.send(html)

		})
});

app.listen(port);