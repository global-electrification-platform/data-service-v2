from fastapi import FastAPI, Body, Path, Query, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

import clickhouse_driver

from pydantic import BaseModel
from typing import List, Optional

import collections
import json
import time
import os
import urllib

from . import expander

import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

CLICKHOUSE_HOST = os.environ.get('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_DB = os.environ.get('CLICKHOUSE_DB', 'gep')

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["OPTIONS","GET"],
    allow_headers=["*"],
    max_age=86400,
)

#
# RISE
#
riseScores = {}

with open(os.path.join(os.path.dirname(__file__), 'rise-indicators.json'), 'r') as f:
    rise = json.load(f)
    riseScores = {r['iso']:r for r in rise}

#
# utils
#
def scenarioId_toModelId(sid):
    return "-".join(sid.split('-')[0:2])

def yearField(field, year):
    if year:
        return "%s%s" %(field, year)
    return field

def yearFieldAs(field, year, as_name=None):
    if year:
        camelCase = as_name or field[0].lower() + field[1:]
        return "%s%s as %s" %(field, year, camelCase)
    return field

def _sum(f, as_name=None):
    if as_name:
        return "sum(%s) as %s" % (f, as_name)
    return "sum(%s)" % f


def model_fromScenario(sid):
    sid = sid.lower()

    modelId = scenarioId_toModelId(sid)

    model= unjson_model(_execute_onerow('select filters, timesteps, baseYear from models where id=%(modelId)s',
                                        {'modelId': modelId}))

    model['filter_dict'] = {f['key']: f for f in model['filters']}
    return model

def unjson_model(model):
    # model was loaded directly from a postgresql dump
    # json fields
    for field in ('attribution', 'map', 'sourceData'):
        if not field in model: continue
        try:
            model[field] = json.loads(model[field])
        except json.JSONDecodeError as msg:
            log.error(msg)
            log.error(model[field])
            raise
    # json[] fields. These aren't actually valid json
    # I'd worry about performance here, but it's just models
    # and we do this max once per request
    for field in('levers', 'filters', 'timesteps'):
        if not field in model: continue
        try:
            arr = json.loads('[' + model[field][1:-1] + ']')
            model[field] = [json.loads(elt) for elt in arr if isinstance(elt, str)]
            if not model[field]:
                model[field] = arr
        except Exception as msg:
            log.error(msg)
            log.error(model[field])
            raise
    return model

#
# Query bits
#
def connection():
    # clickhouse driver client
    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST, database=CLICKHOUSE_DB)
    return client

#
# query executors that return a dict or list of dicts
#
def _execute(sql, params=None):
    client = connection()
    (results, cols) = client.execute(sql, params, with_column_types=True)
    for res in results:
        yield {col[0]:v for col, v in zip(cols,res)}

def _execute_onerow(sql, params=None):
    try:
        return next(_execute(sql, params))
    except StopIteration:
        raise CustomError("Not Found")

class CustomError(Exception):
    pass


class FilterModel(BaseModel):
    key: str
    min: Optional[float]
    max: Optional[float]
    options: Optional[List[str]]

# get + post for all endpoints
@app.get("/")
def read_root():
    return "GEP FastAPI Service"


@app.get('/stats')
def stats():
    client = connection()
    countries = _execute_onerow("select count(distinct country) as countries from models")
    models = _execute_onerow("select count(distinct type) as models from models")
    return {
        "totals": {
        "countries": countries['countries'],
        "models": models['models'],
        }
    }


@app.get("/countries")
def countries():
    client = connection()
    countries = _execute("""
        select id, name from countries
        where id in (select country from models)
        order by name ASC
        """)

    return { 'countries': countries }

@app.get("/countries/{countryId}")
def country(countryId: str):
    client = connection()
    countryId = countryId.upper()
    country = _execute_onerow("select * from countries where id=%(countryId)s", {"countryId":countryId})
    country['models'] = [unjson_model(m) for m in _execute("""
        select
          attribution,
          country,
          description,
          disclaimer,
          filters,
          baseYear,
          timesteps,
          id,
          levers,
          map,
          name,
          version,
          type,
          sourceData,
          date_trunc('day', updatedAt) as updatedAt
          from gep.models
          where country=%(countryId)s
          order by updatedAt desc
          """,
                            {"countryId":countryId})]

    country['riseScores'] = riseScores.get(countryId, None)
    return country

@app.get('/models/{modelId}')
def model(modelId: str):
    client = connection()
    model = unjson_model(_execute_onerow("""
        select
          attribution,
          country,
          description,
          disclaimer,
          filters,
          baseYear,
          timesteps,
          id,
          levers,
          map,
          name,
          version,
          type,
          sourceData,
          date_trunc('day', updatedAt) as updatedAt
          from gep.models
          where id=%(modelId)s
          order by updatedAt desc
          """,
                            {"modelId":modelId}))
    return model


@app.get('/scenarios/{sid}/features/{fid}')
def feature(sid: str, fid: int, year:int = None):
    sid = sid.lower()

    model = model_fromScenario(sid)

    timesteps = model.get('timesteps',[])
    if timesteps:
        if not year:
            year = timesteps[-1]
        if not year in timesteps:
            raise CustomError("The parameter %(year)s is invalid for this scenario, Must be one of %(ts)s" %
                              {'year':year, 'ts': ", ".join(timesteps)})

    fields = [ yearFieldAs('InvestmentCost', year),
               yearFieldAs('NewCapacity', year),
               yearField('Pop', year) +" * " + yearField('ElecStatusIn', year) + " as peopleConnected" ]
    where = "scenarioId=%(scenarioId)s and featureId=%(featureId)s"
    sql = """ select %s
        from scenarios
        where %s """ % (", ".join(fields), where)

    feature = _execute_onerow(sql, {'scenarioId':sid, 'featureId': fid})

    return {k:str(v) for k,v in feature.items()}

@app.get('/scenarios/{sid}')
def scenario(sid: str,  request:Request, year: int = None, filters:List[FilterModel]=None):
    client = connection()
    sid = sid.lower()
    response = {'id': sid,
                'summaryByType': collections.defaultdict(dict),
                }

    if not filters and 'filters' in str(request.query_params):
        # have to parse the request for backwards compatibility
        # querystring can look like this:
        # ?filters%5B0%5D%5Bkey%5D=Pop&filters%5B0%5D%5Bmax%5D=83968&filters%5B1%5D%5Bkey%5D=GridCellArea&filters%5B1%5D%5Bmax%5D=32&year=2030
        elts = [urllib.parse.unquote(f) for f in str(request.query_params).split('&') if f.startswith('filters')]

        _filters = collections.defaultdict(dict)

        for elt in elts:
            # should look like filters[0][key]=Pop ,  filters[0][max]=83968
            try:
                key, val = elt[8:].split('=',1) # remove the filters[, split on =
                filter_no, field = key.replace(']','').split('[',1) # remove all the ], split on the remaining [
                _filters[filter_no][field] = val  # add to a default dict.
            except Exception as msg:
                raise CustomError("Couldn't parse filters")
        try:
            filters = [FilterModel(**kwargs) for kwargs in _filters.values()]  # convert to the filtermodel
        except Exception as msg:
            raise CustomError("Couldn't parse filters")

    if filters:
        for f in filters:
            if not any(getattr(f, att) for att in ('min', 'max', 'options')):
                raise CustomError('Filter must include a valid value parameter name: "min", "max" or "options"')
    else:
        filters = []

    model = model_fromScenario(sid)

    timesteps = model['timesteps']
    baseYear = model['baseYear']
    intermediateYear = timesteps[0]
    finalYear = timesteps[1]

    if timesteps:
        if not year:
            year = timesteps[0]
        if not year in timesteps:
            raise CustomError("The parameter %(year)s is invalid for this scenario, Must be one of %(ts)s" %
                              {'year':year, 'ts': ", ".join(timesteps)})

    includedSteps = [y for y in timesteps if y <=year]
    investmentCostSelector = "+" .join([ "(%s * %s)" % (yearField("InvestmentCost",y),
                                                        yearField("ElecStatusIn", y))
                                         for y in includedSteps ])



    wheres = ["scenarioId = %(scenarioId)s"]
    vals = {'scenarioId': sid}
    for f in filters:
        filterdef = model['filter_dict'].get(f.key, None)
        if not filterdef:
            raise CustomError("Invalid Filter")

        key = filterdef['key']
        if filterdef.get('timestep', None):
            key = yearField(key, year)
        if f.min is not None:
            wheres.append( f"{key} >= %({key}min)s" )
            vals[key + 'min'] = f.min
        if f.max is not None:
            wheres.append( f"{key} <= %({key}max)s" )
            vals[key + 'max'] = f.max
        if f.min is not None:
            wheres.append( f"{key} in %({key}options)s" )
            vals[key + 'options'] = f.options


    fields = [
        _sum(yearField('Pop', baseYear), 'popBaseYear'),
        _sum(yearField('Pop', intermediateYear), 'popIntermediateYear'),
        _sum(yearField('Pop', finalYear), 'popFinalYear'),
        _sum(investmentCostSelector, "investmentCost"),
        _sum(yearField ('NewCapacity', year), "newCapacity"),
        ]

    summary = _execute_onerow("""select %s from scenarios where %s""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    # UNDONE round to 2 digits.
    response['summary'] = summary

    #    original
    # fields = [
    #     "featureId as id",
    #     yearFieldAs('PopConnected', baseYear, 'popConnectedBaseYear'),
    #     yearField('Pop', intermediateYear) + ' * ' +
    #       yearFieldAs('ElecStatusIn', intermediateYear, "popConnectedIntermediateYear"),
    #     yearField('Pop', finalYear) + ' * ' +
    #       yearFieldAs('ElecStatusIn', finalYear, "popConnectedFinalYear"),
    #     yearFieldAs('ElecCode', baseYear, 'elecTypeBaseYear'),
    #     yearFieldAs('ElecCode', intermediateYear, 'elecTypeIntermediateYear'),
    #     yearFieldAs('ElecCode', finalYear, 'elecTypeFinalYear'),
    #     yearFieldAs('FinaleElecCode', year, 'electrificationTech'),
    #     investmentCostSelector + " as investmentCost",
    #     yearFieldAs('NewCapacity', year),
    #     yearFieldAs('ElecStatusIn', year, "electrificationStatus"),
    #     ]

    # features = _execute("""select %s from scenarios where %s order by featureId""" % (
    #     ", ".join(fields), " and ".join(wheres)), vals )

    wheres.append('elecType != 99')

    # base year
    fields = [#_sum(yearFieldAs('PopConnected', baseYear), 'popConnectedBaseYear'),
        _sum(yearFieldAs('Pop', baseYear), 'popConnectedBaseYear'),
        yearFieldAs('ElecCode', baseYear, 'elecType')
    ]

    summary = client.execute("""select %s from scenarios where %s group by elecType""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    for row in summary:
        if row[1]:
            response['summaryByType']['popConnectedBaseYear'][row[1]] = row[0]

    # intermediate year
    fields = [_sum(yearField('Pop', intermediateYear) + ' * ' +
                   yearField('ElecStatusIn', intermediateYear), "popConnectedIntermediateYear"),
              yearFieldAs('FinalElecCode', intermediateYear, 'elecType'),
              ]

    summary = client.execute(
        """select %s from scenarios where %s group by elecType""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    for row in summary:
        response['summaryByType']['popConnectedIntermediateYear'][row[1]] = row[0]


    #final year
    fields = [_sum(yearField('Pop', finalYear) + ' * ' +
                   yearField('ElecStatusIn', finalYear), "popConnectedFinalYear"),
              yearFieldAs('FinalElecCode', finalYear, 'elecType'),
              ]

    summary = client.execute("""select %s from scenarios where %s group by elecType""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    for row in summary:
        response['summaryByType']['popConnectedFinalYear'][row[1]] = row[0]

    # target year
    fields = [_sum(investmentCostSelector, "investmentCost"),
              _sum(yearField('NewCapacity', year), "newCapacity"),
              yearFieldAs('FinalElecCode', year, 'elecType'),
              ]

    summary = client.execute("""select %s from scenarios where %s group by elecType""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    for row in summary:
        response['summaryByType']['investmentCost'][row[2]] = row[0]
        response['summaryByType']['newCapacity'][row[2]] = row[1]


    # featureTypes is a string of ,,,,#,#,#,,,,#,#,  where index = feature id, and # = FinalElecCode
    fields = [
        "featureId as id",
        yearFieldAs('FinalElecCode', year, 'elecType'),
        ]

    features = client.execute("""select %s from scenarios where %s order by featureId asc""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    response['featureTypes'] = ",".join(expander.expand(
        expander.reshape(features, lambda x: (x[0],str(x[1]))),
        default = ''
        ))

    return response
