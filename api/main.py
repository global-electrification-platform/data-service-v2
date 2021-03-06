from fastapi import FastAPI, Body, Path, Query, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

import clickhouse_driver

from pydantic import BaseModel
from typing import List, Optional

import collections
import decimal
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
    # json fields
    for field in ('attribution', 'map', 'sourceData', 'levers',
                  'layers', 'filters', 'timesteps'):
        if not field in model: continue
        try:
            model[field] = json.loads(model[field])
        except json.JSONDecodeError as msg:
            log.error(msg)
            log.error(model[field])
            raise

    # trim the time from the updated at date string
    model['updatedAt'] = str(model.get('updatedAt','')).split(' ')[0]

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
          externalUrl,
          date_trunc('day', updatedAt) as updatedAt
          from gep.models
          where country=%(countryId)s
          order by updatedAt desc
          """,
                            {"countryId":countryId})]

    country['riseScores'] = riseScores.get(countryId, {}).get('data', None)
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
          externalUrl,
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
                try: # options list
                    filter_no, field, index = key.replace(']','').split('[',2) # remove all the ], split on the remaining [
                    opt = _filters[filter_no].get(field,[])
                    opt.append(val)
                    _filters[filter_no][field] = opt
                except:
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
            if f.key == 'Admin1':
                options = [x.replace('+', ' ') for x in f.options]
                f.options = options
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

    def sumYear(year, timesteps, f):
        """ return the sum of the previous years up to the current year, for a specific year function """
        return "+".join([f(y) for y in timesteps if y <= year])

    def investmentCostSelectorYear(year):
        return "(%s * %s)" % (yearField("InvestmentCost", year),
                              yearField("ElecStatusIn", year))

    investmentCostSelector = sumYear(year, timesteps, investmentCostSelectorYear)


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
        if f.options is not None:
            wheres.append( f"{key} in %({key}options)s" )
            vals[key + 'options'] = f.options


    fields = [
        _sum(yearField('Pop', baseYear), 'popBaseYear'),
        _sum(yearField('Pop', intermediateYear), 'popIntermediateYear'),
        _sum(yearField('Pop', finalYear), 'popFinalYear'),
        _sum(investmentCostSelector, "investmentCost"),
        _sum(yearField ('NewCapacity', year), "newCapacity"),
        "max(featureId) as max_feature_id",
        ]

    summary = _execute_onerow("""select %s from scenarios where %s""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    f_max_id = summary['max_feature_id']
    del(summary['max_feature_id'])

    eps = decimal.Decimal("0.01")
    response['summary'] = {k:decimal.Decimal(v).quantize(eps) for k,v in summary.items()}

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
        _sum('PopConnectedBaseYear'),
        yearFieldAs('ElecCode', baseYear, 'elecType')
    ]

    summary = client.execute("""select %s from scenarios where %s group by elecType""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    # we need an empty item here if we don't have records
    response['summaryByType']['popConnectedBaseYear'] = {}
    for row in summary:
        if row[1]:
            response['summaryByType']['popConnectedBaseYear'][row[1]] = row[0]

    # intermediate year
    _investmentCost = collections.defaultdict(dict)
    _newCapacity = collections.defaultdict(dict)
    fields = [_sum(yearField('Pop', intermediateYear) + ' * ' +
                   yearField('ElecStatusIn', intermediateYear), "popConnectedIntermediateYear"),
              yearFieldAs('FinalElecCode', intermediateYear, 'elecType'),
              _sum(investmentCostSelectorYear(intermediateYear), "investmentCost"),
              _sum(yearField('NewCapacity', intermediateYear), "newCapacity"),
              ]

    #log.error("""select %s from scenarios where %s group by elecType""",
    #          ", ".join(fields), " and ".join(wheres) % vals)

    summary = client.execute(
        """select %s from scenarios where %s group by elecType""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    response['summaryByType']['popConnectedIntermediateYear'] = {}
    for row in summary:
        #log.error("Pop: %s, Code: %s, Invest: %d, Cap: %d", *row)
        response['summaryByType']['popConnectedIntermediateYear'][row[1]] = row[0]
        _investmentCost[intermediateYear][row[1]] = row[2]
        _newCapacity[intermediateYear][row[1]] = row[3]



    #final year
    fields = [_sum(yearField('Pop', finalYear) + ' * ' +
                   yearField('ElecStatusIn', finalYear), "popConnectedFinalYear"),
              yearFieldAs('FinalElecCode', finalYear, 'elecType'),
              _sum(investmentCostSelectorYear(finalYear), "investmentCost"),
              _sum(yearField('NewCapacity', finalYear), "newCapacity"),
              ]

    #log.error("""select %s from scenarios where %s group by elecType""",
    #          ", ".join(fields), " and ".join(wheres) % vals)

    summary = client.execute("""select %s from scenarios where %s group by elecType""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    for row in summary:
        #log.error("Pop: %s, Code: %s, Invest: %d, Cap: %d", *row)
        response['summaryByType']['popConnectedFinalYear'][row[1]] = row[0]
        _investmentCost[finalYear][row[1]] = row[2] + _investmentCost[intermediateYear].get(row[1],0)
        _newCapacity[finalYear][row[1]] = row[3] + _newCapacity[intermediateYear].get(row[1],0)

    response['summaryByType']['investmentCost'] = _investmentCost
    response['summaryByType']['newCapacity'] = _newCapacity

    # featureTypes is a string of ,,,,#,#,#,,,,#,#,  where index = feature id, and # = FinalElecCode
    # there's one entry for each feature in the scenario
    # f_max = _execute_onerow("""select max(featureId) as max from scenarios where scenarioId=%(scenarioId)s""",
    #                        vals)
    # f_max_id = f_max['max']

    fields = [
        "featureId as id",
        yearFieldAs('FinalElecCode', year, 'elecType'),
        ]

    features = client.execute("""select %s from scenarios where %s order by featureId asc""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    response['featureTypes'] = ",".join(expander.expand(
        expander.reshape(features, lambda x: (x[0],str(x[1]))),
        default = '',
        max_index=f_max_id
        ))

    return response
