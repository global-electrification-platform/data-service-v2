from fastapi import FastAPI, Body, Path, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

import clickhouse_driver

from pydantic import BaseModel
from typing import List, Optional

import json
import os
import expander

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

riseScores = {}

with open(os.path.join(os.path.dirname(__file__), 'rise-indicators.json'), 'r') as f:
    rise = json.load(f)
    riseScores = {r['iso']:r for r in rise}

def scenarioId_toModelId(sid):
    return sid.split('-')[-1]

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
    client = connection()
    sid = sid.lower()
    
    modelId = scenarioId_toModelId(sid)
    
    res = client.execute('select filters, timesteps, baseYear from models where id=%(modelId)s',
                         {'modelId': modelId})
    if not res:
        raise CustomError('NotFound')
    model = res[0]
    model['filter_dict'] = {f['key']: f for f in model['filters']}

def connection():
    # clickhouse driver client
    client = clickhouse_driver.Client(host=CLICKHOUSE_HOST, database=CLICKHOUSE_DB)
    return client

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
    countries = client.execute("select count(distinct country) from models")
    models = client.execute("select count(distinct type) from models")
    return {
        "totals": {
        "countries": countries,
        "models": models,
        }
    }


@app.get("/countries")
def countries():
    client = connection()
    countries = client.execute("""
        select id, name from countries
        where id in (select country from models)
        order by name ASC
        """)

    return { countries }

@app.get("/countries/{countryId}")
def country(countryId: str):
    client = connection()
    countryId = countryId.upper()
    country = client.execute("select * from countries where id=%(countryId)s", {"countryId":countryId})
    country.models = client.execute("""
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
          date_trunc(updatedAt, 'day') as updatedAt,
          where country=%(countryId)s
          order by updatedAt desc
          """,
                            {"countryId":countryId})

    country.riseScores = riseScores.get(countryId, None)
    return country

@app.get('/models/{modelId}')
def model(modelId: str):
    client = connection()
    model = client.execute("""
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
          date_trunc(updatedAt, 'day') as updatedAt,
          where id=%(modelId)s
          order by updatedAt desc
          """,
                            {"modelId":modelId})
    return model


@app.get('/scenarios/{sid}/features/{fid}')
def feature(sid: str, fid: int, year:int = None):
    client = connection()
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
    feature = client.execute(sql, {'scenarioId':sid, 'featureId': fid})

    if feature:
        return feature[0]

    raise CustomError("Not Found")

@app.get('/secnarios/{sid}')
def scenario(sid: str, year: int = None, filters: List[FilterModel]=None):
    client = connection()
    sid = sid.lower()
    response = {'id': sid,
                'summaryByType': {}
                }
    if filters:
        for f in filters:
            if not any(getattr(f, att) for att in ('min', 'max', 'options')):
                raise CustomError('Filter must include a valid value parameter name: "min", "max" or "options"')
    else:
        filters = []

    model = model_fromScenario(sid)
    
    timesteps = model['timesteps']
    baseYear = model['baseYear']
    intermediateYear = timesteps.get(0, None)
    finalYear = timesteps.get(1, None)
    
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

    

    wheres = ["scenarioId = %(scenaroiId)s"]
    vals = {'scenarioId': sid}
    for f in filters:
        filterdef = model['filters_dict'].get(f['key'], None)
        if filterdef.get('timestamp', None):
            key = yearField(key, year)
        if f.min is not None:
            wheres.append( "%s >= %s" %(key, "%(key)smin") )
            vals[key + 'min'] = f.min
        if f.max is not None:
            wheres.append( "%s <= %s" %(key, "%(key)smax") )
            vals[key + 'max'] = f.max
        if f.min is not None:
            wheres.append( "%s in %s" %(key, "%(key)soptions") )
            vals[key + 'options'] = f.options


    fields = [
        _sum(yearField('Pop', baseYear), 'popBaseYear'),
        _sum(yearField('Pop', intermediateYear), 'popIntermediateYear'),
        _sum(yearField('Pop', finalYear), 'popFinalYear'),
        _sum(investmentCostSelector, "investmentCost"),
        _sum(yearField ('NewCapacity', year), "newCapacity"),
        ]

    summary = client.execute("""select %s from scenarios where %s""" % (
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

    # features = client.execute("""select %s from scenarios where %s order by featureId""" % (
    #     ", ".join(fields), " and ".join(wheres)), vals )
        

    # base year
    fields = [_sum(yearFieldAs('PopConnected', baseYear), 'popConnectedBaseYear'),
              yearFieldAs('ElecCode', baseYear, 'elecTypeBaseYear')
              ]


    baseYearSummary = client.execute("""select %s from scenarios where %s group by elecTypeBaseYear""" % (
        ", ".join(fields), " and ".join(wheres)), vals )
    response['summaryByType'].update(baseYearSummary[0])

    # intermediate year
    fields = [_sum(yearField('Pop', intermediateYear) + ' * ' +
                   yearField('ElecStatusIn', intermediateYear), "popConnectedIntermediateYear"),
              yearFieldAs('ElecCode', intermediateYear, 'elecTypeIntermediateYear'),
              ]
    
    intermediateYearSummary = client.execute(
        """select %s from scenarios where %s group by elecTypeIntermediateYear""" % (
        ", ".join(fields), " and ".join(wheres)), vals )
    response['summaryByType'].update(intermediateYearSummary[0])
    
    #final year
    fields = [_sum(yearField('Pop', finalYear) + ' * ' +
                   yearField('ElecStatusIn', finalYear), "popConnectedFinalYear"),
              yearFieldAs('ElecCode', finalYear, 'elecTypeFinalYear'),
              ]

    finalYearSummary = client.execute("""select %s from scenarios where %s group by elecTypeFinalYear""" % (
        ", ".join(fields), " and ".join(wheres)), vals )
    response['summaryByType'].update(finalYearSummary[0])
    
    # target year
    fields = [_sum(investmentCostSelector, "investmentCost"),
              _sum(yearField('NewCapacity', year), "newCapacity"),
              yearFieldAs('FinaleElecCode', year, 'electrificationTech'),
              ]

    targetYearSummary = client.execute("""select %s from scenarios where %s group by electrificationTech""" % (
        ", ".join(fields), " and ".join(wheres)), vals )
    
    response['summaryByType'].update(targetYearSummary[0])

    # featureTypes is a string of ,,,,#,#,#,,,,#,#,  where index = feature id, and # = FinalElecCode
    fields = [
        "featureId as id",
        yearFieldAs('FinaleElecCode', year, 'tech'),
        ]

    features = client.execute("""select %s from scenarios where %s order by featureId asc""" % (
        ", ".join(fields), " and ".join(wheres)), vals )

    response['featureTypes'] = expander.expand(
        expander.reshape(features, lambda x: (x['id'],x['tech'])),
        default = ''
        )
    
    return response
